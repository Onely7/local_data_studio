"""LLM-backed SQL generation helpers."""

import json
import re
import urllib.error
import urllib.request
from typing import Any

from fastapi import HTTPException

from .config import OPENAI_API_KEY, OPENAI_BASE_URL, OPENAI_MODEL


def _post_json(url: str, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
    """POST JSON and return the decoded response."""
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _extract_openai_text(payload: dict[str, Any]) -> str:
    """Extract text from OpenAI Responses or Chat Completions payloads."""
    if "output_text" in payload:
        return payload.get("output_text", "") or ""
    if "output" in payload:
        for item in payload.get("output", []):
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    return content.get("text", "") or ""
    if "choices" in payload:
        choices = payload.get("choices", [])
        if choices:
            message = choices[0].get("message", {})
            return message.get("content", "") or ""
    return ""


def _clean_sql(text: str) -> str:
    """Strip markdown and validate the SQL is a single SELECT/CTE."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\\n", "", text)
        text = re.sub(r"```$", "", text).strip()
    text = text.strip()
    if text.endswith(";"):
        text = text[:-1].strip()
    if ";" in text:
        raise HTTPException(status_code=400, detail="multi-statement sql is not allowed")
    sql_lower = text.lower()
    if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
        raise HTTPException(status_code=400, detail="only SELECT queries are allowed")
    return text


def generate_sql_from_prompt(
    prompt: str,
    schema: list[dict[str, str]],
    sample: dict[str, Any] | None,
) -> str:
    """Generate a SQL query from a natural language prompt."""
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not set")

    sample_block = ""
    if sample:
        sample_json = json.dumps(sample, ensure_ascii=False)
        sample_block = f"SQL生成対象のデータ例:\n{sample_json}\n\n"
    few_shot = (
        "指示文:\n"
        "「texts_ja」フィールドにおいて、「車」という文字列が含まれるデータを抽出してください。\n\n"
        "```sql\n"
        "SELECT DISTINCT d.*\n"
        "FROM data AS d\n"
        "CROSS JOIN UNNEST(d.texts_ja) AS t(msg)\n"
        "WHERE msg.user LIKE '%車%'\n"
        "   OR msg.assistant LIKE '%車%';\n"
        "```\n\n"
        "指示文:\n"
        "「rating」フィールドにおいて、値が大きい順に並べ替えてください。\n\n"
        "```sql\n"
        "SELECT DISTINCT d.*\n"
        "FROM data AS d\n"
        "ORDER BY d.rating DESC"
        "```\n\n"
    )
    system_prompt = "You generate DuckDB SQL for a table named data. Return only SQL (no markdown, no commentary). Use only SELECT or WITH queries."
    user_prompt = f"{sample_block}{few_shot}指示文:\n{prompt}\n"

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    base_url = OPENAI_BASE_URL.rstrip("/")
    response_payload: dict[str, Any] | None = None

    try:
        response_payload = _post_json(
            f"{base_url}/responses",
            {
                "model": OPENAI_MODEL,
                "input": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0,
                "max_output_tokens": 400,
            },
            headers,
        )
    except urllib.error.HTTPError as err:
        if err.code not in (404, 405):
            detail = err.read().decode("utf-8", "ignore")
            raise HTTPException(status_code=500, detail=f"OpenAI API error: {detail}") from err
    except urllib.error.URLError as err:
        raise HTTPException(status_code=500, detail=f"OpenAI API request failed: {err}") from err

    if response_payload is None:
        try:
            response_payload = _post_json(
                f"{base_url}/chat/completions",
                {
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0,
                    "max_tokens": 400,
                },
                headers,
            )
        except urllib.error.HTTPError as err:
            detail = err.read().decode("utf-8", "ignore")
            raise HTTPException(status_code=500, detail=f"OpenAI API error: {detail}") from err
        except urllib.error.URLError as err:
            raise HTTPException(status_code=500, detail=f"OpenAI API request failed: {err}") from err

    text = _extract_openai_text(response_payload)
    if not text:
        raise HTTPException(status_code=500, detail="OpenAI API returned empty response")
    return _clean_sql(text)
