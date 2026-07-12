"""Provider-neutral prompt construction and generated SQL validation."""

from __future__ import annotations

import json
import re
from typing import Any

from fastapi import HTTPException

MAX_SAMPLE_CONTEXT_CHARS = 16_000

FEW_SHOT_EXAMPLES = (
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
    "ORDER BY d.rating DESC\n"
    "```"
)


def build_sql_generation_messages(
    prompt: str,
    schema: list[dict[str, str]],
    sample: dict[str, Any] | None,
) -> list[dict[str, str]]:
    """Build one text-only user message shared by every provider.

    The sample context is copied into the prompt and bounded to prevent an API
    caller from expanding an LLM request without limit.
    """
    schema_json = json.dumps(schema, ensure_ascii=False, separators=(",", ":"))
    sample_block = ""
    if sample:
        sample_json = json.dumps(sample, ensure_ascii=False, default=str, separators=(",", ":"))
        if len(sample_json) > MAX_SAMPLE_CONTEXT_CHARS:
            sample_json = f"{sample_json[:MAX_SAMPLE_CONTEXT_CHARS]}... (truncated)"
        sample_block = f"SQL生成対象のデータ例:\n{sample_json}\n\n"
    content = (
        "DuckDBのdataテーブルに対するSQLを生成してください。\n"
        "返答はSQLだけにし、説明やMarkdownを含めないでください。\n"
        "単一のSELECTまたはWITH queryだけを使用してください。\n\n"
        f"利用可能なカラム:\n{schema_json}\n\n"
        f"{sample_block}{FEW_SHOT_EXAMPLES}\n\n"
        f"指示文:\n{prompt}\n"
    )
    return [{"role": "user", "content": content}]


def clean_generated_sql(text: str) -> str:
    """Strip Markdown and require one read-only SELECT or WITH statement.

    Raises:
        HTTPException: The model output is empty, contains multiple statements,
            or does not begin with SELECT or WITH.
    """
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\s*\n", "", cleaned)
        cleaned = re.sub(r"\n?```\s*$", "", cleaned).strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="generated SQL is empty")
    if ";" in cleaned:
        raise HTTPException(status_code=400, detail="multi-statement sql is not allowed")
    sql_lower = cleaned.lower()
    if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
        raise HTTPException(status_code=400, detail="only SELECT queries are allowed")
    return cleaned
