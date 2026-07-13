import { elements } from "./dom.js";
import { escapeHtml } from "./formatting.js";
import { extractErrorMessage, fetchJSON } from "./http.js";
import { state } from "./state.js";

function selectedLlmModel() {
  return elements.nlModel ? elements.nlModel.value.trim() : "";
}

export function updateNlGenerateState() {
  if (!elements.nlGenerate) return;
  const selected = selectedLlmModel();
  const available = state.llmModels.some(
    (model) => model.id === selected && model.available,
  );
  elements.nlGenerate.disabled = state.nlGenerating || !available;
}

function renderLlmModelOptions() {
  if (!elements.nlModel) return;
  const current = selectedLlmModel();
  const options = state.llmModels.length
    ? state.llmModels.map((model) => {
        const suffix = model.available ? "" : " (unavailable)";
        const title = model.reason ? ` title="${escapeHtml(model.reason)}"` : "";
        return `<option value="${escapeHtml(model.id)}"${model.available ? "" : " disabled"}${title}>${escapeHtml(model.label)}${suffix}</option>`;
      })
    : ['<option value="">No models configured</option>'];
  elements.nlModel.innerHTML = options.join("");
  const selected = state.llmModels.some(
    (model) => model.id === current && model.available,
  )
    ? current
    : state.llmModels.find(
        (model) => model.id === state.llmDefaultModel && model.available,
      )?.id || state.llmModels.find((model) => model.available)?.id || "";
  elements.nlModel.value = selected;
  elements.nlModel.disabled = !state.llmModels.some((model) => model.available);
  updateNlGenerateState();
}

export async function loadLlmModels() {
  try {
    const data = await fetchJSON("/api/llm_models");
    state.llmModels = data.models || [];
    state.llmDefaultModel = data.default_model || "";
  } catch (err) {
    state.llmModels = [];
    state.llmDefaultModel = "";
    if (elements.nlStatus) {
      elements.nlStatus.textContent = extractErrorMessage(err);
    }
  }
  renderLlmModelOptions();
  if (!state.llmModels.some((model) => model.available) && elements.nlStatus) {
    elements.nlStatus.textContent = "Configure an available LLM model to generate SQL.";
  }
}

export async function runNlQuery() {
  if (!state.file || !elements.nlInput || !elements.nlGenerate) return;
  const prompt = elements.nlInput.value.trim();
  const model = selectedLlmModel();
  if (!model) {
    if (elements.nlStatus) {
      elements.nlStatus.textContent = "Select an available model first.";
    }
    return;
  }
  if (!prompt) {
    if (elements.nlStatus) {
      elements.nlStatus.textContent = "Please enter a request.";
    }
    return;
  }
  const sampleRow = buildSampleRow();
  state.nlGenerating = true;
  updateNlGenerateState();
  if (elements.nlStatus) {
    elements.nlStatus.textContent = "Generating SQL...";
  }
  try {
    const data = await fetchJSON("/api/nl_query", {
      method: "POST",
      body: JSON.stringify({ file: state.file, prompt, sample: sampleRow, model }),
    });
    if (data.sql) {
      elements.sqlInput.value = data.sql;
    }
    if (elements.nlStatus) {
      elements.nlStatus.textContent = data.model_label
        ? `SQL ready with ${data.model_label}.`
        : "SQL ready.";
    }
  } catch (err) {
    if (elements.nlStatus) {
      elements.nlStatus.textContent = extractErrorMessage(err);
    }
    console.error(err);
  } finally {
    state.nlGenerating = false;
    updateNlGenerateState();
  }
}

export function autoResizeNlInput() {
  if (!elements.nlInput) return;
  elements.nlInput.style.height = "auto";
  const next = Math.min(elements.nlInput.scrollHeight, 160);
  elements.nlInput.style.height = `${next}px`;
}

function buildSampleRow() {
  if (!state.columns.length || !state.rows.length) return null;
  let index = state.selectedRowIndex;
  if (index === null || index === undefined || !state.rows[index]) {
    index = 0;
  }
  const row = state.rows[index];
  if (!row) return null;
  const obj = {};
  state.columns.forEach((col, idx) => {
    if (state.hiddenTableColumns.has(col)) {
      return;
    }
    obj[col] = row[idx];
  });
  return obj;
}
