import { elements } from "./dom.js";
import {
  cancelJob,
  extractErrorMessage,
  formatJobProgress,
  startJob,
  waitForJob,
} from "./http.js";
import { escapeHtml } from "./formatting.js";
import { state } from "./state.js";

function selectedAtlasColumn() {
  return elements.atlasColumn ? elements.atlasColumn.value.trim() : "";
}

function selectedAtlasModel() {
  return elements.atlasModel ? elements.atlasModel.value.trim() : "";
}

function selectedAtlasModelMetadata() {
  const selected = selectedAtlasModel();
  return (
    state.embedderModels.find(
      (model) => (model.value || model.name || "") === selected,
    ) || null
  );
}

function selectedAtlasBackend() {
  return elements.atlasBackend ? elements.atlasBackend.value.trim() : "";
}

function atlasBackendCapability(model, backend) {
  return model && model.backends ? model.backends[backend] || null : null;
}

function atlasBackendAvailable(model, backend) {
  const capability = atlasBackendCapability(model, backend);
  return Boolean(capability && capability.available);
}

function normalizeAtlasUrl(url) {
  if (!url) return "";
  try {
    const parsed = new URL(url, window.location.origin);
    if (parsed.origin !== window.location.origin) return "";
    if (parsed.protocol !== window.location.protocol) return "";
    if (parsed.username || parsed.password) return "";
    if (!/^\/atlas\/[A-Za-z0-9_-]+\/$/.test(parsed.pathname)) return "";
    if (parsed.search || parsed.hash) return "";
    return parsed.href;
  } catch (err) {
    return "";
  }
}

export function openAtlasUrl() {
  const url = normalizeAtlasUrl(
    state.atlasUrl || (elements.atlasLink ? elements.atlasLink.href : ""),
  );
  if (!url) {
    if (elements.atlasStatus) {
      elements.atlasStatus.textContent =
        "Atlas is ready, but its proxy link is invalid. Run Atlas again.";
    }
    return;
  }
  window.location.href = url;
}

export function renderAtlasColumnOptions() {
  if (!elements.atlasColumn) return;
  const current = selectedAtlasColumn();
  const options = ['<option value="">Select column</option>']
    .concat(
      state.schema.map(
        (col) =>
          `<option value="${escapeHtml(col.name)}">${escapeHtml(col.name)}</option>`,
      ),
    )
    .join("");
  elements.atlasColumn.innerHTML = options;
  if (current && state.schema.some((col) => col.name === current)) {
    elements.atlasColumn.value = current;
  }
  setAtlasButtonsRunning(state.atlasJobKind);
}

export function renderAtlasModelOptions() {
  if (!elements.atlasModel) return;
  const current = selectedAtlasModel();
  const options = ['<option value="">Select model</option>']
    .concat(
      state.embedderModels.map((model) => {
        const value = model.value || model.name || "";
        const label = model.name || value;
        return `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`;
      }),
    )
    .join("");
  elements.atlasModel.innerHTML = options;
  if (
    current &&
    state.embedderModels.some((model) => (model.value || model.name) === current)
  ) {
    elements.atlasModel.value = current;
  }
  renderAtlasBackendOptions();
}

export function renderAtlasPromptControl() {
  if (!elements.atlasPromptControls || !elements.atlasPrompt) return;
  const model = selectedAtlasModelMetadata();
  const sentenceTransformers =
    selectedAtlasBackend() === "sentence-transformers" &&
    atlasBackendAvailable(model, "sentence-transformers");
  elements.atlasPrompt.disabled = !sentenceTransformers;
  elements.atlasPromptControls.classList.toggle(
    "atlas-prompt-unavailable",
    !sentenceTransformers,
  );
  const preview = model ? model.default_prompt_preview || "" : "";
  elements.atlasPrompt.placeholder = sentenceTransformers
    ? preview
      ? `Model default: ${preview}`
      : "Optional prompt template"
    : "Available with Sentence Transformers only";
}

export function renderAtlasBackendOptions() {
  if (!elements.atlasBackend) return;
  const model = selectedAtlasModelMetadata();
  const current = selectedAtlasBackend();
  const backendDefinitions = [
    ["sentence-transformers", "Sentence Transformers"],
    ["transformers", "Transformers"],
  ];
  const options = ['<option value="">Select backend</option>'].concat(
    backendDefinitions.map(([value, label]) => {
      const capability = atlasBackendCapability(model, value);
      const available = Boolean(capability && capability.available);
      const suffix = model
        ? !available
          ? " (unavailable)"
          : capability.status === "generic_fallback"
            ? " (generic fallback)"
            : ""
        : "";
      return `<option value="${value}"${available ? "" : " disabled"}>${label}${suffix}</option>`;
    }),
  );
  elements.atlasBackend.innerHTML = options.join("");
  const defaultBackend = model ? model.default_backend || "" : "";
  const selected = atlasBackendAvailable(model, current)
    ? current
    : atlasBackendAvailable(model, defaultBackend)
      ? defaultBackend
      : backendDefinitions.find(([value]) => atlasBackendAvailable(model, value))?.[0] || "";
  elements.atlasBackend.value = selected;
  const hasAvailableBackend = backendDefinitions.some(([value]) =>
    atlasBackendAvailable(model, value),
  );
  elements.atlasBackend.disabled = !hasAvailableBackend;
  elements.atlasBackend.classList.toggle(
    "atlas-backend-unavailable",
    !hasAvailableBackend,
  );
  const capability = atlasBackendCapability(model, selected);
  elements.atlasBackend.title = capability ? capability.reason || "" : "";
  renderAtlasPromptControl();
  setAtlasButtonsRunning(state.atlasJobKind);
}

export function setAtlasButtonsRunning(kind) {
  const hasColumn = Boolean(selectedAtlasColumn());
  const model = selectedAtlasModelMetadata();
  const backend = selectedAtlasBackend();
  const ready = hasColumn && Boolean(model) && atlasBackendAvailable(model, backend);
  const cancelling = Boolean(kind && state.atlasCancelling);
  if (elements.runAtlas) {
    const enabled = kind ? kind === "all" && !cancelling : ready;
    elements.runAtlas.disabled = !enabled;
    elements.runAtlas.classList.toggle("atlas-button-disabled", !enabled);
    elements.runAtlas.classList.toggle("atlas-button-ready", enabled);
    elements.runAtlas.textContent =
      kind === "all" ? (cancelling ? "Cancelling..." : "Cancel Atlas") : "Run Atlas";
  }
  if (elements.runAtlasQuery) {
    const enabled = kind ? kind === "query" && !cancelling : ready;
    elements.runAtlasQuery.disabled = !enabled;
    elements.runAtlasQuery.classList.toggle("atlas-button-disabled", !enabled);
    elements.runAtlasQuery.classList.toggle("atlas-button-ready", enabled);
    elements.runAtlasQuery.textContent = kind === "query"
      ? cancelling
        ? "Cancelling..."
        : "Cancel Query Atlas"
      : "Run Atlas on Query Results";
  }
}

function selectedAtlasProjection() {
  return elements.atlasProjection?.value || "umap";
}

function atlasProjectionLabel(method) {
  return { umap: "UMAP", tsne: "t-SNE", pca: "PCA" }[method] || method;
}

async function runAtlasJob(kind) {
  if (!state.file || !elements.runAtlas) return;
  if (state.atlasJobId) {
    const jobId = state.atlasJobId;
    state.atlasCancelling = true;
    setAtlasButtonsRunning(state.atlasJobKind);
    if (elements.atlasStatus) {
      elements.atlasStatus.textContent =
        "Cancellation requested. Waiting for the current Atlas step to stop...";
    }
    try {
      await cancelJob(jobId);
    } catch (err) {
      state.atlasCancelling = false;
      setAtlasButtonsRunning(state.atlasJobKind);
      if (elements.atlasStatus) {
        elements.atlasStatus.textContent = extractErrorMessage(err);
      }
    }
    return;
  }

  const column = selectedAtlasColumn();
  const model = selectedAtlasModel();
  const backend = selectedAtlasBackend();
  if (!column) {
    if (elements.atlasStatus) {
      elements.atlasStatus.textContent = "Select a column first.";
    }
    setAtlasButtonsRunning("");
    return;
  }
  if (!model) {
    if (elements.atlasStatus) {
      elements.atlasStatus.textContent = "Select a model first.";
    }
    setAtlasButtonsRunning("");
    return;
  }

  if (!atlasBackendAvailable(selectedAtlasModelMetadata(), backend)) {
    if (elements.atlasStatus) {
      elements.atlasStatus.textContent = "Select an available backend first.";
    }
    setAtlasButtonsRunning("");
    return;
  }

  const payload = {
    file: state.file,
    column,
    model,
    backend,
    projection_method: selectedAtlasProjection(),
  };
  if (backend === "sentence-transformers" && elements.atlasPrompt) {
    const prompt = elements.atlasPrompt.value;
    if (prompt.trim()) payload.prompt = prompt;
  }
  let jobKind = "atlas";
  let sourceLabel = "Atlas";
  if (kind === "query") {
    const sql = elements.sqlInput.value.trim();
    if (!sql) {
      if (elements.atlasStatus) {
        elements.atlasStatus.textContent = "Enter a SQL query first.";
      }
      return;
    }
    payload.sql = sql;
    jobKind = "atlas_query";
    sourceLabel = "query Atlas";
  }

  state.atlasJobKind = kind;
  state.atlasCancelling = false;
  setAtlasButtonsRunning(kind);
  if (elements.atlasStatus) {
    elements.atlasStatus.textContent =
      kind === "query" ? "Starting query Atlas job..." : "Starting Atlas job...";
  }
  if (elements.atlasLink) {
    elements.atlasLink.style.display = "none";
    elements.atlasLink.textContent = "";
    elements.atlasLink.removeAttribute("href");
  }
  state.atlasUrl = "";

  try {
    const fileAtStart = state.file;
    const job = await startJob(jobKind, payload);
    state.atlasJobId = job.id;
    const data = await waitForJob(job.id, {
      intervalMs: 1000,
      onUpdate: (nextJob) => {
        if (elements.atlasStatus) {
          elements.atlasStatus.textContent = formatJobProgress(
            nextJob,
            kind === "query" ? "Generating query Atlas" : "Generating Atlas",
          );
        }
      },
    });
    if (state.file !== fileAtStart || state.atlasJobId !== job.id) return;
    if (elements.atlasStatus) {
      const modalityNote = data.modality ? ` (${data.modality})` : "";
      const sampleNote = data.sample ? ` Sample limit: ${data.sample}.` : "";
      const rowCountNote = Number.isInteger(data.row_count) ? ` Rows: ${data.row_count}.` : "";
      const modelNote = data.model ? ` Model: ${data.model}.` : "";
      const backendNote = data.backend ? ` Backend: ${data.backend}.` : "";
      const projectionMethod = data.projection_method || selectedAtlasProjection();
      const umapMode = projectionMethod === "umap" && data.umap_projection_mode
        ? ` (${data.umap_projection_mode.replace("_", " ")})`
        : "";
      const projectionNote = ` Projection: ${atlasProjectionLabel(projectionMethod)}${umapMode}.`;
      const dtypeNote = data.embedding_dtype ? ` Embedding: ${data.embedding_dtype}.` : "";
      const cacheNote = data.cache_hit ? " Cache: reused." : data.cache_path ? " Cache: refreshed." : "";
      elements.atlasStatus.textContent = `${sourceLabel} is ready for "${data.column || column}"${modalityNote}.${modelNote}${backendNote}${sampleNote}${rowCountNote}${projectionNote}${dtypeNote}${cacheNote}`;
    }
    const atlasUrl = normalizeAtlasUrl(data.url);
    if (elements.atlasLink && atlasUrl) {
      state.atlasUrl = atlasUrl;
      elements.atlasLink.href = atlasUrl;
      elements.atlasLink.textContent = "Open Atlas";
      elements.atlasLink.style.display = "inline-flex";
    }
  } catch (err) {
    if (elements.atlasStatus) {
      elements.atlasStatus.textContent = extractErrorMessage(err);
    }
    console.error(err);
  } finally {
    state.atlasJobId = null;
    state.atlasJobKind = "";
    state.atlasCancelling = false;
    setAtlasButtonsRunning("");
  }
}

export async function runAtlas() {
  await runAtlasJob("all");
}

export async function runAtlasOnQueryResults() {
  await runAtlasJob("query");
}
