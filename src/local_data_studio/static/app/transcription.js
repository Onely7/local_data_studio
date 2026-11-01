// Speech-to-text for audio cells: model choice, transcription of one field or
// of the visible audio in a column, and transcripts shown under cell chips and
// expanded players. Results are keyed by model and audio key, so switching
// models never shows another model's transcript.
import { audioCellTarget, extractAudioItems, MAX_AUDIO_ITEMS } from "./audio.js";
import { elements } from "./dom.js";
import { encodeCopyValue, escapeHtml } from "./formatting.js";
import { cancelJob, extractErrorMessage, fetchJSON, isAbortError, startJob, waitForJob } from "./http.js";
import { state } from "./state.js";

const MODEL_STORAGE_KEY = "local-data-studio.transcription-model";
const store = {
  models: [],
  defaultModel: "",
  limits: {},
  results: new Map(),
  pending: new Map(),
  jobs: new Map(),
  generation: 0,
};

function storageGet(key) {
  try {
    return globalThis.localStorage?.getItem(key) || "";
  } catch (err) {
    return "";
  }
}

function storageSet(key, value) {
  try {
    globalThis.localStorage?.setItem(key, value);
  } catch (err) {
    // Private browsing can refuse storage; the selection still applies.
  }
}

function selectedModel() {
  const id = elements.transcriptionModel?.value || "";
  return store.models.find((model) => model.id === id && model.available) || null;
}

function resultKey(modelId, audioKey) {
  return `${modelId}|${audioKey}`;
}

export function hasTranscriptionModels() {
  return store.models.some((model) => model.available);
}

export async function loadTranscriptionConfig() {
  const data = await fetchJSON("/api/transcription_models");
  store.models = data.models || [];
  store.defaultModel = data.default_model || "";
  store.limits = data.limits || {};
}

export function renderTranscriptionControls() {
  const select = elements.transcriptionModel;
  if (!select) return;
  const current = select.value || storageGet(MODEL_STORAGE_KEY);
  select.innerHTML = store.models.length
    ? store.models
        .map((model) => {
          const suffix = model.available ? "" : " (unavailable)";
          const title = model.reason ? ` title="${escapeHtml(model.reason)}"` : "";
          return `<option value="${escapeHtml(model.id)}"${model.available ? "" : " disabled"}${title}>${escapeHtml(model.label)}${suffix}</option>`;
        })
        .join("")
    : '<option value="">No transcription models</option>';
  const available = store.models.filter((model) => model.available);
  select.value =
    available.find((model) => model.id === current)?.id ||
    available.find((model) => model.id === store.defaultModel)?.id ||
    available[0]?.id ||
    "";
  select.disabled = !available.length;
}

// Returns one transcription target per audio item shown in a field.
function fieldTargets(rowIndex, columnIndex) {
  const value = state.rows[rowIndex]?.[columnIndex];
  return extractAudioItems(value)
    .slice(0, MAX_AUDIO_ITEMS)
    .map((item) => audioCellTarget(rowIndex, columnIndex, item.pointer))
    .filter((target) => target?.request);
}

function columnTargets(column) {
  const columnIndex = state.columns.indexOf(column);
  if (columnIndex < 0) return [];
  return state.rows.flatMap((row, rowIndex) => fieldTargets(rowIndex, columnIndex));
}

function needsTranscription(model, target) {
  const key = resultKey(model.id, target.key);
  return !store.pending.has(key) && !store.results.has(key);
}

export function expandedTranscriptionButtonMarkup(rowIndex, columnIndex, value) {
  if (!store.models.length || !extractAudioItems(value).length) return "";
  const model = selectedModel();
  const targets = fieldTargets(rowIndex, columnIndex);
  const pending = Boolean(model) && targets.some((target) => store.pending.has(resultKey(model.id, target.key)));
  if (model && targets.length && !pending && !targets.some((target) => needsTranscription(model, target))) return "";
  const disabled = pending || !model || !targets.length;
  const title = !model ? "Select a transcription model" : !targets.length ? "Audio for this row cannot be transcribed" : "Transcribe audio";
  return `<button class="field-transcribe-btn" data-row="${rowIndex}" data-col="${columnIndex}" type="button" title="${escapeHtml(title)}" aria-busy="${pending ? "true" : "false"}"${disabled ? " disabled" : ""}><img src="icons/transcribe.svg" alt="" aria-hidden="true" />${pending ? "Transcribing..." : "Transcribe"}</button>`;
}

export function createColumnTranscriptionButton(column) {
  if (!store.models.length) return null;
  const targets = columnTargets(column);
  if (!targets.length) return null;
  const model = selectedModel();
  const pending = Boolean(model) && targets.some((target) => store.pending.has(resultKey(model.id, target.key)));
  const remaining = model ? targets.filter((target) => needsTranscription(model, target)).length : 0;
  const button = document.createElement("button");
  button.type = "button";
  button.className = "col-transcribe-btn";
  button.dataset.col = column;
  button.disabled = !model || (!pending && !remaining);
  button.setAttribute("aria-busy", pending ? "true" : "false");
  const label = pending
    ? `Transcribing visible audio in ${column}`
    : remaining
      ? `Transcribe ${remaining} visible audio value${remaining === 1 ? "" : "s"} in ${column}`
      : `Visible audio in ${column} is transcribed`;
  button.title = pending ? `${label} (click to cancel)` : label;
  button.setAttribute("aria-label", button.title);
  button.innerHTML = '<img src="icons/transcribe.svg" alt="" aria-hidden="true" />';
  return button;
}

function showError(message) {
  if (elements.errorMessage) elements.errorMessage.textContent = message;
  if (elements.errorOverlay) {
    elements.errorOverlay.classList.add("active");
  } else {
    console.error(message);
  }
}

async function runBatch(model, targets, rerender, generation) {
  const keys = targets.map((target) => resultKey(model.id, target.key));
  const controller = new AbortController();
  let jobId = "";
  keys.forEach((key) => store.pending.set(key, "Waiting for the transcription model"));
  rerender();
  try {
    const started = await startJob("transcription", {
      model: model.id,
      items: targets.map((target, index) => ({
        id: `a${index}`,
        ...target.request,
        column: target.column,
        pointer: target.pointer,
      })),
    });
    jobId = started.id;
    store.jobs.set(jobId, { controller, keys });
    const result = await waitForJob(jobId, {
      signal: controller.signal,
      isCurrent: () => generation === store.generation,
      onUpdate: (job) => {
        if (!job.message) return;
        keys.forEach((key) => {
          if (store.pending.has(key)) store.pending.set(key, job.message);
        });
        renderTranscripts(elements.tableBody);
      },
    });
    const byId = new Map((result.items || []).map((item) => [item.id, item]));
    targets.forEach((target, index) => {
      const item = byId.get(`a${index}`);
      if (item) store.results.set(keys[index], item);
    });
  } catch (err) {
    if (!isAbortError(err) && generation === store.generation) {
      const message = extractErrorMessage(err);
      keys.forEach((key) => store.results.set(key, { error: message }));
      showError(`Transcription failed: ${message}`);
    }
  } finally {
    if (jobId) store.jobs.delete(jobId);
    keys.forEach((key) => store.pending.delete(key));
    if (generation === store.generation) rerender();
  }
}

async function transcribeTargets(targets, rerender) {
  const model = selectedModel();
  if (!model) return;
  const remaining = targets.filter((target) => needsTranscription(model, target));
  if (!remaining.length) return;
  const generation = store.generation;
  const batchSize = Math.max(1, Number(store.limits.max_batch_items) || 100);
  // Batches run one after another; the server loads one model at a time.
  for (let start = 0; start < remaining.length && generation === store.generation; start += batchSize) {
    await runBatch(model, remaining.slice(start, start + batchSize), rerender, generation);
  }
}

export function transcribeField(rowIndex, columnIndex, rerender) {
  return transcribeTargets(fieldTargets(rowIndex, columnIndex), rerender);
}

export function transcribeColumn(column, rerender) {
  const model = selectedModel();
  const targets = columnTargets(column);
  if (model && targets.some((target) => store.pending.has(resultKey(model.id, target.key)))) {
    cancelTranscriptions(rerender);
    return undefined;
  }
  return transcribeTargets(targets, rerender);
}

export function cancelTranscriptions(rerender = null) {
  store.generation += 1;
  store.jobs.forEach(({ controller, keys }, jobId) => {
    controller.abort();
    keys.forEach((key) => store.pending.delete(key));
    cancelJob(jobId).catch(() => {});
  });
  store.jobs.clear();
  store.pending.clear();
  if (rerender) rerender();
}

function transcriptForKey(audioKey) {
  const model = selectedModel();
  if (!model || !audioKey) return null;
  const key = resultKey(model.id, audioKey);
  if (store.pending.has(key)) return { pending: true, message: store.pending.get(key) };
  return store.results.get(key) || null;
}

function compactMarkup(transcript, label) {
  if (transcript.pending) {
    return `<div class="transcript-item is-pending" role="status">${label}<span class="transcript-text">${escapeHtml(transcript.message || "Transcribing...")}</span></div>`;
  }
  if (transcript.error) {
    return `<div class="transcript-item is-error">${label}<span class="transcript-text">${escapeHtml(transcript.error)}</span></div>`;
  }
  const text = transcript.text || "(no speech detected)";
  return `<div class="transcript-item" title="${escapeHtml(transcript.text || "")}">${label}<span class="transcript-text">${escapeHtml(text)}</span></div>`;
}

function blockMarkup(transcript) {
  if (transcript.pending) {
    return `<div class="transcript-block is-pending" role="status"><div class="transcript-header"><span class="transcript-label">Transcript</span></div><div class="transcript-text">${escapeHtml(transcript.message || "Transcribing...")}</div></div>`;
  }
  if (transcript.error) {
    return `<div class="transcript-block is-error"><div class="transcript-header"><span class="transcript-label">Transcript</span></div><div class="transcript-text">${escapeHtml(transcript.error)}</div></div>`;
  }
  const language = transcript.language ? `<span class="transcript-language">${escapeHtml(transcript.language)}</span>` : "";
  const copy = transcript.text
    ? `<button class="expanded-copy-btn icon-action-btn" data-copy="${encodeCopyValue(transcript.text)}" type="button" title="Copy transcript" aria-label="Copy transcript"><img src="icons/content-copy.svg" alt="" aria-hidden="true" /></button>`
    : "";
  return `<div class="transcript-block"><div class="transcript-header"><span class="transcript-label">Transcript</span>${language}${copy}</div><div class="transcript-text">${escapeHtml(transcript.text || "(no speech detected)")}</div></div>`;
}

// Inserts transcripts after cell chips and inside expanded players. Runs after
// every table render and whenever a running job reports progress.
export function renderTranscripts(root = document) {
  root.querySelectorAll(".audio-list").forEach((list) => {
    const next = list.nextElementSibling;
    if (next?.classList.contains("transcript-list")) next.remove();
    const chips = [...list.querySelectorAll(".audio-chip")];
    const items = chips
      .map((chip, index) => {
        const transcript = transcriptForKey(chip.dataset.audioKey);
        if (!transcript) return "";
        const label = chips.length > 1 ? `<span class="transcript-index">${index + 1}</span>` : "";
        return compactMarkup(transcript, label);
      })
      .filter(Boolean);
    if (items.length) list.insertAdjacentHTML("afterend", `<div class="transcript-list">${items.join("")}</div>`);
  });
  root.querySelectorAll(".audio-player").forEach((player) => {
    player.querySelector(":scope > .transcript-block")?.remove();
    const transcript = transcriptForKey(player.dataset.audioKey);
    if (transcript) player.insertAdjacentHTML("beforeend", blockMarkup(transcript));
  });
}

export function bindTranscriptionControls(rerender) {
  elements.transcriptionModel?.addEventListener("change", () => {
    storageSet(MODEL_STORAGE_KEY, elements.transcriptionModel.value);
    rerender();
  });
}
