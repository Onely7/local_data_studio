import { elements } from "./dom.js";
import { encodeCopyValue, escapeHtml } from "./formatting.js";
import {
  cancelJob,
  extractErrorMessage,
  fetchJSON,
  startJob,
  waitForJob,
} from "./http.js";
import { formatCell, formatExpandedCell } from "./images.js";
import { state } from "./state.js";

const MODEL_STORAGE_KEY = "local-data-studio.translation-model";
const LANGUAGE_STORAGE_KEY = "local-data-studio.translation-language";
const MEDIA_PATH_RE =
  /\.(?:aac|avif|bmp|flac|gif|heic|jpe?g|m4a|mp3|mp4|oga|ogg|png|svg|tiff?|wav|webm|webp)(?:[?#].*)?$/i;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const NUMERIC_STRING_RE =
  /^[+-]?(?:(?:\d+(?:\.\d*)?)|(?:\.\d+))(?:e[+-]?\d+)?$/i;
const MEDIA_HEX_PREFIX_RE =
  /^(?:89504e47|ffd8ff|47494638|424d|49492a00|4d4d002a|3c737667|494433|4f676753|664c6143|1a45dfa3|52494646)/i;
const MEDIA_BASE64_PREFIX_RE =
  /^(?:iVBORw0KGgo|\/9j\/|R0lGOD|Qk|SUkq|TU0AKg|PHN2Zy|PD94bW|SUQz|T2dnUw|ZkxhQw|GkXfo|UklGR)/;
let confirmationResolver = null;
let confirmationFocus = null;

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
    // Storage may be disabled; selection still works for this page session.
  }
}

export function isTranslatableString(value) {
  if (typeof value !== "string") return false;
  const text = value.trim();
  if (!text || /^(?:data:|<binary )/i.test(text)) {
    return false;
  }
  if (/^(?:https?|ftp|file):\/\//i.test(text) || /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(text)) return false;
  if (MEDIA_PATH_RE.test(text) || UUID_RE.test(text)) return false;
  if (NUMERIC_STRING_RE.test(text) || /^(?:true|false|null)$/i.test(text)) {
    return false;
  }
  if (!/\s/.test(text) && /[\\/]/.test(text)) return false;
  const compact = text.replace(/\s/g, "");
  if (MEDIA_HEX_PREFIX_RE.test(compact) || MEDIA_BASE64_PREFIX_RE.test(compact)) {
    return false;
  }
  if (compact.length >= 64 && /^[0-9a-f]+$/i.test(compact)) return false;
  if (compact.length >= 96 && compact.length % 4 === 0 && /^[A-Za-z0-9+/]+={0,2}$/.test(compact)) return false;
  if (/(?:^|\s)(?:def |class |function |SELECT\s|INSERT\s|UPDATE\s|DELETE\s|import )/i.test(text)) return false;
  return true;
}

function isBinarySequence(value) {
  if (typeof ArrayBuffer !== "undefined") {
    if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) return true;
  }
  return typeof Blob !== "undefined" && value instanceof Blob;
}

function isMediaOrBinaryObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  if (isBinarySequence(value)) return true;
  if (Object.hasOwn(value, "bytes")) return true;
  const path = typeof value.path === "string" ? value.path.trim() : "";
  return Boolean(path && MEDIA_PATH_RE.test(path));
}

export function collectTranslatableStrings(value, output = [], depth = 0) {
  if (depth > 64) return output;
  if (isBinarySequence(value) || isMediaOrBinaryObject(value)) return output;
  if (typeof value === "string") {
    if (isTranslatableString(value)) output.push(value);
    return output;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectTranslatableStrings(item, output, depth + 1));
  } else if (value && typeof value === "object") {
    Object.values(value).forEach((item) => collectTranslatableStrings(item, output, depth + 1));
  }
  return output;
}

export function hasTranslatableText(value) {
  return collectTranslatableStrings(value).length > 0;
}

function selectedModel() {
  return elements.translationModel?.value || "";
}

function selectedLanguage() {
  return elements.translationLanguage?.value || "";
}

function selectedModelMetadata() {
  const id = selectedModel();
  return state.llmModels.find((model) => model.id === id && model.translation && model.available);
}

function selectedLanguageMetadata() {
  const code = selectedLanguage();
  return state.translationLanguages.find((language) => language.code === code);
}

function browserLanguageDefault() {
  const available = new Set(state.translationLanguages.map((language) => language.code));
  if (available.has(state.translationConfiguredDefaultLanguage)) {
    return state.translationConfiguredDefaultLanguage;
  }
  const stored = storageGet(LANGUAGE_STORAGE_KEY);
  if (available.has(stored)) return stored;
  const browser = String(globalThis.navigator?.language || "").toLowerCase();
  if (available.has(browser)) return browser;
  const base = browser.split("-", 1)[0];
  if (available.has(base)) return base;
  return available.has(state.translationDefaultLanguage) ? state.translationDefaultLanguage : state.translationLanguages[0]?.code || "";
}

export function renderTranslationControls() {
  if (!elements.translationModel || !elements.translationLanguage) return;
  const models = state.llmModels.filter((model) => model.translation);
  const currentModel = elements.translationModel.value || storageGet(MODEL_STORAGE_KEY);
  elements.translationModel.innerHTML = models.length
    ? models
        .map((model) => {
          const suffix = model.available ? "" : " (unavailable)";
          const title = model.reason ? ` title="${escapeHtml(model.reason)}"` : "";
          return `<option value="${escapeHtml(model.id)}"${model.available ? "" : " disabled"}${title}>${escapeHtml(model.label)}${suffix}</option>`;
        })
        .join("")
    : '<option value="">No translation models</option>';
  const model = models.find((item) => item.id === currentModel && item.available)?.id || models.find((item) => item.id === state.translationDefaultModel && item.available)?.id || models.find((item) => item.available)?.id || "";
  elements.translationModel.value = model;
  elements.translationModel.disabled = !models.some((item) => item.available);

  const currentLanguage = elements.translationLanguage.value || browserLanguageDefault();
  elements.translationLanguage.innerHTML = state.translationLanguages
    .map((language) => `<option value="${escapeHtml(language.code)}">${escapeHtml(language.native_name)} · ${escapeHtml(language.name)}</option>`)
    .join("");
  elements.translationLanguage.value = state.translationLanguages.some((item) => item.code === currentLanguage) ? currentLanguage : browserLanguageDefault();
  elements.translationLanguage.disabled = !state.translationLanguages.length;
  updateTranslationButtons();
}

export async function loadTranslationConfig() {
  const data = await fetchJSON("/api/translation_languages");
  state.translationLanguages = data.languages || [];
  state.translationLimits = data.limits || {};
  state.translationConfiguredDefaultLanguage = data.configured_default_language || "";
  state.translationDefaultLanguage = data.default_language || "ja";
}

function fingerprint(value) {
  let text;
  try {
    text = JSON.stringify(value);
  } catch (err) {
    text = String(value);
  }
  let hash = 2166136261;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16);
}

function rowIdentity(rowIndex) {
  return state.rowIds[rowIndex] ?? `${state.pageIndex}:${state.offset}:${rowIndex}`;
}

function cacheKey(rowIndex, columnIndex, source) {
  return [
    state.file || "",
    state.view,
    state.searchQuery,
    state.querySql,
    state.pageIndex,
    state.offset,
    rowIdentity(rowIndex),
    columnIndex,
    selectedModel(),
    selectedLanguage(),
    fingerprint(source),
  ].join("|");
}

export function cachedTranslation(rowIndex, columnIndex) {
  const source = state.rows[rowIndex]?.[columnIndex];
  if (source === undefined) return undefined;
  return state.translationCache.get(cacheKey(rowIndex, columnIndex, source));
}

export function translationResultMarkup(rowIndex, columnIndex, expanded = false) {
  const translated = cachedTranslation(rowIndex, columnIndex);
  if (translated === undefined) return "";
  const value = expanded ? formatExpandedCell(translated) : formatCell(translated);
  const copyButton = expanded
    ? `<button class="expanded-copy-btn icon-action-btn" data-copy="${encodeCopyValue(translated)}" type="button" title="Copy translation" aria-label="Copy translation"><img src="icons/content-copy.svg" alt="" aria-hidden="true" /></button>`
    : "";
  return `<div class="translation-result${expanded ? " translation-result-expanded" : ""}"><div class="translation-result-header"><div class="translation-result-label">Translation</div>${copyButton}</div><div class="translation-result-value">${value}</div></div>`;
}

export function expandedTranslationButtonMarkup(rowIndex, columnIndex, value) {
  if (!hasTranslatableText(value)) return "";
  const translated = cachedTranslation(rowIndex, columnIndex);
  const key = cacheKey(rowIndex, columnIndex, value);
  const pending = state.translationPendingKeys.has(key);
  const disabled = pending || !selectedModelMetadata() || !selectedLanguageMetadata();
  const label = pending ? "Translating..." : translated === undefined ? "Translate" : "Retranslate";
  return `<button class="field-translate-btn" data-row="${rowIndex}" data-col="${columnIndex}" type="button" aria-busy="${pending ? "true" : "false"}"${disabled ? " disabled" : ""}><img src="icons/translation.svg" alt="" aria-hidden="true" />${label}</button>`;
}

export function createColumnTranslationButton(column) {
  const columnIndex = state.columns.indexOf(column);
  if (
    columnIndex < 0 ||
    !state.rows.some((row) => hasTranslatableText(row[columnIndex]))
  ) {
    return null;
  }
  const button = document.createElement("button");
  button.type = "button";
  button.className = "col-translate-btn";
  button.dataset.col = column;
  button.title = `Translate visible values in ${column}`;
  button.setAttribute("aria-label", `Translate visible values in ${column}`);
  const pending = state.rows.some((row, rowIndex) => state.translationPendingKeys.has(cacheKey(rowIndex, columnIndex, row[columnIndex])));
  button.disabled = pending || !selectedModelMetadata() || !selectedLanguageMetadata();
  button.setAttribute("aria-busy", pending ? "true" : "false");
  button.innerHTML = '<img src="icons/translation.svg" alt="" aria-hidden="true" />';
  return button;
}

function updateTranslationButtons() {
  const enabled = Boolean(selectedModelMetadata() && selectedLanguageMetadata());
  document.querySelectorAll(".field-translate-btn, .col-translate-btn").forEach((button) => {
    button.disabled = !enabled || button.getAttribute("aria-busy") === "true";
  });
}

function showTranslationError(message) {
  if (elements.errorMessage) elements.errorMessage.textContent = message;
  if (elements.errorOverlay) {
    elements.errorOverlay.classList.add("active");
  } else {
    console.error(message);
  }
}

function requestConfirmation({ rows, characters }) {
  const limits = state.translationLimits;
  if (rows <= (limits.confirm_rows || 25) && characters <= (limits.confirm_characters || 10000)) return Promise.resolve(true);
  const model = selectedModelMetadata();
  const language = selectedLanguageMetadata();
  if (!elements.translationConfirmOverlay || !elements.translationConfirmMessage) return Promise.resolve(false);
  elements.translationConfirmMessage.textContent = `${rows} visible row${rows === 1 ? "" : "s"} and ${characters.toLocaleString()} characters will be sent to ${model?.label || "the selected LLM"} for translation into ${language?.name || "the selected language"}.`;
  elements.translationConfirmOverlay.classList.add("active");
  confirmationFocus = document.activeElement;
  elements.translationConfirmRun?.focus();
  return new Promise((resolve) => {
    confirmationResolver = resolve;
  });
}

function closeConfirmation(accepted) {
  elements.translationConfirmOverlay?.classList.remove("active");
  if (confirmationFocus?.focus) confirmationFocus.focus();
  confirmationFocus = null;
  if (confirmationResolver) {
    const resolve = confirmationResolver;
    confirmationResolver = null;
    resolve(accepted);
  }
}

async function cancelActiveTranslation() {
  state.translationController?.abort();
  state.translationController = null;
  const jobId = state.translationJobId;
  state.translationJobId = null;
  if (jobId) {
    try {
      await cancelJob(jobId);
    } catch (err) {
      console.error(err);
    }
  }
  state.translationPendingKeys.clear();
}

async function translateTargets(targets, columnName, renderTable) {
  const model = selectedModelMetadata();
  const language = selectedLanguageMetadata();
  if (!model || !language || !targets.length) return;
  const strings = targets.flatMap((target) => collectTranslatableStrings(target.value));
  const characters = strings.reduce((total, value) => total + value.length, 0);
  const limits = state.translationLimits;
  if (targets.length > (limits.max_batch_rows || 500) || strings.length > (limits.max_strings || 2000) || characters > (limits.max_total_characters || 50000)) {
    showTranslationError("Visible values exceed the configured translation limit.");
    return;
  }
  if (!(await requestConfirmation({ rows: targets.length, characters }))) return;

  await cancelActiveTranslation();
  const generation = state.translationGeneration + 1;
  state.translationGeneration = generation;
  const controller = new AbortController();
  state.translationController = controller;
  targets.forEach((target) => state.translationPendingKeys.add(target.key));
  renderTable();
  try {
    const started = await startJob("translation", {
      model: model.id,
      target_language: language.code,
      column_name: columnName,
      items: targets.map((target, index) => ({ id: `r${index}`, value: target.value })),
    });
    state.translationJobId = started.id;
    const result = await waitForJob(started.id, {
      signal: controller.signal,
    });
    if (generation !== state.translationGeneration) return;
    const byId = new Map((result.items || []).map((item) => [item.id, item.value]));
    targets.forEach((target, index) => {
      if (byId.has(`r${index}`)) state.translationCache.set(target.key, byId.get(`r${index}`));
    });
    renderTable();
  } catch (err) {
    if (err?.name !== "AbortError" && generation === state.translationGeneration) {
      showTranslationError(extractErrorMessage(err));
    }
  } finally {
    if (generation === state.translationGeneration) {
      state.translationJobId = null;
      state.translationController = null;
      targets.forEach((target) => state.translationPendingKeys.delete(target.key));
      renderTable();
    }
  }
}

export function translateCell(rowIndex, columnIndex, renderTable) {
  const value = state.rows[rowIndex]?.[columnIndex];
  if (!hasTranslatableText(value)) return;
  return translateTargets([{ rowIndex, columnIndex, value, key: cacheKey(rowIndex, columnIndex, value) }], state.columns[columnIndex] || "value", renderTable);
}

export function translateColumn(column, renderTable) {
  const columnIndex = state.columns.indexOf(column);
  if (columnIndex < 0 || state.hiddenTableColumns.has(column)) return;
  const targets = state.rows
    .map((row, rowIndex) => ({ rowIndex, columnIndex, value: row[columnIndex], key: cacheKey(rowIndex, columnIndex, row[columnIndex]) }))
    .filter((target) => hasTranslatableText(target.value));
  if (!targets.length) {
    showTranslationError(`No translatable text is visible in ${column}.`);
    return;
  }
  return translateTargets(targets, column, renderTable);
}

export function bindTranslationControls(renderTable) {
  elements.translationModel?.addEventListener("change", async () => {
    storageSet(MODEL_STORAGE_KEY, selectedModel());
    state.translationGeneration += 1;
    await cancelActiveTranslation();
    renderTable();
  });
  elements.translationLanguage?.addEventListener("change", async () => {
    storageSet(LANGUAGE_STORAGE_KEY, selectedLanguage());
    state.translationGeneration += 1;
    await cancelActiveTranslation();
    renderTable();
  });
  elements.translationConfirmCancel?.addEventListener("click", () => closeConfirmation(false));
  elements.translationConfirmRun?.addEventListener("click", () => closeConfirmation(true));
  elements.translationConfirmOverlay?.addEventListener("click", (event) => {
    if (event.target === elements.translationConfirmOverlay) closeConfirmation(false);
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && elements.translationConfirmOverlay?.classList.contains("active")) closeConfirmation(false);
  });
}
