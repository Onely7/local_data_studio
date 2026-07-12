const state = {
  files: [],
  file: null,
  schema: [],
  columns: [],
  rows: [],
  limit: 100,
  offset: 0,
  view: "data",
  searchQuery: "",
  querySql: "",
  rowCount: null,
  pageTokens: [null],
  pageIndex: 0,
  nextPageToken: null,
  hasNextPage: false,
  expandedRowIndex: null,
  overlayRowIndex: null,
  overlayColumnIndex: null,
  overlayImageUrl: "",
  overlayImages: [],
  overlayImageIndex: 0,
  overlayRawFields: new Set(),
  columnStats: {},
  selectedRowIndex: null,
  selectedRowId: null,
  rowInspectorRaw: false,
  rowInspectorRawColumns: [],
  rowInspectorRawValues: null,
  rowInspectorRawLoading: false,
  rowInspectorRawRequest: 0,
  pendingDeleteRowId: null,
  pendingDeleteColumn: null,
  allowDeleteData: true,
  datasetQuery: "",
  datasetWarning: "",
  hiddenTableColumns: new Set(),
  rowIds: [],
  hiddenColumns: new Set(),
  countJobId: null,
  statsJobId: null,
  searchJobId: null,
  queryJobId: null,
  edaJobId: null,
  edaJobKind: "",
  atlasJobId: null,
  atlasJobKind: "",
  atlasCancelling: false,
  atlasUrl: "",
  embedderModels: [],
  llmModels: [],
  llmDefaultModel: "",
  nlGenerating: false,
};

const UPLOAD_EXTENSIONS = new Set([".jsonl", ".parquet", ".csv", ".tsv"]);
const MAX_IMAGE_CANDIDATES = 30;
const ROW_INSPECTOR_VALUE_MAX = 320;

const elements = {
  fileList: document.getElementById("file-list"),
  fileEmpty: document.getElementById("file-empty"),
  datasetSearch: document.getElementById("dataset-search"),
  datasetEmpty: document.getElementById("dataset-empty"),
  currentFile: document.getElementById("current-file"),
  refreshFiles: document.getElementById("refresh-files"),
  sidebar: document.querySelector(".sidebar"),
  dropHint: document.getElementById("drop-hint"),
  errorOverlay: document.getElementById("error-overlay"),
  errorMessage: document.getElementById("error-message"),
  errorOk: document.getElementById("error-ok"),
  searchInput: document.getElementById("search-input"),
  searchBtn: document.getElementById("search-btn"),
  clearSearch: document.getElementById("clear-search"),
  pageSize: document.getElementById("page-size"),
  prevPage: document.getElementById("prev-page"),
  nextPage: document.getElementById("next-page"),
  pageInfo: document.getElementById("page-info"),
  viewLabel: document.getElementById("view-label"),
  datasetMeta: document.getElementById("dataset-meta"),
  tableHead: document.querySelector("#data-table thead"),
  tableBody: document.querySelector("#data-table tbody"),
  sqlInput: document.getElementById("sql-input"),
  runQuery: document.getElementById("run-query"),
  resetView: document.getElementById("reset-view"),
  countRows: document.getElementById("count-rows"),
  rowCount: document.getElementById("row-count"),
  rowInspector: document.getElementById("row-inspector"),
  rowInspectorRaw: document.getElementById("row-inspector-raw"),
  runEda: document.getElementById("run-eda"),
  runEdaQuery: document.getElementById("run-eda-query"),
  edaProfileMode: document.getElementById("eda-profile-mode"),
  edaStatus: document.getElementById("eda-status"),
  edaLink: document.getElementById("eda-link"),
  atlasColumn: document.getElementById("atlas-column"),
  atlasModel: document.getElementById("atlas-model"),
  atlasBackend: document.getElementById("atlas-backend"),
  atlasProjection: document.getElementById("atlas-projection"),
  atlasPromptControls: document.getElementById("atlas-prompt-controls"),
  atlasPrompt: document.getElementById("atlas-prompt"),
  runAtlas: document.getElementById("run-atlas"),
  runAtlasQuery: document.getElementById("run-atlas-query"),
  atlasStatus: document.getElementById("atlas-status"),
  atlasLink: document.getElementById("atlas-link"),
  nlInput: document.getElementById("nl-input"),
  nlModel: document.getElementById("nl-model"),
  nlGenerate: document.getElementById("nl-generate"),
  nlStatus: document.getElementById("nl-status"),
  imageOverlay: document.getElementById("image-overlay"),
  overlayClose: document.getElementById("overlay-close"),
  overlayImage: document.getElementById("overlay-image"),
  overlayImageLabel: document.getElementById("overlay-image-label"),
  overlayTitle: document.getElementById("overlay-title"),
  overlayFields: document.getElementById("overlay-fields"),
  overlayNav: document.getElementById("overlay-nav"),
  overlayPrev: document.getElementById("overlay-prev"),
  overlayNext: document.getElementById("overlay-next"),
  overlayIndex: document.getElementById("overlay-index"),
  jsonOverlay: document.getElementById("json-overlay"),
  jsonClose: document.getElementById("json-close"),
  jsonTitle: document.getElementById("json-title"),
  jsonBody: document.getElementById("json-body"),
  copyJson: document.getElementById("copy-json"),
  copyRow: document.getElementById("copy-row"),
  deleteRow: document.getElementById("delete-row"),
  deleteOverlay: document.getElementById("delete-overlay"),
  deleteMessage: document.getElementById("delete-message"),
  deleteCancel: document.getElementById("delete-cancel"),
  deleteSoft: document.getElementById("delete-soft"),
  deleteHard: document.getElementById("delete-hard"),
  columnDeleteOverlay: document.getElementById("column-delete-overlay"),
  columnDeleteMessage: document.getElementById("column-delete-message"),
  columnDeleteCancel: document.getElementById("column-delete-cancel"),
  columnDeleteSoft: document.getElementById("column-delete-soft"),
  columnDeleteHard: document.getElementById("column-delete-hard"),
};

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function shorten(text, max = 240) {
  if (text.length <= max) return text;
  return text.slice(0, max) + "...";
}

function compactInspectorValue(value) {
  if (typeof value === "string") {
    if (value.length <= ROW_INSPECTOR_VALUE_MAX) return value;
    return `${value.slice(0, ROW_INSPECTOR_VALUE_MAX)}... (truncated; use Raw)`;
  }
  if (value === null || typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  try {
    const json = JSON.stringify(value);
    if (!json || json.length <= ROW_INSPECTOR_VALUE_MAX) return value;
    return `${json.slice(0, ROW_INSPECTOR_VALUE_MAX)}... (truncated; use Raw)`;
  } catch (err) {
    const text = String(value);
    return text.length <= ROW_INSPECTOR_VALUE_MAX
      ? text
      : `${text.slice(0, ROW_INSPECTOR_VALUE_MAX)}... (truncated; use Raw)`;
  }
}

function isInspectorValueTruncated(value) {
  if (typeof value === "string") {
    return value.length > ROW_INSPECTOR_VALUE_MAX;
  }
  if (value === null || typeof value === "number" || typeof value === "boolean") {
    return false;
  }
  try {
    const json = JSON.stringify(value);
    return Boolean(json && json.length > ROW_INSPECTOR_VALUE_MAX);
  } catch (err) {
    return String(value).length > ROW_INSPECTOR_VALUE_MAX;
  }
}

function resetRowInspectorRaw() {
  state.rowInspectorRaw = false;
  state.rowInspectorRawColumns = [];
  state.rowInspectorRawValues = null;
  state.rowInspectorRawLoading = false;
  state.rowInspectorRawRequest += 1;
}

function isImageUrl(text) {
  if (!text) return false;
  if (text.startsWith("data:image")) return true;
  return /\.(png|jpg|jpeg|gif|webp|svg)(\?.*)?$/i.test(text);
}

function imageMimeFromPath(path) {
  const text = String(path || "").toLowerCase().split("?")[0];
  if (text.endsWith(".png")) return "image/png";
  if (text.endsWith(".jpg") || text.endsWith(".jpeg")) return "image/jpeg";
  if (text.endsWith(".gif")) return "image/gif";
  if (text.endsWith(".webp")) return "image/webp";
  if (text.endsWith(".svg")) return "image/svg+xml";
  return "image/png";
}

function imageMimeFromBytes(bytes) {
  const text = String(bytes || "").trim();
  const compact = text.replace(/\s+/g, "");
  const lowered = compact.toLowerCase();
  if (text.startsWith("data:image")) {
    const match = text.match(/^data:([^;,]+)/);
    return match ? match[1] : "image/png";
  }
  if (lowered.startsWith("89504e47") || compact.startsWith("iVBORw0KGgo")) {
    return "image/png";
  }
  if (lowered.startsWith("ffd8ff") || compact.startsWith("/9j/")) {
    return "image/jpeg";
  }
  if (lowered.startsWith("47494638") || compact.startsWith("R0lGOD")) {
    return "image/gif";
  }
  if (
    (lowered.startsWith("52494646") && lowered.slice(16, 24) === "57454250") ||
    compact.startsWith("UklGR")
  ) {
    return "image/webp";
  }
  return "";
}

function normalizeImageUrl(text) {
  if (!text) return "";
  if (text.startsWith("data:image")) return text;
  if (/^https?:\/\//i.test(text)) return text;
  if (text.startsWith("/data/")) return text;
  if (text.startsWith("/cache/")) return text;
  if (text.startsWith("/api/")) return text;
  if (text.startsWith("file://")) {
    return `/api/raw?path=${encodeURIComponent(text)}`;
  }
  if (text.startsWith("/")) {
    return `/api/raw?path=${encodeURIComponent(text)}`;
  }
  const cleaned = text.replace(/^\.\//, "");
  return `/data/${cleaned}`;
}

function isBase64ImageBytes(text) {
  const compact = text.replace(/\s+/g, "");
  if (!compact || compact.length % 4 !== 0) return false;
  return /^[A-Za-z0-9+/]+={0,2}$/.test(compact);
}

function isHexImageBytes(text) {
  const compact = text.replace(/\s+/g, "");
  return compact.length >= 16 && compact.length % 2 === 0 && /^[0-9a-f]+$/i.test(compact);
}

function hexToBase64(hex) {
  const compact = hex.replace(/\s+/g, "");
  let binary = "";
  for (let index = 0; index < compact.length; index += 2) {
    binary += String.fromCharCode(parseInt(compact.slice(index, index + 2), 16));
  }
  return btoa(binary);
}

function bytesToImageDataUrl(bytes, pathHint = "") {
  if (typeof bytes !== "string" || !bytes.trim()) return "";
  const text = bytes.trim();
  if (text.startsWith("data:image")) return text;
  const mime = imageMimeFromBytes(text) || imageMimeFromPath(pathHint);
  if (isHexImageBytes(text)) {
    return `data:${mime};base64,${hexToBase64(text)}`;
  }
  if (isBase64ImageBytes(text)) {
    return `data:${mime};base64,${text.replace(/\s+/g, "")}`;
  }
  return "";
}

function imageCandidate(src, fallback, fallbackSrc = "") {
  if (!src) return null;
  return { src, fallback: fallback === undefined ? src : fallback, fallbackSrc };
}

function extractImageCandidates(value) {
  if (typeof value === "string" && isImageUrl(value)) {
    return [imageCandidate(normalizeImageUrl(value), value)];
  }
  if (Array.isArray(value)) {
    return value
      .flatMap((item) => extractImageCandidates(item))
      .slice(0, MAX_IMAGE_CANDIDATES);
  }
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const fallback = JSON.stringify(value);
    const pathSrc =
      typeof value.path === "string" && isImageUrl(value.path)
        ? normalizeImageUrl(value.path)
        : "";
    if (typeof value.bytes === "string") {
      const dataUrl = bytesToImageDataUrl(value.bytes, value.path);
      if (dataUrl) {
        return [imageCandidate(dataUrl, fallback, pathSrc)].filter(Boolean);
      }
    }
    if (pathSrc) {
      return [imageCandidate(pathSrc, fallback)].filter(Boolean);
    }
    return [];
  }
  return [];
}

function extractImageUrls(value) {
  const urls = [];
  extractImageCandidates(value).forEach((candidate) => {
    if (candidate.src && !urls.includes(candidate.src)) {
      urls.push(candidate.src);
    }
  });
  return urls;
}

function renderImageGrid(candidates, options = {}) {
  const max = Number.isFinite(options.max) ? options.max : 4;
  const className = options.className || "image-grid";
  const imageCandidates = candidates.map((item) =>
    typeof item === "string" ? imageCandidate(item, item) : item,
  ).filter(Boolean);
  const shown = imageCandidates.slice(0, max);
  const extra = imageCandidates.length - shown.length;
  const items = shown
    .map(
      (candidate, idx) => `
        <span class="image-cell">
          <img src="${escapeHtml(candidate.src)}" alt="image ${
            idx + 1
          }" data-image-src="${escapeHtml(candidate.src)}" data-fallback-src="${escapeHtml(candidate.fallbackSrc || "")}" onerror="if(this.dataset.fallbackSrc && this.dataset.fallbackUsed !== '1'){this.dataset.fallbackUsed='1';this.src=this.dataset.fallbackSrc;}else{this.style.display='none';this.nextElementSibling.style.display='inline';}" />
          <span class="cell image-fallback" style="display:none">${escapeHtml(shorten(candidate.fallback))}</span>
        </span>
      `,
    )
    .join("");
  const badge = extra > 0 ? `<div class="image-count">+${extra}</div>` : "";
  return `<div class="${className}">${items}${badge}</div>`;
}

function formatCell(value) {
  if (value === null || value === undefined) {
    return '<span class="muted">null</span>';
  }
  const imageCandidates = extractImageCandidates(value);
  if (imageCandidates.length) {
    return renderImageGrid(imageCandidates, { className: "image-grid", max: 4 });
  }
  if (typeof value === "string") {
    return `<span class="cell">${escapeHtml(shorten(value))}</span>`;
  }
  if (Array.isArray(value)) {
    try {
      const json = JSON.stringify(value);
      return `<span class="cell">${escapeHtml(shorten(json))}</span>`;
    } catch (err) {
      return `<span class="cell">${escapeHtml(String(value))}</span>`;
    }
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return `<span class="cell">${escapeHtml(value)}</span>`;
  }
  try {
    const json = JSON.stringify(value);
    return `<span class="cell">${escapeHtml(shorten(json))}</span>`;
  } catch (err) {
    return `<span class="cell">${escapeHtml(String(value))}</span>`;
  }
}

function formatExpandedCell(value) {
  if (value === null || value === undefined) {
    return '<span class="muted">null</span>';
  }
  const imageCandidates = extractImageCandidates(value);
  if (imageCandidates.length) {
    return renderImageGrid(imageCandidates, {
      className: "image-grid expanded-image-grid",
      max: 8,
    });
  }
  if (typeof value === "string") {
    return `<pre class="expanded-text">${escapeHtml(value)}</pre>`;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return `<pre class="expanded-text">${escapeHtml(value)}</pre>`;
  }
  if (Array.isArray(value)) {
    try {
      const json = JSON.stringify(value, null, 2);
      return `<pre class="expanded-text">${escapeHtml(json)}</pre>`;
    } catch (err) {
      return `<pre class="expanded-text">${escapeHtml(String(value))}</pre>`;
    }
  }
  try {
    const json = JSON.stringify(value, null, 2);
    return `<pre class="expanded-text">${escapeHtml(json)}</pre>`;
  } catch (err) {
    return `<pre class="expanded-text">${escapeHtml(String(value))}</pre>`;
  }
}

function formatOverlayValue(value) {
  if (value === null || value === undefined) {
    return '<span class="muted">null</span>';
  }
  if (typeof value === "string") {
    return `<span>${escapeHtml(value)}</span>`;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return `<span>${escapeHtml(value)}</span>`;
  }
  try {
    const json = JSON.stringify(value, null, 2);
    return `<pre class="overlay-json">${highlightJson(json)}</pre>`;
  } catch (err) {
    return `<span>${escapeHtml(String(value))}</span>`;
  }
}

function renderOverlayFields() {
  if (!elements.overlayFields || state.overlayRowIndex === null) return;
  const row = state.rows[state.overlayRowIndex] || [];
  const fields = state.columns
    .map((col, idx) => {
      const rawValue = row[idx];
      const fieldKey = String(idx);
      const truncated = isInspectorValueTruncated(rawValue);
      const isRaw = state.overlayRawFields.has(fieldKey);
      const displayValue =
        truncated && !isRaw ? compactInspectorValue(rawValue) : rawValue;
      const copyValue = encodeCopyValue(rawValue);
      const rawButton = truncated
        ? `<button class="overlay-copy overlay-raw-field" data-raw-field="${escapeHtml(
            fieldKey,
          )}" type="button">${isRaw ? "Compact" : "Raw"}</button>`
        : "";
      return `
        <div class="overlay-field">
          <div class="overlay-field-header">
            <div class="overlay-field-label">${escapeHtml(col)}</div>
            <div class="overlay-field-actions">
              ${rawButton}
              <button class="overlay-copy" data-copy="${copyValue}" type="button">Copy</button>
            </div>
          </div>
          <div class="overlay-field-value">${formatOverlayValue(displayValue)}</div>
        </div>
      `;
    })
    .join("");

  elements.overlayFields.innerHTML =
    fields || '<div class="muted">No extra fields</div>';
}

function toggleOverlayFieldRaw(fieldKey) {
  if (state.overlayRowIndex === null) return;
  const key = String(fieldKey);
  if (state.overlayRawFields.has(key)) {
    state.overlayRawFields.delete(key);
  } else {
    state.overlayRawFields.add(key);
  }
  renderOverlayFields();
}

function encodeCopyValue(value) {
  let text;
  try {
    text = JSON.stringify(value, null, 2);
  } catch (err) {
    text = String(value);
  }
  return encodeURIComponent(text);
}

function sameImageCandidate(left, right) {
  if (!left || !right) return false;
  return (
    left.src === right.src &&
    (left.fallbackSrc || "") === (right.fallbackSrc || "")
  );
}

function imageCandidateMatchesUrl(candidate, url) {
  if (!candidate || !url) return false;
  return candidate.src === url || candidate.fallbackSrc === url;
}

function columnNameAt(index) {
  return state.columns[index] || "";
}

function collectRowImageCandidates(row) {
  const candidates = [];
  row.forEach((value, columnIndex) => {
    const images = extractImageCandidates(value);
    images.forEach((candidate) => {
      if (
        candidates.length < MAX_IMAGE_CANDIDATES &&
        !candidates.some((item) => sameImageCandidate(item, candidate))
      ) {
        candidates.push({
          ...candidate,
          columnIndex,
          columnName: columnNameAt(columnIndex),
        });
      }
    });
  });
  return candidates;
}

function isJsonDetailValue(value) {
  if (value === null || value === undefined) return false;
  if (extractImageUrls(value).length) return false;
  if (Array.isArray(value)) return true;
  return typeof value === "object";
}

async function fetchJSON(url, options = {}) {
  const headers = options.headers ? { ...options.headers } : {};
  if (!(options.body instanceof FormData) && !headers["Content-Type"]) {
    headers["Content-Type"] = "application/json";
  }
  const response = await fetch(url, {
    headers,
    ...options,
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || "Request failed");
  }
  return response.json();
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function startJob(kind, payload) {
  return fetchJSON(`/api/jobs/${kind}`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

async function cancelJob(jobId) {
  if (!jobId) return null;
  return fetchJSON(`/api/jobs/${jobId}/cancel`, { method: "POST" });
}

async function waitForJob(jobId, options = {}) {
  const intervalMs = options.intervalMs || 600;
  while (true) {
    const job = await fetchJSON(`/api/jobs/${jobId}`);
    if (options.onUpdate) {
      options.onUpdate(job);
    }
    if (job.status === "succeeded") {
      return job.result || {};
    }
    if (job.status === "failed") {
      throw new Error(job.error || "Job failed");
    }
    if (job.status === "cancelled") {
      throw new Error(job.message || "Job cancelled");
    }
    await sleep(intervalMs);
  }
}

function formatJobProgress(job, fallback) {
  if (Number.isFinite(job.progress)) {
    const message = job.message ? ` · ${job.message}` : "";
    return `${fallback} ${Math.round(job.progress * 100)}%${message}`;
  }
  return job.message || fallback;
}

function formatFileSize(bytes) {
  const value = Number(bytes);
  if (!Number.isFinite(value) || value < 0) return "";
  const units = ["Bytes", "kB", "MB", "GB", "TB"];
  let size = value;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  if (unitIndex === 0) {
    return `${Math.round(size)} ${units[unitIndex]}`;
  }
  const exponent = Math.floor(Math.log10(size));
  const scale = 10 ** (2 - exponent);
  const formatted = Number(
    (Math.floor(size * scale) / scale).toPrecision(3),
  ).toString();
  return `${formatted} ${units[unitIndex]}`;
}

function extractErrorMessage(err) {
  let message = err && err.message ? err.message : "Request failed";
  try {
    const parsed = JSON.parse(message);
    if (parsed && parsed.detail) {
      message = parsed.detail;
    }
  } catch (parseErr) {
    // ignore JSON parse errors
  }
  return message;
}

function renderFiles() {
  elements.fileList.innerHTML = "";
  if (state.files.length === 0) {
    elements.fileEmpty.style.display = "block";
    if (elements.datasetEmpty) {
      elements.datasetEmpty.style.display = "none";
    }
    return;
  }
  elements.fileEmpty.style.display = "none";

  const query = state.datasetQuery.trim().toLowerCase();
  const visibleFiles = query
    ? state.files.filter((file) => file.name.toLowerCase().includes(query))
    : state.files;

  if (elements.datasetEmpty) {
    elements.datasetEmpty.style.display = visibleFiles.length
      ? "none"
      : "block";
  }

  visibleFiles.forEach((file) => {
    const div = document.createElement("div");
    div.className = "file-item" + (state.file === file.name ? " active" : "");
    div.dataset.file = file.name;
    div.title = file.name;
    div.innerHTML = `
      <div class="file-name">${escapeHtml(file.name)}</div>
      <div class="meta">${escapeHtml(formatFileSize(file.size))}</div>
    `;
    elements.fileList.appendChild(div);
  });
}

function applyRowIndexStyle(cell) {
  cell.style.color = "rgba(100, 116, 139, 0.42)";
  cell.style.fontWeight = "400";
  cell.style.opacity = "0.72";
  cell.style.textAlign = "right";
  cell.style.userSelect = "none";
}

function renderTable() {
  elements.tableHead.innerHTML = "";
  elements.tableBody.innerHTML = "";

  if (!state.columns.length) {
    elements.tableHead.innerHTML = "<tr><th>No data</th></tr>";
    return;
  }

  const visibleColumns = state.columns.filter(
    (col) => !state.hiddenTableColumns.has(col),
  );
  if (!visibleColumns.length) {
    elements.tableHead.innerHTML = "<tr><th>No visible columns</th></tr>";
    return;
  }

  const headRow = document.createElement("tr");
  const indexHead = document.createElement("th");
  indexHead.className = "row-index-header";
  indexHead.setAttribute("aria-label", "Row index");
  applyRowIndexStyle(indexHead);
  headRow.appendChild(indexHead);

  visibleColumns.forEach((col) => {
    const th = document.createElement("th");
    th.classList.add("col-header-cell");
    th.dataset.col = col;
    const isHidden = state.hiddenColumns.has(col);
    if (isHidden) {
      th.classList.add("col-hidden");
      th.title = "Click to show in row details";
    } else {
      th.title = "Click to hide in row details";
    }
    const wrapper = document.createElement("div");
    wrapper.className = "col-header";
    const name = document.createElement("div");
    name.className = "col-name";
    name.textContent = col;
    wrapper.appendChild(name);

    const stats = state.columnStats[col];
    if (stats) {
      if (stats.label) {
        const meta = document.createElement("div");
        meta.className = "col-meta";
        meta.textContent = stats.label;
        wrapper.appendChild(meta);
      }

      if (Array.isArray(stats.bins) && stats.bins.length) {
        const sparkline = document.createElement("div");
        sparkline.className = "sparkline";
        if (stats.kind) {
          sparkline.dataset.kind = stats.kind;
        }
        sparkline.style.gridTemplateColumns = `repeat(${stats.bins.length}, minmax(8px, 1fr))`;
        const max = Math.max(...stats.bins, 1);
        stats.bins.forEach((count) => {
          const bar = document.createElement("span");
          const height = Math.max(15, Math.round((count / max) * 100));
          bar.style.height = `${height}%`;
          sparkline.appendChild(bar);
        });
        wrapper.appendChild(sparkline);

        if (
          Array.isArray(stats.labels) &&
          stats.labels.length === stats.bins.length
        ) {
          const labels = document.createElement("div");
          labels.className = "sparkline-labels";
          labels.style.gridTemplateColumns = `repeat(${stats.labels.length}, minmax(8px, 1fr))`;
          stats.labels.forEach((label) => {
            const span = document.createElement("span");
            span.textContent = label;
            span.title = label;
            labels.appendChild(span);
          });
          wrapper.appendChild(labels);
        }
      }

      if (stats.axis) {
        const axis = document.createElement("div");
        axis.className = "sparkline-axis";
        const left = document.createElement("span");
        left.textContent = stats.axis.left;
        const right = document.createElement("span");
        right.textContent = stats.axis.right;
        axis.appendChild(left);
        axis.appendChild(right);
        wrapper.appendChild(axis);
      } else if (stats.note) {
        const axis = document.createElement("div");
        axis.className = "sparkline-axis center";
        axis.textContent = stats.note;
        wrapper.appendChild(axis);
      }
    }

    th.appendChild(wrapper);

    const deleteBtn = document.createElement("button");
    deleteBtn.type = "button";
    deleteBtn.className = "col-delete-btn";
    deleteBtn.dataset.col = col;
    deleteBtn.title = "Delete column";
    deleteBtn.textContent = "DEL";
    th.appendChild(deleteBtn);

    headRow.appendChild(th);
  });
  elements.tableHead.appendChild(headRow);

  state.rows.forEach((row, rowIndex) => {
    const tr = document.createElement("tr");
    tr.dataset.index = rowIndex;
    if (state.expandedRowIndex === rowIndex) {
      tr.classList.add("expanded");
    }
    const indexCell = document.createElement("td");
    indexCell.className = "row-index-cell";
    indexCell.textContent = String(previewRowNumber(rowIndex));
    applyRowIndexStyle(indexCell);
    tr.appendChild(indexCell);

    state.columns.forEach((col, idx) => {
      if (state.hiddenTableColumns.has(col)) {
        return;
      }
      const cell = row[idx];
      const td = document.createElement("td");
      td.dataset.colIndex = String(idx);
      td.innerHTML = formatCell(cell);
      tr.appendChild(td);
    });
    elements.tableBody.appendChild(tr);

    if (state.expandedRowIndex === rowIndex) {
      const details = document.createElement("tr");
      details.className = "expanded-row";
      details.dataset.index = rowIndex;
      const td = document.createElement("td");
      td.colSpan = visibleColumns.length + 1;
      let visibleCount = 0;
      const content = state.columns
        .map((col, idx) => {
          if (
            state.hiddenTableColumns.has(col) ||
            state.hiddenColumns.has(col)
          ) {
            return "";
          }
          visibleCount += 1;
          const value = row[idx];
          const showJson = isJsonDetailValue(value);
          const jsonButton = showJson
            ? `<button class="json-detail-btn" data-row="${rowIndex}" data-col="${idx}" type="button" title="JSON View">{}</button>`
            : "";
          return `
            <div class="expanded-field">
              <div class="expanded-field-header">
                <div class="expanded-label">${escapeHtml(col)}</div>
                ${jsonButton}
              </div>
              <div class="expanded-value">${formatExpandedCell(value)}</div>
            </div>
          `;
        })
        .join("");
      td.innerHTML = visibleCount
        ? `<div class="expanded-grid">${content}</div>`
        : `<div class="expanded-empty">All columns are hidden. Click a column header to show.</div>`;
      details.appendChild(td);
      elements.tableBody.appendChild(details);
    }
  });
}

function renderPagination() {
  const page =
    state.view === "query"
      ? Math.floor(state.offset / state.limit) + 1
      : state.pageIndex + 1;
  elements.pageInfo.textContent =
    state.view === "search" ? "Search results" : `Page ${page}`;
  if (elements.prevPage) {
    elements.prevPage.disabled =
      state.view === "query" ? state.offset <= 0 : state.pageIndex <= 0;
  }
  if (elements.nextPage) {
    elements.nextPage.disabled =
      state.view === "query"
        ? false
        : state.view === "search" || !state.nextPageToken;
  }
}

function previewRowNumber(rowIndex) {
  const base =
    state.view === "query" ? state.offset : state.pageIndex * state.limit;
  return base + rowIndex + 1;
}

function renderMeta() {
  const columnCount = state.schema.length;
  const mode =
    state.view === "query" ? "Query" : state.view === "search" ? "Search" : "Preview";
  elements.viewLabel.textContent = state.datasetWarning ? `${mode} | Warning` : mode;
  elements.datasetMeta.textContent = state.file
    ? state.datasetWarning ||
      `${columnCount} columns | page ${
        state.view === "query"
          ? Math.floor(state.offset / state.limit) + 1
          : state.pageIndex + 1
      }`
    : "";
}

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
    const parsed = new URL(url);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return "";
    return parsed.href;
  } catch (err) {
    return "";
  }
}

function openAtlasUrl() {
  const url = normalizeAtlasUrl(
    state.atlasUrl || (elements.atlasLink ? elements.atlasLink.href : ""),
  );
  if (!url) {
    if (elements.atlasStatus) {
      elements.atlasStatus.textContent =
        "Atlas is ready, but the Atlas URL is missing. Open http://localhost:5055/ directly.";
    }
    return;
  }
  window.location.href = url;
}

function renderAtlasColumnOptions() {
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

function renderAtlasModelOptions() {
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

function renderAtlasPromptControl() {
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

function renderAtlasBackendOptions() {
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

function renderDatasetInfo() {
  if (!elements.datasetInfo) return;
  if (!state.schema.length) {
    elements.datasetInfo.innerHTML =
      '<div class="muted">No schema loaded</div>';
    return;
  }
  const rows = state.schema
    .map(
      (col) =>
        `<div class="info-row"><span>${escapeHtml(
          col.name,
        )}</span><span>${escapeHtml(col.type)}</span></div>`,
    )
    .join("");
  elements.datasetInfo.innerHTML = `<div class="info-grid">${rows}</div>`;
}

function renderColumnFocus(columnName, values = []) {
  if (!elements.columnFocus) return;
  if (!columnName) {
    elements.columnFocus.innerHTML =
      '<div class="muted">Click a column header</div>';
    return;
  }
  const col = state.schema.find((item) => item.name === columnName);
  const type = col ? col.type : "";
  const list = values
    .map((val) => `<div class="chip">${escapeHtml(String(val))}</div>`)
    .join("");
  elements.columnFocus.innerHTML = `
    <div><strong>${escapeHtml(columnName)}</strong></div>
    <div class="muted">${escapeHtml(type)}</div>
    <div class="chip-grid">${
      list || '<div class="muted">No samples</div>'
    }</div>
  `;
}

function renderRowInspector(rowIndex) {
  if (rowIndex === null || rowIndex === undefined) {
    elements.rowInspector.textContent = "Select a row to inspect";
    state.selectedRowIndex = null;
    state.selectedRowId = null;
    resetRowInspectorRaw();
    if (elements.rowInspectorRaw) {
      elements.rowInspectorRaw.disabled = true;
      elements.rowInspectorRaw.textContent = "Raw";
    }
    updateRowActions();
    return;
  }
  if (state.selectedRowIndex !== rowIndex && state.rowInspectorRaw) {
    resetRowInspectorRaw();
  }
  state.selectedRowIndex = rowIndex;
  state.selectedRowId =
    Array.isArray(state.rowIds) && Number.isFinite(state.rowIds[rowIndex])
      ? state.rowIds[rowIndex]
      : null;
  const row = state.rows[rowIndex];
  const rowObj = {};
  if (state.rowInspectorRaw && Array.isArray(state.rowInspectorRawValues)) {
    state.rowInspectorRawColumns.forEach((col, idx) => {
      rowObj[col] = state.rowInspectorRawValues[idx];
    });
  } else if (!state.rowInspectorRawLoading) {
    state.columns.forEach((col, idx) => {
      if (state.hiddenTableColumns.has(col)) {
        return;
      }
      rowObj[col] = compactInspectorValue(row[idx]);
    });
  }
  if (elements.rowInspectorRaw) {
    elements.rowInspectorRaw.disabled = state.rowInspectorRawLoading;
    elements.rowInspectorRaw.textContent = state.rowInspectorRawLoading
      ? "Loading..."
      : state.rowInspectorRaw
        ? "Compact"
        : "Raw";
  }
  elements.rowInspector.textContent = state.rowInspectorRawLoading
    ? "Loading full row data..."
    : Object.keys(rowObj).length
      ? JSON.stringify(rowObj, null, 2)
      : "No visible columns";
  updateRowActions();
}

async function toggleRowInspectorRaw() {
  if (state.selectedRowIndex === null || state.selectedRowIndex === undefined) {
    return;
  }
  if (state.rowInspectorRaw) {
    resetRowInspectorRaw();
    renderRowInspector(state.selectedRowIndex);
    return;
  }

  const rowIndex = state.selectedRowIndex;
  const fileAtStart = state.file;
  const rowId = state.rowIds[rowIndex];
  const request = { file: fileAtStart };
  if (Number.isFinite(rowId)) {
    request.row_id = rowId;
  } else if (state.view === "query" && state.querySql) {
    request.sql = state.querySql;
    request.offset = state.offset + rowIndex;
  } else {
    showError("Full Raw data is unavailable for this row.");
    return;
  }

  state.rowInspectorRaw = true;
  state.rowInspectorRawColumns = [];
  state.rowInspectorRawValues = null;
  state.rowInspectorRawLoading = true;
  const requestId = state.rowInspectorRawRequest + 1;
  state.rowInspectorRawRequest = requestId;
  renderRowInspector(rowIndex);

  try {
    const data = await fetchJSON("/api/raw_row", {
      method: "POST",
      body: JSON.stringify(request),
    });
    if (
      state.rowInspectorRawRequest !== requestId ||
      state.file !== fileAtStart ||
      state.selectedRowIndex !== rowIndex
    ) {
      return;
    }
    state.rowInspectorRawColumns = data.columns || [];
    state.rowInspectorRawValues = data.row || [];
    state.rowInspectorRawLoading = false;
    renderRowInspector(rowIndex);
  } catch (err) {
    if (state.rowInspectorRawRequest !== requestId) return;
    resetRowInspectorRaw();
    renderRowInspector(rowIndex);
    showError(extractErrorMessage(err));
  }
}

function updateRowActions() {
  if (!elements.deleteRow) return;
  const canDelete = Number.isFinite(state.selectedRowId);
  elements.deleteRow.disabled = !canDelete;
  updateDeleteUi();
}

function updateDeleteUi() {
  if (elements.deleteHard) {
    elements.deleteHard.style.display = state.allowDeleteData
      ? "inline-flex"
      : "none";
  }
  if (elements.columnDeleteHard) {
    elements.columnDeleteHard.style.display = state.allowDeleteData
      ? "inline-flex"
      : "none";
  }
}

function showError(message) {
  if (elements.errorMessage) {
    elements.errorMessage.textContent = message;
  }
  if (elements.errorOverlay) {
    elements.errorOverlay.classList.add("active");
  } else {
    window.alert(message);
  }
}

function closeError() {
  if (!elements.errorOverlay) return;
  elements.errorOverlay.classList.remove("active");
}

function resetPreviewPaging() {
  state.offset = 0;
  state.pageIndex = 0;
  state.pageTokens = [null];
  state.nextPageToken = null;
  state.hasNextPage = false;
}

function currentPageToken() {
  return state.pageTokens[state.pageIndex] || null;
}

function applyTableData(data) {
  state.columns = data.columns || [];
  state.rows = data.rows || [];
  state.rowIds = data.row_ids || [];
  state.datasetWarning = data.warning || "";
  state.nextPageToken = data.next_page_token || null;
  state.hasNextPage = Boolean(data.has_next);
  if (Number.isFinite(data.offset)) {
    state.offset = data.offset;
  }
  state.expandedRowIndex = null;
  state.selectedRowIndex = null;
  state.selectedRowId = null;
  resetRowInspectorRaw();
  closeImageOverlay();
  closeJsonOverlay();
  renderTable();
  renderPagination();
  renderMeta();
  updateRowActions();
}

async function copyRowInspector() {
  if (!elements.rowInspector) return;
  const text = elements.rowInspector.textContent || "";
  if (!text || text === "Select a row to inspect") return;
  try {
    await navigator.clipboard.writeText(text);
    if (elements.copyRow) {
      elements.copyRow.textContent = "Copied";
      setTimeout(() => {
        elements.copyRow.textContent = "Copy";
      }, 1200);
    }
  } catch (err) {
    console.error(err);
  }
}

function setDeleteOverlayBusy(isBusy) {
  if (elements.deleteSoft) elements.deleteSoft.disabled = isBusy;
  if (elements.deleteHard) elements.deleteHard.disabled = isBusy;
  if (elements.deleteCancel) elements.deleteCancel.disabled = isBusy;
}

function openDeleteOverlay(rowId) {
  if (!elements.deleteOverlay) return;
  state.pendingDeleteRowId = rowId;
  if (elements.deleteMessage) {
    elements.deleteMessage.textContent = state.allowDeleteData
      ? `Row ${rowId} will be removed from the view. Choose whether to delete it from the file.`
      : `Row ${rowId} will be removed from the view. Delete from file is disabled.`;
  }
  updateDeleteUi();
  elements.deleteOverlay.classList.add("active");
}

function closeDeleteOverlay() {
  if (!elements.deleteOverlay) return;
  elements.deleteOverlay.classList.remove("active");
  state.pendingDeleteRowId = null;
  setDeleteOverlayBusy(false);
}

function setColumnDeleteOverlayBusy(isBusy) {
  if (elements.columnDeleteSoft) elements.columnDeleteSoft.disabled = isBusy;
  if (elements.columnDeleteHard) elements.columnDeleteHard.disabled = isBusy;
  if (elements.columnDeleteCancel)
    elements.columnDeleteCancel.disabled = isBusy;
}

function openColumnDeleteOverlay(columnName) {
  if (!elements.columnDeleteOverlay) return;
  state.pendingDeleteColumn = columnName;
  if (elements.columnDeleteMessage) {
    elements.columnDeleteMessage.textContent = state.allowDeleteData
      ? `"${columnName}" will be removed from the view. Choose whether to delete it from the file.`
      : `"${columnName}" will be removed from the view. Delete from file is disabled.`;
  }
  updateDeleteUi();
  elements.columnDeleteOverlay.classList.add("active");
}

function closeColumnDeleteOverlay() {
  if (!elements.columnDeleteOverlay) return;
  elements.columnDeleteOverlay.classList.remove("active");
  state.pendingDeleteColumn = null;
  setColumnDeleteOverlayBusy(false);
}

async function performColumnDelete(persist) {
  if (!state.file) return;
  if (persist && !state.allowDeleteData) {
    closeColumnDeleteOverlay();
    return;
  }
  const columnName = state.pendingDeleteColumn;
  if (!columnName) return;
  setColumnDeleteOverlayBusy(true);
  try {
    if (persist) {
      await fetchJSON("/api/delete_column", {
        method: "POST",
        body: JSON.stringify({
          file: state.file,
          column: columnName,
          persist: true,
        }),
      });
      state.hiddenTableColumns.delete(columnName);
      await loadSchema();
      await loadCurrentPage();
    } else {
      state.hiddenTableColumns.add(columnName);
      renderTable();
      if (state.selectedRowIndex !== null) {
        renderRowInspector(state.selectedRowIndex);
      }
    }
  } catch (err) {
    console.error(err);
  } finally {
    closeColumnDeleteOverlay();
  }
}

function requestColumnDelete(columnName) {
  if (!columnName) return;
  openColumnDeleteOverlay(columnName);
}

function setDragActive(active) {
  if (!elements.sidebar) return;
  elements.sidebar.classList.toggle("drag-over", active);
}

async function uploadFiles(files) {
  if (!files.length) return;
  const invalid = files.filter((file) => {
    const ext = file.name.includes(".")
      ? `.${file.name.split(".").pop()}`.toLowerCase()
      : "";
    return !UPLOAD_EXTENSIONS.has(ext);
  });
  if (invalid.length) {
    showError("[Error] Unsupported file extension");
  }
  const valid = files.filter((file) => {
    const ext = file.name.includes(".")
      ? `.${file.name.split(".").pop()}`.toLowerCase()
      : "";
    return UPLOAD_EXTENSIONS.has(ext);
  });
  if (!valid.length) {
    return;
  }
  const formData = new FormData();
  valid.forEach((file) => {
    formData.append("files", file, file.name);
  });
  try {
    await fetchJSON("/api/upload", {
      method: "POST",
      body: formData,
    });
    await loadFiles();
  } catch (err) {
    showError(extractErrorMessage(err));
    console.error(err);
  }
}

async function performDelete(persist) {
  if (!state.file) return;
  if (persist && !state.allowDeleteData) {
    closeDeleteOverlay();
    return;
  }
  const rowId = state.pendingDeleteRowId;
  if (!Number.isFinite(rowId)) return;
  setDeleteOverlayBusy(true);
  try {
    await fetchJSON("/api/delete_row", {
      method: "POST",
      body: JSON.stringify({ file: state.file, row_id: rowId, persist }),
    });
    state.expandedRowIndex = null;
    state.selectedRowIndex = null;
    state.selectedRowId = null;
    renderRowInspector(null);
    await loadCurrentPage();
    if (state.rowCount !== null) {
      state.rowCount = Math.max(0, state.rowCount - 1);
      elements.rowCount.textContent = `All Data Num: ${state.rowCount.toLocaleString()}`;
    }
  } catch (err) {
    console.error(err);
  } finally {
    closeDeleteOverlay();
    updateRowActions();
  }
}

function requestDeleteRow() {
  if (!state.file || !elements.deleteRow) return;
  if (!Number.isFinite(state.selectedRowId)) return;
  openDeleteOverlay(state.selectedRowId);
}

async function copyJsonOverlay() {
  if (!elements.jsonBody) return;
  const text = elements.jsonBody.textContent || "";
  if (!text) return;
  try {
    await navigator.clipboard.writeText(text);
    if (elements.copyJson) {
      elements.copyJson.textContent = "Copied";
      setTimeout(() => {
        elements.copyJson.textContent = "Copy";
      }, 1200);
    }
  } catch (err) {
    console.error(err);
  }
}

function updateOverlayImage() {
  if (!elements.overlayImage) return;
  const total = state.overlayImages.length;
  const currentIndex = Math.max(
    0,
    Math.min(state.overlayImageIndex, total - 1),
  );
  state.overlayImageIndex = currentIndex;
  const candidate = total ? state.overlayImages[currentIndex] : null;
  const url = candidate ? candidate.src : "";
  const columnName =
    candidate?.columnName ||
    (Number.isFinite(state.overlayColumnIndex)
      ? columnNameAt(state.overlayColumnIndex)
      : "");
  if (elements.overlayImageLabel) {
    elements.overlayImageLabel.textContent = columnName || "";
    elements.overlayImageLabel.hidden = !columnName;
  }
  elements.overlayImage.dataset.fallbackUsed = "0";
  elements.overlayImage.onerror = () => {
    if (
      candidate &&
      candidate.fallbackSrc &&
      elements.overlayImage.dataset.fallbackUsed !== "1"
    ) {
      elements.overlayImage.dataset.fallbackUsed = "1";
      elements.overlayImage.src = candidate.fallbackSrc;
      return;
    }
    elements.overlayImage.removeAttribute("src");
  };
  if (url) {
    elements.overlayImage.src = url;
  } else {
    elements.overlayImage.removeAttribute("src");
  }

  const rowLabel =
    state.overlayRowIndex !== null
      ? previewRowNumber(state.overlayRowIndex)
      : "-";
  const indexLabel = total > 1 ? ` | ${currentIndex + 1}/${total}` : "";
  elements.overlayTitle.textContent = `Row ${rowLabel}${indexLabel}`;

  if (elements.overlayIndex) {
    elements.overlayIndex.textContent =
      total > 1 ? `${currentIndex + 1} / ${total}` : "";
  }
  if (elements.overlayNav) {
    elements.overlayNav.classList.toggle("active", total > 1);
  }
}

function openImageOverlay(rowIndex, imageUrl, imageColumnIndex) {
  if (!elements.imageOverlay) return;
  closeJsonOverlay();
  const row = state.rows[rowIndex] || [];
  const hasImageColumn = Number.isFinite(imageColumnIndex);
  const cellValue = hasImageColumn ? row[imageColumnIndex] : null;
  const normalizedImage = imageUrl ? normalizeImageUrl(imageUrl) : "";
  const rowImages = collectRowImageCandidates(row);
  const cellImages = extractImageCandidates(cellValue);
  const imageList = rowImages.length ? rowImages : cellImages;
  let imageIndex = imageList.findIndex((item) =>
    imageCandidateMatchesUrl(item, normalizedImage),
  );
  if (imageIndex < 0) imageIndex = 0;

  state.overlayRowIndex = rowIndex;
  state.overlayColumnIndex = hasImageColumn ? imageColumnIndex : null;
  state.overlayImages = imageList.length
    ? imageList
    : normalizedImage
      ? [
          {
            ...imageCandidate(normalizedImage, normalizedImage),
            columnIndex: hasImageColumn ? imageColumnIndex : null,
            columnName: hasImageColumn ? columnNameAt(imageColumnIndex) : "",
          },
        ]
      : [];
  state.overlayImageIndex = imageIndex;
  state.overlayImageUrl = normalizedImage;
  state.overlayRawFields = new Set();
  renderOverlayFields();
  updateOverlayImage();
  elements.imageOverlay.classList.add("active");
}

function closeImageOverlay() {
  if (!elements.imageOverlay) return;
  state.overlayRowIndex = null;
  state.overlayColumnIndex = null;
  state.overlayImageUrl = "";
  state.overlayImages = [];
  state.overlayImageIndex = 0;
  state.overlayRawFields = new Set();
  elements.imageOverlay.classList.remove("active");
  elements.overlayImage.onerror = null;
  delete elements.overlayImage.dataset.fallbackUsed;
  elements.overlayImage.removeAttribute("src");
  elements.overlayFields.innerHTML = "";
  if (elements.overlayImageLabel) {
    elements.overlayImageLabel.textContent = "";
    elements.overlayImageLabel.hidden = true;
  }
  if (elements.overlayNav) {
    elements.overlayNav.classList.remove("active");
  }
  if (elements.overlayIndex) {
    elements.overlayIndex.textContent = "";
  }
}

function navigateOverlay(step) {
  const total = state.overlayImages.length;
  if (total <= 1) return;
  const nextIndex = (state.overlayImageIndex + step + total) % total;
  state.overlayImageIndex = nextIndex;
  updateOverlayImage();
}

function openJsonOverlay(rowIndex, colIndex) {
  if (!elements.jsonOverlay) return;
  closeImageOverlay();
  const value = state.rows[rowIndex]?.[colIndex];
  const columnName = state.columns[colIndex] || "Value";
  let jsonText = "";
  try {
    jsonText = JSON.stringify(value, null, 2);
  } catch (err) {
    jsonText = String(value);
  }
  const rowLabel = state.offset + rowIndex + 1;
  elements.jsonTitle.textContent = `${columnName} | Row ${rowLabel}`;
  elements.jsonBody.innerHTML = highlightJson(jsonText);
  elements.jsonOverlay.classList.add("active");
}

function closeJsonOverlay() {
  if (!elements.jsonOverlay) return;
  elements.jsonOverlay.classList.remove("active");
  elements.jsonTitle.textContent = "JSON View";
  elements.jsonBody.textContent = "";
}

function highlightJson(text) {
  const tokenRegex =
    /(\"(?:\\u[a-fA-F0-9]{4}|\\[^u]|[^\\\"])*\"|\\btrue\\b|\\bfalse\\b|\\bnull\\b|-?\\d+(?:\\.\\d+)?(?:[eE][+\\-]?\\d+)?)/g;
  let result = "";
  let lastIndex = 0;
  let match = tokenRegex.exec(text);
  while (match) {
    const token = match[0];
    const start = match.index;
    result += escapeHtml(text.slice(lastIndex, start));
    let className = "json-number";
    if (token.startsWith('"')) {
      let index = start + token.length;
      while (index < text.length && /\s/.test(text[index])) {
        index += 1;
      }
      className = text[index] === ":" ? "json-key" : "json-string";
    } else if (token === "true" || token === "false") {
      className = "json-boolean";
    } else if (token === "null") {
      className = "json-null";
    }
    result += `<span class=\"${className}\">${escapeHtml(token)}</span>`;
    lastIndex = start + token.length;
    match = tokenRegex.exec(text);
  }
  result += escapeHtml(text.slice(lastIndex));
  return result;
}

async function loadFiles() {
  const data = await fetchJSON("/api/files");
  state.files = data.files || [];
  renderFiles();
  if (!state.file && state.files.length) {
    selectFile(state.files[0].name);
  }
}

async function loadConfig() {
  try {
    const data = await fetchJSON("/api/config");
    state.allowDeleteData = data.allow_delete_data !== false;
  } catch (err) {
    state.allowDeleteData = true;
  }
  updateDeleteUi();
}

async function loadEmbedderModels() {
  try {
    const data = await fetchJSON("/api/embedder_models");
    state.embedderModels = data.models || [];
  } catch (err) {
    state.embedderModels = [];
    if (elements.atlasStatus) {
      elements.atlasStatus.textContent = extractErrorMessage(err);
    }
  }
  renderAtlasModelOptions();
}

function selectedLlmModel() {
  return elements.nlModel ? elements.nlModel.value.trim() : "";
}

function updateNlGenerateState() {
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

async function loadLlmModels() {
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

async function selectFile(fileName) {
  state.file = fileName;
  renderFiles();
  resetPreviewPaging();
  state.view = "data";
  state.searchQuery = "";
  state.querySql = "";
  state.rowCount = null;
  state.datasetWarning = "";
  state.expandedRowIndex = null;
  state.columnStats = {};
  state.selectedRowIndex = null;
  state.selectedRowId = null;
  resetRowInspectorRaw();
  state.pendingDeleteColumn = null;
  state.rowIds = [];
  state.hiddenTableColumns = new Set();
  state.hiddenColumns = new Set();
  state.countJobId = null;
  state.statsJobId = null;
  state.searchJobId = null;
  state.queryJobId = null;
  state.edaJobId = null;
  state.edaJobKind = "";
  state.atlasJobId = null;
  state.atlasJobKind = "";
  state.atlasCancelling = false;
  state.atlasUrl = "";
  closeImageOverlay();
  closeJsonOverlay();
  closeDeleteOverlay();
  closeColumnDeleteOverlay();
  elements.searchInput.value = "";
  elements.currentFile.textContent = fileName;
  elements.rowCount.textContent = "";
  if (elements.edaStatus) {
    elements.edaStatus.textContent = "";
  }
  if (elements.edaLink) {
    elements.edaLink.textContent = "";
    elements.edaLink.style.display = "none";
  }
  if (elements.atlasStatus) {
    elements.atlasStatus.textContent = "";
  }
  if (elements.atlasLink) {
    elements.atlasLink.textContent = "";
    elements.atlasLink.style.display = "none";
    elements.atlasLink.removeAttribute("href");
  }
  if (elements.atlasColumn) {
    elements.atlasColumn.value = "";
  }
  setAtlasButtonsRunning("");
  if (elements.nlStatus) {
    elements.nlStatus.textContent = "";
  }
  await loadSchema();
  await loadCurrentPage();
}

async function loadSchema() {
  if (!state.file) return;
  const data = await fetchJSON(
    `/api/schema?file=${encodeURIComponent(state.file)}`,
  );
  state.schema = data.columns || [];
  state.datasetWarning = data.warning || "";
  renderAtlasColumnOptions();
  renderDatasetInfo();
  renderColumnFocus(null);
  renderMeta();
}

async function loadColumnStats() {
  if (!state.file) return;
  const fileAtStart = state.file;
  const job = await startJob("stats", { file: state.file });
  state.statsJobId = job.id;
  const data = await waitForJob(job.id);
  if (state.file !== fileAtStart || state.statsJobId !== job.id) return;
  const stats = data.columns || [];
  state.columnStats = stats.reduce((acc, item) => {
    acc[item.name] = item;
    return acc;
  }, {});
  renderTable();
}

async function loadPreview() {
  if (!state.file) return;
  const token = currentPageToken();
  const params = new URLSearchParams({
    file: state.file,
    limit: String(state.limit),
  });
  if (token) {
    params.set("page_token", token);
  }
  const data = await fetchJSON(`/api/preview?${params.toString()}`);
  applyTableData(data);
}

async function loadSearch() {
  if (!state.file) return;
  const fileAtStart = state.file;
  const job = await startJob("search", {
    file: state.file,
    query: state.searchQuery,
    limit: state.limit,
  });
  state.searchJobId = job.id;
  elements.viewLabel.textContent = "Searching";
  const data = await waitForJob(job.id, {
    onUpdate: (nextJob) => {
      elements.datasetMeta.textContent = formatJobProgress(
        nextJob,
        "Searching",
      );
    },
  });
  if (state.file !== fileAtStart || state.searchJobId !== job.id) return;
  applyTableData(data);
}

async function loadQuery(sql) {
  if (!state.file) return;
  const fileAtStart = state.file;
  const job = await startJob("query", {
    file: state.file,
    sql,
    limit: state.limit,
    offset: state.offset,
  });
  state.queryJobId = job.id;
  elements.viewLabel.textContent = "Running query";
  try {
    const data = await waitForJob(job.id, {
      onUpdate: (nextJob) => {
        elements.datasetMeta.textContent = formatJobProgress(
          nextJob,
          "Running query",
        );
      },
    });
    if (state.file !== fileAtStart || state.queryJobId !== job.id) return;
    applyTableData(data);
  } finally {
    if (state.queryJobId === job.id) {
      state.queryJobId = null;
    }
  }
}

async function loadCurrentPage() {
  if (state.view === "query" && state.querySql) {
    await loadQuery(state.querySql);
    return;
  }
  if (state.searchQuery) {
    await loadSearch();
    return;
  }
  await loadPreview();
}

async function handleSearch() {
  const query = elements.searchInput.value.trim();
  if (!query) return;
  state.searchQuery = query;
  resetPreviewPaging();
  state.view = "search";
  await loadSearch();
}

function handleDatasetSearch() {
  if (!elements.datasetSearch) return;
  state.datasetQuery = elements.datasetSearch.value.trim();
  renderFiles();
}

function clearDatasetSearch() {
  state.datasetQuery = "";
  if (elements.datasetSearch) {
    elements.datasetSearch.value = "";
  }
  renderFiles();
}

async function clearSearch() {
  state.searchQuery = "";
  resetPreviewPaging();
  state.view = "data";
  await loadPreview();
}

async function runQuery() {
  const sql = elements.sqlInput.value.trim();
  if (!sql) return;
  state.view = "query";
  state.querySql = sql;
  state.offset = 0;
  state.pageIndex = 0;
  await loadQuery(sql);
}

function resetView() {
  state.view = "data";
  state.querySql = "";
  state.searchQuery = "";
  resetPreviewPaging();
  loadCurrentPage();
}

async function countRows() {
  if (!state.file) return;
  if (state.countJobId) {
    await cancelJob(state.countJobId);
    state.countJobId = null;
    elements.countRows.textContent = "Count Rows";
    elements.rowCount.textContent = "Count cancelled.";
    return;
  }
  const fileAtStart = state.file;
  elements.rowCount.textContent = "Counting...";
  elements.countRows.textContent = "Cancel Count";
  try {
    const job = await startJob("count", { file: state.file });
    state.countJobId = job.id;
    const data = await waitForJob(job.id, {
      onUpdate: (nextJob) => {
        elements.rowCount.textContent = formatJobProgress(
          nextJob,
          "Counting",
        );
      },
    });
    if (state.file !== fileAtStart || state.countJobId !== job.id) return;
    state.rowCount = data.count;
    elements.rowCount.textContent = `All Data Num: ${data.count.toLocaleString()}`;
  } catch (err) {
    elements.rowCount.textContent = extractErrorMessage(err);
  } finally {
    state.countJobId = null;
    elements.countRows.textContent = "Count Rows";
  }
}

function setEdaButtonsRunning(kind) {
  if (elements.runEda) {
    elements.runEda.disabled = Boolean(kind && kind !== "all");
    elements.runEda.textContent = kind === "all" ? "Cancel EDA" : "Run EDA";
  }
  if (elements.runEdaQuery) {
    elements.runEdaQuery.disabled = Boolean(kind && kind !== "query");
    elements.runEdaQuery.textContent =
      kind === "query" ? "Cancel Query EDA" : "Run EDA on Query Results";
  }
  if (elements.edaProfileMode) {
    elements.edaProfileMode.disabled = Boolean(kind);
  }
}

async function runEdaJob(kind) {
  if (!state.file || !elements.runEda) return;
  if (state.edaJobId) {
    await cancelJob(state.edaJobId);
    state.edaJobId = null;
    state.edaJobKind = "";
    setEdaButtonsRunning("");
    if (elements.edaStatus) {
      elements.edaStatus.textContent = "EDA cancelled.";
    }
    return;
  }

  const mode = elements.edaProfileMode?.value || "minimal";
  const payload = { file: state.file, mode };
  let jobKind = "eda";
  let sourceLabel = "EDA report";
  if (kind === "query") {
    const sql = elements.sqlInput.value.trim();
    if (!sql) {
      if (elements.edaStatus) {
        elements.edaStatus.textContent = "Enter a SQL query first.";
      }
      return;
    }
    payload.sql = sql;
    jobKind = "eda_query";
    sourceLabel = "query EDA report";
  }

  state.edaJobKind = kind;
  setEdaButtonsRunning(kind);
  if (elements.edaStatus) {
    elements.edaStatus.textContent =
      kind === "query" ? "Starting query EDA job..." : "Starting EDA job...";
  }
  if (elements.edaLink) {
    elements.edaLink.style.display = "none";
    elements.edaLink.textContent = "";
  }
  try {
    const fileAtStart = state.file;
    const job = await startJob(jobKind, payload);
    state.edaJobId = job.id;
    const data = await waitForJob(job.id, {
      intervalMs: 1000,
      onUpdate: (nextJob) => {
        if (elements.edaStatus) {
          elements.edaStatus.textContent = formatJobProgress(
            nextJob,
            kind === "query" ? "Generating query EDA report" : "Generating EDA report",
          );
        }
      },
    });
    if (state.file !== fileAtStart || state.edaJobId !== job.id) return;
    if (elements.edaStatus) {
      const sampleNote =
        data.sample === -1
          ? " Row limit: Unlimited."
          : data.sample
            ? ` Row limit: ${data.sample.toLocaleString()}.`
            : "";
      elements.edaStatus.textContent = data.cached
        ? `Cached ${sourceLabel} ready.${sampleNote}`
        : `${sourceLabel} generated.${sampleNote}`;
    }
    if (elements.edaLink && data.url) {
      elements.edaLink.href = data.url;
      elements.edaLink.textContent = "Open report";
      elements.edaLink.style.display = "inline-flex";
    }
  } catch (err) {
    if (elements.edaStatus) {
      elements.edaStatus.textContent = extractErrorMessage(err);
    }
    console.error(err);
  } finally {
    state.edaJobId = null;
    state.edaJobKind = "";
    setEdaButtonsRunning("");
  }
}

async function runEda() {
  await runEdaJob("all");
}

async function runEdaOnQueryResults() {
  await runEdaJob("query");
}

function setAtlasButtonsRunning(kind) {
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

async function runAtlas() {
  await runAtlasJob("all");
}

async function runAtlasOnQueryResults() {
  await runAtlasJob("query");
}

async function runNlQuery() {
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

function autoResizeNlInput() {
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

async function loadColumnSample(columnName) {
  if (!state.file || !elements.columnFocus) return;
  try {
    const data = await fetchJSON(
      `/api/column_sample?file=${encodeURIComponent(
        state.file,
      )}&column=${encodeURIComponent(columnName)}`,
    );
    renderColumnFocus(columnName, data.values || []);
  } catch (err) {
    renderColumnFocus(columnName, []);
  }
}

function attachEvents() {
  elements.refreshFiles.addEventListener("click", () => loadFiles());
  elements.fileList.addEventListener("click", (event) => {
    const target = event.target.closest(".file-item");
    if (!target) return;
    elements.fileList
      .querySelectorAll(".file-item.active")
      .forEach((item) => item.classList.remove("active"));
    target.classList.add("active");
    selectFile(target.dataset.file);
  });

  if (elements.datasetSearch) {
    elements.datasetSearch.addEventListener("input", handleDatasetSearch);
    elements.datasetSearch.addEventListener("keydown", (event) => {
      if (event.key === "Escape") clearDatasetSearch();
    });
  }
  elements.searchBtn.addEventListener("click", handleSearch);
  elements.searchInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") handleSearch();
  });
  elements.clearSearch.addEventListener("click", clearSearch);

  elements.pageSize.addEventListener("change", (event) => {
    state.limit = Number(event.target.value);
    resetPreviewPaging();
    loadCurrentPage();
  });

  elements.prevPage.addEventListener("click", () => {
    if (state.view === "query") {
      state.offset = Math.max(0, state.offset - state.limit);
      loadCurrentPage();
      return;
    }
    if (state.view === "search" || state.pageIndex <= 0) return;
    state.pageIndex -= 1;
    loadCurrentPage();
  });

  elements.nextPage.addEventListener("click", () => {
    if (state.view === "query") {
      state.offset += state.limit;
      loadCurrentPage();
      return;
    }
    if (state.view === "search" || !state.nextPageToken) return;
    state.pageTokens[state.pageIndex + 1] = state.nextPageToken;
    state.pageIndex += 1;
    loadCurrentPage();
  });

  elements.runQuery.addEventListener("click", runQuery);
  elements.resetView.addEventListener("click", resetView);

  elements.countRows.addEventListener("click", countRows);
  if (elements.runEda) {
    elements.runEda.addEventListener("click", runEda);
  }
  if (elements.runEdaQuery) {
    elements.runEdaQuery.addEventListener("click", runEdaOnQueryResults);
  }
  if (elements.atlasColumn) {
    elements.atlasColumn.addEventListener("change", () => {
      if (elements.atlasStatus) {
        elements.atlasStatus.textContent = "";
      }
      setAtlasButtonsRunning(state.atlasJobKind);
    });
  }
  if (elements.atlasModel) {
    elements.atlasModel.addEventListener("change", () => {
      if (elements.atlasStatus) {
        elements.atlasStatus.textContent = "";
      }
      if (elements.atlasPrompt) elements.atlasPrompt.value = "";
      renderAtlasBackendOptions();
    });
  }
  if (elements.atlasBackend) {
    elements.atlasBackend.addEventListener("change", () => {
      if (elements.atlasStatus) {
        elements.atlasStatus.textContent = "";
      }
      renderAtlasPromptControl();
      setAtlasButtonsRunning(state.atlasJobKind);
    });
  }
  if (elements.atlasProjection) {
    elements.atlasProjection.addEventListener("change", () => {
      if (elements.atlasStatus) {
        elements.atlasStatus.textContent = "";
      }
    });
  }
  if (elements.runAtlas) {
    elements.runAtlas.addEventListener("click", runAtlas);
  }
  if (elements.runAtlasQuery) {
    elements.runAtlasQuery.addEventListener("click", runAtlasOnQueryResults);
  }
  if (elements.atlasLink) {
    elements.atlasLink.addEventListener("click", (event) => {
      event.preventDefault();
      openAtlasUrl();
    });
  }
  if (elements.copyRow) {
    elements.copyRow.addEventListener("click", copyRowInspector);
  }
  if (elements.deleteRow) {
    elements.deleteRow.addEventListener("click", requestDeleteRow);
  }
  if (elements.deleteOverlay) {
    elements.deleteOverlay.addEventListener("click", (event) => {
      if (event.target === elements.deleteOverlay) {
        closeDeleteOverlay();
      }
    });
  }
  if (elements.deleteCancel) {
    elements.deleteCancel.addEventListener("click", closeDeleteOverlay);
  }
  if (elements.deleteSoft) {
    elements.deleteSoft.addEventListener("click", () => performDelete(false));
  }
  if (elements.deleteHard) {
    elements.deleteHard.addEventListener("click", () => performDelete(true));
  }
  if (elements.errorOverlay) {
    elements.errorOverlay.addEventListener("click", (event) => {
      if (event.target === elements.errorOverlay) {
        closeError();
      }
    });
  }
  if (elements.errorOk) {
    elements.errorOk.addEventListener("click", closeError);
  }
  if (elements.columnDeleteOverlay) {
    elements.columnDeleteOverlay.addEventListener("click", (event) => {
      if (event.target === elements.columnDeleteOverlay) {
        closeColumnDeleteOverlay();
      }
    });
  }
  if (elements.columnDeleteCancel) {
    elements.columnDeleteCancel.addEventListener(
      "click",
      closeColumnDeleteOverlay,
    );
  }
  if (elements.columnDeleteSoft) {
    elements.columnDeleteSoft.addEventListener("click", () =>
      performColumnDelete(false),
    );
  }
  if (elements.columnDeleteHard) {
    elements.columnDeleteHard.addEventListener("click", () =>
      performColumnDelete(true),
    );
  }
  if (elements.copyJson) {
    elements.copyJson.addEventListener("click", copyJsonOverlay);
  }
  if (elements.rowInspectorRaw) {
    elements.rowInspectorRaw.addEventListener("click", toggleRowInspectorRaw);
  }
  if (elements.nlGenerate) {
    elements.nlGenerate.addEventListener("click", runNlQuery);
  }
  if (elements.nlModel) {
    elements.nlModel.addEventListener("change", () => {
      if (elements.nlStatus) {
        elements.nlStatus.textContent = "";
      }
      updateNlGenerateState();
    });
  }
  if (elements.nlInput) {
    elements.nlInput.addEventListener("keydown", (event) => {
      if (
        event.key === "Enter" &&
        (event.metaKey || event.ctrlKey || event.shiftKey)
      ) {
        runNlQuery();
      }
    });
    elements.nlInput.addEventListener("input", autoResizeNlInput);
    autoResizeNlInput();
  }

  if (elements.tableHead) {
    elements.tableHead.addEventListener("click", (event) => {
      const deleteBtn = event.target.closest(".col-delete-btn");
      if (deleteBtn) {
        event.stopPropagation();
        requestColumnDelete(deleteBtn.dataset.col);
        return;
      }
      const target = event.target.closest("th");
      if (!target) return;
      const col = target.dataset.col;
      if (!col) return;
      if (state.hiddenColumns.has(col)) {
        state.hiddenColumns.delete(col);
      } else {
        state.hiddenColumns.add(col);
      }
      renderTable();
    });
  }

  if (elements.sidebar) {
    elements.sidebar.addEventListener("dragenter", (event) => {
      event.preventDefault();
      setDragActive(true);
    });
    elements.sidebar.addEventListener("dragover", (event) => {
      event.preventDefault();
      if (event.dataTransfer) {
        event.dataTransfer.dropEffect = "copy";
      }
      setDragActive(true);
    });
    elements.sidebar.addEventListener("dragleave", (event) => {
      if (!elements.sidebar.contains(event.relatedTarget)) {
        setDragActive(false);
      }
    });
    elements.sidebar.addEventListener("drop", (event) => {
      event.preventDefault();
      setDragActive(false);
      const files = Array.from(event.dataTransfer?.files || []);
      uploadFiles(files);
    });
  }

  elements.tableBody.addEventListener("click", (event) => {
    const jsonBtn = event.target.closest(".json-detail-btn");
    if (jsonBtn) {
      const rowIndex = Number(jsonBtn.dataset.row);
      const colIndex = Number(jsonBtn.dataset.col);
      if (Number.isFinite(rowIndex) && Number.isFinite(colIndex)) {
        openJsonOverlay(rowIndex, colIndex);
      }
      return;
    }

    const image = event.target.closest("img");
    if (image) {
      const row = image.closest("tr");
      const cell = image.closest("td");
      if (!row) return;
      let rowIndex = Number(row.dataset.index);
      if (!Number.isFinite(rowIndex)) {
        const previous = row.previousElementSibling;
        rowIndex = previous ? Number(previous.dataset.index) : NaN;
      }
      if (!Number.isFinite(rowIndex)) return;
      const cellIndex = cell ? Number(cell.dataset.colIndex) : null;
      const value =
        Number.isFinite(cellIndex) && state.rows[rowIndex]
          ? state.rows[rowIndex][cellIndex]
          : null;
      const imageUrl =
        image.getAttribute("src") ||
        image.getAttribute("data-image-src") ||
        (typeof value === "string"
          ? normalizeImageUrl(value)
          : image.getAttribute("src"));
      openImageOverlay(rowIndex, imageUrl, cellIndex);
      return;
    }

    const row = event.target.closest("tr");
    if (!row) return;
    if (row.classList.contains("expanded-row")) return;
    const rowIndex = Number(row.dataset.index);
    if (!Number.isFinite(rowIndex)) return;
    if (state.expandedRowIndex === rowIndex) {
      state.expandedRowIndex = null;
    } else {
      state.expandedRowIndex = rowIndex;
    }
    renderTable();
    renderRowInspector(rowIndex);
  });

  if (elements.imageOverlay) {
    elements.overlayClose.addEventListener("click", () => closeImageOverlay());
    elements.imageOverlay.addEventListener("click", (event) => {
      if (event.target === elements.imageOverlay) {
        closeImageOverlay();
      }
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") closeImageOverlay();
      if (event.key === "ArrowLeft") navigateOverlay(-1);
      if (event.key === "ArrowRight") navigateOverlay(1);
    });
    elements.overlayPrev.addEventListener("click", () => navigateOverlay(-1));
    elements.overlayNext.addEventListener("click", () => navigateOverlay(1));
    if (elements.overlayFields) {
      elements.overlayFields.addEventListener("click", async (event) => {
        const rawBtn = event.target.closest(".overlay-raw-field");
        if (rawBtn) {
          toggleOverlayFieldRaw(rawBtn.dataset.rawField);
          return;
        }
        const btn = event.target.closest(".overlay-copy");
        if (!btn) return;
        const encoded = btn.dataset.copy || "";
        if (!encoded) return;
        try {
          await navigator.clipboard.writeText(decodeURIComponent(encoded));
          btn.textContent = "Copied";
          setTimeout(() => {
            btn.textContent = "Copy";
          }, 1200);
        } catch (err) {
          console.error(err);
        }
      });
    }
  }

  if (elements.jsonOverlay) {
    elements.jsonClose.addEventListener("click", () => closeJsonOverlay());
    elements.jsonOverlay.addEventListener("click", (event) => {
      if (event.target === elements.jsonOverlay) {
        closeJsonOverlay();
      }
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") closeJsonOverlay();
    });
  }
}

function addInfoStyles() {
  const style = document.createElement("style");
  style.textContent = `
    .info-grid {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 6px 12px;
      font-size: 12px;
    }
    .info-row {
      display: contents;
    }
    .chip-grid {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }
    .chip {
      padding: 4px 8px;
      border-radius: 999px;
      background: rgba(42, 157, 143, 0.15);
      font-size: 11px;
    }
  `;
  document.head.appendChild(style);
}

addInfoStyles();
attachEvents();
updateRowActions();
loadConfig()
  .then(() => Promise.all([loadEmbedderModels(), loadLlmModels()]))
  .then(() => loadFiles())
  .catch((err) => {
    console.error(err);
    Promise.all([loadEmbedderModels(), loadLlmModels()])
      .catch((error) => {
        console.error(error);
      })
      .finally(() => {
        loadFiles().catch((error) => {
          console.error(error);
        });
      });
  });
