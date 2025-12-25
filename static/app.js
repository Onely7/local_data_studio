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
  expandedRowIndex: null,
  overlayRowIndex: null,
  overlayColumnIndex: null,
  overlayImageUrl: "",
  overlayImages: [],
  overlayImageIndex: 0,
  columnStats: {},
  selectedRowIndex: null,
  selectedRowId: null,
  pendingDeleteRowId: null,
  pendingDeleteColumn: null,
  allowDeleteData: true,
  datasetQuery: "",
  hiddenTableColumns: new Set(),
  rowIds: [],
  hiddenColumns: new Set(),
};

const UPLOAD_EXTENSIONS = new Set([".jsonl", ".parquet", ".csv", ".tsv"]);

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
  runEda: document.getElementById("run-eda"),
  edaStatus: document.getElementById("eda-status"),
  edaLink: document.getElementById("eda-link"),
  edaSample: document.getElementById("eda-sample"),
  nlInput: document.getElementById("nl-input"),
  nlGenerate: document.getElementById("nl-generate"),
  nlStatus: document.getElementById("nl-status"),
  imageOverlay: document.getElementById("image-overlay"),
  overlayClose: document.getElementById("overlay-close"),
  overlayImage: document.getElementById("overlay-image"),
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

function isImageUrl(text) {
  if (!text) return false;
  if (text.startsWith("data:image")) return true;
  return /\.(png|jpg|jpeg|gif|webp|svg)(\?.*)?$/i.test(text);
}

function normalizeImageUrl(text) {
  if (!text) return "";
  if (text.startsWith("data:image")) return text;
  if (/^https?:\/\//i.test(text)) return text;
  if (text.startsWith("/data/")) return text;
  if (text.startsWith("/")) return text;
  const cleaned = text.replace(/^\.\//, "");
  return `/data/${cleaned}`;
}

function extractImageUrls(value) {
  if (typeof value === "string" && isImageUrl(value)) {
    return [normalizeImageUrl(value)];
  }
  if (Array.isArray(value)) {
    return value
      .filter((item) => typeof item === "string" && isImageUrl(item))
      .map((item) => normalizeImageUrl(item));
  }
  return [];
}

function renderImageGrid(urls, options = {}) {
  const max = Number.isFinite(options.max) ? options.max : 4;
  const className = options.className || "image-grid";
  const shown = urls.slice(0, max);
  const extra = urls.length - shown.length;
  const items = shown
    .map(
      (url, idx) =>
        `<img src="${escapeHtml(url)}" alt="image ${
          idx + 1
        }" data-image-src="${escapeHtml(url)}" />`
    )
    .join("");
  const badge = extra > 0 ? `<div class="image-count">+${extra}</div>` : "";
  return `<div class="${className}">${items}${badge}</div>`;
}

function formatCell(value) {
  if (value === null || value === undefined) {
    return '<span class="muted">null</span>';
  }
  const imageUrls = extractImageUrls(value);
  if (imageUrls.length) {
    return renderImageGrid(imageUrls, { className: "image-grid", max: 4 });
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
  const imageUrls = extractImageUrls(value);
  if (imageUrls.length) {
    return renderImageGrid(imageUrls, {
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

function encodeCopyValue(value) {
  let text;
  try {
    text = JSON.stringify(value, null, 2);
  } catch (err) {
    text = String(value);
  }
  return encodeURIComponent(text);
}

function collectRowImageUrls(row) {
  const urls = [];
  row.forEach((value) => {
    const images = extractImageUrls(value);
    images.forEach((img) => {
      if (!urls.includes(img)) {
        urls.push(img);
      }
    });
  });
  return urls;
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
    elements.datasetEmpty.style.display = visibleFiles.length ? "none" : "block";
  }

  visibleFiles.forEach((file) => {
    const div = document.createElement("div");
    div.className = "file-item" + (state.file === file.name ? " active" : "");
    div.dataset.file = file.name;
    div.innerHTML = `
      <div>${escapeHtml(file.name)}</div>
      <div class="meta">${(file.size / 1024).toFixed(1)} KB</div>
    `;
    elements.fileList.appendChild(div);
  });
}

function renderTable() {
  elements.tableHead.innerHTML = "";
  elements.tableBody.innerHTML = "";

  if (!state.columns.length) {
    elements.tableHead.innerHTML = "<tr><th>No data</th></tr>";
    return;
  }

  const visibleColumns = state.columns.filter(
    (col) => !state.hiddenTableColumns.has(col)
  );
  if (!visibleColumns.length) {
    elements.tableHead.innerHTML = "<tr><th>No visible columns</th></tr>";
    return;
  }

  const headRow = document.createElement("tr");
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
      td.colSpan = visibleColumns.length;
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
  const page = Math.floor(state.offset / state.limit) + 1;
  elements.pageInfo.textContent = `Page ${page}`;
}

function renderMeta() {
  const columnCount = state.schema.length;
  const mode =
    state.view === "query" ? "Query" : state.searchQuery ? "Search" : "Preview";
  elements.viewLabel.textContent = mode;
  elements.datasetMeta.textContent = state.file
    ? `${columnCount} columns | offset ${state.offset}`
    : "";
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
          col.name
        )}</span><span>${escapeHtml(col.type)}</span></div>`
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
    updateRowActions();
    return;
  }
  state.selectedRowIndex = rowIndex;
  state.selectedRowId =
    Array.isArray(state.rowIds) && Number.isFinite(state.rowIds[rowIndex])
      ? state.rowIds[rowIndex]
      : null;
  const row = state.rows[rowIndex];
  const rowObj = {};
  state.columns.forEach((col, idx) => {
    if (state.hiddenTableColumns.has(col)) {
      return;
    }
    rowObj[col] = row[idx];
  });
  elements.rowInspector.textContent = Object.keys(rowObj).length
    ? JSON.stringify(rowObj, null, 2)
    : "No visible columns";
  updateRowActions();
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

function applyTableData(data) {
  state.columns = data.columns || [];
  state.rows = data.rows || [];
  state.rowIds = data.row_ids || [];
  state.expandedRowIndex = null;
  state.selectedRowIndex = null;
  state.selectedRowId = null;
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
      loadColumnStats().catch((err) => console.error(err));
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
  if (!elements.imageOverlay) return;
  const total = state.overlayImages.length;
  const currentIndex = Math.max(
    0,
    Math.min(state.overlayImageIndex, total - 1)
  );
  state.overlayImageIndex = currentIndex;
  const url = total ? state.overlayImages[currentIndex] : "";
  elements.overlayImage.src = url;

  const rowLabel =
    state.overlayRowIndex !== null
      ? state.offset + state.overlayRowIndex + 1
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
  const cellValue = imageColumnIndex !== null ? row[imageColumnIndex] : null;
  const cellImages = extractImageUrls(cellValue);
  const rowImages = collectRowImageUrls(row);
  const imageList = cellImages.length ? cellImages : rowImages;
  const normalizedImage = imageUrl ? normalizeImageUrl(imageUrl) : "";
  let imageIndex = imageList.findIndex((item) => item === normalizedImage);
  if (imageIndex < 0) imageIndex = 0;

  state.overlayRowIndex = rowIndex;
  state.overlayColumnIndex = imageColumnIndex;
  state.overlayImages = imageList.length
    ? imageList
    : normalizedImage
    ? [normalizedImage]
    : [];
  state.overlayImageIndex = imageIndex;
  state.overlayImageUrl = normalizedImage;

  const fields = state.columns
    .map((col, idx) => {
      const value = state.rows[rowIndex][idx];
      const copyValue = encodeCopyValue(value);
      return `
        <div class="overlay-field">
          <div class="overlay-field-header">
            <div class="overlay-field-label">${escapeHtml(col)}</div>
            <button class="overlay-copy" data-copy="${copyValue}" type="button">Copy</button>
          </div>
          <div class="overlay-field-value">${formatOverlayValue(value)}</div>
        </div>
      `;
    })
    .join("");

  elements.overlayFields.innerHTML =
    fields || '<div class="muted">No extra fields</div>';
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
  elements.imageOverlay.classList.remove("active");
  elements.overlayImage.removeAttribute("src");
  elements.overlayFields.innerHTML = "";
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
    if (token.startsWith("\"")) {
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

async function selectFile(fileName) {
  state.file = fileName;
  renderFiles();
  state.offset = 0;
  state.view = "data";
  state.searchQuery = "";
  state.querySql = "";
  state.rowCount = null;
  state.expandedRowIndex = null;
  state.columnStats = {};
  state.selectedRowIndex = null;
  state.selectedRowId = null;
  state.pendingDeleteColumn = null;
  state.rowIds = [];
  state.hiddenTableColumns = new Set();
  state.hiddenColumns = new Set();
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
  if (elements.nlStatus) {
    elements.nlStatus.textContent = "";
  }
  await loadSchema();
  loadColumnStats().catch((err) => console.error(err));
  await loadCurrentPage();
}

async function loadSchema() {
  if (!state.file) return;
  const data = await fetchJSON(
    `/api/schema?file=${encodeURIComponent(state.file)}`
  );
  state.schema = data.columns || [];
  renderDatasetInfo();
  renderColumnFocus(null);
}

async function loadColumnStats() {
  if (!state.file) return;
  const data = await fetchJSON(
    `/api/column_stats?file=${encodeURIComponent(state.file)}`
  );
  const stats = data.columns || [];
  state.columnStats = stats.reduce((acc, item) => {
    acc[item.name] = item;
    return acc;
  }, {});
  renderTable();
}

async function loadPreview() {
  if (!state.file) return;
  const data = await fetchJSON(
    `/api/preview?file=${encodeURIComponent(state.file)}&limit=${
      state.limit
    }&offset=${state.offset}`
  );
  applyTableData(data);
}

async function loadSearch() {
  if (!state.file) return;
  const data = await fetchJSON(
    `/api/search?file=${encodeURIComponent(
      state.file
    )}&query=${encodeURIComponent(state.searchQuery)}&limit=${
      state.limit
    }&offset=${state.offset}`
  );
  applyTableData(data);
}

async function loadQuery(sql) {
  if (!state.file) return;
  const data = await fetchJSON("/api/query", {
    method: "POST",
    body: JSON.stringify({
      file: state.file,
      sql,
      limit: state.limit,
      offset: state.offset,
    }),
  });
  applyTableData(data);
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
  state.offset = 0;
  state.view = "data";
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
  state.offset = 0;
  state.view = "data";
  await loadPreview();
}

async function runQuery() {
  const sql = elements.sqlInput.value.trim();
  if (!sql) return;
  state.view = "query";
  state.querySql = sql;
  state.offset = 0;
  await loadQuery(sql);
}

function resetView() {
  state.view = "data";
  state.querySql = "";
  state.offset = 0;
  loadCurrentPage();
}

async function countRows() {
  if (!state.file) return;
  elements.rowCount.textContent = "Counting...";
  const data = await fetchJSON(
    `/api/count?file=${encodeURIComponent(state.file)}`
  );
  state.rowCount = data.count;
  elements.rowCount.textContent = `All Data Num: ${data.count.toLocaleString()}`;
}

async function runEda() {
  if (!state.file || !elements.runEda) return;
  const sampleValue = elements.edaSample
    ? Number(elements.edaSample.value)
    : null;
  const sample =
    Number.isFinite(sampleValue) && sampleValue > 0 ? sampleValue : undefined;
  elements.runEda.disabled = true;
  if (elements.edaStatus) {
    elements.edaStatus.textContent = "Generating EDA report...";
  }
  if (elements.edaLink) {
    elements.edaLink.style.display = "none";
    elements.edaLink.textContent = "";
  }
  try {
    const data = await fetchJSON("/api/eda", {
      method: "POST",
      body: JSON.stringify({ file: state.file, sample }),
    });
    if (elements.edaStatus) {
      const sampleNote = data.sample
        ? ` EDA target data maximum number of records: ${data.sample}.`
        : "";
      elements.edaStatus.textContent = data.cached
        ? `Cached report ready.${sampleNote}`
        : `Report generated.${sampleNote}`;
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
    elements.runEda.disabled = false;
  }
}

async function runNlQuery() {
  if (!state.file || !elements.nlInput || !elements.nlGenerate) return;
  const prompt = elements.nlInput.value.trim();
  if (!prompt) {
    if (elements.nlStatus) {
      elements.nlStatus.textContent = "Please enter a request.";
    }
    return;
  }
  const sampleRow = buildSampleRow();
  elements.nlGenerate.disabled = true;
  if (elements.nlStatus) {
    elements.nlStatus.textContent = "Generating SQL...";
  }
  try {
    const data = await fetchJSON("/api/nl_query", {
      method: "POST",
      body: JSON.stringify({ file: state.file, prompt, sample: sampleRow }),
    });
    if (data.sql) {
      elements.sqlInput.value = data.sql;
    }
    if (elements.nlStatus) {
      elements.nlStatus.textContent = "SQL ready.";
    }
  } catch (err) {
    if (elements.nlStatus) {
      elements.nlStatus.textContent = extractErrorMessage(err);
    }
    console.error(err);
  } finally {
    elements.nlGenerate.disabled = false;
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
        state.file
      )}&column=${encodeURIComponent(columnName)}`
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
    state.offset = 0;
    loadCurrentPage();
  });

  elements.prevPage.addEventListener("click", () => {
    state.offset = Math.max(0, state.offset - state.limit);
    loadCurrentPage();
  });

  elements.nextPage.addEventListener("click", () => {
    state.offset += state.limit;
    loadCurrentPage();
  });

  elements.runQuery.addEventListener("click", runQuery);
  elements.resetView.addEventListener("click", resetView);

  elements.countRows.addEventListener("click", countRows);
  if (elements.runEda) {
    elements.runEda.addEventListener("click", runEda);
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
      closeColumnDeleteOverlay
    );
  }
  if (elements.columnDeleteSoft) {
    elements.columnDeleteSoft.addEventListener("click", () =>
      performColumnDelete(false)
    );
  }
  if (elements.columnDeleteHard) {
    elements.columnDeleteHard.addEventListener("click", () =>
      performColumnDelete(true)
    );
  }
  if (elements.copyJson) {
    elements.copyJson.addEventListener("click", copyJsonOverlay);
  }
  if (elements.nlGenerate) {
    elements.nlGenerate.addEventListener("click", runNlQuery);
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
  .then(() => loadFiles())
  .catch((err) => {
    console.error(err);
    loadFiles().catch((error) => {
      console.error(error);
    });
  });
