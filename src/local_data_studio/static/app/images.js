import { escapeHtml, shorten } from "./formatting.js";
import { MAX_IMAGE_CANDIDATES } from "./state.js";

export function isImageUrl(text) {
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

export function normalizeImageUrl(text) {
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

export function extractImageCandidates(value) {
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

export function extractImageUrls(value) {
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
  const imageCandidates = candidates
    .map((item) => (typeof item === "string" ? imageCandidate(item, item) : item))
    .filter(Boolean);
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

export function formatCell(value) {
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
      return `<span class="cell">${escapeHtml(shorten(JSON.stringify(value)))}</span>`;
    } catch (err) {
      return `<span class="cell">${escapeHtml(String(value))}</span>`;
    }
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return `<span class="cell">${escapeHtml(value)}</span>`;
  }
  try {
    return `<span class="cell">${escapeHtml(shorten(JSON.stringify(value)))}</span>`;
  } catch (err) {
    return `<span class="cell">${escapeHtml(String(value))}</span>`;
  }
}

export function formatExpandedCell(value) {
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
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return `<pre class="expanded-text">${escapeHtml(value)}</pre>`;
  }
  try {
    return `<pre class="expanded-text">${escapeHtml(JSON.stringify(value, null, 2))}</pre>`;
  } catch (err) {
    return `<pre class="expanded-text">${escapeHtml(String(value))}</pre>`;
  }
}
