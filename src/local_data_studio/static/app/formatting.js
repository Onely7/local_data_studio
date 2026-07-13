import { ROW_INSPECTOR_VALUE_MAX } from "./state.js";

export function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

export function shorten(text, max = 240) {
  if (text.length <= max) return text;
  return text.slice(0, max) + "...";
}

export function compactInspectorValue(value) {
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

export function isInspectorValueTruncated(value) {
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

export function formatFileSize(bytes) {
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
  const formatted = Number((Math.floor(size * scale) / scale).toPrecision(3)).toString();
  return `${formatted} ${units[unitIndex]}`;
}
