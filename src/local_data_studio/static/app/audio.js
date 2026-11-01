// Audio values in preview cells: detection, compact cell chips, expanded
// players, and on-demand loading of embedded audio from /api/raw_media.
//
// Preview pages carry payload-free markers such as
// "data:audio/wav;preview=1;size=1234;duration=2.500;base64," in place of
// embedded audio bytes. Playing a cell requests its bytes for the row, column,
// and pointer (the keys and list indexes from the cell value to the audio).
import { escapeHtml, formatFileSize } from "./formatting.js";
import { extractErrorMessage } from "./http.js";

const AUDIO_SUFFIX_RE = /\.(wav|wave|mp3|flac|ogg|oga|opus|m4a|aac|aif|aiff|weba)(?:[?#].*)?$/i;
const MAX_AUDIO_ITEMS = 4;
const MAX_AUDIO_DEPTH = 4;
const AUDIO_URL_CACHE_LIMIT = 24;
const SUFFIX_TYPES = {
  wav: "audio/wav",
  wave: "audio/wav",
  mp3: "audio/mpeg",
  flac: "audio/flac",
  ogg: "audio/ogg",
  oga: "audio/ogg",
  opus: "audio/ogg",
  m4a: "audio/mp4",
  aac: "audio/aac",
  aif: "audio/aiff",
  aiff: "audio/aiff",
  weba: "audio/webm",
};
const FORMAT_LABELS = {
  "audio/wav": "WAV",
  "audio/x-wav": "WAV",
  "audio/wave": "WAV",
  "audio/mpeg": "MP3",
  "audio/mp3": "MP3",
  "audio/flac": "FLAC",
  "audio/x-flac": "FLAC",
  "audio/ogg": "OGG",
  "audio/opus": "OPUS",
  "audio/mp4": "M4A",
  "audio/x-m4a": "M4A",
  "audio/aac": "AAC",
  "audio/aiff": "AIFF",
  "audio/x-aiff": "AIFF",
  "audio/webm": "WEBM",
};

let resolveCell = () => null;
let listenersInstalled = false;
const urlCache = new Map();
const players = new Map();
const playerErrors = new Map();
const playersLoading = new Set();
const knownDurations = new Map();
const inline = { audio: null, key: "", loading: false, error: "" };

export function isAudioPreviewMarker(text) {
  if (typeof text !== "string" || !text.startsWith("data:audio/")) return false;
  const comma = text.indexOf(",");
  return text
    .slice(0, comma >= 0 ? comma : text.length)
    .split(";")
    .includes("preview=1");
}

export function isAudioReference(text) {
  if (typeof text !== "string") return false;
  const value = text.trim();
  if (!value) return false;
  if (value.startsWith("data:audio/")) return true;
  return value.length <= 4096 && AUDIO_SUFFIX_RE.test(value);
}

function dataUrlInfo(text) {
  const comma = text.indexOf(",");
  const [mime, ...parameters] = text.slice(5, comma >= 0 ? comma : text.length).split(";");
  const info = { mime };
  parameters.forEach((parameter) => {
    const [name, value] = parameter.split("=");
    if (name === "size") info.size = Number(value);
    if (name === "duration") info.duration = Number(value);
  });
  if (!info.size && comma >= 0) {
    info.size = Math.floor(((text.length - comma - 1) * 3) / 4);
  }
  return info;
}

function baseName(text) {
  const clean = String(text || "").split(/[?#]/)[0];
  const name = clean.split(/[\\/]/).pop() || clean;
  try {
    return decodeURIComponent(name);
  } catch (err) {
    return name;
  }
}

function isBrowserUrl(text) {
  return (
    /^(?:https?:|blob:)/i.test(text) ||
    text.startsWith("/data/") ||
    text.startsWith("/cache/") ||
    text.startsWith("/api/")
  );
}

function referenceItem(text, pointer, name) {
  const value = text.trim();
  if (value.startsWith("data:audio/")) {
    const info = dataUrlInfo(value);
    return isAudioPreviewMarker(value)
      ? { pointer, kind: "embedded", name, ...info }
      : { pointer, kind: "url", src: value, name, ...info };
  }
  const match = value.match(AUDIO_SUFFIX_RE);
  if (!match) return null;
  const item = {
    pointer,
    name: name || baseName(value),
    mime: SUFFIX_TYPES[match[1].toLowerCase()],
  };
  // The server resolves local paths beside the dataset; browsers fetch URLs.
  return isBrowserUrl(value)
    ? { ...item, kind: "url", src: value }
    : { ...item, kind: "embedded" };
}

export function extractAudioItems(value, pointer = [], depth = 0) {
  if (depth > MAX_AUDIO_DEPTH || value === null || value === undefined) return [];
  if (typeof value === "string") {
    const item = isAudioReference(value) ? referenceItem(value, pointer, "") : null;
    return item ? [item] : [];
  }
  if (Array.isArray(value)) {
    const items = [];
    for (let index = 0; index < value.length && items.length <= MAX_AUDIO_ITEMS; index += 1) {
      items.push(...extractAudioItems(value[index], [...pointer, index], depth + 1));
    }
    return items;
  }
  if (typeof value === "object") {
    const path = typeof value.path === "string" ? value.path : "";
    const name = path ? baseName(path) : "";
    if (typeof value.bytes === "string" && value.bytes.startsWith("data:audio/")) {
      const item = referenceItem(value.bytes, pointer, name);
      return item ? [item] : [];
    }
    const hasBytes = value.bytes !== null && value.bytes !== undefined && value.bytes !== "";
    if (path && !hasBytes && isAudioReference(path)) {
      const item = referenceItem(path, pointer, name);
      return item ? [item] : [];
    }
  }
  return [];
}

export function hasAudio(value) {
  return extractAudioItems(value).length > 0;
}

export function formatDuration(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return "--:--";
  const total = Math.round(seconds);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const rest = String(total % 60).padStart(2, "0");
  return hours ? `${hours}:${String(minutes).padStart(2, "0")}:${rest}` : `${minutes}:${rest}`;
}

function formatLabel(item) {
  const format = FORMAT_LABELS[item.mime] || String(item.mime || "audio").replace(/^audio\//, "").toUpperCase();
  return Number.isFinite(item.size) && item.size > 0 ? `${format} · ${formatFileSize(item.size)}` : format;
}

function itemAttributes(item) {
  const duration = Number.isFinite(item.duration) ? item.duration : "";
  return `data-audio-pointer="${escapeHtml(JSON.stringify(item.pointer))}" data-audio-kind="${item.kind}" data-audio-src="${escapeHtml(item.src || "")}" data-duration="${duration}"`;
}

export function formatAudioCell(items) {
  const shown = items.slice(0, MAX_AUDIO_ITEMS);
  const chips = shown
    .map(
      (item) => `<span class="audio-chip" ${itemAttributes(item)} title="${escapeHtml(item.name || "Audio")}"><button class="audio-play-btn" type="button" aria-label="Play audio" title="Play audio"><img src="icons/play.svg" alt="" aria-hidden="true" /></button><span class="audio-chip-text"><span class="audio-time">${formatDuration(item.duration)}</span><span class="audio-format">${escapeHtml(formatLabel(item))}</span></span></span>`,
    )
    .join("");
  const extra = items.length - shown.length;
  return `<div class="audio-list">${chips}${extra > 0 ? `<span class="audio-count">+${extra}</span>` : ""}</div>`;
}

export function formatExpandedAudio(items) {
  const shown = items.slice(0, MAX_AUDIO_ITEMS);
  const players = shown
    .map((item, index) => {
      const details = [formatLabel(item)];
      if (Number.isFinite(item.duration)) details.push(formatDuration(item.duration));
      return `<div class="audio-player" ${itemAttributes(item)}><div class="audio-player-head"><span class="audio-name">${escapeHtml(item.name || `Audio ${index + 1}`)}</span><span class="audio-format">${escapeHtml(details.join(" · "))}</span></div><div class="audio-player-slot"></div><div class="audio-player-status" role="status"></div></div>`;
    })
    .join("");
  const extra = items.length - shown.length;
  return `<div class="audio-player-list">${players}${extra > 0 ? `<div class="audio-count">+${extra} more</div>` : ""}</div>`;
}

function textHash(text) {
  let hash = 5381;
  for (let index = 0; index < text.length; index += 1) {
    hash = (hash * 33 + text.charCodeAt(index)) >>> 0;
  }
  return hash.toString(36);
}

// Returns the row request, column name, and a stable key for one audio item.
// SQL result rows include a hash of the query, so the same offset in another
// query never reuses a clip or transcript.
export function audioCellTarget(rowIndex, columnIndex, pointer = []) {
  const cell = resolveCell(rowIndex, columnIndex);
  if (!cell) return null;
  const request = cell.request || null;
  const rowKey = Number.isFinite(request?.row_id)
    ? String(request.row_id)
    : request
      ? `q${request.offset}:${textHash(request.sql || "")}`
      : `i${rowIndex}`;
  const key = `${cell.file}|${rowKey}|${cell.column}|${JSON.stringify(pointer)}`;
  return { ...cell, request, rowIndex, columnIndex, pointer, key };
}

export function audioContext(element) {
  const row = element.closest("tr[data-index]");
  const field = element.closest("[data-col-index]");
  const rowIndex = Number(row?.dataset.index);
  const columnIndex = Number(field?.dataset.colIndex);
  if (!Number.isFinite(rowIndex) || !Number.isFinite(columnIndex)) return null;
  let pointer = [];
  try {
    pointer = JSON.parse(element.dataset.audioPointer || "[]");
  } catch (err) {
    pointer = [];
  }
  return audioCellTarget(rowIndex, columnIndex, pointer);
}

function urlInUse(url) {
  if (inline.audio?.src === url) return true;
  return [...players.values()].some((player) => player.src === url);
}

async function sourceUrl(element, context) {
  if (element.dataset.audioKind === "url") return element.dataset.audioSrc;
  if (!context?.request) throw new Error("Audio for this row cannot be loaded.");
  const cached = urlCache.get(context.key);
  if (cached) {
    urlCache.delete(context.key);
    urlCache.set(context.key, cached);
    return cached;
  }
  const response = await fetch("/api/raw_media", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...context.request, column: context.column, pointer: context.pointer }),
  });
  if (!response.ok) {
    throw new Error((await response.text()) || "Audio request failed");
  }
  const url = URL.createObjectURL(await response.blob());
  urlCache.set(context.key, url);
  for (const [key, oldUrl] of urlCache) {
    if (urlCache.size <= AUDIO_URL_CACHE_LIMIT) break;
    if (key === context.key || urlInUse(oldUrl)) continue;
    URL.revokeObjectURL(oldUrl);
    urlCache.delete(key);
  }
  return url;
}

function chipKey(chip) {
  if (!chip.dataset.audioKey) {
    chip.dataset.audioKey = audioContext(chip)?.key || chip.dataset.audioSrc || "";
  }
  return chip.dataset.audioKey;
}

function updateChips() {
  const audio = inline.audio;
  document.querySelectorAll(".audio-chip").forEach((chip) => {
    const active = Boolean(inline.key) && chipKey(chip) === inline.key;
    const playing = active && audio && !audio.paused && !audio.ended;
    const button = chip.querySelector(".audio-play-btn");
    const icon = button?.querySelector("img");
    chip.classList.toggle("is-active", active);
    chip.classList.toggle("is-playing", Boolean(playing));
    chip.classList.toggle("is-loading", active && inline.loading);
    chip.classList.toggle("has-error", active && Boolean(inline.error));
    if (button) {
      const label = playing ? "Pause audio" : "Play audio";
      button.setAttribute("aria-label", label);
      button.title = active && inline.error ? inline.error : label;
    }
    if (icon) icon.src = playing ? "icons/pause.svg" : "icons/play.svg";
    const stored = chip.dataset.duration === "" ? knownDurations.get(chipKey(chip)) : Number(chip.dataset.duration);
    const duration = active && Number.isFinite(audio?.duration) ? audio.duration : stored;
    const time = chip.querySelector(".audio-time");
    if (time) {
      time.textContent =
        active && audio && (audio.currentTime > 0 || playing)
          ? `${formatDuration(audio.currentTime)} / ${formatDuration(duration)}`
          : formatDuration(duration);
    }
    const progress = active && duration > 0 ? Math.min(1, (audio?.currentTime || 0) / duration) : 0;
    chip.style.setProperty("--audio-progress", String(progress));
  });
}

function inlineAudio() {
  if (!inline.audio) {
    inline.audio = new Audio();
    inline.audio.preload = "auto";
    ["play", "pause", "ended", "timeupdate", "loadedmetadata", "durationchange"].forEach((name) => {
      inline.audio.addEventListener(name, () => {
        if (inline.key && Number.isFinite(inline.audio.duration)) {
          knownDurations.set(inline.key, inline.audio.duration);
        }
        updateChips();
      });
    });
    inline.audio.addEventListener("error", () => {
      inline.error = "This browser cannot play the audio.";
      inline.loading = false;
      updateChips();
    });
  }
  return inline.audio;
}

function pauseAll(except = null) {
  if (inline.audio && inline.audio !== except) inline.audio.pause();
  players.forEach((player) => {
    if (player !== except) player.pause();
  });
}

async function playSafely(audio) {
  try {
    await audio.play();
  } catch (err) {
    if (err?.name !== "AbortError") throw err;
  }
}

export async function toggleAudioChip(button) {
  const chip = button.closest(".audio-chip");
  if (!chip) return;
  const key = chipKey(chip);
  if (!key) return;
  const audio = inlineAudio();
  if (inline.key === key && audio.src && !inline.error) {
    if (audio.paused || audio.ended) {
      pauseAll(audio);
      await playSafely(audio);
    } else {
      audio.pause();
    }
    return;
  }
  pauseAll();
  inline.key = key;
  inline.error = "";
  inline.loading = true;
  updateChips();
  try {
    const url = await sourceUrl(chip, audioContext(chip));
    if (inline.key !== key) return;
    audio.src = url;
    await playSafely(audio);
  } catch (err) {
    if (inline.key === key) inline.error = extractErrorMessage(err);
    console.warn(err);
  } finally {
    if (inline.key === key) {
      inline.loading = false;
      updateChips();
    }
  }
}

function playerContainers(key) {
  return [...document.querySelectorAll(".audio-player")].filter((container) => container.dataset.audioKey === key);
}

function showPlayerState(container, key) {
  const loading = playersLoading.has(key);
  const message = loading ? "Loading audio..." : playerErrors.get(key) || "";
  const status = container.querySelector(".audio-player-status");
  if (status) status.textContent = message;
  container.classList.toggle("is-loading", loading);
  container.classList.toggle("has-error", !loading && playerErrors.has(key));
}

async function loadPlayerSource(container, player, context) {
  const key = context.key;
  playersLoading.add(key);
  playerErrors.delete(key);
  showPlayerState(container, key);
  try {
    const url = await sourceUrl(container, context);
    if (players.get(key) === player) player.src = url;
  } catch (err) {
    playerErrors.set(key, extractErrorMessage(err));
    console.warn(err);
  } finally {
    playersLoading.delete(key);
    playerContainers(key).forEach((current) => showPlayerState(current, key));
  }
}

function mountPlayers(root) {
  const mounted = new Set();
  root.querySelectorAll(".audio-player").forEach((container) => {
    const context = audioContext(container);
    const key = context?.key || container.dataset.audioSrc || "";
    const slot = container.querySelector(".audio-player-slot");
    if (!key || !slot) return;
    container.dataset.audioKey = key;
    mounted.add(key);
    let player = players.get(key);
    const created = !player;
    if (created) {
      player = document.createElement("audio");
      player.controls = true;
      player.preload = "metadata";
      player.className = "audio-player-control";
      player.addEventListener("error", () => {
        if (!player.getAttribute("src")) return;
        playerErrors.set(key, "This browser cannot play the audio.");
        playerContainers(key).forEach((current) => showPlayerState(current, key));
      });
      player.addEventListener("loadedmetadata", () => {
        if (Number.isFinite(player.duration)) knownDurations.set(key, player.duration);
      });
      players.set(key, player);
    }
    slot.appendChild(player);
    showPlayerState(container, key);
    if (!player.getAttribute("src") && !playersLoading.has(key) && !playerErrors.has(key)) {
      if (container.dataset.audioKind === "url") {
        player.src = container.dataset.audioSrc;
      } else {
        loadPlayerSource(container, player, context);
      }
    }
  });
  return mounted;
}

// Re-attaches expanded players and chip state after the table is re-rendered.
// Players are moved into the new markup within the same task, so playback
// continues across re-renders.
export function refreshAudio(root = document) {
  root.querySelectorAll(".audio-chip").forEach(chipKey);
  const mounted = mountPlayers(root);
  players.forEach((player, key) => {
    if (mounted.has(key)) return;
    player.pause();
    player.removeAttribute("src");
    player.load();
    players.delete(key);
    playerErrors.delete(key);
    playersLoading.delete(key);
  });
  if (inline.key) {
    const visible = [...document.querySelectorAll(".audio-chip")].some((chip) => chipKey(chip) === inline.key);
    if (!visible) {
      inline.audio?.pause();
      inline.key = "";
      inline.error = "";
      inline.loading = false;
    }
  }
  updateChips();
}

export function resetAudio() {
  pauseAll();
  if (inline.audio) {
    inline.audio.removeAttribute("src");
    inline.audio.load();
  }
  inline.key = "";
  inline.error = "";
  inline.loading = false;
  players.forEach((player) => {
    player.pause();
    player.removeAttribute("src");
  });
  players.clear();
  playerErrors.clear();
  playersLoading.clear();
  knownDurations.clear();
  urlCache.forEach((url) => URL.revokeObjectURL(url));
  urlCache.clear();
}

// resolveCellRequest(rowIndex, columnIndex) returns
// { file, column, request } where request selects the row for /api/raw_row.
export function configureAudio(resolveCellRequest) {
  resolveCell = resolveCellRequest;
  if (listenersInstalled) return;
  listenersInstalled = true;
  // Only one clip plays at a time across cell chips and expanded players.
  document.addEventListener(
    "play",
    (event) => {
      if (event.target instanceof HTMLMediaElement) pauseAll(event.target);
    },
    true,
  );
}
