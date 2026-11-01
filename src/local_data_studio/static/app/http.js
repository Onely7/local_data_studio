export async function fetchJSON(url, options = {}) {
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

export async function startJob(kind, payload) {
  return fetchJSON(`/api/jobs/${kind}`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function cancelJob(jobId) {
  if (!jobId) return null;
  return fetchJSON(`/api/jobs/${jobId}/cancel`, { method: "POST" });
}

function abortError() {
  const error = new Error("Operation aborted");
  error.name = "AbortError";
  return error;
}

// `isCurrent` lets callers drop a job that a newer action superseded: progress
// stops reaching the page and the wait rejects with an AbortError.
export async function waitForJob(jobId, options = {}) {
  const intervalMs = options.intervalMs || 600;
  const isCurrent = options.isCurrent || (() => true);
  while (true) {
    if (options.signal?.aborted || !isCurrent()) {
      throw abortError();
    }
    const job = await fetchJSON(`/api/jobs/${jobId}`);
    if (!isCurrent()) {
      throw abortError();
    }
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

export function formatJobProgress(job, fallback) {
  if (Number.isFinite(job.progress)) {
    const message = job.message ? ` · ${job.message}` : "";
    return `${fallback} ${Math.round(job.progress * 100)}%${message}`;
  }
  return job.message || fallback;
}

export function isAbortError(err) {
  return err?.name === "AbortError";
}

// Structured API errors carry an object detail, such as
// {code: "row_index_required", row, message}; plain errors return null.
export function errorDetail(err) {
  try {
    const parsed = JSON.parse(err?.message || "");
    return parsed && typeof parsed.detail === "object" ? parsed.detail : null;
  } catch (parseErr) {
    return null;
  }
}

export function extractErrorMessage(err) {
  let message = err && err.message ? err.message : "Request failed";
  try {
    const parsed = JSON.parse(message);
    if (parsed && typeof parsed.detail === "object" && parsed.detail) {
      message = parsed.detail.message || JSON.stringify(parsed.detail);
    } else if (parsed && parsed.detail) {
      message = parsed.detail;
    }
  } catch (parseErr) {
    // A provider or API may return plain text instead of JSON.
  }
  return message;
}
