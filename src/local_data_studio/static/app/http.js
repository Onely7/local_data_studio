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

export async function waitForJob(jobId, options = {}) {
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

export function formatJobProgress(job, fallback) {
  if (Number.isFinite(job.progress)) {
    const message = job.message ? ` · ${job.message}` : "";
    return `${fallback} ${Math.round(job.progress * 100)}%${message}`;
  }
  return job.message || fallback;
}

export function extractErrorMessage(err) {
  let message = err && err.message ? err.message : "Request failed";
  try {
    const parsed = JSON.parse(message);
    if (parsed && parsed.detail) {
      message = parsed.detail;
    }
  } catch (parseErr) {
    // A provider or API may return plain text instead of JSON.
  }
  return message;
}
