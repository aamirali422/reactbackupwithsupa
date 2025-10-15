// src/lib/apiBase.js
export function apiUrl(path) {
  if (!path.startsWith("/")) return `/api${path}`;
  return path.startsWith("/api") ? path : `/api${path}`;
}

export async function ensureJson(res) {
  const ct = res.headers.get("content-type") || "";

  // happy path: JSON
  if (res.ok && ct.includes("application/json")) {
    return res.json();
  }

  // read body once, then decide
  const raw = await res.text();

  if (!res.ok) {
    // Try to extract JSON error first
    if (ct.includes("application/json")) {
      try {
        const data = JSON.parse(raw);
        const detail = data?.error || data?.message || JSON.stringify(data);
        throw new Error(detail || `HTTP ${res.status}`);
      } catch {
        // fall through to text error
      }
    }
    // Non-JSON error (HTML/text). Include a short preview to help debugging.
    const preview = raw.slice(0, 300).replace(/\s+/g, " ").trim();
    throw new Error(preview ? `HTTP ${res.status}: ${preview}` : `HTTP ${res.status}`);
  }

  // 2xx but not JSON (rare) – still try to parse JSON, else error
  if (ct.includes("application/json")) {
    try {
      return JSON.parse(raw);
    } catch (e) {
      throw new Error("Malformed JSON response.");
    }
  }

  // Unexpected success payload type
  throw new Error("Expected JSON but received non-JSON response.");
}
