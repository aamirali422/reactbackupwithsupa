// src/lib/internalClient.js
// All internal API client functions – cleaned & valid JS

function apiUrl(path) {
  if (!path.startsWith("/")) return `/api${path}`;
  return path.startsWith("/api") ? path : `/api${path}`;
}

async function readOnce(res) {
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  const text = await res.text().catch(() => "");
  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    if (ct.includes("application/json")) {
      try {
        const j = JSON.parse(text || "{}");
        msg = j.error || j.message || msg;
      } catch {}
    } else if (text) {
      msg += `: ${text.slice(0, 200).replace(/\s+/g, " ").trim()}`;
    }
    throw new Error(msg);
  }
  if (!text) return {};
  if (ct.includes("application/json")) {
    try { return JSON.parse(text); } 
    catch { throw new Error("Response was not valid JSON."); }
  }
  return text;
}

async function request(path, opts = {}) {
  const res = await fetch(apiUrl(path), {
    credentials: "include",
    ...opts,
  });
  return readOnce(res);
}

/* ============ AUTH ============ */
export async function getInternalSession() {
  return request("/api/internal/session");
}

export async function loginInternal({ email, password }) {
  return request("/api/internal/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
}

export async function logoutInternal() {
  return request("/api/internal/logout", { method: "POST" });
}

/* ============ TICKETS ============ */
export async function listTickets(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/api/internal/tickets${qs ? `?${qs}` : ""}`);
}

export async function getTicket(id) {
  return request(`/api/internal/tickets/${encodeURIComponent(id)}`);
}

export async function listTicketAttachments(id) {
  return request(`/api/internal/tickets/${encodeURIComponent(id)}/attachments`);
}

/* ============ USERS ============ */
export async function listUsers(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/api/internal/users${qs ? `?${qs}` : ""}`);
}

/* ============ ORGANIZATIONS ============ */
export async function listOrganizations(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/api/internal/organizations${qs ? `?${qs}` : ""}`);
}

/* ============ VIEWS ============ */
export async function listViews(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/api/internal/views${qs ? `?${qs}` : ""}`);
}

/* ============ TRIGGERS ============ */
export async function listTriggers(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/api/internal/triggers${qs ? `?${qs}` : ""}`);
}

/* ============ TRIGGER CATEGORIES ============ */
export async function listTriggerCategories(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/api/internal/trigger-categories${qs ? `?${qs}` : ""}`);
}

/* ============ MACROS ============ */
export async function listMacros(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/api/internal/macros${qs ? `?${qs}` : ""}`);
}

/* Backward aliases */
export { loginInternal as internalLogin };
export { getInternalSession as apiGetInternalSession };
export { logoutInternal as internalLogout };
