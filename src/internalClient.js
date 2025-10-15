// src/lib/internalClient.js
// Normalized internal API client for your Express server

// --- Base URL handling -------------------------------------------------------
const RAW = (import.meta.env.VITE_API_BASE || "http://localhost:4000").trim();
// remove any trailing slash so we don't get double slashes when joining
const BASE = RAW.replace(/\/+$/, "");

// --- Core request helper -----------------------------------------------------
async function request(path, opts = {}) {
  const url = `${BASE}${path.startsWith("/") ? "" : "/"}${path}`;
  const res = await fetch(url, {
    credentials: "include", // send/receive cookie "int"
    headers: { "Content-Type": "application/json" },
    ...opts,
  });

  const text = await res.text();

  // Try to parse JSON either way
  let data;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }

  if (!res.ok) {
    // Surface server error details if present
    const detail = typeof data === "object" && data?.error ? `: ${data.error}` : "";
    throw new Error(`HTTP ${res.status}${detail} ${typeof data === "string" ? data : ""}`.trim());
  }

  return data;
}

// --- Auth helpers (optional, but handy) --------------------------------------
export function loginInternal({ email, password }) {
  return request("/api/internal/login", {
    method: "POST",
    body: JSON.stringify({ email, password }),
  });
}
export function getSession() {
  return request("/api/internal/session");
}
export function logoutInternal() {
  return request("/api/internal/logout", { method: "POST" });
}

// --- Lists (all normalized to { rows, limit }) -------------------------------
export async function listUsers({ q = "", limit = 100 } = {}) {
  const data = await request(`/api/internal/users?q=${encodeURIComponent(q)}&limit=${limit}`);
  return { rows: data.rows || data.users || [], limit: data.limit ?? limit };
}

export async function listViews({ q = "", limit = 100 } = {}) {
  const data = await request(`/api/internal/views?q=${encodeURIComponent(q)}&limit=${limit}`);
  return { rows: data.rows || [], limit: data.limit ?? limit };
}

export async function listTriggers({ category_id = "", q = "", limit = 100 } = {}) {
  const params = new URLSearchParams();
  if (category_id) params.set("category_id", category_id);
  if (q) params.set("q", q);
  params.set("limit", String(limit));
  const data = await request(`/api/internal/triggers?${params.toString()}`);
  return { rows: data.rows || [], limit: data.limit ?? limit };
}

export async function listTickets({ q = "", limit = 100 } = {}) {
  const data = await request(`/api/internal/tickets?q=${encodeURIComponent(q)}&limit=${limit}`);
  // server may return { rows } (recommended) or { tickets } (older); support both
  return { rows: data.rows || data.tickets || [], limit: data.limit ?? limit };
}

export async function listOrganizations({ q = "", limit = 100 } = {}) {
  const data = await request(`/api/internal/organizations?q=${encodeURIComponent(q)}&limit=${limit}`);
  return { rows: data.rows || [], limit: data.limit ?? limit };
}

export async function listMacros({ q = "", limit = 100 } = {}) {
  const data = await request(`/api/internal/macros?q=${encodeURIComponent(q)}&limit=${limit}`);
  return { rows: data.rows || [], limit: data.limit ?? limit };
}

// --- Ticket details / attachments -------------------------------------------
export const getTicket = (id) => request(`/api/internal/tickets/${id}`);

export const listTicketAttachments = (id) =>
  request(`/api/internal/tickets/${id}/attachments`);

// --- (Optional) export BASE for debugging -----------------------------------
export const apiBase = BASE;
