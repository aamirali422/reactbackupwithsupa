// --- Decide API base ----------------------------------------------------------
const RAW = (import.meta.env.VITE_API_BASE || '/api').trim();
const BASE = RAW.replace(/\/+$/, ''); // strip trailing slash

if (!window.__API_BASE_LOGGED__) {
  console.log('[internalClient] API BASE =', BASE);
  window.__API_BASE_LOGGED__ = true;
}

function joinUrl(base, path) {
  const left = base.endsWith('/') ? base.slice(0, -1) : base;
  const right = path.startsWith('/') ? path : `/${path}`;
  return `${left}${right}`;
}

// --- Core request helper ------------------------------------------------------
async function request(path, opts = {}) {
  const url = joinUrl(BASE, path);
  const res = await fetch(url, {
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
    ...opts,
  });

  const text = await res.text();
  let data;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }

  if (!res.ok) {
    const detail = typeof data === 'object' && data?.error ? `: ${data.error}` : '';
    throw new Error(`HTTP ${res.status}${detail} ${typeof data === 'string' ? data : ''}`.trim());
  }
  return data;
}

/* ============ AUTH ============ */
export function loginInternal({ email, password }) {
  return request('/internal/login', {
    method: 'POST',
    body: JSON.stringify({ email, password }),
  });
}
export function getInternalSession() { return request('/internal/session'); }
export function logoutInternal() { return request('/internal/logout', { method: 'POST' }); }

/* ============ TICKETS ============ */
export function listTickets(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/internal/tickets${qs ? `?${qs}` : ''}`);
}
export const getTicket = (id) => request(`/internal/tickets/${encodeURIComponent(id)}`);
export const listTicketAttachments = (id) =>
  request(`/internal/tickets/${encodeURIComponent(id)}/attachments`);

/* ============ USERS ============ */
export function listUsers(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/internal/users${qs ? `?${qs}` : ''}`);
}

/* ============ ORGANIZATIONS ============ */
export function listOrganizations(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/internal/organizations${qs ? `?${qs}` : ''}`);
}

/* ============ VIEWS ============ */
export function listViews(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/internal/views${qs ? `?${qs}` : ''}`);
}

/* ============ TRIGGERS & CATEGORIES ============ */
export function listTriggers(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/internal/triggers${qs ? `?${qs}` : ''}`);
}
export function listTriggerCategories(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/internal/trigger-categories${qs ? `?${qs}` : ''}`);
}

/* ============ MACROS ============ */
export function listMacros(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return request(`/internal/macros${qs ? `?${qs}` : ''}`);
}

export const apiBase = BASE;
