// src/server-lib/zd.js
import { getSessionFromCookie } from "./cookies.js";

function ensurePath(path) {
  if (!path || !path.startsWith("/api/v2")) {
    const e = new Error("Path must start with /api/v2");
    e.status = 400;
    throw e;
  }
  return path;
}

export function requireSession(req) {
  const s = getSessionFromCookie(req);
  if (!s || !s.email || !s.token || !s.subdomain) {
    const e = new Error("Not authenticated");
    e.status = 401;
    throw e;
  }
  return s;
}

export function zendeskBaseUrl(subdomain) {
  return `https://${subdomain}.zendesk.com`;
}

function buildAuthHeader(session) {
  if (session.accessToken) {
    return { Authorization: `Bearer ${session.accessToken}` };
  }
  const basic = Buffer.from(`${session.email}/token:${session.token}`).toString("base64");
  return { Authorization: `Basic ${basic}` };
}

export async function zdFetch(session, path, init = {}, { timeoutMs = 15000, retries = 1 } = {}) {
  ensurePath(path);
  const base = zendeskBaseUrl(session.subdomain);
  const url = `${base}${path}`;

  // Build headers safely
  const outHeaders = new Headers(init.headers || {});
  // Only set Content-Type if caller explicitly provided or we are sending JSON
  const hasExplicitCT = outHeaders.has("Content-Type");
  if (init.body && !hasExplicitCT && typeof init.body === "string") {
    // Assume JSON string body unless caller set a CT
    outHeaders.set("Content-Type", "application/json");
  }
  if (!outHeaders.has("Accept")) outHeaders.set("Accept", "application/json");

  // Auth
  const auth = buildAuthHeader(session);
  Object.entries(auth).forEach(([k, v]) => outHeaders.set(k, v));

  // Timeout via AbortController
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(new Error("Request timeout")), timeoutMs);

  // Minimal retry for 429 with Retry-After
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const resp = await fetch(url, { ...init, headers: outHeaders, signal: ac.signal });

      if (resp.status === 429 && attempt < retries) {
        const ra = Number(resp.headers.get("Retry-After")) || 1;
        await new Promise(r => setTimeout(r, ra * 1000));
        continue;
      }

      if (!resp.ok) {
        const ct = resp.headers.get("content-type") || "";
        const txt = await resp.text();
        let message = `Zendesk HTTP ${resp.status}`;
        if (ct.includes("application/json")) {
          try {
            const j = JSON.parse(txt);
            message = j.error || j.description || message;
          } catch {/* ignore */}
        } else if (txt) {
          message += `: ${txt.slice(0, 300).replace(/\s+/g, " ").trim()}`;
        }
        const err = new Error(message);
        err.status = resp.status;
        err.response = resp;
        throw err;
      }

      // Success: return JSON if available, else text
      const ct = resp.headers.get("content-type") || "";
      clearTimeout(t);
      if (ct.includes("application/json")) return resp.json();
      return resp.text();
    } catch (e) {
      lastErr = e;
      if (e.name === "AbortError") {
        const err = new Error("Zendesk request timed out");
        err.status = 504;
        clearTimeout(t);
        throw err;
      }
      if (attempt === retries) {
        clearTimeout(t);
        throw e;
      }
    }
  }
  clearTimeout(t);
  throw lastErr; // should not reach here
}

/** Minimal login: trust provided creds; caller should set cookie afterward. */
export async function doLogin(req, res) {
  try {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const raw = Buffer.concat(chunks).toString("utf8");

    let parsed;
    try {
      parsed = JSON.parse(raw || "{}");
    } catch {
      res.statusCode = 400;
      return res.end(JSON.stringify({ error: "Invalid JSON body" }));
    }

    const { email, token, subdomain } = parsed || {};
    if (!email || !token || !subdomain) {
      res.statusCode = 400;
      return res.end(JSON.stringify({ error: "email, token, subdomain required" }));
    }

    // Optional live validation:
    // try { await zdFetch({ email, token, subdomain }, "/api/v2/users/me.json"); }
    // catch { res.statusCode = 401; return res.end(JSON.stringify({ error: "Invalid Zendesk credentials" })); }

    // IMPORTANT: Do not set the cookie here if this layer shouldn't own cookie I/O.
    // Return session; the caller should persist server-side & set a session id cookie.
    return { email, token, subdomain };
  } catch (e) {
    res.statusCode = 500;
    return res.end(JSON.stringify({ error: e.message || "Login failed" }));
  }
}

/** Proxy GET to Zendesk: /api/zendesk?path=/api/v2/... */
export async function proxyZendesk(req, res, session, path) {
  try {
    // We want to forward Zendesk's content type if possible
    const base = zendeskBaseUrl(session.subdomain);
    const url = `${base}${ensurePath(path)}`;

    const auth = buildAuthHeader(session);
    const upstream = await fetch(url, {
      headers: { Accept: "application/json", ...auth },
    });

    const ct = upstream.headers.get("content-type") || "application/json";
    res.setHeader("Content-Type", ct);

    const body = await upstream.text();
    res.statusCode = upstream.status;
    res.end(body);
  } catch (err) {
    res.statusCode = err.status || 500;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: err.message || "Proxy error" }));
  }
}
