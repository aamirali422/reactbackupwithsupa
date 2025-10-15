// src/server-lib/cookies.js
const COOKIE_NAME = "zd";

function buildCookieString(name, value, { maxAge, secure = true } = {}) {
  const parts = [
    `${name}=${encodeURIComponent(value)}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Lax",
  ];
  if (secure) parts.push("Secure");
  if (typeof maxAge === "number") parts.push(`Max-Age=${maxAge}`);
  return parts.join("; ");
}

export function parseCookies(req) {
  const header = req.headers.cookie || "";
  return header.split(";").reduce((acc, part) => {
    const [k, v] = part.split("=").map((s) => s && s.trim());
    if (!k) return acc;
    acc[k] = decodeURIComponent(v || "");
    return acc;
  }, {});
}

export function getSessionFromCookie(req) {
  const cookies = parseCookies(req);
  const raw = cookies[COOKIE_NAME];
  if (!raw) return null;
  try {
    return JSON.parse(Buffer.from(raw, "base64url").toString("utf8"));
  } catch {
    return null;
  }
}

export function setSessionCookie(res, session, { maxDays = 30 } = {}) {
  const value = Buffer.from(JSON.stringify(session), "utf8").toString("base64url");
  const maxAge = maxDays * 24 * 3600;

  // Only secure in production (Vercel); not on localhost http
  const isProd = process.env.VERCEL === "1" || process.env.NODE_ENV === "production";
  const cookie = buildCookieString(COOKIE_NAME, value, { maxAge, secure: isProd });

  res.setHeader("Set-Cookie", cookie);
}

export function clearSessionCookie(res) {
  const isProd = process.env.VERCEL === "1" || process.env.NODE_ENV === "production";
  const cookie = buildCookieString(COOKIE_NAME, "", { maxAge: 0, secure: isProd });
  res.setHeader("Set-Cookie", cookie);
}
