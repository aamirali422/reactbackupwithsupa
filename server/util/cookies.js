// server/util/cookies.js
const COOKIE_NAME = "app";

function buildCookieString(name, value, { maxAgeSec, secure = true } = {}) {
  const parts = [
    `${name}=${encodeURIComponent(value)}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Lax",
  ];
  if (secure) parts.push("Secure");
  if (typeof maxAgeSec === "number") parts.push(`Max-Age=${maxAgeSec}`);
  return parts.join("; ");
}

export function setSessionCookie(res, payload, { days = 30 } = {}) {
  const b64 = Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
  const isProd = process.env.VERCEL === "1" || process.env.NODE_ENV === "production";
  const cookie = buildCookieString(COOKIE_NAME, b64, { maxAgeSec: days * 86400, secure: isProd });
  res.setHeader("Set-Cookie", cookie);
}

export function clearSessionCookie(res) {
  const isProd = process.env.VERCEL === "1" || process.env.NODE_ENV === "production";
  const cookie = buildCookieString(COOKIE_NAME, "", { maxAgeSec: 0, secure: isProd });
  res.setHeader("Set-Cookie", cookie);
}
