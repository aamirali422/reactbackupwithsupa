// server/routes/internalAuth.js
import express from "express";

const router = express.Router();

const INTERNAL_USER = {
  email: process.env.INTERNAL_USER_EMAIL || "backup@mahimediasolutions.com",
  password: process.env.INTERNAL_USER_PASSWORD || "mahimediasolutions",
  name: "Internal Admin",
};

const COOKIE_NAME = "int";
const isProd = process.env.NODE_ENV === "production";

function setSessionCookie(res, session) {
  const value = Buffer.from(JSON.stringify(session), "utf8").toString("base64url");
  res.cookie(COOKIE_NAME, value, {
    httpOnly: true,
    sameSite: "lax",
    secure: isProd,
    path: "/",
    maxAge: 30 * 24 * 3600 * 1000,
  });
}

function getSessionFromCookie(req) {
  const raw = req.cookies?.[COOKIE_NAME];
  if (!raw) return null;
  try {
    return JSON.parse(Buffer.from(raw, "base64url").toString("utf8"));
  } catch {
    return null;
  }
}

router.post("/login", (req, res) => {
  const { email, password } = req.body || {};
  if (email === INTERNAL_USER.email && password === INTERNAL_USER.password) {
    const session = { user: { email, name: INTERNAL_USER.name } };
    setSessionCookie(res, session);
    return res.json(session);
  }
  return res.status(401).json({ error: "Invalid email or password" });
});

router.get("/session", (req, res) => {
  const s = getSessionFromCookie(req);
  if (!s) return res.status(401).json({ error: "Not authenticated" });
  return res.json(s);
});

router.post("/logout", (req, res) => {
  res.clearCookie(COOKIE_NAME, {
    httpOnly: true,
    sameSite: "lax",
    secure: isProd,
    path: "/",
  });
  return res.json({ ok: true });
});

export default router;
