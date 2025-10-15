// server/index.js
import "dotenv/config";
import express from "express";
import cors from "cors";
import cookieParser from "cookie-parser";

import pool from "./db.js";

// Routers
import internalAuthRouter from "./routes/internalAuth.js";
import ticketsRouter from "./routes/tickets.js";
import usersRouter from "./routes/users.js";
import viewsRouter from "./routes/views.js";
import triggersRouter from "./routes/triggers.js";
import triggerCategoriesRouter from "./routes/triggerCategories.js";
import organizationsRouter from "./routes/organizations.js";
import macrosRouter from "./routes/macros.js";

const app = express();

// Log DB url in dev to ensure it’s the Supabase pooler
if (process.env.DATABASE_URL) {
  console.log("DB:", process.env.DATABASE_URL);
}

// Try a connection early so errors show clearly
(async () => {
  try {
    await pool.query("select 1");
    console.log("✅ Postgres connected");
  } catch (e) {
    console.error("❌ Postgres connection error:", e);
  }
})();

// ---- Middleware
app.use(
  cors({
    origin: (origin, cb) => {
      const allowed = [
        process.env.FRONTEND_ORIGIN || "http://localhost:5173",
        "http://127.0.0.1:5173",
      ];
      if (!origin || allowed.includes(origin)) return cb(null, true);
      return cb(new Error("Not allowed by CORS"));
    },
    credentials: true,
  })
);
app.use(express.json());
app.use(cookieParser());

// ---- Auth routes
app.use("/api/internal", internalAuthRouter);

// ---- Tiny guard (protect data routes with the cookie set by internalAuth)
app.use((req, res, next) => {
  if (req.path.startsWith("/api/internal/login")) return next();
  if (req.path.startsWith("/api/internal/session")) return next();
  if (req.path.startsWith("/api/internal/logout")) return next();
  if (!req.path.startsWith("/api/internal")) return next();

  const raw = req.cookies?.int;
  if (!raw) return res.status(401).json({ error: "Not authenticated" });
  try {
    JSON.parse(Buffer.from(raw, "base64url").toString("utf8"));
    return next();
  } catch {
    return res.status(401).json({ error: "Bad session" });
  }
});

// ---- Data routes (all under /api/internal/*)
app.use("/api/internal/tickets", ticketsRouter);
app.use("/api/internal/users", usersRouter);
app.use("/api/internal/views", viewsRouter);
app.use("/api/internal/triggers", triggersRouter);
app.use("/api/internal/trigger-categories", triggerCategoriesRouter);
app.use("/api/internal/organizations", organizationsRouter);
app.use("/api/internal/macros", macrosRouter);
// Health
app.get("/healthz", (_req, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`API listening on http://localhost:${PORT}`);
  console.log("Frontend origin:", process.env.FRONTEND_ORIGIN || "http://localhost:5173");
});
