// api/index.js
import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import cookieParser from 'cookie-parser';
import serverless from 'serverless-http';

import pool from '../server/db.js';

// Routers (reuse your existing files)
import internalAuthRouter from '../server/routes/internalAuth.js';
import ticketsRouter from '../server/routes/tickets.js';
import usersRouter from '../server/routes/users.js';
import viewsRouter from '../server/routes/views.js';
import triggersRouter from '../server/routes/triggers.js';
import triggerCategoriesRouter from '../server/routes/triggerCategories.js';
import organizationsRouter from '../server/routes/organizations.js';
import macrosRouter from '../server/routes/macros.js';

const app = express();

// Note: On Vercel this API is same-origin with your frontend.
// You can allow same-origin and localhost (for previews if needed).
app.use(
  cors({
    origin: (origin, cb) => {
      const allowed = [
        // same-origin calls may send no Origin; allow them
        undefined,
        null,
        '',
        'https://reactbackupwithsupa.vercel.app',
        'http://localhost:5173',
        'http://127.0.0.1:5173',
      ];
      if (allowed.includes(origin)) return cb(null, true);
      return cb(new Error('Not allowed by CORS'));
    },
    credentials: true,
  })
);

app.use(express.json());
app.use(cookieParser());

// Auth routes (mounted under /api/internal in your client)
app.use('/internal', internalAuthRouter);

// Tiny guard (same as your server/index.js, adjusted for our base path)
app.use((req, res, next) => {
  // these are the unprotected endpoints
  if (req.path.startsWith('/internal/login')) return next();
  if (req.path.startsWith('/internal/session')) return next();
  if (req.path.startsWith('/internal/logout')) return next();

  // protect the rest under /internal/*
  if (!req.path.startsWith('/internal')) return next();

  const raw = req.cookies?.int;
  if (!raw) return res.status(401).json({ error: 'Not authenticated' });
  try {
    JSON.parse(Buffer.from(raw, 'base64url').toString('utf8'));
    return next();
  } catch {
    return res.status(401).json({ error: 'Bad session' });
  }
});

// Data routes (same routers)
app.use('/internal', ticketsRouter);
app.use('/internal/users', usersRouter);
app.use('/internal/views', viewsRouter);
app.use('/internal/triggers', triggersRouter);
app.use('/internal/trigger-categories', triggerCategoriesRouter);
app.use('/internal/organizations', organizationsRouter);
app.use('/internal/macros', macrosRouter);

// Health
app.get('/healthz', (_req, res) => res.json({ ok: true }));

// Optional: light DB ping on cold start (won't crash function)
(async () => {
  try {
    await pool.query('select 1');
    console.log('✅ Postgres connected (serverless)');
  } catch (e) {
    console.error('❌ Postgres connection error (serverless):', e.message);
  }
})();

// Tell Vercel not to parse body itself (Express handles it)
export const config = { api: { bodyParser: false } };

// Export serverless handler
export default serverless(app);
