// api/index.js
import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import cookieParser from 'cookie-parser';
import pg from 'pg';

// --- crash guards so Vercel logs show real errors
process.on('unhandledRejection', (e) => console.error('[unhandledRejection]', e));
process.on('uncaughtException', (e) => console.error('[uncaughtException]', e));

const isProd = process.env.VERCEL === '1' || process.env.NODE_ENV === 'production';

// --- DB (lazy pool) ----------------------------------------------------------
process.env.NODE_TLS_REJECT_UNAUTHORIZED =
  process.env.NODE_TLS_REJECT_UNAUTHORIZED || '0';

function normalizeDatabaseUrl(v) {
  if (!v) return v;
  let s = String(v).trim();
  if (s.startsWith('DATABASE_URL=')) s = s.slice('DATABASE_URL='.length).trim();
  if ((s.startsWith('"') && s.endsWith('"')) || (s.startsWith("'") && s.endsWith("'"))) {
    s = s.slice(1, -1);
  }
  return s;
}
const { Pool } = pg;
const DB_URL = normalizeDatabaseUrl(process.env.DATABASE_URL || '');

let _pool = null;
function pool() {
  if (!_pool) {
    _pool = new Pool({
      connectionString: DB_URL,
      ssl: { rejectUnauthorized: false },
      max: 2,
      idleTimeoutMillis: 5000,
      connectionTimeoutMillis: 10000,
    });
    _pool.on('error', (err) => console.error('PG idle error:', err));
  }
  return _pool;
}
const q = (sql, params) => pool().query(sql, params);

// --- App & middleware ---------------------------------------------------------
const app = express();

const ALLOWED_ORIGINS = [
  process.env.FRONTEND_ORIGIN, // e.g. https://reactbackupwithsupa.vercel.app
  'http://localhost:5173',
  'http://127.0.0.1:5173',
].filter(Boolean);
const norm = (s) => String(s || '').replace(/\/+$/, '');

app.use(
  cors({
    origin: (origin, cb) => {
      if (!isProd) return cb(null, true);
      if (!origin) return cb(null, true); // same-origin
      const ok = ALLOWED_ORIGINS.some((o) => norm(o) === norm(origin));
      return cb(ok ? null : new Error(`CORS: ${origin} not allowed`), ok);
    },
    credentials: true,
  })
);
app.use(express.json());
app.use(cookieParser());

// --- simple health & ping (never DB/cookies)
app.get('/api/healthz', (_req, res) => res.json({ ok: true }));
app.get('/api/internal/ping', (_req, res) => res.json({ ok: true, ts: Date.now() }));

// --- cookie helpers -----------------------------------------------------------
const COOKIE_NAME = 'int';
const b64u = {
  enc: (s) =>
    Buffer.from(s, 'utf8').toString('base64').replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_'),
  dec: (u) => {
    const pad = u.length % 4 === 2 ? '==' : u.length % 4 === 3 ? '=' : '';
    return Buffer.from(u.replace(/-/g, '+').replace(/_/g, '/') + pad, 'base64').toString('utf8');
  },
};
function setSessionCookie(res, session, { days = 30 } = {}) {
  const value = b64u.enc(JSON.stringify(session));
  res.cookie(COOKIE_NAME, value, {
    httpOnly: true,
    sameSite: 'lax',
    secure: isProd,
    path: '/',
    maxAge: days * 24 * 3600 * 1000,
  });
}
function getSession(req) {
  const raw = req.cookies?.[COOKIE_NAME];
  if (!raw) return null;
  try { return JSON.parse(b64u.dec(raw)); } catch { return null; }
}

// --- internal auth ------------------------------------------------------------
const INTERNAL_USER = {
  email: process.env.INTERNAL_USER_EMAIL || 'backup@mahimediasolutions.com',
  password: process.env.INTERNAL_USER_PASSWORD || 'mahimediasolutions',
  name: 'Internal Admin',
};

app.use((req, _res, next) => {
  if (req.path.startsWith('/api/internal')) {
    console.log('[INT]', req.method, req.path);
  }
  next();
});

app.post('/api/internal/login', (req, res, next) => {
  try {
    const { email, password } = req.body || {};
    if (!email || !password) return res.status(400).json({ error: 'email and password required' });
    if (email === INTERNAL_USER.email && password === INTERNAL_USER.password) {
      const session = { user: { email, name: INTERNAL_USER.name } };
      setSessionCookie(res, session);
      return res.json(session);
    }
    return res.status(401).json({ error: 'Invalid email or password' });
  } catch (e) { next(e); }
});

app.get('/api/internal/session', (req, res) => {
  const s = getSession(req);
  if (!s) return res.status(401).json({ error: 'Not authenticated' });
  res.json(s);
});

app.post('/api/internal/logout', (req, res) => {
  res.clearCookie(COOKIE_NAME, {
    httpOnly: true,
    sameSite: 'lax',
    secure: isProd,
    path: '/',
  });
  res.json({ ok: true });
});

// guard for protected /api/internal/*
app.use((req, res, next) => {
  if (!req.path.startsWith('/api/internal')) return next();
  if (
    req.path.startsWith('/api/internal/login') ||
    req.path.startsWith('/api/internal/session') ||
    req.path.startsWith('/api/internal/logout') ||
    req.path.startsWith('/api/internal/ping')
  ) return next();

  const s = getSession(req);
  if (!s) return res.status(401).json({ error: 'Not authenticated' });
  next();
});

// --- data routes (tickets/users/org/views/triggers/categories/macros) ---------
// TICKETS
app.get('/api/internal/tickets', async (req, res) => {
  const search = String(req.query.q || '').trim().toLowerCase();
  const limit = Math.min(200, parseInt(req.query.limit, 10) || 100);
  let sql = `
    SELECT id, subject, description, status, priority, type,
           requester_id, assignee_id, organization_id,
           created_at, updated_at, due_at
    FROM tickets
  `;
  const p = [];
  if (search) { p.push(`%${search}%`); sql += ` WHERE LOWER(subject) LIKE $1 OR CAST(id AS TEXT) LIKE $1 `; }
  sql += ` ORDER BY updated_at DESC NULLS LAST, id DESC LIMIT ${limit}`;
  try {
    const { rows } = await q(sql, p);
    res.json({ rows, limit });
  } catch (e) {
    console.error('tickets.list error:', e);
    res.status(500).json({ error: 'DB query failed' });
  }
});

app.get('/api/internal/tickets/:id', async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'Bad ticket id' });
  try {
    const t = await q(
      `SELECT t.*, ru.name AS requester_name, ru.email AS requester_email,
              au.name AS assignee_name, au.email AS assignee_email,
              o.name AS organization_name
       FROM tickets t
       LEFT JOIN users ru ON ru.id = t.requester_id
       LEFT JOIN users au ON au.id = t.assignee_id
       LEFT JOIN organizations o ON o.id = t.organization_id
       WHERE t.id = $1`,
      [id]
    );
    if (t.rowCount === 0) return res.status(404).json({ error: 'Ticket not found' });
    const comments = (await q(
      `SELECT id, ticket_id, author_id, public, body, created_at, updated_at
       FROM ticket_comments WHERE ticket_id = $1 ORDER BY created_at ASC`,
      [id]
    )).rows;
    const attachments = (await q(
      `SELECT id, ticket_id, comment_id, file_name, content_url, local_path,
              content_type, size, created_at
       FROM attachments WHERE ticket_id = $1 ORDER BY created_at ASC`,
      [id]
    )).rows;
    res.json({ ticket: t.rows[0], comments, attachments });
  } catch (e) {
    console.error('tickets.detail error:', e);
    res.status(500).json({ error: 'DB query failed' });
  }
});

app.get('/api/internal/tickets/:id/attachments', async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'Bad ticket id' });
  try {
    const rows = (await q(
      `SELECT id, ticket_id, comment_id, file_name, content_url, local_path,
              content_type, size, created_at
       FROM attachments WHERE ticket_id = $1 ORDER BY created_at ASC`,
      [id]
    )).rows;
    res.json({ rows });
  } catch (e) {
    console.error('tickets.attach error:', e);
    res.status(500).json({ error: 'DB query failed' });
  }
});

// USERS
app.get('/api/internal/users', async (req, res) => {
  const search = String(req.query.q || '').trim().toLowerCase();
  const limit = Math.min(200, parseInt(req.query.limit, 10) || 100);
  let sql = `SELECT id, name, email, role, active, created_at, updated_at FROM users`;
  const p = [];
  if (search) { p.push(`%${search}%`); sql += ` WHERE LOWER(name) LIKE $1 OR LOWER(email) LIKE $1 OR CAST(id AS TEXT) LIKE $1 `; }
  sql += ` ORDER BY updated_at DESC NULLS LAST LIMIT ${limit}`;
  try {
    const { rows } = await q(sql, p);
    res.json({ rows, limit });
  } catch (e) {
    console.error('users.list error:', e);
    res.status(500).json({ error: 'DB error' });
  }
});

// ORGANIZATIONS
app.get('/api/internal/organizations', async (req, res) => {
  try {
    const search = (req.query.q || '').toString().trim().toLowerCase();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);
    const p = [];
    const w = [];
    if (search) { p.push(`%${search}%`); w.push('LOWER(name) LIKE $1'); }
    const sql = `
      SELECT id, name, external_id, created_at, updated_at
      FROM organizations
      ${w.length ? 'WHERE ' + w.join(' AND ') : ''}
      ORDER BY updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;
    const { rows } = await q(sql, p);
    res.json({ rows, limit: L, offset: O });
  } catch (e) {
    console.error('orgs.list error:', e);
    res.status(500).json({ error: 'DB error' });
  }
});

// VIEWS
app.get('/api/internal/views', async (req, res) => {
  try {
    const search = (req.query.q || '').toString().trim().toLowerCase();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);
    const p = [];
    const w = [];
    if (search) { w.push('LOWER(title) LIKE $1'); p.push(`%${search}%`); }
    const sql = `
      SELECT id, title, description, active, position, default_view,
             created_at, updated_at
      FROM views
      ${w.length ? 'WHERE ' + w.join(' AND ') : ''}
      ORDER BY position ASC NULLS LAST, updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;
    const { rows } = await q(sql, p);
    res.json({ rows, limit: L, offset: O });
  } catch (e) {
    console.error('views.list error:', e);
    res.status(500).json({ error: 'DB error' });
  }
});

// TRIGGERS
app.get('/api/internal/triggers', async (req, res) => {
  try {
    const search = (req.query.q || '').toString().trim().toLowerCase();
    const categoryId = (req.query.category_id || '').toString().trim();
    const active = (req.query.active || '').toString().trim();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const w = [];
    const p = [];
    let i = 1;

    if (search) { w.push(`LOWER(title) LIKE $${i++}`); p.push(`%${search}%`); }
    if (categoryId) { w.push(`category_id = $${i++}`); p.push(categoryId); }
    if (active) { w.push(`active = $${i++}`); p.push(active === 'true'); }

    const sql = `
      SELECT id, title, description, active, position, category_id, raw_title,
             default_trigger, created_at, updated_at
      FROM triggers
      ${w.length ? 'WHERE ' + w.join(' AND ') : ''}
      ORDER BY position ASC NULLS LAST, updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;
    const { rows } = await q(sql, p);
    res.json({ rows, limit: L, offset: O });
  } catch (e) {
    console.error('triggers.list error:', e);
    res.status(500).json({ error: 'DB error' });
  }
});

// TRIGGER CATEGORIES
app.get('/api/internal/trigger-categories', async (req, res) => {
  try {
    const search = (req.query.q || '').toString().trim().toLowerCase();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);
    const w = [];
    const p = [];
    if (search) { w.push('LOWER(name) LIKE $1'); p.push(`%${search}%`); }
    const sql = `
      SELECT id, name, position, created_at, updated_at
      FROM trigger_categories
      ${w.length ? 'WHERE ' + w.join(' AND ') : ''}
      ORDER BY position ASC NULLS LAST, updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;
    const { rows } = await q(sql, p);
    res.json({ rows, limit: L, offset: O });
  } catch (e) {
    console.error('trigger_categories.list error:', e);
    res.status(500).json({ error: 'DB error' });
  }
});

// MACROS
app.get('/api/internal/macros', async (req, res) => {
  try {
    const search = (req.query.q || '').toString().trim().toLowerCase();
    const active = (req.query.active || '').toString().trim();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const w = [];
    const p = [];
    let i = 1;

    if (search) { w.push(`LOWER(title) LIKE $${i++}`); p.push(`%${search}%`); }
    if (active) { w.push(`active = $${i++}`); p.push(active === 'true'); }

    const sql = `
      SELECT id, title, description, active, position, default_macro,
             created_at, updated_at
      FROM macros
      ${w.length ? 'WHERE ' + w.join(' AND ') : ''}
      ORDER BY position ASC NULLS LAST, updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;
    const { rows } = await q(sql, p);
    res.json({ rows, limit: L, offset: O });
  } catch (e) {
    console.error('macros.list error:', e);
    res.status(500).json({ error: 'DB error' });
  }
});

// --- export native handler for Vercel
export default function handler(req, res) {
  return app(req, res);
}

// Local dev server
if (process.env.VERCEL !== '1') {
  const PORT = process.env.PORT || 4000;
  app.listen(PORT, () => {
    console.log(`Local API on http://localhost:${PORT}`);
  });
}
