// api/index.js
import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import cookieParser from 'cookie-parser';
import serverless from 'serverless-http';
import pg from 'pg';

// ===================================================
//  DB (self-signed SSL fix + pool)
// ===================================================
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
const RAW_DB_URL = process.env.DATABASE_URL;
const DB_URL = normalizeDatabaseUrl(RAW_DB_URL);
const pool = new Pool({
  connectionString: DB_URL,
  ssl: { rejectUnauthorized: false },
});
pool.on('error', (err) => console.error('Unexpected PG idle client error:', err));
const q = (text, params) => pool.query(text, params);

// ===================================================
//  App + Middleware
// ===================================================
const app = express();

app.use(
  cors({
    origin: (origin, cb) => {
      const allowed = new Set([
        process.env.FRONTEND_ORIGIN || 'http://localhost:5173',
        'http://127.0.0.1:5173',
        'https://reactbackupwithsupa.vercel.app',
        undefined, null, ''
      ]);
      if (allowed.has(origin)) return cb(null, true);
      return cb(new Error('Not allowed by CORS'));
    },
    credentials: true,
  })
);

// Important: put JSON parser BEFORE routes
app.use(express.json());
app.use(cookieParser());

// Health (under /api for proxy + prod)
app.get('/api/healthz', (_req, res) => res.json({ ok: true }));

// ===================================================
//  Cookie helpers (for internal admin auth)
// ===================================================
const COOKIE_NAME = 'int';
const isProd = process.env.VERCEL === '1' || process.env.NODE_ENV === 'production';

// robust base64url (some Node versions are picky)
function base64urlEncode(str) {
  return Buffer.from(str, 'utf8')
    .toString('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}
function base64urlDecode(b64u) {
  const pad = b64u.length % 4 === 2 ? '==' : b64u.length % 4 === 3 ? '=' : '';
  const b64 = b64u.replace(/-/g, '+').replace(/_/g, '/') + pad;
  return Buffer.from(b64, 'base64').toString('utf8');
}

function setSessionCookie(res, session, { days = 30 } = {}) {
  const value = base64urlEncode(JSON.stringify(session));
  res.cookie(COOKIE_NAME, value, {
    httpOnly: true,
    sameSite: 'lax',
    secure: isProd,
    path: '/',
    maxAge: days * 24 * 3600 * 1000,
  });
}
function getSessionFromCookie(req) {
  const raw = req.cookies?.[COOKIE_NAME];
  if (!raw) return null;
  try { return JSON.parse(base64urlDecode(raw)); }
  catch { return null; }
}

// ===================================================
//  Internal Admin Auth (single-file)
//  Routes: /api/internal/login|session|logout
// ===================================================
const INTERNAL_USER = {
  email: process.env.INTERNAL_USER_EMAIL || 'backup@mahimediasolutions.com',
  password: process.env.INTERNAL_USER_PASSWORD || 'mahimediasolutions',
  name: 'Internal Admin',
};

// Log incoming auth requests for debugging (method, path only)
app.use((req, _res, next) => {
  if (req.path.startsWith('/api/internal'))
    console.log(`[INT] ${req.method} ${req.path}`);
  next();
});

app.post('/api/internal/login', (req, res, next) => {
  try {
    if (!req.is('application/json')) {
      // Some proxies mis-set headers; try to parse raw text as JSON
      if (typeof req.body !== 'object') {
        return res.status(400).json({ error: 'Expected JSON body' });
      }
    }
    const { email, password } = req.body || {};
    if (!email || !password) {
      return res.status(400).json({ error: 'email and password required' });
    }

    if (email === INTERNAL_USER.email && password === INTERNAL_USER.password) {
      const session = { user: { email, name: INTERNAL_USER.name } };
      setSessionCookie(res, session);
      return res.json(session);
    }
    return res.status(401).json({ error: 'Invalid email or password' });
  } catch (err) {
    console.error('LOGIN error:', err);
    next(err);
  }
});

app.get('/api/internal/session', (req, res) => {
  const s = getSessionFromCookie(req);
  if (!s) return res.status(401).json({ error: 'Not authenticated' });
  return res.json(s);
});

app.post('/api/internal/logout', (req, res) => {
  res.clearCookie(COOKIE_NAME, {
    httpOnly: true,
    sameSite: 'lax',
    secure: isProd,
    path: '/',
  });
  return res.json({ ok: true });
});

// ===================================================
//  Session guard for /api/internal/* (except auth endpoints)
// ===================================================
app.use((req, res, next) => {
  if (!req.path.startsWith('/api/internal')) return next();
  if (
    req.path.startsWith('/api/internal/login') ||
    req.path.startsWith('/api/internal/session') ||
    req.path.startsWith('/api/internal/logout')
  ) return next();

  const s = getSessionFromCookie(req);
  if (!s) return res.status(401).json({ error: 'Not authenticated' });
  return next();
});

// ===================================================
//  Data Routes (single-file)
//  All mounted under /api/internal/*
// ===================================================

// ---- TICKETS -------------------------------------------------
app.get('/api/internal/tickets', async (req, res) => {
  const search = String(req.query.q || '').trim().toLowerCase();
  const limit = Math.min(200, parseInt(req.query.limit, 10) || 100);

  let sql = `
    SELECT id, subject, description, status, priority, type,
           requester_id, assignee_id, organization_id,
           created_at, updated_at, due_at
    FROM tickets
  `;
  const params = [];

  if (search) {
    params.push(`%${search}%`);
    sql += `
      WHERE LOWER(subject) LIKE $1
         OR CAST(id AS TEXT) LIKE $1
    `;
  }

  sql += ` ORDER BY updated_at DESC NULLS LAST, id DESC LIMIT ${limit}`;

  try {
    const { rows } = await q(sql, params);
    console.log('GET /api/internal/tickets ->', rows.length, 'rows');
    res.json({ rows, limit });
  } catch (err) {
    console.error('GET /api/internal/tickets error:', err);
    res.status(500).json({ error: 'DB query failed' });
  }
});

app.get('/api/internal/tickets/:id', async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'Bad ticket id' });

  try {
    const tRs = await q(
      `
      SELECT
        t.*,
        ru.name  AS requester_name,
        ru.email AS requester_email,
        au.name  AS assignee_name,
        au.email AS assignee_email,
        o.name   AS organization_name
      FROM tickets t
      LEFT JOIN users ru ON ru.id = t.requester_id
      LEFT JOIN users au ON au.id = t.assignee_id
      LEFT JOIN organizations o ON o.id = t.organization_id
      WHERE t.id = $1
      `,
      [id]
    );
    if (tRs.rowCount === 0) return res.status(404).json({ error: 'Ticket not found' });

    const comments = (
      await q(
        `
        SELECT id, ticket_id, author_id, public, body, created_at, updated_at
        FROM ticket_comments
        WHERE ticket_id = $1
        ORDER BY created_at ASC
        `,
        [id]
      )
    ).rows;

    const attachments = (
      await q(
        `
        SELECT id, ticket_id, comment_id, file_name, content_url, local_path,
               content_type, size, created_at
        FROM attachments
        WHERE ticket_id = $1
        ORDER BY created_at ASC
        `,
        [id]
      )
    ).rows;

    res.json({ ticket: tRs.rows[0], comments, attachments });
  } catch (err) {
    console.error('GET /api/internal/tickets/:id error:', err);
    res.status(500).json({ error: 'DB query failed' });
  }
});

app.get('/api/internal/tickets/:id/attachments', async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'Bad ticket id' });
  try {
    const rows = (
      await q(
        `
        SELECT id, ticket_id, comment_id, file_name, content_url, local_path,
               content_type, size, created_at
        FROM attachments
        WHERE ticket_id = $1
        ORDER BY created_at ASC
        `,
        [id]
      )
    ).rows;
    res.json({ rows });
  } catch (err) {
    console.error('GET /api/internal/tickets/:id/attachments error:', err);
    res.status(500).json({ error: 'DB query failed' });
  }
});

// ---- USERS ---------------------------------------------------
app.get('/api/internal/users', async (req, res) => {
  const search = String(req.query.q || '').trim().toLowerCase();
  const limit = Math.min(200, parseInt(req.query.limit, 10) || 100);

  let sql = `
    SELECT id, name, email, role, active, created_at, updated_at
    FROM users
  `;
  const params = [];
  if (search) {
    params.push(`%${search}%`);
    sql += `
      WHERE LOWER(name) LIKE $1
         OR LOWER(email) LIKE $1
         OR CAST(id AS TEXT) LIKE $1
    `;
  }
  sql += ` ORDER BY updated_at DESC NULLS LAST LIMIT ${limit}`;

  try {
    const { rows } = await q(sql, params);
    console.log('GET /api/internal/users ->', rows.length, 'rows');
    res.json({ rows, limit });
  } catch (e) {
    console.error('GET /api/internal/users error:', e);
    res.status(500).json({ error: 'DB error' });
  }
});

// ---- ORGANIZATIONS -------------------------------------------
app.get('/api/internal/organizations', async (req, res) => {
  try {
    const search = (req.query.q || '').toString().trim().toLowerCase();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const params = [];
    const where = [];
    if (search) {
      params.push(`%${search}%`);
      where.push('LOWER(name) LIKE $1');
    }

    const sql = `
      SELECT id, name, external_id, created_at, updated_at
      FROM organizations
      ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
      ORDER BY updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;

    const { rows } = await q(sql, params);
    res.json({ rows, limit: L, offset: O });
  } catch (err) {
    console.error('organizations.list error:', err);
    res.status(500).json({ error: 'DB error' });
  }
});

// ---- VIEWS ---------------------------------------------------
app.get('/api/internal/views', async (req, res) => {
  try {
    const search = (req.query.q || '').toString().trim().toLowerCase();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const params = [];
    const where = [];

    if (search) {
      where.push('LOWER(title) LIKE $1');
      params.push(`%${search}%`);
    }

    const sql = `
      SELECT id, title, description, active, position, default_view,
             created_at, updated_at
      FROM views
      ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
      ORDER BY position ASC NULLS LAST, updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;

    const { rows } = await q(sql, params);
    res.json({ rows, limit: L, offset: O });
  } catch (err) {
    console.error('views.list error:', err);
    res.status(500).json({ error: 'DB error' });
  }
});

// ---- TRIGGERS ------------------------------------------------
app.get('/api/internal/triggers', async (req, res) => {
  try {
    const search = (req.query.q || '').toString().trim().toLowerCase();
    const categoryId = (req.query.category_id || '').toString().trim();
    const active = (req.query.active || '').toString().trim();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const where = [];
    const params = [];
    let i = 1;

    if (search) {
      where.push(`LOWER(title) LIKE $${i++}`);
      params.push(`%${search}%`);
    }
    if (categoryId) {
      where.push(`category_id = $${i++}`);
      params.push(categoryId);
    }
    if (active) {
      where.push(`active = $${i++}`);
      params.push(active === 'true');
    }

    const sql = `
      SELECT id, title, description, active, position, category_id, raw_title,
             default_trigger, created_at, updated_at
      FROM triggers
      ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
      ORDER BY position ASC NULLS LAST, updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;

    const { rows } = await q(sql, params);
    res.json({ rows, limit: L, offset: O });
  } catch (err) {
    console.error('triggers.list error:', err);
    res.status(500).json({ error: 'DB error' });
  }
});

// ---- TRIGGER CATEGORIES --------------------------------------
app.get('/api/internal/trigger-categories', async (req, res) => {
  try {
    const search = (req.query.q || '').toString().trim().toLowerCase();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const where = [];
    const params = [];

    if (search) {
      where.push('LOWER(name) LIKE $1');
      params.push(`%${search}%`);
    }

    const sql = `
      SELECT id, name, position, created_at, updated_at
      FROM trigger_categories
      ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
      ORDER BY position ASC NULLS LAST, updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;

    const { rows } = await q(sql, params);
    res.json({ rows, limit: L, offset: O });
  } catch (err) {
    console.error('trigger_categories.list error:', err);
    res.status(500).json({ error: 'DB error' });
  }
});

// ---- MACROS --------------------------------------------------
app.get('/api/internal/macros', async (req, res) => {
  try {
    const search = (req.query.q || '').toString().trim().toLowerCase();
    const active = (req.query.active || '').toString().trim();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const where = [];
    const params = [];
    let i = 1;

    if (search) {
      where.push(`LOWER(title) LIKE $${i++}`);
      params.push(`%${search}%`);
    }
    if (active) {
      where.push(`active = $${i++}`);
      params.push(active === 'true');
    }

    const sql = `
      SELECT id, title, description, active, position, default_macro,
             created_at, updated_at
      FROM macros
      ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
      ORDER BY position ASC NULLS LAST, updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;

    const { rows } = await q(sql, params);
    res.json({ rows, limit: L, offset: O });
  } catch (err) {
    console.error('macros.list error:', err);
    res.status(500).json({ error: 'DB error' });
  }
});

// ===================================================
//  Global Error Handler (prevents raw 500s)
// ===================================================
app.use((err, _req, res, _next) => {
  const code = err?.status || 500;
  const msg = err?.message || 'Internal Server Error';
  console.error('ERROR middleware:', code, msg);
  res.status(code).json({ error: msg });
});

// ===================================================
//  DB check on boot
// ===================================================
(async () => {
  try {
    await pool.query('select 1');
    console.log('✅ Postgres connected (serverless)');
  } catch (e) {
    console.error('❌ Postgres connection error (serverless):', e.message);
  }
})();

// ===================================================
//  Export for Vercel + local runner
// ===================================================
export const config = { api: { bodyParser: false } };
export default serverless(app);

if (process.env.VERCEL !== '1') {
  const PORT = process.env.PORT || 4000;
  app.listen(PORT, () => {
    console.log(`Local API running on http://localhost:${PORT}`);
    console.log('Try:  curl http://localhost:4000/api/healthz');
  });
}
