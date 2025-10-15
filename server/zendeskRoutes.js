// server/zendeskRoutes.js
import express from 'express';
import fetch from 'node-fetch';
import multer from 'multer';

const router = express.Router();
const upload = multer({ storage: multer.memoryStorage() });

const ZD_BASE = process.env.ZD_BASE;

// Build Authorization header
function authHeader() {
  if (process.env.ZD_OAUTH_ACCESS_TOKEN) {
    return { Authorization: `Bearer ${process.env.ZD_OAUTH_ACCESS_TOKEN}` };
  }
  const email = process.env.ZD_EMAIL;
  const token = process.env.ZD_API_TOKEN;
  const basic = Buffer.from(`${email}/token:${token}`).toString('base64');
  return { Authorization: `Basic ${basic}` };
}

async function zdFetch(path, init = {}) {
  const url = path.startsWith('http') ? path : `${ZD_BASE}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Accept: 'application/json',
      ...(init.headers || {}),
      ...authHeader(),
    },
  });
  // Try to return JSON, otherwise text
  const text = await res.text();
  const body = text ? (() => { try { return JSON.parse(text); } catch { return { raw: text }; } })() : null;
  if (!res.ok) {
    const msg = body?.error || body?.description || res.statusText;
    throw Object.assign(new Error(`Zendesk ${res.status}: ${msg}`), { status: res.status, body });
  }
  return body;
}

/** ---------- READS ---------- **/

// GET ticket core (+ include users, organizations, groups)
router.get('/tickets/:id', async (req, res) => {
  try {
    const include = req.query.include || 'users,organizations,groups';
    const data = await zdFetch(`/api/v2/tickets/${req.params.id}.json?include=${encodeURIComponent(include)}`);
    res.json(data);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message, details: e.body });
  }
});

// GET ticket comments (optionally include users / inline images)
router.get('/tickets/:id/comments', async (req, res) => {
  try {
    const include = req.query.include || 'users';
    const inline = req.query.inline === 'true' ? '&include_inline_images=true' : '';
    const data = await zdFetch(`/api/v2/tickets/${req.params.id}/comments.json?include=${encodeURIComponent(include)}${inline}`);
    res.json(data);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message, details: e.body });
  }
});

/** ---------- WRITES ---------- **/

// PUT ticket (update fields/status/tags/etc.)
router.put('/tickets/:id', async (req, res) => {
  try {
    const payload = { ticket: req.body.ticket || {} };
    const data = await zdFetch(`/api/v2/tickets/${req.params.id}.json`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    res.json(data);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message, details: e.body });
  }
});

// POST comment (public/internal) with optional file uploads
router.post('/tickets/:id/comment', upload.array('files'), async (req, res) => {
  try {
    const { body, html_body, isPublic } = req.body;
    const files = req.files || [];
    const tokens = [];

    // Upload each file to Zendesk Uploads API to get tokens
    for (const f of files) {
      const up = await fetch(`${ZD_BASE}/api/v2/uploads.json?filename=${encodeURIComponent(f.originalname)}`, {
        method: 'POST',
        headers: {
          ...authHeader(),
          'Content-Type': f.mimetype || 'application/octet-stream',
        },
        body: f.buffer,
      });
      const text = await up.text();
      const j = text ? JSON.parse(text) : {};
      if (!up.ok) {
        return res.status(up.status).json({ error: 'Upload failed', details: j });
      }
      tokens.push(j.upload.token);
    }

    // Add the comment to the ticket
    const payload = {
      ticket: {
        comment: {
          ...(html_body ? { html_body } : { body }),
          public: String(isPublic) === 'true',
          ...(tokens.length ? { uploads: tokens } : {}),
        },
      },
    };

    const data = await zdFetch(`/api/v2/tickets/${req.params.id}.json`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    res.json(data);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message, details: e.body });
  }
});

export default router;
