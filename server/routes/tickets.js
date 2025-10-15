// server/routes/tickets.js
import { Router } from "express";
import { q } from "../db.js";

const router = Router();

/**
 * GET /api/internal/tickets
 * q, limit
 * Returns { rows, limit }
 */
router.get("/", async (req, res) => {
  const search = String(req.query.q || "").trim().toLowerCase();
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
    // Log for visibility while debugging
    console.log("GET /api/internal/tickets ->", rows.length, "rows");
    res.json({ rows, limit });
  } catch (err) {
    console.error("GET /api/internal/tickets error:", err);
    res.status(500).json({ error: "DB query failed" });
  }
});

/**
 * GET /api/internal/tickets/:id
 * Returns { ticket, comments, attachments }
 */
router.get("/:id", async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: "Bad ticket id" });

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
    if (tRs.rowCount === 0) return res.status(404).json({ error: "Ticket not found" });

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
    console.error("GET /api/internal/tickets/:id error:", err);
    res.status(500).json({ error: "DB query failed" });
  }
});

/**
 * GET /api/internal/tickets/:id/attachments
 * Returns { rows }
 */
router.get("/:id/attachments", async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: "Bad ticket id" });
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
    console.error("GET /api/internal/tickets/:id/attachments error:", err);
    res.status(500).json({ error: "DB query failed" });
  }
});

export default router;
