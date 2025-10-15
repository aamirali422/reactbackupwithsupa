// server/routes/triggers.js
import { Router } from "express";
import { q } from "../db.js";

const router = Router();

/**
 * GET /api/internal/triggers
 * q (title), category_id, active, limit, offset
 */
router.get("/", async (req, res) => {
  try {
    const search = (req.query.q || "").toString().trim().toLowerCase();
    const categoryId = (req.query.category_id || "").toString().trim();
    const active = (req.query.active || "").toString().trim();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const where = [];
    const params = [];
    let i = 1;

    if (search) {
      where.push(`LOWER(title) LIKE $${i}`);
      params.push(`%${search}%`);
      i++;
    }
    if (categoryId) {
      where.push(`category_id = $${i}`);
      params.push(categoryId);
      i++;
    }
    if (active) {
      where.push(`active = $${i}`);
      params.push(active === "true");
      i++;
    }

    const sql = `
      SELECT id, title, description, active, position, category_id, raw_title,
             default_trigger, created_at, updated_at
      FROM triggers
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY position ASC NULLS LAST, updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;

    const { rows } = await q(sql, params);
    res.json({ rows, limit: L, offset: O });
  } catch (err) {
    console.error("triggers.list error:", err);
    res.status(500).json({ error: "DB error" });
  }
});

export default router;
