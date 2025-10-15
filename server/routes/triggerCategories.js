// server/routes/triggerCategories.js
import { Router } from "express";
import { q } from "../db.js";

const router = Router();

/**
 * GET /api/internal/trigger-categories
 * q (name), limit, offset
 */
router.get("/", async (req, res) => {
  try {
    const search = (req.query.q || "").toString().trim().toLowerCase();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const where = [];
    const params = [];

    if (search) {
      where.push(`LOWER(name) LIKE $1`);
      params.push(`%${search}%`);
    }

    const sql = `
      SELECT id, name, position, created_at, updated_at
      FROM trigger_categories
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY position ASC NULLS LAST, updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;

    const { rows } = await q(sql, params);
    res.json({ rows, limit: L, offset: O });
  } catch (err) {
    console.error("trigger_categories.list error:", err);
    res.status(500).json({ error: "DB error" });
  }
});

export default router;
