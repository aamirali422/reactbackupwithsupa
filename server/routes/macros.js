// server/routes/macros.js
import { Router } from "express";
import { q } from "../db.js";

const router = Router();

/**
 * GET /api/internal/macros
 * q (title), active, limit, offset
 */
router.get("/", async (req, res) => {
  try {
    const search = (req.query.q || "").toString().trim().toLowerCase();
    const active = (req.query.active || "").toString().trim();
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
      params.push(active === "true");
    }

    const sql = `
      SELECT id, title, description, active, position, default_macro,
             created_at, updated_at
      FROM macros
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY position ASC NULLS LAST, updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;

    const { rows } = await q(sql, params);
    res.json({ rows, limit: L, offset: O });
  } catch (err) {
    console.error("macros.list error:", err);
    res.status(500).json({ error: "DB error" });
  }
});

export default router;
