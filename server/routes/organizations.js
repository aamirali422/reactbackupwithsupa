// server/routes/organizations.js
import { Router } from "express";
import { q } from "../db.js";

const router = Router();

/**
 * GET /api/internal/organizations
 * q, limit, offset
 */
router.get("/", async (req, res) => {
  try {
    const search = (req.query.q || "").toString().trim().toLowerCase();
    const L = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const O = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const params = [];
    const where = [];
    if (search) {
      params.push(`%${search}%`);
      where.push("LOWER(name) LIKE $1");
    }

    const sql = `
      SELECT id, name, external_id, created_at, updated_at
      FROM organizations
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY updated_at DESC NULLS LAST
      LIMIT ${L} OFFSET ${O}
    `;

    const { rows } = await q(sql, params);
    res.json({ rows, limit: L, offset: O });
  } catch (err) {
    console.error("organizations.list error:", err);
    res.status(500).json({ error: "DB error" });
  }
});

export default router;
