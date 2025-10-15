// server/routes/users.js
import { Router } from "express";
import { q } from "../db.js";

const router = Router();

router.get("/", async (req, res) => {
  const search = String(req.query.q || "").trim().toLowerCase();
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
    res.json({ rows, limit });
  } catch (e) {
    console.error("GET /api/internal/users error:", e);
    res.status(500).json({ error: "DB error" });
  }
});

export default router;
