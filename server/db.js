// server/db.js
import pg from "pg";
const { Pool } = pg;

// DEV: don't reject self-signed certs
process.env.NODE_TLS_REJECT_UNAUTHORIZED = process.env.NODE_TLS_REJECT_UNAUTHORIZED || "0";

// --- Normalize DATABASE_URL ---
// You accidentally have a value like: "DATABASE_URL=postgresql://..."
// Strip that prefix and any surrounding quotes/newlines.
function normalizeDatabaseUrl(v) {
  if (!v) return v;
  let s = String(v).trim();
  if (s.startsWith("DATABASE_URL=")) s = s.slice("DATABASE_URL=".length).trim();
  if ((s.startsWith('"') && s.endsWith('"')) || (s.startsWith("'") && s.endsWith("'"))) {
    s = s.slice(1, -1);
  }
  return s;
}

const RAW_URL = process.env.DATABASE_URL;
const DB_URL = normalizeDatabaseUrl(RAW_URL);

console.log("Raw DATABASE_URL from env:", RAW_URL);
console.log("Normalized DB URL used   :", DB_URL);

// One shared pool
const pool = new Pool({
  connectionString: DB_URL,
  ssl: { rejectUnauthorized: false },
});

pool.on("error", (err) => {
  console.error("Unexpected PG idle client error:", err);
});

export default pool;
export const q = (text, params) => pool.query(text, params);
