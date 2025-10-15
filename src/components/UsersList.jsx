// src/components/UsersList.jsx
import { useEffect, useState } from "react";
import { listUsers } from "@/lib/internalClient";
import ListTable from "./ListTable";

export default function UsersList() {
  const [rows, setRows] = useState([]);
  const [q, setQ] = useState("");
  const [err, setErr] = useState("");

  useEffect(() => {
    let alive = true;
    (async () => {
      setErr("");
      // in users list handler, just before res.json
console.log("GET /users ->", rows.length, "rows");
      try {
        const { rows } = await listUsers({ q, limit: 100 });
        if (alive) setRows(rows);
      } catch (e) {
        if (alive) setErr(e?.message || "Failed to load users");
      }
    })();
    return () => { alive = false; };
  }, [q]);

  return (
    <div className="p-4">
      <div className="mb-3 flex gap-2">
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          className="border rounded px-3 py-2 w-full"
          placeholder="Search users…"
        />
      </div>

      {!!err && (
        <div className="rounded border border-red-200 bg-red-50 p-3 text-red-700 text-sm mb-3">
          {err}
        </div>
      )}

      <ListTable
        columns={[
          { key: "id", label: "ID" },
          { key: "name", label: "Name" },
          { key: "email", label: "Email" },
          { key: "role", label: "Role" },
          { key: "active", label: "Active" },
        ]}
        rows={rows}
      />
    </div>
  );
}
