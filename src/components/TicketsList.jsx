// src/components/TicketsList.jsx
import { useEffect, useState } from "react";
import { listTickets } from "@/lib/internalClient";
import ListTable from "./ListTable";

export default function TicketsList({ onOpen }) {
  const [rows, setRows] = useState([]);
  const [q, setQ] = useState("");
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

useEffect(() => {
  let alive = true;
  (async () => {
    setLoading(true);
    setErr("");
    try {
      // ✅ use q, not search
      const { rows } = await listTickets({ q, limit: 100 });
      if (alive) setRows(Array.isArray(rows) ? rows : []);
    } catch (e) {
      if (alive) setErr(e?.message || "Failed to load tickets");
    } finally {
      if (alive) setLoading(false);
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
          placeholder="Search subject…"
        />
      </div>

      {loading && <div className="p-4 text-gray-500">Loading…</div>}
      {!!err && (
        <div className="rounded border border-red-200 bg-red-50 p-3 text-red-700 text-sm mb-3">
          {err}
        </div>
      )}

      {!loading && !err && (
        <ListTable
          columns={[
            { key: "id", label: "ID" },
            { key: "subject", label: "Subject" },
            { key: "status", label: "Status" },
            { key: "priority", label: "Priority" },
            { key: "type", label: "Type" },
            {
              key: "actions",
              label: "",
              render: (r) => (
                <button
                  className="px-3 py-1 rounded bg-gray-900 text-white"
                  onClick={() => onOpen(r.id)}
                >
                  Open
                </button>
              ),
            },
          ]}
          rows={rows}
        />
      )}
    </div>
  );
}
