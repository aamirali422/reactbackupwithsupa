// src/components/ViewsList.jsx
import { useEffect, useState } from "react";
import { listViews } from "@/lib/internalClient";
import ListTable from "./ListTable";

export default function ViewsList() {
  const [rows, setRows] = useState([]);
  const [q, setQ] = useState("");

  useEffect(() => {
    let alive = true;
    (async () => {
      const data = await listViews({ q, limit: 100 });
      if (alive) setRows(data.rows || []);
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
          placeholder="Search views…"
        />
      </div>
      <ListTable
        columns={[
          { key: "id", label: "ID" },
          { key: "title", label: "Title" },
          { key: "active", label: "Active" },
          { key: "position", label: "Position" },
        ]}
        rows={rows}
      />
    </div>
  );
}
