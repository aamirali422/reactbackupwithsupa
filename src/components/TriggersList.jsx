// src/components/TriggersList.jsx
import { useEffect, useState } from "react";
import { listTriggers, listTriggerCategories } from "@/lib/internalClient";

import ListTable from "./ListTable";

export default function TriggersList() {
  const [rows, setRows] = useState([]);
  const [cat, setCat] = useState("");
  const [cats, setCats] = useState([]);

  useEffect(() => {
    let alive = true;
    (async () => {
      const c = await listTriggerCategories({ limit: 200 });
      if (alive) setCats(c.rows || []);
    })();
    return () => { alive = false; };
  }, []);

  useEffect(() => {
    let alive = true;
    (async () => {
      const data = await listTriggers({ category_id: cat, limit: 100 });
      if (alive) setRows(data.rows || []);
    })();
    return () => { alive = false; };
  }, [cat]);

  return (
    <div className="p-4">
      <div className="mb-3 flex gap-2">
        <select
          value={cat}
          onChange={(e) => setCat(e.target.value)}
          className="border rounded px-3 py-2"
        >
          <option value="">All categories</option>
          {cats.map((c) => (
            <option key={c.id} value={c.id}>{c.name}</option>
          ))}
        </select>
      </div>
      <ListTable
        columns={[
          { key: "id", label: "ID" },
          { key: "title", label: "Title" },
          { key: "category_id", label: "Category" },
          { key: "active", label: "Active" },
          { key: "position", label: "Position" },
        ]}
        rows={rows}
      />
    </div>
  );
}
