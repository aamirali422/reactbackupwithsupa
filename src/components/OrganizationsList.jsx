// src/components/OrganizationsList.jsx
import { useEffect, useState } from "react";
import { listOrganizations } from "@/lib/internalClient";
import ListTable from "./ListTable";

export default function OrganizationsList() {
  const [rows, setRows] = useState([]);
  const [q, setQ] = useState("");

  useEffect(() => {
    let alive = true;
    (async () => {
      const data = await listOrganizations({ q, limit: 100 });
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
          placeholder="Search organizations…"
        />
      </div>
      <ListTable
        columns={[
          { key: "id", label: "ID" },
          { key: "name", label: "Name" },
          { key: "external_id", label: "External ID" },
        ]}
        rows={rows}
      />
    </div>
  );
}
