export default function ListTable({
  columns = [],
  rows = [],
  keyField = "id",
  emptyText = "No data found.",
}) {
  const cols = Array.isArray(columns) ? columns : [];
  const data = Array.isArray(rows) ? rows : [];

  if (cols.length === 0) return null;

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="bg-gray-50">
            {cols.map((c) => (
              <th key={c.key} className="text-left px-3 py-2 font-medium text-gray-600">
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.length === 0 ? (
            <tr>
              <td colSpan={cols.length} className="px-3 py-4 text-gray-500">
                {emptyText}
              </td>
            </tr>
          ) : (
            data.map((r) => (
              <tr key={r?.[keyField] ?? Math.random()} className="border-b last:border-0">
                {cols.map((c) => (
                  <td key={c.key} className="px-3 py-2">
                    {c.render ? c.render(r) : r?.[c.key]}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
