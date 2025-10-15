// src/components/OrdersTable.jsx
import { useEffect, useMemo, useRef, useState } from "react";

const STATUSES = ["All", "Pending", "Processing", "Shipped", "Cancelled"];

const PRODUCTS = [
  "CFexpress™ v4 Type A",
  "CFexpress™ 2.0 Type B",
  "SDXC™ UHS-II",
  "microSDXC™ UHS-I",
  "Portable SSD",
  "Card Reader CFast 2.0",
  "Tech Pouch",
];

function randBetween(min, max) {
  return Math.round((Math.random() * (max - min) + min) * 100) / 100;
}

function generateOrders() {
  const now = new Date();
  const orders = [];
  for (let i = 1; i <= 150; i++) {
    const daysAgo = Math.floor(Math.random() * 365);
    const createdAt = new Date(now);
    createdAt.setDate(now.getDate() - daysAgo);

    const product = PRODUCTS[i % PRODUCTS.length];
    const status = STATUSES.slice(1)[i % (STATUSES.length - 1)];
    const price = randBetween(49, 899);
    const orderNo = `ORD-${String(i).padStart(4, "0")}`;
    const customer = ["john@example.com", "sara@example.com", "mike@example.com", "lisa@example.com", "tom@example.com"][i % 5];

    orders.push({ orderNo, status, product, customer, price, createdAt });
  }
  return orders;
}

function withinPeriod(date, period) {
  const d = new Date(date);
  const now = new Date();
  if (period === "week") {
    const start = new Date(now);
    start.setDate(now.getDate() - 7);
    return d >= start && d <= now;
  }
  if (period === "month") {
    return d.getFullYear() === now.getFullYear() && d.getMonth() === now.getMonth();
  }
  if (period === "year") {
    return d.getFullYear() === now.getFullYear();
  }
  return true; // "all"
}

export default function OrdersTable() {
  // data
  const allOrders = useRef(generateOrders());

  // filters
  const [query, setQuery] = useState(""); // search order no or customer
  const [product, setProduct] = useState("All");
  const [status, setStatus] = useState("All");
  const [minPrice, setMinPrice] = useState("");
  const [maxPrice, setMaxPrice] = useState("");
  const [period, setPeriod] = useState("all"); // all|year|month|week

  // paging
  const [visible, setVisible] = useState(20);
  const loadMoreRef = useRef(null);

  const productOptions = useMemo(() => ["All", ...PRODUCTS], []);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return allOrders.current.filter((o) => {
      const matchQ =
        !q ||
        o.orderNo.toLowerCase().includes(q) ||
        o.customer.toLowerCase().includes(q);

      const matchProduct = product === "All" || o.product === product;
      const matchStatus = status === "All" || o.status === status;
      const matchPeriod = withinPeriod(o.createdAt, period);

      const min = Number(minPrice);
      const max = Number(maxPrice);
      const matchMin = minPrice === "" || o.price >= min;
      const matchMax = maxPrice === "" || o.price <= max;

      return matchQ && matchProduct && matchStatus && matchPeriod && matchMin && matchMax;
    });
  }, [query, product, status, period, minPrice, maxPrice]);

  const rows = useMemo(() => filtered.slice(0, visible), [filtered, visible]);

  useEffect(() => {
    // reset paging on filter change
    setVisible(20);
  }, [query, product, status, period, minPrice, maxPrice]);

  useEffect(() => {
    if (!loadMoreRef.current) return;
    const ob = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting && rows.length < filtered.length) {
          setVisible((v) => v + 20);
        }
      },
      { root: null, threshold: 1 }
    );
    ob.observe(loadMoreRef.current);
    return () => ob.disconnect();
  }, [rows.length, filtered.length]);

  const clear = () => {
    setQuery("");
    setProduct("All");
    setStatus("All");
    setMinPrice("");
    setMaxPrice("");
    setPeriod("all");
  };

  return (
    <div className="p-6">
      <h3 className="text-xl font-semibold mb-4">Orders</h3>

      {/* Filters */}
      <div className="mb-4 grid grid-cols-1 gap-3 md:grid-cols-5">
        <div className="md:col-span-2">
          <label className="mb-1 block text-sm font-medium text-gray-700">
            Search (Order No / Customer)
          </label>
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="e.g. ORD-0012 or sara@example.com"
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-200"
          />
        </div>

        <div>
          <label className="mb-1 block text-sm font-medium text-gray-700">Product</label>
          <select
            value={product}
            onChange={(e) => setProduct(e.target.value)}
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-200"
          >
            {productOptions.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="mb-1 block text-sm font-medium text-gray-700">Status</label>
          <select
            value={status}
            onChange={(e) => setStatus(e.target.value)}
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-200"
          >
            {STATUSES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="mb-1 block text-sm font-medium text-gray-700">Period</label>
          <div className="flex gap-2">
            {[
              { key: "all", label: "All" },
              { key: "year", label: "Year" },
              { key: "month", label: "Month" },
              { key: "week", label: "Week" },
            ].map((p) => (
              <button
                key={p.key}
                onClick={() => setPeriod(p.key)}
                className={`flex-1 rounded-lg border px-3 py-2 text-sm transition ${
                  period === p.key
                    ? "bg-black text-white border-black"
                    : "bg-white text-gray-800 border-gray-200 hover:bg-gray-50"
                }`}
              >
                {p.label}
              </button>
            ))}
          </div>
        </div>

        <div className="md:col-span-2 flex gap-3">
          <div className="flex-1">
            <label className="mb-1 block text-sm font-medium text-gray-700">Min Price (€)</label>
            <input
              type="number"
              min="0"
              value={minPrice}
              onChange={(e) => setMinPrice(e.target.value)}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-200"
            />
          </div>
          <div className="flex-1">
            <label className="mb-1 block text-sm font-medium text-gray-700">Max Price (€)</label>
            <input
              type="number"
              min="0"
              value={maxPrice}
              onChange={(e) => setMaxPrice(e.target.value)}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-200"
            />
          </div>
        </div>

        <div className="md:self-end">
          <button
            onClick={clear}
            className="h-10 w-full rounded-lg bg-black px-4 text-sm font-medium text-white hover:bg-gray-800"
          >
            Clear
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto rounded-lg border border-gray-200 bg-white">
        <table className="min-w-full text-sm">
          <thead className="bg-gray-50 text-gray-700">
            <tr>
              <th className="px-4 py-2 text-left font-semibold">Order No</th>
              <th className="px-4 py-2 text-left font-semibold">Status</th>
              <th className="px-4 py-2 text-left font-semibold">Product</th>
              <th className="px-4 py-2 text-left font-semibold">Customer</th>
              <th className="px-4 py-2 text-left font-semibold">Pricing</th>
              <th className="px-4 py-2 text-left font-semibold">Created</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((o) => (
              <tr key={o.orderNo} className="border-t hover:bg-gray-50">
                <td className="px-4 py-2 font-medium">{o.orderNo}</td>
                <td className="px-4 py-2">
                  <span
                    className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
                      o.status === "Shipped"
                        ? "bg-green-100 text-green-700"
                        : o.status === "Processing"
                        ? "bg-blue-100 text-blue-700"
                        : o.status === "Pending"
                        ? "bg-yellow-100 text-yellow-700"
                        : "bg-red-100 text-red-700"
                    }`}
                  >
                    {o.status}
                  </span>
                </td>
                <td className="px-4 py-2">{o.product}</td>
                <td className="px-4 py-2">{o.customer}</td>
                <td className="px-4 py-2">€{o.price.toFixed(2)}</td>
                <td className="px-4 py-2">{o.createdAt.toLocaleDateString()}</td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td colSpan={6} className="px-4 py-6 text-center text-gray-500">
                  No orders match your filters.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Sentinel */}
      <div ref={loadMoreRef} className="p-4 text-center text-gray-500">
        {rows.length > 0 && rows.length < filtered.length
          ? "Loading more…"
          : rows.length >= filtered.length && filtered.length > 0
          ? "All orders loaded."
          : ""}
      </div>
    </div>
  );
}
