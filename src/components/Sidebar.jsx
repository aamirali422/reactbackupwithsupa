// src/components/Sidebar.jsx
import { FiX } from "react-icons/fi";

export default function Sidebar({ onSelect, onLogout, isOpen, onClose }) {
  const Item = ({ id, label }) => (
    <button
      onClick={() => onSelect(id)}
      className="w-full text-left px-4 py-2 rounded hover:bg-gray-100"
    >
      {label}
    </button>
  );

  return (
    <aside className={`${isOpen ? "block" : "hidden"} md:block w-64 bg-white border-r shadow-sm`}>
      {/* Mobile close */}
      <div className="md:hidden flex justify-end p-2">
        <button onClick={onClose} className="p-2">
          <FiX />
        </button>
      </div>

      <div className="p-4">
        <img
          src="https://www.angelbird.com/static/web/img/AB_Logo.svg"
          alt="Logo"
          className="h-6 mb-4"
        />
        <div className="space-y-1">
          <Item id="tickets" label="Tickets" />
          <Item id="users" label="Users" />
          <Item id="organizations" label="Organizations" />
          <Item id="views" label="Views" />
          <Item id="triggers" label="Triggers" />
          <Item id="trigger-categories" label="Trigger Categories" />
          <Item id="macros" label="Macros" />
        </div>

        <hr className="my-4" />
        <button
          onClick={onLogout}
          className="w-full px-4 py-2 rounded bg-gray-900 text-white hover:bg-gray-800"
        >
          Logout
        </button>
      </div>
    </aside>
  );
}
