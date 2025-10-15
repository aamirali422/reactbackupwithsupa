// src/pages/Dashboard.jsx
import { useState } from "react";
import { FiMenu } from "react-icons/fi";
import Sidebar from "@/components/Sidebar";
import TicketsList from "@/components/TicketsList";
import TicketDetail from "@/components/TicketDetail";
import UsersList from "@/components/UsersList";
import OrganizationsList from "@/components/OrganizationsList";
import ViewsList from "@/components/ViewsList";
import TriggersList from "@/components/TriggersList";
import MacrosList from "@/components/MacrosList";
import { useAuth } from "@/contexts/AuthContext";

export default function Dashboard() {
  const [view, setView] = useState("tickets");
  const [ticketId, setTicketId] = useState(null);
  const [isSidebarOpen, setSidebarOpen] = useState(false);
  const { logout } = useAuth();

  const openTicket = (id) => {
    setTicketId(id);
    setView("ticket-detail");
  };

  return (
    <div className="flex h-screen bg-gray-100">
      <Sidebar
        onSelect={(k) => { setSidebarOpen(false); setView(k); setTicketId(null); }}
        onLogout={logout}
        isOpen={isSidebarOpen}
        onClose={() => setSidebarOpen(false)}
      />

      <main className="flex-1 overflow-y-auto p-6">
        <div className="mb-4 flex items-center gap-3 md:hidden">
          <button
            onClick={() => setSidebarOpen(true)}
            className="rounded-lg border border-gray-200 bg-white p-2 hover:bg-gray-50"
            aria-label="Open menu"
          >
            <FiMenu />
          </button>
          <div className="text-lg font-semibold">Dashboard</div>
        </div>

        {view === "tickets" && <TicketsList onOpen={openTicket} />}
        {view === "ticket-detail" && ticketId && (
          <TicketDetail ticketId={ticketId} onBack={() => setView("tickets")} />
        )}
        {view === "users" && <UsersList />}
        {view === "organizations" && <OrganizationsList />}
        {view === "views" && <ViewsList />}
        {view === "triggers" && <TriggersList />}
        {view === "trigger-categories" && <TriggersList />} {/* reuse with filter */}
        {view === "macros" && <MacrosList />}
      </main>
    </div>
  );
}
