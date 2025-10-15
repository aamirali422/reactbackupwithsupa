// src/components/TicketDetail.jsx
import { useEffect, useState, useMemo } from "react";
import { getTicket } from "@/lib/internalClient";

function Badge({ children }) {
  return (
    <span className="inline-block rounded-full border px-2 py-0.5 text-xs font-medium text-gray-700 bg-white">
      {children}
    </span>
  );
}

function Section({ title, children, right }) {
  return (
    <div className="bg-white rounded-2xl shadow-sm border p-4 md:p-5">
      <div className="flex items-start justify-between gap-3 border-b pb-3 mb-3">
        <h3 className="text-base md:text-lg font-semibold">{title}</h3>
        {right}
      </div>
      {children}
    </div>
  );
}

function initials(nameOrEmail = "") {
  const s = String(nameOrEmail).trim();
  if (!s) return "?";
  const parts = s.split(/\s+/).filter(Boolean);
  if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
  const first = s[0];
  const local = s.includes("@") ? s.split("@")[0] : s.slice(0, 2);
  const second = local[1] || "";
  return (first + second).toUpperCase();
}

function Avatar({ name, email }) {
  const label = name || email || "?";
  return (
    <div
      className="h-9 w-9 rounded-full bg-gray-200 flex items-center justify-center text-xs font-semibold text-gray-700"
      title={label}
    >
      {initials(label)}
    </div>
  );
}

function fmt(dt) {
  try {
    const d = new Date(dt);
    if (Number.isNaN(d.getTime())) return String(dt);
    return d.toLocaleString();
  } catch {
    return String(dt);
  }
}

function maskEmail(email = "") {
  const s = String(email).trim();
  if (!s.includes("@")) return s ? s[0] + "•••" : "";
  const [local, domain] = s.split("@");
  if (!local) return "•••@" + domain;
  const visible = local.slice(0, 1);
  return `${visible}***@${domain}`;
}

function displayUser({ name, email, id }) {
  if (name) return name;
  if (email) return maskEmail(email);
  if (id) return `User #${id}`;
  return "Unknown";
}

export default function TicketDetail({ ticketId, onBack }) {
  const [ticket, setTicket] = useState(null);
  const [attachments, setAttachments] = useState([]);
  const [comments, setComments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setErr("");

    (async () => {
      try {
        const data = await getTicket(ticketId); // { ticket, comments, attachments }
        if (!alive) return;
        setTicket(data.ticket || null);
        setComments(Array.isArray(data.comments) ? data.comments : []);
        setAttachments(Array.isArray(data.attachments) ? data.attachments : []);
      } catch (e) {
        if (!alive) return;
        setErr(e?.message || "Failed to load ticket");
      } finally {
        if (alive) setLoading(false);
      }
    })();

    return () => {
      alive = false;
    };
  }, [ticketId]);

  // Map attachments to their comment_id so we can show them in-thread
  const attachmentsByComment = useMemo(() => {
    const map = new Map();
    for (const a of attachments) {
      const key = a.comment_id || 0;
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(a);
    }
    return map;
  }, [attachments]);

  const metaBadges = useMemo(() => {
    if (!ticket) return null;
    return (
      <div className="flex flex-wrap gap-2">
        {ticket.status && <Badge>Status: {ticket.status}</Badge>}
        {ticket.priority && <Badge>Priority: {ticket.priority}</Badge>}
        {ticket.type && <Badge>Type: {ticket.type}</Badge>}
      </div>
    );
  }, [ticket]);

  if (loading) return <div className="p-4">Loading…</div>;

  if (err)
    return (
      <div className="p-4">
        <button onClick={onBack} className="px-3 py-1 rounded-lg border mb-4 hover:bg-gray-50">
          ← Back
        </button>
        <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-red-700 text-sm">
          {err}
        </div>
      </div>
    );

  if (!ticket)
    return (
      <div className="p-4">
        <button onClick={onBack} className="px-3 py-1 rounded-lg border mb-4 hover:bg-gray-50">
          ← Back
        </button>
        <div className="text-sm text-gray-600">Ticket not found.</div>
      </div>
    );

  const requesterLabel = displayUser({
    name: ticket.requester_name,
    email: ticket.requester_email,
    id: ticket.requester_id,
  });
  const assigneeLabel = displayUser({
    name: ticket.assignee_name,
    email: ticket.assignee_email,
    id: ticket.assignee_id,
  });

  return (
    <div className="p-4 space-y-4">
      <div className="flex items-center justify-between">
        <button onClick={onBack} className="px-3 py-1.5 rounded-lg border hover:bg-gray-50">
          ← Back
        </button>
        {metaBadges}
      </div>

      {/* Header Card */}
      <div className="bg-white rounded-2xl shadow-sm border p-4 md:p-6">
        <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-3">
          <div>
            <h2 className="text-xl md:text-2xl font-bold mb-2">Ticket #{ticket.id}</h2>
            <p className="text-sm text-gray-500">
              Created {ticket.created_at ? fmt(ticket.created_at) : "—"}
              {ticket.updated_at ? ` • Updated ${fmt(ticket.updated_at)}` : ""}
            </p>
          </div>
          <div className="flex flex-col items-start md:items-end gap-1 text-sm">
            {ticket.organization_name && (
              <div>
                <span className="font-medium">Org:</span> {ticket.organization_name}
              </div>
            )}
            {(ticket.assignee_name || ticket.assignee_email || ticket.assignee_id) && (
              <div>
                <span className="font-medium">Assignee:</span> {assigneeLabel}
              </div>
            )}
          </div>
        </div>

        <div className="mt-5 grid md:grid-cols-2 gap-6 text-sm">
          <div className="space-y-2">
            <div>
              <div className="text-gray-500 font-medium">Subject</div>
              <div className="font-medium">{ticket.subject || "—"}</div>
            </div>
            <div>
              <div className="text-gray-500 font-medium">Requester</div>
              <div>{requesterLabel}</div>
            </div>
          </div>

          <div className="space-y-2">
            <div>
              <div className="text-gray-500 font-medium">Status</div>
              <div className="font-medium capitalize">{ticket.status || "—"}</div>
            </div>
            <div>
              <div className="text-gray-500 font-medium">Priority</div>
              <div className="font-medium capitalize">{ticket.priority || "—"}</div>
            </div>
          </div>

          <div className="md:col-span-2">
            <div className="text-gray-500 font-medium mb-1">Description</div>
            <div className="mt-1 whitespace-pre-wrap leading-relaxed">{ticket.description}</div>
          </div>
        </div>
      </div>

      {/* Comments - scrollable thread with per-comment attachments */}
      <Section
        title={`Conversation (${comments.length})`}
        right={<div className="text-xs text-gray-500">oldest first</div>}
      >
        <div className="max-h-[60vh] overflow-y-auto pr-2">
          {comments.length === 0 ? (
            <div className="text-sm text-gray-500">No comments yet.</div>
          ) : (
            <ul className="space-y-4">
              {comments.map((c) => {
                const commentFiles = attachmentsByComment.get(c.id) || [];
                const authorLabel = displayUser({
                  name: c.author_name,
                  email: c.author_email,
                  id: c.author_id,
                });

                return (
                  <li key={c.id} className="flex items-start gap-3">
                    <Avatar name={c.author_name} email={c.author_email} />
                    <div className="flex-1 min-w-0">
                      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                        <span className="font-medium">{authorLabel}</span>
                        <span className="text-xs text-gray-500">• {fmt(c.created_at)}</span>
                        {!c.public && (
                          <span className="text-[10px] uppercase tracking-wide bg-yellow-100 text-yellow-800 px-1.5 py-0.5 rounded">
                            Internal
                          </span>
                        )}
                      </div>

                      <div className="mt-1 whitespace-pre-wrap text-sm leading-relaxed">
                        {c.body}
                      </div>

                      {/* Inline attachments for this comment */}
                      {commentFiles.length > 0 && (
                        <ul className="mt-2 text-xs bg-gray-50 rounded-lg border px-3 py-2 divide-y">
                          {commentFiles.map((a) => (
                            <li key={a.id} className="py-1 flex items-center justify-between gap-3">
                              <div className="flex items-center gap-2 min-w-0">
                                <span className="truncate">{a.file_name}</span>
                                {a.size ? (
                                  <span className="text-gray-500 whitespace-nowrap">
                                    ({a.size} bytes)
                                  </span>
                                ) : null}
                              </div>
                              <div className="flex items-center gap-2 shrink-0">
                                {a.local_path && (
                                  <code className="bg-gray-100 px-1 rounded">{a.local_path}</code>
                                )}
                                {!a.local_path && a.content_url && (
                                  <a
                                    className="underline"
                                    href={a.content_url}
                                    target="_blank"
                                    rel="noreferrer"
                                  >
                                    remote url
                                  </a>
                                )}
                              </div>
                            </li>
                          ))}
                        </ul>
                      )}
                    </div>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      </Section>

      {/* (Optional) All attachments section kept for quick scanning */}
      <Section title="All Attachments">
        {attachments.length === 0 ? (
          <div className="text-sm text-gray-500">No attachments found.</div>
        ) : (
          <ul className="text-sm divide-y">
            {attachments.map((a) => (
              <li key={a.id} className="py-2 flex items-center justify-between gap-3">
                <div className="flex items-center gap-2 min-w-0">
                  <span className="truncate">{a.file_name}</span>
                  {a.size ? (
                    <span className="text-xs text-gray-500 whitespace-nowrap">
                      ({a.size} bytes)
                    </span>
                  ) : null}
                </div>
                <div className="flex items-center gap-2">
                  {a.local_path && (
                    <code className="bg-gray-100 px-1 rounded">{a.local_path}</code>
                  )}
                  {!a.local_path && a.content_url && (
                    <a
                      className="text-blue-600 underline"
                      href={a.content_url}
                      target="_blank"
                      rel="noreferrer"
                    >
                      remote url
                    </a>
                  )}
                </div>
              </li>
            ))}
          </ul>
        )}
      </Section>
    </div>
  );
}
