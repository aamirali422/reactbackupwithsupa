// src/lib/auth.js
import { apiUrl, ensureJson } from "./apiBase";

export async function login({ email, token, subdomain }) {
  const res = await fetch(apiUrl("/login"), {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, token, subdomain }),
  });
  return ensureJson(res);
}

export async function getSession() {
  const res = await fetch(apiUrl("/session"), { credentials: "include" });
  return ensureJson(res);
}

export async function logout() {
  const res = await fetch(apiUrl("/logout"), {
    method: "POST",
    credentials: "include",
  });
  return ensureJson(res);
}
