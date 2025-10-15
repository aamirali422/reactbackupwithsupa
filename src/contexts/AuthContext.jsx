// src/contexts/AuthContext.jsx
import { createContext, useState, useContext, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  getInternalSession,
  loginInternal,
  logoutInternal,
} from "@/lib/internalClient";

const AuthContext = createContext();

export function AuthProvider({ children }) {
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [authLoading, setAuthLoading] = useState(true);
  const [user, setUser] = useState(null);
  const navigate = useNavigate();

  // 🔹 Check if the user already has a valid session on load
  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        setAuthLoading(true);
        const data = await getInternalSession(); // GET /api/internal/session
        if (!alive) return;
        setIsLoggedIn(true);
        setUser(data.user || null);
      } catch {
        if (!alive) return;
        setIsLoggedIn(false);
        setUser(null);
      } finally {
        if (alive) setAuthLoading(false);
      }
    })();
    return () => {
      alive = false;
    };
  }, []);

  // 🔹 Internal login handler (email + password)
  const login = async ({ email, password }) => {
    const resp = await loginInternal({ email, password }); // POST /api/internal/login
    setIsLoggedIn(true);
    setUser(resp.user || null);
    localStorage.setItem("isLoggedIn", "true");
  };

  // 🔹 Internal logout handler
  const logout = async () => {
    try {
      await logoutInternal();
    } catch {}
    setIsLoggedIn(false);
    setUser(null);
    localStorage.removeItem("isLoggedIn");
    navigate("/login");
  };

  return (
    <AuthContext.Provider
      value={{
        isLoggedIn,
        authLoading,
        user,
        login,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
