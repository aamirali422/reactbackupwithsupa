// src/components/ProtectedRoute.jsx
import { Navigate } from "react-router-dom";
import { useAuth } from "@/contexts/AuthContext";

export default function ProtectedRoute({ children }) {
  const { isLoggedIn, authLoading } = useAuth();

  if (authLoading) {
    return (
      <div className="min-h-[40vh] flex items-center justify-center text-gray-600">
        Checking session…
      </div>
    );
  }

  return isLoggedIn ? children : <Navigate to="/login" replace />;
}
