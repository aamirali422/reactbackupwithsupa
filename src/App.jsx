// src/App.jsx
import { Routes, Route, Navigate } from "react-router-dom";
import LoginPage from "./pages/LoginPage";     // file must be src/pages/LoginPage.jsx
import Dashboard from "./pages/Dashboard";     // file must be src/pages/Dashboard.jsx
import ProtectedRoute from "./components/ProtectedRoute";

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        path="/dashboard/*"
        element={
          <ProtectedRoute>
            <Dashboard />
          </ProtectedRoute>
        }
      />
      {/* fallback to login or dashboard as you prefer */}
      <Route path="*" element={<Navigate to="/login" replace />} />
    </Routes>
  );
}
