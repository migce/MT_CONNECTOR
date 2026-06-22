import { Routes, Route, Navigate } from 'react-router-dom'
import { useAuth } from './auth/AuthContext'
import Login from './pages/Login'
import Dashboard from './pages/Dashboard'
import AccountDetail from './pages/AccountDetail'
import Deals from './pages/Deals'
import Positions from './pages/Positions'
import ChartPage from './pages/ChartPage'
import DataMonitor from './pages/DataMonitor'
import SpreadAnalytics from './pages/SpreadAnalytics'
import AdminUsers from './pages/admin/Users'
import AdminSystem from './pages/admin/System'
import AdminAccounts from './pages/admin/Accounts'
import Settings from './pages/Settings'
import Layout from './components/Layout'

function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const { user } = useAuth()
  if (!user) return <Navigate to="/login" replace />
  return <>{children}</>
}

function AdminRoute({ children }: { children: React.ReactNode }) {
  const { user } = useAuth()
  if (!user) return <Navigate to="/login" replace />
  if (user.role !== 'admin') return <Navigate to="/dashboard" replace />
  return <>{children}</>
}

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route path="/" element={<ProtectedRoute><Layout /></ProtectedRoute>}>
        <Route index element={<Navigate to="/dashboard" replace />} />
        <Route path="dashboard" element={<Dashboard />} />
        <Route path="data" element={<DataMonitor />} />
        <Route path="accounts/:id" element={<AccountDetail />} />
        <Route path="accounts/:id/deals" element={<Deals />} />
        <Route path="accounts/:id/positions" element={<Positions />} />
        <Route path="accounts/:id/chart" element={<ChartPage />} />
        <Route path="accounts/:id/spread" element={<SpreadAnalytics />} />
        <Route path="settings" element={<Settings />} />
        {/* Admin routes */}
        <Route path="admin/users" element={<AdminRoute><AdminUsers /></AdminRoute>} />
        <Route path="admin/system" element={<AdminRoute><AdminSystem /></AdminRoute>} />
        <Route path="admin/accounts" element={<AdminRoute><AdminAccounts /></AdminRoute>} />
      </Route>
    </Routes>
  )
}
