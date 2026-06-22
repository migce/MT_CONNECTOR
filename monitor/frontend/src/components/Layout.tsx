import { Outlet, NavLink, useNavigate } from 'react-router-dom'
import { useAuth } from '../auth/AuthContext'
import {
  LayoutDashboard,
  Settings,
  Users,
  Activity,
  Wallet,
  LogOut,
  BarChart3,
} from 'lucide-react'

const linkClass = ({ isActive }: { isActive: boolean }) =>
  `flex items-center gap-3 px-4 py-2.5 rounded-lg text-sm transition-colors ${
    isActive
      ? 'bg-brand-600/20 text-brand-400 font-medium'
      : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800/50'
  }`

export default function Layout() {
  const { user, logout } = useAuth()
  const navigate = useNavigate()

  const handleLogout = () => {
    logout()
    navigate('/login')
  }

  return (
    <div className="flex h-screen">
      {/* Sidebar */}
      <aside className="w-60 flex flex-col bg-gray-900 border-r border-gray-800">
        <div className="px-5 py-6">
          <h1 className="text-lg font-bold text-brand-400">MT5 Monitor</h1>
          <p className="text-xs text-gray-500 mt-1">{user?.username} · {user?.role}</p>
        </div>

        <nav className="flex-1 px-3 space-y-1">
          <NavLink to="/dashboard" className={linkClass}>
            <LayoutDashboard size={18} /> Dashboard
          </NavLink>
          <NavLink to="/data" className={linkClass}>
            <BarChart3 size={18} /> Data Monitor
          </NavLink>
          <NavLink to="/settings" className={linkClass}>
            <Settings size={18} /> Settings
          </NavLink>

          {user?.role === 'admin' && (
            <>
              <div className="pt-4 pb-2 px-4">
                <span className="text-xs font-semibold uppercase tracking-wider text-gray-600">
                  Admin
                </span>
              </div>
              <NavLink to="/admin/users" className={linkClass}>
                <Users size={18} /> Users
              </NavLink>
              <NavLink to="/admin/accounts" className={linkClass}>
                <Wallet size={18} /> MT5 Accounts
              </NavLink>
              <NavLink to="/admin/system" className={linkClass}>
                <Activity size={18} /> System
              </NavLink>
            </>
          )}
        </nav>

        <div className="px-3 pb-4">
          <button
            onClick={handleLogout}
            className="flex items-center gap-3 px-4 py-2.5 rounded-lg text-sm text-gray-400 hover:text-red-400 hover:bg-gray-800/50 w-full transition-colors"
          >
            <LogOut size={18} /> Logout
          </button>
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-auto bg-gray-950 p-6">
        <Outlet />
      </main>
    </div>
  )
}
