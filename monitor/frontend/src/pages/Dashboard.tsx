import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import api from '../api/client'
import { useAuth } from '../auth/AuthContext'
import { Activity, TrendingUp, Wallet } from 'lucide-react'

interface Account {
  id: number
  label: string
  mt5_login: number
  mt5_server: string
  enabled: boolean
}

interface Health {
  status: string
  mt5_connected: boolean
  db_connected: boolean
  redis_connected: boolean
  uptime_sec: number
  symbols_active: number
}

export default function Dashboard() {
  const { user } = useAuth()
  const [accounts, setAccounts] = useState<Account[]>([])
  const [health, setHealth] = useState<Health | null>(null)

  useEffect(() => {
    api.get('/trading/accounts').then((r) => setAccounts(r.data))
    api.get('/system/health').then((r) => setHealth(r.data)).catch(() => {})
  }, [])

  const uptimeStr = health
    ? `${Math.floor(health.uptime_sec / 3600)}h ${Math.floor((health.uptime_sec % 3600) / 60)}m`
    : '—'

  return (
    <div className="space-y-8">
      <div>
        <h2 className="text-2xl font-bold text-gray-100">Dashboard</h2>
        <p className="text-sm text-gray-500 mt-1">Welcome back, {user?.username}</p>
      </div>

      {/* Status cards */}
      {health && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <StatusCard
            label="MT5"
            ok={health.mt5_connected}
            detail={health.mt5_connected ? 'Connected' : 'Disconnected'}
          />
          <StatusCard
            label="Database"
            ok={health.db_connected}
            detail={health.db_connected ? 'Healthy' : 'Down'}
          />
          <StatusCard
            label="Redis"
            ok={health.redis_connected}
            detail={health.redis_connected ? 'Healthy' : 'Down'}
          />
          <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
            <div className="flex items-center gap-2 text-gray-400 text-sm mb-1">
              <Activity size={16} /> Uptime
            </div>
            <p className="text-xl font-semibold text-gray-100">{uptimeStr}</p>
            <p className="text-xs text-gray-500 mt-1">{health.symbols_active} symbols active</p>
          </div>
        </div>
      )}

      {/* Accounts grid */}
      <div>
        <h3 className="text-lg font-semibold text-gray-200 mb-4">
          <Wallet size={20} className="inline mr-2 text-brand-400" />
          Trading Accounts
        </h3>
        {accounts.length === 0 ? (
          <p className="text-gray-500 text-sm">No accounts available.</p>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {accounts.map((acc) => (
              <Link
                key={acc.id}
                to={`/accounts/${acc.id}`}
                className="bg-gray-900 border border-gray-800 rounded-xl p-5 hover:border-brand-600/50 transition group"
              >
                <div className="flex items-center justify-between mb-3">
                  <h4 className="font-semibold text-gray-100 group-hover:text-brand-400 transition">
                    {acc.label}
                  </h4>
                  <span
                    className={`w-2.5 h-2.5 rounded-full ${
                      acc.enabled ? 'bg-green-400' : 'bg-gray-600'
                    }`}
                  />
                </div>
                <div className="text-sm text-gray-400 space-y-1">
                  <p>Login: {acc.mt5_login}</p>
                  <p className="truncate">Server: {acc.mt5_server}</p>
                </div>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function StatusCard({ label, ok, detail }: { label: string; ok: boolean; detail: string }) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
      <div className="flex items-center gap-2 text-gray-400 text-sm mb-1">
        <span className={`w-2 h-2 rounded-full ${ok ? 'bg-green-400' : 'bg-red-400'}`} />
        {label}
      </div>
      <p className={`text-lg font-semibold ${ok ? 'text-green-400' : 'text-red-400'}`}>
        {detail}
      </p>
    </div>
  )
}
