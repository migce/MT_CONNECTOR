import { useEffect, useState } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar } from 'recharts'
import api from '../../api/client'

interface Health {
  status: string
  mt5_connected: boolean
  db_connected: boolean
  redis_connected: boolean
  uptime_sec: number
  symbols_active: number
}

interface DailyStat {
  date: string
  ticks_received: number
  candles_upserted: number
  poller_errors: number
  api_requests: number
  api_errors: number
}

export default function AdminSystem() {
  const [health, setHealth] = useState<Health | null>(null)
  const [stats, setStats] = useState<any>(null)
  const [daily, setDaily] = useState<DailyStat[]>([])
  const [uptime, setUptime] = useState<any>(null)

  useEffect(() => {
    api.get('/system/health').then((r) => setHealth(r.data)).catch(() => {})
    api.get('/system/stats').then((r) => setStats(r.data)).catch(() => {})
    api.get('/system/stats/daily', { params: { limit: 30 } }).then((r) => {
      const rows = r.data.data || r.data
      setDaily(Array.isArray(rows) ? [...rows].reverse() : rows)
    }).catch(() => {})
    api.get('/system/uptime').then((r) => setUptime(r.data)).catch(() => {})
  }, [])

  return (
    <div className="space-y-8">
      <h2 className="text-2xl font-bold text-gray-100">System Monitor</h2>

      {/* Health cards */}
      {health && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
          <HealthCard label="Status" value={health.status} ok={health.status === 'ok'} />
          <HealthCard label="MT5" value={health.mt5_connected ? 'Connected' : 'Down'} ok={health.mt5_connected} />
          <HealthCard label="Database" value={health.db_connected ? 'OK' : 'Down'} ok={health.db_connected} />
          <HealthCard label="Redis" value={health.redis_connected ? 'OK' : 'Down'} ok={health.redis_connected} />
          <HealthCard label="Symbols" value={String(health.symbols_active)} ok={health.symbols_active > 0} />
        </div>
      )}

      {/* Live stats */}
      {stats && (
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <h3 className="text-lg font-semibold text-gray-200 mb-4">Live API Stats</h3>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <StatItem label="Total requests" value={stats.total_requests?.toLocaleString()} />
            <StatItem label="Total errors" value={stats.total_errors?.toLocaleString()} />
            <StatItem label="Requests (1h)" value={stats.requests_1h?.toLocaleString()} />
            <StatItem label="Avg latency (1h)" value={`${stats.avg_latency_ms_1h?.toFixed(1)} ms`} />
          </div>
        </div>
      )}

      {/* Uptime table */}
      {uptime && (
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <h3 className="text-lg font-semibold text-gray-200 mb-4">Uptime</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <UptimeBlock title="Last 24h" data={uptime.period_24h} />
            <UptimeBlock title="Last 30d" data={uptime.period_30d} />
          </div>
        </div>
      )}

      {/* Daily charts */}
      {daily.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
            <h3 className="text-sm font-semibold text-gray-400 mb-3">Ticks / Day</h3>
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={daily}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="date" tick={{ fill: '#6b7280', fontSize: 10 }} />
                <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} />
                <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }} />
                <Bar dataKey="ticks_received" fill="#6366f1" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
            <h3 className="text-sm font-semibold text-gray-400 mb-3">API Requests / Day</h3>
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={daily}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="date" tick={{ fill: '#6b7280', fontSize: 10 }} />
                <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} />
                <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }} />
                <Bar dataKey="api_requests" fill="#22c55e" radius={[4, 4, 0, 0]} />
                <Bar dataKey="api_errors" fill="#ef4444" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
            <h3 className="text-sm font-semibold text-gray-400 mb-3">Poller Errors / Day</h3>
            <ResponsiveContainer width="100%" height={250}>
              <LineChart data={daily}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="date" tick={{ fill: '#6b7280', fontSize: 10 }} />
                <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} />
                <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }} />
                <Line type="monotone" dataKey="poller_errors" stroke="#ef4444" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>

          <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
            <h3 className="text-sm font-semibold text-gray-400 mb-3">Candles / Day</h3>
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={daily}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="date" tick={{ fill: '#6b7280', fontSize: 10 }} />
                <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} />
                <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }} />
                <Bar dataKey="candles_upserted" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </div>
  )
}

function HealthCard({ label, value, ok }: { label: string; value: string; ok: boolean }) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
      <p className="text-xs text-gray-500 mb-1">{label}</p>
      <p className={`text-lg font-semibold ${ok ? 'text-green-400' : 'text-red-400'}`}>{value}</p>
    </div>
  )
}

function StatItem({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-gray-500 text-xs">{label}</p>
      <p className="text-gray-200 font-medium">{value}</p>
    </div>
  )
}

function UptimeBlock({ title, data }: { title: string; data: any[] }) {
  if (!data || data.length === 0) return null
  return (
    <div>
      <p className="text-sm text-gray-400 mb-2">{title}</p>
      <div className="space-y-2">
        {data.map((entry: any) => {
          const pct = entry.uptime_pct ?? 0
          const color = pct >= 99.9 ? 'text-green-400' : pct >= 95 ? 'text-yellow-400' : 'text-red-400'
          return (
            <div key={entry.service} className="flex justify-between text-sm">
              <span className="text-gray-300 capitalize">{entry.service}</span>
              <span className={`font-medium ${color}`}>{pct.toFixed(2)}%</span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
