import { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart } from 'recharts'
import api from '../api/client'
import { ArrowLeft } from 'lucide-react'

export default function SpreadAnalytics() {
  const { id } = useParams<{ id: string }>()
  const [symbols, setSymbols] = useState<string[]>([])
  const [symbol, setSymbol] = useState('')
  const [data, setData] = useState<any[]>([])
  const [bucket, setBucket] = useState('15min')
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    api.get('/market/symbols').then((r) => {
      const syms = r.data.map((s: any) => s.symbol)
      setSymbols(syms)
      if (syms.length > 0 && !symbol) setSymbol(syms[0])
    })
  }, [])

  useEffect(() => {
    if (!symbol) return
    setLoading(true)
    api
      .get(`/market/spread/${symbol}`, {
        params: { source: 'ticks_agg', bucket, limit: 200 },
      })
      .then((r) => {
        const items = (r.data.data || r.data).map((p: any) => ({
          time: new Date(p.time).toLocaleString(),
          avg: p.spread_avg,
          min: p.spread_min,
          max: p.spread_max,
        }))
        setData(items)
      })
      .catch(() => setData([]))
      .finally(() => setLoading(false))
  }, [symbol, bucket])

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Link to={`/accounts/${id}`} className="text-gray-500 hover:text-gray-300 transition">
          <ArrowLeft size={20} />
        </Link>
        <h2 className="text-2xl font-bold text-gray-100">Spread Analytics</h2>
      </div>

      <div className="flex gap-3 flex-wrap">
        <select
          value={symbol}
          onChange={(e) => setSymbol(e.target.value)}
          className="px-3 py-2 bg-gray-900 border border-gray-700 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-brand-500"
        >
          {symbols.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>

        <div className="flex rounded-lg overflow-hidden border border-gray-700">
          {['1min', '5min', '15min', '1h', '4h', '1d'].map((b) => (
            <button
              key={b}
              onClick={() => setBucket(b)}
              className={`px-3 py-2 text-sm transition ${
                bucket === b
                  ? 'bg-brand-600 text-white'
                  : 'bg-gray-900 text-gray-400 hover:text-gray-200'
              }`}
            >
              {b}
            </button>
          ))}
        </div>
      </div>

      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
        {loading ? (
          <p className="text-gray-500 text-sm">Loading...</p>
        ) : data.length === 0 ? (
          <p className="text-gray-500 text-sm">No spread data available</p>
        ) : (
          <ResponsiveContainer width="100%" height={400}>
            <AreaChart data={data}>
              <defs>
                <linearGradient id="spreadGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#6366f1" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#6366f1" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis dataKey="time" tick={{ fill: '#6b7280', fontSize: 11 }} />
              <YAxis tick={{ fill: '#6b7280', fontSize: 11 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                labelStyle={{ color: '#9ca3af' }}
              />
              <Area type="monotone" dataKey="max" stroke="#ef4444" fill="none" strokeWidth={1} dot={false} />
              <Area type="monotone" dataKey="avg" stroke="#6366f1" fill="url(#spreadGrad)" strokeWidth={2} dot={false} />
              <Area type="monotone" dataKey="min" stroke="#22c55e" fill="none" strokeWidth={1} dot={false} />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
}
