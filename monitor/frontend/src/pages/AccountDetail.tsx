import { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import api from '../api/client'
import { BarChart3, CandlestickChart, TrendingUp, ArrowLeft } from 'lucide-react'

interface Account {
  id: number
  label: string
  mt5_login: number
  mt5_server: string
  enabled: boolean
}

interface Position {
  ticket: number
  symbol: string
  type: number
  volume: number
  price_open: number
  price_current: number
  profit: number
}

export default function AccountDetail() {
  const { id } = useParams<{ id: string }>()
  const [account, setAccount] = useState<Account | null>(null)
  const [positions, setPositions] = useState<Position[]>([])

  useEffect(() => {
    api.get('/trading/accounts').then((r) => {
      const acc = r.data.find((a: Account) => a.id === Number(id))
      setAccount(acc || null)
    })
    api.get(`/trading/positions/${id}`).then((r) => setPositions(r.data)).catch(() => {})
  }, [id])

  const totalProfit = positions.reduce((sum, p) => sum + p.profit, 0)

  if (!account) return <p className="text-gray-500">Loading...</p>

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Link to="/dashboard" className="text-gray-500 hover:text-gray-300 transition">
          <ArrowLeft size={20} />
        </Link>
        <div>
          <h2 className="text-2xl font-bold text-gray-100">{account.label}</h2>
          <p className="text-sm text-gray-500">Login {account.mt5_login} · {account.mt5_server}</p>
        </div>
      </div>

      {/* Quick links */}
      <div className="flex gap-3">
        <Link to={`/accounts/${id}/positions`} className="flex items-center gap-2 px-4 py-2 bg-gray-900 border border-gray-800 rounded-lg text-sm text-gray-300 hover:border-brand-600/50 transition">
          <TrendingUp size={16} /> Positions
        </Link>
        <Link to={`/accounts/${id}/deals`} className="flex items-center gap-2 px-4 py-2 bg-gray-900 border border-gray-800 rounded-lg text-sm text-gray-300 hover:border-brand-600/50 transition">
          <BarChart3 size={16} /> Deals
        </Link>
        <Link to={`/accounts/${id}/chart`} className="flex items-center gap-2 px-4 py-2 bg-gray-900 border border-gray-800 rounded-lg text-sm text-gray-300 hover:border-brand-600/50 transition">
          <CandlestickChart size={16} /> Chart
        </Link>
        <Link to={`/accounts/${id}/spread`} className="flex items-center gap-2 px-4 py-2 bg-gray-900 border border-gray-800 rounded-lg text-sm text-gray-300 hover:border-brand-600/50 transition">
          <BarChart3 size={16} /> Spread
        </Link>
      </div>

      {/* Open positions summary */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <h3 className="text-lg font-semibold text-gray-200 mb-4">Open Positions</h3>
        {positions.length === 0 ? (
          <p className="text-sm text-gray-500">No open positions</p>
        ) : (
          <>
            <div className="mb-3 text-sm">
              <span className="text-gray-400">Total P/L: </span>
              <span className={totalProfit >= 0 ? 'text-green-400 font-semibold' : 'text-red-400 font-semibold'}>
                {totalProfit >= 0 ? '+' : ''}{totalProfit.toFixed(2)}
              </span>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-gray-500 border-b border-gray-800">
                    <th className="text-left py-2 px-2">Symbol</th>
                    <th className="text-left py-2 px-2">Type</th>
                    <th className="text-right py-2 px-2">Volume</th>
                    <th className="text-right py-2 px-2">Open</th>
                    <th className="text-right py-2 px-2">Current</th>
                    <th className="text-right py-2 px-2">Profit</th>
                  </tr>
                </thead>
                <tbody>
                  {positions.map((p) => (
                    <tr key={p.ticket} className="border-b border-gray-800/50 hover:bg-gray-800/30">
                      <td className="py-2 px-2 text-gray-200">{p.symbol}</td>
                      <td className="py-2 px-2">
                        <span className={p.type === 0 ? 'text-green-400' : 'text-red-400'}>
                          {p.type === 0 ? 'BUY' : 'SELL'}
                        </span>
                      </td>
                      <td className="py-2 px-2 text-right text-gray-300">{p.volume}</td>
                      <td className="py-2 px-2 text-right text-gray-400">{p.price_open}</td>
                      <td className="py-2 px-2 text-right text-gray-300">{p.price_current}</td>
                      <td className={`py-2 px-2 text-right font-medium ${p.profit >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                        {p.profit >= 0 ? '+' : ''}{p.profit.toFixed(2)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    </div>
  )
}
