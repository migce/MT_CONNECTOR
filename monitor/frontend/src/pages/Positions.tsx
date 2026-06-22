import { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import api from '../api/client'
import { ArrowLeft } from 'lucide-react'

interface Position {
  ticket: number
  symbol: string
  type: number
  volume: number
  price_open: number
  price_current: number
  sl: number
  tp: number
  swap: number
  profit: number
  time: string
}

export default function Positions() {
  const { id } = useParams<{ id: string }>()
  const [positions, setPositions] = useState<Position[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    api
      .get(`/trading/positions/${id}`)
      .then((r) => setPositions(r.data))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [id])

  const totalProfit = positions.reduce((sum, p) => sum + p.profit, 0)

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Link to={`/accounts/${id}`} className="text-gray-500 hover:text-gray-300 transition">
          <ArrowLeft size={20} />
        </Link>
        <h2 className="text-2xl font-bold text-gray-100">Open Positions</h2>
        <span className={`ml-auto text-lg font-semibold ${totalProfit >= 0 ? 'text-green-400' : 'text-red-400'}`}>
          P/L: {totalProfit >= 0 ? '+' : ''}{totalProfit.toFixed(2)}
        </span>
      </div>

      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5 overflow-x-auto">
        {loading ? (
          <p className="text-gray-500 text-sm">Loading...</p>
        ) : positions.length === 0 ? (
          <p className="text-gray-500 text-sm">No open positions</p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-500 border-b border-gray-800">
                <th className="text-left py-2 px-2">Ticket</th>
                <th className="text-left py-2 px-2">Symbol</th>
                <th className="text-left py-2 px-2">Type</th>
                <th className="text-right py-2 px-2">Volume</th>
                <th className="text-right py-2 px-2">Open</th>
                <th className="text-right py-2 px-2">Current</th>
                <th className="text-right py-2 px-2">SL</th>
                <th className="text-right py-2 px-2">TP</th>
                <th className="text-right py-2 px-2">Swap</th>
                <th className="text-right py-2 px-2">Profit</th>
              </tr>
            </thead>
            <tbody>
              {positions.map((p) => (
                <tr key={p.ticket} className="border-b border-gray-800/50 hover:bg-gray-800/30">
                  <td className="py-2 px-2 text-gray-500">{p.ticket}</td>
                  <td className="py-2 px-2 text-gray-200">{p.symbol}</td>
                  <td className="py-2 px-2">
                    <span className={p.type === 0 ? 'text-green-400' : 'text-red-400'}>
                      {p.type === 0 ? 'BUY' : 'SELL'}
                    </span>
                  </td>
                  <td className="py-2 px-2 text-right text-gray-300">{p.volume}</td>
                  <td className="py-2 px-2 text-right text-gray-400">{p.price_open}</td>
                  <td className="py-2 px-2 text-right text-gray-300">{p.price_current}</td>
                  <td className="py-2 px-2 text-right text-gray-500">{p.sl || '—'}</td>
                  <td className="py-2 px-2 text-right text-gray-500">{p.tp || '—'}</td>
                  <td className="py-2 px-2 text-right text-gray-500">{p.swap.toFixed(2)}</td>
                  <td className={`py-2 px-2 text-right font-medium ${p.profit >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {p.profit >= 0 ? '+' : ''}{p.profit.toFixed(2)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
