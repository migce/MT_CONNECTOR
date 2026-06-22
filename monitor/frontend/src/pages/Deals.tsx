import { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import api from '../api/client'
import { ArrowLeft } from 'lucide-react'

interface Deal {
  ticket: number
  symbol: string
  type: number
  entry: number
  volume: number
  price: number
  profit: number
  commission: number
  swap: number
  time: string
  comment: string
}

export default function Deals() {
  const { id } = useParams<{ id: string }>()
  const [deals, setDeals] = useState<Deal[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    api
      .get(`/trading/deals/${id}`)
      .then((r) => setDeals(r.data.data || r.data))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [id])

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Link to={`/accounts/${id}`} className="text-gray-500 hover:text-gray-300 transition">
          <ArrowLeft size={20} />
        </Link>
        <h2 className="text-2xl font-bold text-gray-100">Deal History</h2>
      </div>

      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5 overflow-x-auto">
        {loading ? (
          <p className="text-gray-500 text-sm">Loading...</p>
        ) : deals.length === 0 ? (
          <p className="text-gray-500 text-sm">No deals found</p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-500 border-b border-gray-800">
                <th className="text-left py-2 px-2">Time</th>
                <th className="text-left py-2 px-2">Symbol</th>
                <th className="text-left py-2 px-2">Type</th>
                <th className="text-right py-2 px-2">Volume</th>
                <th className="text-right py-2 px-2">Price</th>
                <th className="text-right py-2 px-2">Profit</th>
                <th className="text-right py-2 px-2">Commission</th>
                <th className="text-right py-2 px-2">Swap</th>
              </tr>
            </thead>
            <tbody>
              {deals.map((d) => (
                <tr key={d.ticket} className="border-b border-gray-800/50 hover:bg-gray-800/30">
                  <td className="py-2 px-2 text-gray-400 whitespace-nowrap">
                    {new Date(d.time).toLocaleString()}
                  </td>
                  <td className="py-2 px-2 text-gray-200">{d.symbol}</td>
                  <td className="py-2 px-2">
                    <span className={d.type === 0 ? 'text-green-400' : 'text-red-400'}>
                      {d.type === 0 ? 'BUY' : 'SELL'}
                    </span>
                  </td>
                  <td className="py-2 px-2 text-right text-gray-300">{d.volume}</td>
                  <td className="py-2 px-2 text-right text-gray-400">{d.price}</td>
                  <td className={`py-2 px-2 text-right font-medium ${d.profit >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {d.profit >= 0 ? '+' : ''}{d.profit.toFixed(2)}
                  </td>
                  <td className="py-2 px-2 text-right text-gray-500">{d.commission.toFixed(2)}</td>
                  <td className="py-2 px-2 text-right text-gray-500">{d.swap.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
