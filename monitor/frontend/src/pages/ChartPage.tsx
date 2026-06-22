import { useEffect, useRef, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { createChart, ColorType, IChartApi } from 'lightweight-charts'
import api from '../api/client'
import { ArrowLeft } from 'lucide-react'

const STANDARD_TFS = ['M1', 'M5', 'M15', 'H1', 'H4', 'D1']
const PRESETS = ['M1', 'M5', 'M15', 'H1', 'H4', 'D1']

const CUSTOM_UNITS = [
  { value: 'M', label: 'Minutes' },
  { value: 'H', label: 'Hours' },
  { value: 'D', label: 'Days' },
  { value: 'W', label: 'Weeks' },
  { value: 'T', label: 'Ticks' },
]

function candleUrl(symbol: string, tf: string) {
  if (STANDARD_TFS.includes(tf)) return `/market/candles/${symbol}`
  return `/market/candles/custom/${symbol}`
}

function isValidTf(tf: string): boolean {
  return /^[MHDWT]\d+$/i.test(tf.trim())
}

export default function ChartPage() {
  const { id } = useParams<{ id: string }>()
  const chartRef = useRef<HTMLDivElement>(null)
  const chartApi = useRef<IChartApi | null>(null)
  const [symbols, setSymbols] = useState<string[]>([])
  const [symbol, setSymbol] = useState('')
  const [timeframe, setTimeframe] = useState('H1')
  // Custom TF builder
  const [customUnit, setCustomUnit] = useState('M')
  const [customValue, setCustomValue] = useState('3')

  useEffect(() => {
    api.get('/market/symbols').then((r) => {
      const syms = r.data.map((s: any) => s.symbol)
      setSymbols(syms)
      if (syms.length > 0 && !symbol) setSymbol(syms[0])
    })
  }, [])

  const applyCustomTf = () => {
    const v = parseInt(customValue, 10)
    if (!v || v <= 0) return
    const tf = `${customUnit}${v}`.toUpperCase()
    if (isValidTf(tf)) setTimeframe(tf)
  }

  useEffect(() => {
    if (!chartRef.current || !symbol) return

    // Clean up previous chart
    if (chartApi.current) {
      chartApi.current.remove()
      chartApi.current = null
    }

    const chart = createChart(chartRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: '#0a0a0f' },
        textColor: '#6b7280',
      },
      grid: {
        vertLines: { color: '#1f2937' },
        horzLines: { color: '#1f2937' },
      },
      width: chartRef.current.clientWidth,
      height: 500,
      crosshair: {
        vertLine: { color: '#4f46e5', width: 1, style: 2 },
        horzLine: { color: '#4f46e5', width: 1, style: 2 },
      },
    })
    chartApi.current = chart

    const candleSeries = chart.addCandlestickSeries({
      upColor: '#22c55e',
      downColor: '#ef4444',
      borderDownColor: '#ef4444',
      borderUpColor: '#22c55e',
      wickDownColor: '#ef4444',
      wickUpColor: '#22c55e',
    })

    api
      .get(candleUrl(symbol, timeframe), {
        params: { timeframe, limit: 500 },
      })
      .then((r) => {
        const data = (r.data.data || r.data).map((c: any) => ({
          time: Math.floor(new Date(c.time).getTime() / 1000),
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close,
        }))
        candleSeries.setData(data)
        chart.timeScale().fitContent()
      })
      .catch(() => {})

    const handleResize = () => {
      if (chartRef.current) {
        chart.applyOptions({ width: chartRef.current.clientWidth })
      }
    }
    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      chart.remove()
      chartApi.current = null
    }
  }, [symbol, timeframe])

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Link to={`/accounts/${id}`} className="text-gray-500 hover:text-gray-300 transition">
          <ArrowLeft size={20} />
        </Link>
        <h2 className="text-2xl font-bold text-gray-100">Chart</h2>
      </div>

      {/* Controls */}
      <div className="flex gap-3 flex-wrap items-center">
        <select
          value={symbol}
          onChange={(e) => setSymbol(e.target.value)}
          className="px-3 py-2 bg-gray-900 border border-gray-700 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-brand-500"
        >
          {symbols.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>

        {/* Preset timeframes */}
        <div className="flex rounded-lg overflow-hidden border border-gray-700">
          {PRESETS.map((tf) => (
            <button
              key={tf}
              onClick={() => setTimeframe(tf)}
              className={`px-3 py-2 text-sm transition ${
                timeframe === tf
                  ? 'bg-brand-600 text-white'
                  : 'bg-gray-900 text-gray-400 hover:text-gray-200'
              }`}
            >
              {tf}
            </button>
          ))}
        </div>

        {/* Custom timeframe builder */}
        <div className="flex items-center gap-1 border border-gray-700 rounded-lg overflow-hidden">
          <select
            value={customUnit}
            onChange={(e) => setCustomUnit(e.target.value)}
            className="px-2 py-2 bg-gray-900 text-gray-200 text-sm border-none focus:outline-none"
          >
            {CUSTOM_UNITS.map((u) => (
              <option key={u.value} value={u.value}>{u.label}</option>
            ))}
          </select>
          <input
            type="number"
            min={1}
            value={customValue}
            onChange={(e) => setCustomValue(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && applyCustomTf()}
            className="w-16 px-2 py-2 bg-gray-900 text-gray-200 text-sm border-none focus:outline-none text-center"
            placeholder="#"
          />
          <button
            onClick={applyCustomTf}
            className="px-3 py-2 bg-brand-600 hover:bg-brand-500 text-white text-sm font-medium transition"
          >
            Go
          </button>
        </div>

        {/* Active TF indicator */}
        {!PRESETS.includes(timeframe) && (
          <span className="px-3 py-1.5 bg-amber-600/20 text-amber-400 text-sm font-medium rounded-lg border border-amber-600/30">
            {timeframe}
          </span>
        )}
      </div>

      {/* Chart container */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-3">
        <div ref={chartRef} />
      </div>
    </div>
  )
}
