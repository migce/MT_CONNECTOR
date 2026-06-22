import { useEffect, useRef, useState, useCallback } from 'react'
import { createChart, ColorType, IChartApi } from 'lightweight-charts'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts'
import api from '../api/client'

const STANDARD_TFS = ['M1', 'M5', 'M15', 'H1', 'H4', 'D1']
const PRESETS = ['M1', 'M5', 'M15', 'H1', 'H4', 'D1']
const MAX_RETRIES = 3
const RETRY_DELAY = 2000

const CUSTOM_UNITS = [
  { value: 'M', label: 'Min' },
  { value: 'H', label: 'Hour' },
  { value: 'D', label: 'Day' },
  { value: 'W', label: 'Week' },
  { value: 'T', label: 'Tick' },
]

function candleUrl(symbol: string, tf: string) {
  if (STANDARD_TFS.includes(tf)) return `/market/candles/${symbol}`
  return `/market/candles/custom/${symbol}`
}

type Tab = 'chart' | 'details'

interface SpreadBucket {
  label: string
  min: number
  max: number
  avg: number
}

export default function DataMonitor() {
  const chartRef = useRef<HTMLDivElement>(null)
  const chartApi = useRef<IChartApi | null>(null)
  const [symbols, setSymbols] = useState<string[]>([])
  const [symbol, setSymbol] = useState('')
  const [timeframe, setTimeframe] = useState('H1')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [empty, setEmpty] = useState(false)
  const [retryKey, setRetryKey] = useState(0)
  const [tab, setTab] = useState<Tab>('chart')
  const [spreadData, setSpreadData] = useState<SpreadBucket[]>([])
  const [spreadLoading, setSpreadLoading] = useState(false)
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
    if (/^[MHDWT]\d+$/i.test(tf)) setTimeframe(tf)
  }

  const fetchCandles = useCallback(
    async (chart: IChartApi, candleSeries: any, attempt = 0): Promise<void> => {
      try {
        const r = await api.get(candleUrl(symbol, timeframe), {
          params: { timeframe, limit: 500 },
        })
        const raw = r.data.data || r.data
        if (!raw || raw.length === 0) {
          setEmpty(true)
          setLoading(false)
          return
        }
        const data = raw.map((c: any) => ({
          time: Math.floor(new Date(c.time).getTime() / 1000),
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close,
        }))

        // Detect precision from actual data values
        const samplePrices = raw.slice(0, 20).flatMap((c: any) => [c.open, c.close, c.high, c.low])
        const precision = Math.max(
          ...samplePrices.map((p: number) => {
            const s = p.toString()
            const dot = s.indexOf('.')
            return dot === -1 ? 0 : s.length - dot - 1
          }),
        )
        candleSeries.applyOptions({
          priceFormat: { type: 'price', precision, minMove: 1 / Math.pow(10, precision) },
        })

        candleSeries.setData(data)
        chart.timeScale().fitContent()
        setLoading(false)
      } catch (err: any) {
        const status = err?.response?.status
        if (status === 429 && attempt < MAX_RETRIES) {
          setError(`Rate limited — retrying (${attempt + 1}/${MAX_RETRIES})...`)
          await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY))
          return fetchCandles(chart, candleSeries, attempt + 1)
        }
        const msg =
          err?.response?.data?.detail ||
          err?.response?.data?.message ||
          err?.message ||
          'Failed to load candles'
        setError(msg)
        setLoading(false)
      }
    },
    [symbol, timeframe],
  )

  useEffect(() => {
    if (!chartRef.current || !symbol) return

    if (chartApi.current) {
      chartApi.current.remove()
      chartApi.current = null
    }

    setLoading(true)
    setError('')
    setEmpty(false)

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
      height: chartRef.current.clientHeight || 500,
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
    let cancelled = false
    ;(async () => {
      await fetchCandles(chart, candleSeries)
      // If cleanup already ran, chart was removed — don't touch it
      if (cancelled) return
    })()

    const handleResize = () => {
      if (chartRef.current) {
        chart.applyOptions({
          width: chartRef.current.clientWidth,
          height: chartRef.current.clientHeight,
        })
      }
    }
    window.addEventListener('resize', handleResize)

    return () => {
      cancelled = true
      window.removeEventListener('resize', handleResize)
      chart.remove()
      chartApi.current = null
    }
  }, [symbol, timeframe, fetchCandles, retryKey])

  // Fetch spread data for Symbol Details tab
  useEffect(() => {
    if (!symbol || tab !== 'details') return
    setSpreadLoading(true)

    // Fetch the last 24 5-min buckets (= 2 hours of data) — no `from` filter
    // so it works even if the latest data isn't from "now"
    api
      .get(`/market/spread/${symbol}`, {
        params: { source: 'ticks_agg', bucket: '5 min', limit: 24 },
      })
      .then((r) => {
        const raw = r.data.data || r.data
        if (!raw || raw.length === 0) {
          setSpreadData([])
          setSpreadLoading(false)
          return
        }

        // Detect pip size from the first avg value
        // For 5-digit pairs (EURUSD) pip ≈ 0.00001, for 3-digit (USDJPY) pip ≈ 0.001
        const sampleAvg = raw[0].spread_avg
        let pipMultiplier = 1
        if (sampleAvg < 0.01) {
          // 5-digit pair: convert to pips (÷ 0.00001)
          pipMultiplier = sampleAvg < 0.001 ? 100000 : 1000
        }

        const result: SpreadBucket[] = raw.map((pt: any) => {
          const d = new Date(pt.time)
          const label = `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
          return {
            label,
            min: +(pt.spread_min * pipMultiplier).toFixed(1),
            avg: +(pt.spread_avg * pipMultiplier).toFixed(1),
            max: +(pt.spread_max * pipMultiplier).toFixed(1),
          }
        })
        setSpreadData(result)
        setSpreadLoading(false)
      })
      .catch(() => {
        setSpreadData([])
        setSpreadLoading(false)
      })
  }, [symbol, tab])

  return (
    <div className="flex h-full gap-4">
      {/* Symbol list — left panel */}
      <div className="w-44 shrink-0 bg-gray-900 border border-gray-800 rounded-xl overflow-hidden flex flex-col">
        <div className="px-3 py-2.5 border-b border-gray-800">
          <span className="text-xs font-semibold uppercase tracking-wider text-gray-500">Symbols</span>
        </div>
        <div className="flex-1 overflow-y-auto">
          {symbols.map((s) => (
            <button
              key={s}
              onClick={() => setSymbol(s)}
              className={`w-full text-left px-3 py-2 text-sm transition-colors ${
                symbol === s
                  ? 'bg-brand-600/20 text-brand-400 font-medium'
                  : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800/50'
              }`}
            >
              {s}
            </button>
          ))}
        </div>
      </div>

      {/* Right panel with tabs */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Tab bar */}
        <div className="flex items-center gap-1 mb-3">
          <button
            onClick={() => setTab('chart')}
            className={`px-4 py-1.5 text-xs font-medium rounded-t-lg transition ${
              tab === 'chart'
                ? 'bg-gray-900 text-brand-400 border border-gray-800 border-b-gray-900'
                : 'bg-gray-800/50 text-gray-500 hover:text-gray-300'
            }`}
          >
            Symbol Chart
          </button>
          <button
            onClick={() => setTab('details')}
            className={`px-4 py-1.5 text-xs font-medium rounded-t-lg transition ${
              tab === 'details'
                ? 'bg-gray-900 text-brand-400 border border-gray-800 border-b-gray-900'
                : 'bg-gray-800/50 text-gray-500 hover:text-gray-300'
            }`}
          >
            Symbol Details
          </button>
        </div>

        {/* === Symbol Chart tab === */}
        {tab === 'chart' && (
          <>
            {/* Timeframe selector */}
            <div className="flex items-center gap-2 mb-3 flex-wrap">
              <span className="text-sm font-medium text-gray-200">{symbol || '—'}</span>

              {/* Presets */}
              <div className="flex rounded-lg overflow-hidden border border-gray-700">
                {PRESETS.map((tf) => (
                  <button
                    key={tf}
                    onClick={() => setTimeframe(tf)}
                    className={`px-3 py-1.5 text-xs font-medium transition ${
                      timeframe === tf
                        ? 'bg-brand-600 text-white'
                        : 'bg-gray-900 text-gray-400 hover:text-gray-200'
                    }`}
                  >
                    {tf}
                  </button>
                ))}
              </div>

              {/* Custom TF builder */}
              <div className="flex items-center gap-0.5 border border-gray-700 rounded-lg overflow-hidden">
                <select
                  value={customUnit}
                  onChange={(e) => setCustomUnit(e.target.value)}
                  className="px-1.5 py-1.5 bg-gray-900 text-gray-200 text-xs border-none focus:outline-none"
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
                  className="w-12 px-1 py-1.5 bg-gray-900 text-gray-200 text-xs border-none focus:outline-none text-center"
                  placeholder="#"
                />
                <button
                  onClick={applyCustomTf}
                  className="px-2 py-1.5 bg-brand-600 hover:bg-brand-500 text-white text-xs font-medium transition"
                >
                  Go
                </button>
              </div>

              {/* Active custom TF badge */}
              {!PRESETS.includes(timeframe) && (
                <span className="px-2 py-1 bg-amber-600/20 text-amber-400 text-xs font-medium rounded-md border border-amber-600/30">
                  {timeframe}
                </span>
              )}
            </div>

            {/* Chart */}
            <div className="relative flex-1 bg-gray-900 border border-gray-800 rounded-xl p-2 min-h-[400px]">
              <div ref={chartRef} className="w-full h-full" />

              {/* Loading overlay */}
              {loading && (
                <div className="absolute inset-0 flex flex-col items-center justify-center bg-gray-900/80 rounded-xl z-10">
                  <div className="w-8 h-8 border-2 border-brand-500 border-t-transparent rounded-full animate-spin mb-3" />
                  <span className="text-sm text-gray-400">
                    {error || `Loading ${symbol} ${timeframe}...`}
                  </span>
                </div>
              )}

              {/* Error (final) */}
              {!loading && error && (
                <div className="absolute inset-0 flex flex-col items-center justify-center bg-gray-900/80 rounded-xl z-10">
                  <span className="text-red-400 text-sm mb-2">{error}</span>
                  <button
                    onClick={() => setRetryKey((k) => k + 1)}
                    className="text-xs text-brand-400 hover:text-brand-300 underline"
                  >
                    Retry
                  </button>
                </div>
              )}

              {/* Empty data */}
              {!loading && !error && empty && (
                <div className="absolute inset-0 flex items-center justify-center bg-gray-900/80 rounded-xl z-10">
                  <span className="text-gray-500 text-sm">
                    No data for {symbol} / {timeframe}
                  </span>
                </div>
              )}
            </div>
          </>
        )}

        {/* === Symbol Details tab === */}
        {tab === 'details' && (
          <div className="flex-1 flex flex-col gap-4 overflow-y-auto">
            {/* Spread chart */}
            <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
              <h3 className="text-sm font-medium text-gray-200 mb-1">
                Spread — {symbol || '—'}
              </h3>
              <p className="text-xs text-gray-500 mb-4">
                Min / Avg / Max spread in pips, last 24 × 5-min buckets (tick data)
              </p>

              {spreadLoading ? (
                <div className="flex items-center justify-center h-48">
                  <div className="w-6 h-6 border-2 border-brand-500 border-t-transparent rounded-full animate-spin" />
                </div>
              ) : spreadData.length === 0 ? (
                <div className="flex items-center justify-center h-48 text-gray-500 text-sm">
                  No spread data available
                </div>
              ) : (
                <ResponsiveContainer width="100%" height={260}>
                  <BarChart
                    data={spreadData}
                    margin={{ top: 5, right: 20, left: 0, bottom: 5 }}
                    barGap={1}
                    barCategoryGap="20%"
                  >
                    <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                    <XAxis
                      dataKey="label"
                      tick={{ fill: '#6b7280', fontSize: 10 }}
                      axisLine={{ stroke: '#374151' }}
                      tickLine={false}
                    />
                    <YAxis
                      tick={{ fill: '#6b7280', fontSize: 10 }}
                      axisLine={{ stroke: '#374151' }}
                      tickLine={false}
                      allowDecimals
                    />
                    <Tooltip
                      contentStyle={{
                        background: '#111827',
                        border: '1px solid #374151',
                        borderRadius: 8,
                        fontSize: 12,
                      }}
                      labelStyle={{ color: '#d1d5db' }}
                    />
                    <Legend
                      wrapperStyle={{ fontSize: 11, color: '#9ca3af' }}
                    />
                    <ReferenceLine
                      y={spreadData.length > 0
                        ? +(spreadData.reduce((s, d) => s + d.avg, 0) / spreadData.length).toFixed(2)
                        : 0}
                      stroke="#6366f1"
                      strokeDasharray="4 4"
                      label={{ value: 'avg', fill: '#6366f1', fontSize: 10, position: 'right' }}
                    />
                    <Bar dataKey="min" name="Min" fill="#22c55e" radius={[2, 2, 0, 0]} />
                    <Bar dataKey="avg" name="Avg" fill="#6366f1" radius={[2, 2, 0, 0]} />
                    <Bar dataKey="max" name="Max" fill="#ef4444" radius={[2, 2, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
