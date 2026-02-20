"use client"

import { useEffect, useId, useRef, useState } from "react"
import { useReducedMotion } from "framer-motion"
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts"

/* ---------- TYPES ---------- */
type Props = {
  source: string
  dimension?: string
  metric?: string
  datasetType: "sales" | "claims"
  bucket?: "day" | "week" | "month"
  jobId?: string | null
  fromDate?: string
  toDate?: string
  fetchDelayMs?: number
  deferUntilVisible?: boolean
  onDataReady?: (snapshot: GraphDataSnapshot) => void
}

type Row = Record<string, unknown>

export type GraphDataSnapshot = {
  rows: Row[]
  measure: string
  dimensionKey: string
  compareMode: boolean
}

/* ---------- HELPERS ---------- */
const toSafeKey = (key: string) =>
  key
    ?.toLowerCase()
    .trim()
    .replace(/\s+/g, "_")
    .replace(/[()%'.]/g, "") || ""

const prettyLabel = (key: string) =>
  key.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase())

/* ---------- FORMATTERS ---------- */
const formatValue = (value: number, measure: string) => {
  const m = measure.toLowerCase()
  if (m.includes("loss_ratio")) {
    return `${value.toFixed(2)}%`
  }
  if (m.includes("quantity") || m.includes("count")) {
    return value.toLocaleString()
  }
  const absValue = Math.abs(value)
  const sign = value < 0 ? "-" : ""
  if (absValue >= 1e7) return `Rs ${sign}${(absValue / 1e7).toFixed(2)} Cr`
  if (absValue >= 1e5) return `Rs ${sign}${(absValue / 1e5).toFixed(2)} L`
  if (absValue >= 1e3) return `Rs ${sign}${(absValue / 1e3).toFixed(1)} K`
  return `Rs ${value.toLocaleString()}`
}

const formatMonth = (value: string) => {
  if (!value) return value
  if (typeof value === "string") {
    const shortMatch = value.match(/^[A-Za-z]{3}[-/]\d{2}$/)
    if (shortMatch) {
      return value.replace("-", " ").replace("/", " ")
    }
  }
  const d = new Date(value)
  if (isNaN(d.getTime())) return value
  return d.toLocaleString("en-US", {
    month: "short",
    year: "2-digit",
  })
}

const normalizeDimValue = (value: unknown, dimKey: string) => {
  if (value == null) return "Unknown"
  const raw = String(value).trim()
  if (!raw) return "Unknown"
  if (dimKey.includes("month") || dimKey.includes("date")) {
    const shortMatch = raw.match(/^([A-Za-z]{3})[-/](\d{2})$/)
    if (shortMatch) {
      const monthMap: Record<string, number> = {
        jan: 1,
        feb: 2,
        mar: 3,
        apr: 4,
        may: 5,
        jun: 6,
        jul: 7,
        aug: 8,
        sep: 9,
        oct: 10,
        nov: 11,
        dec: 12,
      }
      const monthKey = shortMatch[1].toLowerCase()
      const month = monthMap[monthKey]
      if (month) {
        const year = 2000 + Number(shortMatch[2])
        return `${year}-${String(month).padStart(2, "0")}-01`
      }
    }
    const d = new Date(raw)
    if (!isNaN(d.getTime())) {
      const year = d.getFullYear()
      const month = String(d.getMonth() + 1).padStart(2, "0")
      return `${year}-${month}-01`
    }
  }
  return raw
}

const DIMENSION_ALIASES: Record<string, string[]> = {
  plan_category: ["device_plan_category"],
  device_plan_category: ["plan_category"],
}

const pickDimensionValue = (row: Row, dimKey: string) => {
  const primary = row[dimKey]
  if (primary != null && String(primary).trim() !== "") return primary

  const aliases = DIMENSION_ALIASES[dimKey] || []
  for (const alias of aliases) {
    const value = row[alias]
    if (value != null && String(value).trim() !== "") return value
  }

  return primary
}

const asNumber = (value: unknown) => {
  const n = Number(value ?? 0)
  return Number.isFinite(n) ? n : 0
}

const toTimeValue = (value: unknown) => {
  const raw = String(value ?? "").trim()
  if (!raw) return Number.NaN

  const shortMatch = raw.match(/^([A-Za-z]{3})[-/\s](\d{2}|\d{4})$/)
  if (shortMatch) {
    const monthMap: Record<string, number> = {
      jan: 1,
      feb: 2,
      mar: 3,
      apr: 4,
      may: 5,
      jun: 6,
      jul: 7,
      aug: 8,
      sep: 9,
      oct: 10,
      nov: 11,
      dec: 12,
    }
    const month = monthMap[shortMatch[1].toLowerCase()]
    if (month) {
      const rawYear = Number(shortMatch[2])
      const year = shortMatch[2].length === 2 ? 2000 + rawYear : rawYear
      return new Date(`${year}-${String(month).padStart(2, "0")}-01`).getTime()
    }
  }

  const normalized = normalizeDimValue(raw, "month")
  const normalizedTs = new Date(normalized).getTime()
  if (!Number.isNaN(normalizedTs)) return normalizedTs

  const directTs = new Date(raw).getTime()
  return directTs
}

const sortTemporalRows = (rows: Row[], dimKey: string) =>
  [...rows].sort((a, b) => {
    const at = toTimeValue(a[dimKey])
    const bt = toTimeValue(b[dimKey])
    const aValid = Number.isFinite(at)
    const bValid = Number.isFinite(bt)
    if (aValid && bValid) return at - bt
    if (aValid) return -1
    if (bValid) return 1
    return String(a[dimKey] ?? "").localeCompare(String(b[dimKey] ?? ""))
  })

/* ---------- DATA FETCH ---------- */
type FetchParams = {
  source: string
  dimension: string
  metric: string
  datasetType: "sales" | "claims"
  bucket?: "day" | "week" | "month"
  jobId?: string | null
  from_date?: string
  to_date?: string
}

type FetchRowsResult = {
  ts: number
  data: Row[]
  measure: string
  usedRangeFallback?: boolean
}

const GRAPH_RESULT_TTL_MS = 120000
const graphResultCache = new Map<string, { expiresAt: number; value: FetchRowsResult }>()
const graphInFlight = new Map<string, Promise<FetchRowsResult>>()

export const clearGraphDataCache = () => {
  graphResultCache.clear()
  graphInFlight.clear()
}

const DEFAULT_API_BASE =
  typeof window !== "undefined"
    ? (window.location.origin || "")
    : "http://127.0.0.1:8000"

const normalizeApiBase = (value: string) => {
  const cleaned = value.replace(/\s+/g, "").replace(/^['"]+|['"]+$/g, "")
  const withoutMarker = cleaned.replace(/^-?NoNewline/i, "")
  const match = withoutMarker.match(/https?:\/\/.*/)
  const normalized = match ? match[0] : withoutMarker
  return normalized.replace(/\/+$/, "")
}

const API_BASE = normalizeApiBase(process.env.NEXT_PUBLIC_API_BASE || DEFAULT_API_BASE)
const runtimeOverride =
  typeof window !== "undefined"
    ? normalizeApiBase(new URLSearchParams(window.location.search).get("api") || "")
    : ""

const browserOriginApiBases =
  typeof window !== "undefined"
    ? [
        window.location.origin,
        `${window.location.origin}/api`,
      ]
    : []

const browserHostApiBases =
  typeof window !== "undefined"
    ? [
        `${window.location.protocol}//${window.location.hostname}:8000`,
        `http://${window.location.hostname}:8000`,
      ]
    : []

const API_FALLBACKS = Array.from(
  new Set(
    [
      runtimeOverride,
      ...browserOriginApiBases,
      ...browserHostApiBases,
      API_BASE,
      "http://127.0.0.1:8000",
      "http://localhost:8000",
      "http://0.0.0.0:8000",
    ]
      .map(v => normalizeApiBase(v))
      .filter(Boolean)
  )
)
const API_REQUEST_TIMEOUT_MS = Number(process.env.NEXT_PUBLIC_API_TIMEOUT_MS || 20000)
let preferredApiBase = API_FALLBACKS[0] || ""

const orderedApiBases = () => {
  const ordered = [preferredApiBase, ...API_FALLBACKS].filter(Boolean)
  return Array.from(new Set(ordered))
}

const normalizeToken = (value: string | null) => {
  if (!value) return null
  let token = value.trim()
  token = token.replace(/^['"]+|['"]+$/g, "")
  token = token.replace(/^Bearer\s+/i, "").trim()
  if (!token || token === "null" || token === "undefined") return null
  return token
}

const getAuthToken = () =>
  typeof window !== "undefined"
    ? normalizeToken(localStorage.getItem("auth_token"))
    : null

const buildQuery = ({
  source,
  dimension,
  metric,
  datasetType,
  bucket,
  jobId,
  from_date,
  to_date,
}: FetchParams) => {
  let safeFrom = from_date
  let safeTo = to_date
  if (safeFrom && safeTo && safeFrom > safeTo) {
    const swappedFrom = safeTo
    const swappedTo = safeFrom
    safeFrom = swappedFrom
    safeTo = swappedTo
  }

  const query = new URLSearchParams({
    dimension,
    metric,
    source,
    dataset_type: datasetType,
  })
  if (jobId) query.set("job_id", jobId)
  if (bucket) query.set("bucket", bucket)
  if (safeFrom) query.set("from_date", safeFrom)
  if (safeTo) query.set("to_date", safeTo)
  return query.toString()
}

const buildUrl = (base: string, query: string) => `${base}/analytics/by-dimension?${query}`

const fetchWithTimeout = async (url: string, init: RequestInit) => {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), API_REQUEST_TIMEOUT_MS)
  try {
    return await fetch(url, { ...init, signal: controller.signal })
  } finally {
    clearTimeout(timer)
  }
}

class NoFallbackError extends Error {
  noFallback = true
}

const fetchRows = async (params: FetchParams): Promise<FetchRowsResult> => {
  const query = buildQuery(params)
  const cacheKey = `${params.source}|${params.datasetType}|${query}`
  const now = Date.now()
  const cached = graphResultCache.get(cacheKey)
  if (cached && cached.expiresAt > now) {
    return cached.value
  }
  const inFlight = graphInFlight.get(cacheKey)
  if (inFlight) {
    return inFlight
  }

  const dimKey = toSafeKey(params.dimension)
  const metricKey = toSafeKey(params.metric)
  const headers = new Headers()
  const token = getAuthToken()
  if (token) headers.set("Authorization", `Bearer ${token}`)

  const requestPromise = (async (): Promise<FetchRowsResult> => {
    const errors: string[] = []
    let sawUnauthorized = false
    for (const base of orderedApiBases()) {
      const url = buildUrl(base, query)
      try {
        const res = await fetchWithTimeout(url, { headers, mode: "cors" })
        if (!res.ok) {
          let detail = ""
          try {
            const data = await res.json()
            if (data?.detail) detail = data.detail
          } catch {
            // ignore non-json error body
          }
          const message = detail || `HTTP ${res.status}`
          if (res.status === 401 || res.status === 403) {
            sawUnauthorized = true
            errors.push(`${url} -> ${message}`)
            continue
          }
          if (res.status >= 400 && res.status < 500) {
            throw new NoFallbackError(message)
          }
          throw new Error(message)
        }

        const raw = await res.json()
        preferredApiBase = base
        if (!Array.isArray(raw) || raw.length === 0) {
          return { ts: Date.now(), data: [], measure: metricKey, usedRangeFallback: false }
        }

        const processed: Row[] = raw.map(row => {
          const out: Row = {}
          Object.entries(row).forEach(([k, v]) => {
            out[toSafeKey(k)] = v
          })
          out[dimKey] = normalizeDimValue(pickDimensionValue(out, dimKey), dimKey)
          return out
        })

        return {
          ts: Date.now(),
          data: processed,
          measure: metricKey,
          usedRangeFallback: false,
        }
      } catch (error) {
        if (error instanceof NoFallbackError) {
          throw error
        }
        const msg = error instanceof Error ? error.message : String(error)
        errors.push(`${url} -> ${msg}`)
      }
    }
    if (sawUnauthorized) {
      throw new NoFallbackError("Not authenticated")
    }
    throw new Error(`Failed to fetch analytics. Tried: ${errors.join(" | ")}`)
  })()

  graphInFlight.set(cacheKey, requestPromise)
  try {
    const result = await requestPromise
    if (result.data.length > 0) {
      graphResultCache.set(cacheKey, {
        expiresAt: Date.now() + GRAPH_RESULT_TTL_MS,
        value: result,
      })
    } else {
      graphResultCache.delete(cacheKey)
    }
    return result
  } finally {
    graphInFlight.delete(cacheKey)
  }
}

const fetchRowsWithRangeFallback = async (params: FetchParams): Promise<FetchRowsResult> => {
  return fetchRows(params)
}

export const prefetchGraphData = async (params: FetchParams) => {
  if (!params.source || !params.dimension || !params.metric) return
  await fetchRows(params)
}

export const hasGraphData = async (params: FetchParams): Promise<boolean> => {
  if (!params.source || !params.dimension || !params.metric) return false
  const result = await fetchRowsWithRangeFallback(params)
  return result.data.length > 0
}

/* ---------- COLOR HELPERS ---------- */
const SALES_PALETTE = ["#6366f1", "#22c55e", "#06b6d4", "#f97316", "#a855f7", "#84cc16"]
const CLAIMS_PALETTE = ["#f43f5e", "#f59e0b", "#0ea5e9", "#14b8a6", "#8b5cf6", "#22c55e"]

const hashString = (value: string) => {
  let hash = 0
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(i)
    hash |= 0
  }
  return Math.abs(hash)
}

const pickColor = (key: string, palette: string[]) => {
  if (!palette.length) return "#6366f1"
  return palette[hashString(key) % palette.length]
}

const hexToRgb = (hex: string) => {
  const clean = hex.replace("#", "")
  const num = parseInt(clean, 16)
  return {
    r: (num >> 16) & 255,
    g: (num >> 8) & 255,
    b: num & 255,
  }
}

const mixWithWhite = (hex: string, amount: number) => {
  const { r, g, b } = hexToRgb(hex)
  const mix = (c: number) => Math.round(c + (255 - c) * amount)
  return `#${[mix(r), mix(g), mix(b)]
    .map(v => v.toString(16).padStart(2, "0"))
    .join("")}`
}

/* ---------- TOOLTIP ---------- */
type TooltipEntry = {
  dataKey: string
  color: string
  name?: string
  value: number
}

type CustomTooltipProps = {
  active?: boolean
  payload?: TooltipEntry[]
  label?: string
  measure: string
}

const CustomTooltip = ({ active, payload, label, measure }: CustomTooltipProps) => {
  if (!active || !payload?.length) return null
  const formattedLabel = formatMonth(label || "")

  return (
    <div className="bg-white p-3 border shadow rounded-lg">
      <p className="text-xs text-gray-400 font-bold">{formattedLabel}</p>
      <div className="space-y-1">
        {payload.map((p) => (
          <div key={p.dataKey} className="flex items-center gap-2 text-sm font-semibold">
            <span
              className="inline-block h-2.5 w-2.5 rounded-full"
              style={{ backgroundColor: p.color }}
            />
            <span className="text-slate-700">
              {p.name || p.dataKey}
            </span>
            <span className="ml-auto text-slate-900">
              {formatValue(p.value, measure)}
            </span>
          </div>
        ))}
      </div>
      <p className="text-[10px] text-indigo-500 font-semibold mt-2">
        {prettyLabel(measure)}
      </p>
    </div>
  )
}

/* ---------- COMPONENT ---------- */
export default function GraphView({
  source,
  dimension,
  metric,
  datasetType,
  bucket,
  jobId,
  fromDate,
  toDate,
  fetchDelayMs,
  deferUntilVisible = false,
  onDataReady,
}: Props) {
  const prefersReducedMotion = useReducedMotion()
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [isVisible, setIsVisible] = useState(!deferUntilVisible)
  const [data, setData] = useState<Row[]>([])
  const [measure, setMeasure] = useState("")
  const [compareMode, setCompareMode] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const requestIdRef = useRef(0)
  const gradientId = useId()
  const gradientIdAlt = useId()

  useEffect(() => {
    if (!deferUntilVisible) {
      setIsVisible(true)
      return
    }
    if (isVisible) return
    const node = containerRef.current
    if (!node || typeof IntersectionObserver === "undefined") {
      setIsVisible(true)
      return
    }
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some(entry => entry.isIntersecting)) {
          setIsVisible(true)
          observer.disconnect()
        }
      },
      { rootMargin: "240px 0px 240px 0px" }
    )
    observer.observe(node)
    return () => observer.disconnect()
  }, [deferUntilVisible, isVisible, source, dimension, metric, datasetType, fromDate, toDate])

  useEffect(() => {
    if (!dimension || !source || !metric) return
    if (deferUntilVisible && !isVisible) return
    const requestId = ++requestIdRef.current

    const fetchData = async () => {
      setLoading(true)
      setError(null)

      try {
        const dimKey = toSafeKey(dimension)
        const metricKey = toSafeKey(metric)

        if (source === "samsung") {
          setCompareMode(true)
          // Backend returns both partners in one response to avoid 2 requests per graph.
          const combined = await fetchRowsWithRangeFallback({
            source: "samsung",
            dimension,
            metric,
            datasetType,
            bucket,
            jobId,
            from_date: fromDate,
            to_date: toDate,
          })

          let merged: Row[] = (combined.data || []).map(row => ({
            ...row,
            samsung_vs: asNumber(row["samsung_vs"]),
            samsung_croma: asNumber(row["samsung_croma"]),
          }))

          if (dimKey.includes("month") || dimKey.includes("date")) {
            merged = sortTemporalRows(merged, dimKey)
          }

          if (requestId !== requestIdRef.current) return
          setMeasure(metricKey)
          setData(merged)
          onDataReady?.({
            rows: merged,
            measure: metricKey,
            dimensionKey: dimKey,
            compareMode: true,
          })
          return
        }

        setCompareMode(false)
        const single = await fetchRowsWithRangeFallback({
          source,
          dimension,
          metric,
          datasetType,
          bucket,
          jobId,
          from_date: fromDate,
          to_date: toDate,
        })

        if (!single.data.length) {
          onDataReady?.({
            rows: [],
            measure: metricKey,
            dimensionKey: dimKey,
            compareMode: false,
          })
          setData([])
          return
        }

        let normalizedSingle = single.data
        if (!(metricKey in normalizedSingle[0])) {
          const partnerMetricKey =
            source === "samsung_vs" || source === "samsung_vijay_sales"
              ? "samsung_vs"
              : source === "samsung_croma"
                ? "samsung_croma"
                : ""

          if (partnerMetricKey && partnerMetricKey in normalizedSingle[0]) {
            normalizedSingle = normalizedSingle.map((row) => ({
              ...row,
              [metricKey]: asNumber(row[partnerMetricKey]),
            }))
          } else {
            if (requestId !== requestIdRef.current) return
            onDataReady?.({
              rows: [],
              measure: metricKey,
              dimensionKey: dimKey,
              compareMode: false,
            })
            setData([])
            return
          }
        }

        let next = normalizedSingle
        if (dimKey.includes("month") || dimKey.includes("date")) {
          next = sortTemporalRows(next, dimKey)
        }

        if (requestId !== requestIdRef.current) return
        setMeasure(metricKey)
        setData(next)
        onDataReady?.({
          rows: next,
          measure: metricKey,
          dimensionKey: dimKey,
          compareMode: false,
        })
      } catch (e: unknown) {
        if (requestId !== requestIdRef.current) return
        const msg = e instanceof Error ? e.message : "Failed"
        console.error("GraphView fetch error:", msg, { source, dimension, metric })
        setError(msg)
        setData([])
        onDataReady?.({
          rows: [],
          measure: "",
          dimensionKey: toSafeKey(dimension || ""),
          compareMode: false,
        })
      } finally {
        if (requestId !== requestIdRef.current) return
        setLoading(false)
      }
    }

    let timer: ReturnType<typeof setTimeout> | null = null
    if (fetchDelayMs && fetchDelayMs > 0) {
      timer = setTimeout(fetchData, fetchDelayMs)
    } else {
      fetchData()
    }

    return () => {
      if (timer) clearTimeout(timer)
    }
  }, [source, dimension, metric, datasetType, bucket, jobId, fromDate, toDate, fetchDelayMs, onDataReady, deferUntilVisible, isVisible])

  if (deferUntilVisible && !isVisible) {
    return (
      <div ref={containerRef} className="h-72 flex items-center justify-center text-sm text-gray-400">
        Loading chart...
      </div>
    )
  }

  if (loading) {
    return (
      <div ref={containerRef} className="h-72 flex items-center justify-center text-sm text-gray-500">
        Loading...
      </div>
    )
  }

  if (error || !data.length || !measure) {
    return (
      <div ref={containerRef} className="h-72 flex items-center justify-center text-sm text-gray-400">
        No Data Available
      </div>
    )
  }

  const dimKey = toSafeKey(dimension!)
  const palette = datasetType === "sales" ? SALES_PALETTE : CLAIMS_PALETTE
  const baseKey = `${dimension}-${metric}-${datasetType}`
  const primaryColor = pickColor(baseKey, palette)
  let secondaryColor = pickColor(`${baseKey}-alt`, palette)
  if (secondaryColor === primaryColor) {
    secondaryColor = palette[(palette.indexOf(primaryColor) + 1) % palette.length]
  }

  const showEwCounts =
    !compareMode &&
    measure.includes("quantity") &&
    data.some(row => row.ew_count != null)

  const isLossRatio = measure.includes("loss_ratio")
  const isTemporalDimension = dimKey.includes("month") || dimKey.includes("date")
  const clampToZero = !isLossRatio || source === "reliance"
  const chartData: Row[] = clampToZero
    ? data.map((row) => {
        const next: Row = { ...row }
        if (compareMode) {
          next.samsung_vs = Math.max(0, asNumber(row.samsung_vs))
          next.samsung_croma = Math.max(0, asNumber(row.samsung_croma))
          return next
        }
        next[measure] = Math.max(0, asNumber(row[measure]))
        if (showEwCounts) {
          next.ew_count = Math.max(0, asNumber(row.ew_count))
        }
        return next
      })
    : data
  const shouldAnimateBars = !prefersReducedMotion && chartData.length <= 36
  const barAnimationDuration = shouldAnimateBars ? 500 : 0

  return (
    <div ref={containerRef} className="smooth-surface h-72">
      {compareMode && (
        <div className="flex items-center gap-3 text-[11px] font-semibold text-slate-500 mb-2">
          <span className="flex items-center gap-1.5">
            <span
              className="inline-block h-2.5 w-2.5 rounded-full"
              style={{ backgroundColor: primaryColor }}
            />
            Vijay Sales
          </span>
          <span className="flex items-center gap-1.5">
            <span
              className="inline-block h-2.5 w-2.5 rounded-full"
              style={{ backgroundColor: secondaryColor }}
            />
            Croma
          </span>
        </div>
      )}
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} margin={{ top: 12, right: 8, left: 0, bottom: 6 }} barCategoryGap={14}>
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={mixWithWhite(primaryColor, 0.35)} />
              <stop offset="100%" stopColor={primaryColor} />
            </linearGradient>
            <linearGradient id={gradientIdAlt} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={mixWithWhite(secondaryColor, 0.35)} />
              <stop offset="100%" stopColor={secondaryColor} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="4 4" vertical={false} stroke="#e2e8f0" />
          <XAxis
            dataKey={dimKey}
            interval={isTemporalDimension ? "preserveStartEnd" : "preserveEnd"}
            minTickGap={isTemporalDimension ? 16 : 8}
            tick={{ fontSize: 11 }}
            tickFormatter={(v) => (isTemporalDimension ? formatMonth(v) : v)}
          />
          <YAxis
            domain={clampToZero ? [0, "auto"] : ["auto", "auto"]}
            tick={{ fontSize: 11 }}
            tickFormatter={(v) => formatValue(v as number, measure)}
          />
          <Tooltip content={<CustomTooltip measure={measure} />} />
          {compareMode ? (
            <>
              <Bar
                dataKey="samsung_vs"
                name="Vijay Sales"
                barSize={18}
                radius={[8, 8, 2, 2]}
                fill={`url(#${gradientId})`}
                isAnimationActive={shouldAnimateBars}
                animationDuration={barAnimationDuration}
                animationBegin={150}
              />
              <Bar
                dataKey="samsung_croma"
                name="Croma"
                barSize={18}
                radius={[8, 8, 2, 2]}
                fill={`url(#${gradientIdAlt})`}
                isAnimationActive={shouldAnimateBars}
                animationDuration={barAnimationDuration}
                animationBegin={250}
              />
            </>
          ) : showEwCounts ? (
            <>
              <Bar
                dataKey={measure}
                name="Units Sold"
                barSize={18}
                radius={[8, 8, 2, 2]}
                fill={`url(#${gradientId})`}
                isAnimationActive={shouldAnimateBars}
                animationDuration={barAnimationDuration}
                animationBegin={120}
              />
              <Bar
                dataKey="ew_count"
                name="EW Count"
                barSize={18}
                radius={[8, 8, 2, 2]}
                fill={`url(#${gradientIdAlt})`}
                isAnimationActive={shouldAnimateBars}
                animationDuration={barAnimationDuration}
                animationBegin={200}
              />
            </>
          ) : (
            <Bar
              dataKey={measure}
              barSize={28}
              radius={[10, 10, 2, 2]}
              fill={`url(#${gradientId})`}
              isAnimationActive={shouldAnimateBars}
              animationDuration={barAnimationDuration}
              animationBegin={120}
            />
          )}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
