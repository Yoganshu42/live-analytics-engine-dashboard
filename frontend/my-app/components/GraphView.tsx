"use client"

import { Fragment, type MouseEvent, type TouchEvent, useEffect, useId, useMemo, useRef, useState } from "react"
import { useReducedMotion } from "framer-motion"
import indiaSvgMap from "@svg-maps/india"
import {
  Area,
  AreaChart,
  BarChart,
  Bar,
  Cell,
  Line,
  Pie,
  PieChart,
  Legend,
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts"
import {
  SAMSUNG_PARTNERS,
  isSamsungPartnerSource,
  normalizeSamsungSource,
  sumSamsungPartnerValues,
  type SamsungPartnerKey,
} from "@/lib/samsungPartners"

/* ---------- TYPES ---------- */
export type GraphChartType = "bar" | "line" | "pie" | "radar" | "india_map"

export type GraphCategoryFilter = {
  dimension: string
  values: string[]
}

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
  chartType?: GraphChartType
  tooltipMetricOverride?: string
  heightClassName?: string
  categoryFilters?: GraphCategoryFilter[]
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

const truncateLabel = (value: string, maxLength = 18) => {
  const text = String(value || "").trim()
  if (text.length <= maxLength) return text
  return `${text.slice(0, maxLength - 1)}...`
}

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
  if (dimKey.includes("product_category")) {
    const compact = raw.toLowerCase().replace(/[^a-z0-9]/g, "")
    if (!compact) return "Unknown"

    if (
      compact.includes("aircondition") ||
      compact.includes("aircondit") ||
      compact === "ac" ||
      compact.includes("splitac") ||
      compact.includes("windowac")
    ) {
      return "Air Conditioner"
    }
    if (compact.includes("aircooler")) return "Air Cooler"
    if (
      compact.includes("refrigerator") ||
      compact.includes("refrigrator") ||
      compact.includes("refrigator") ||
      compact.includes("frigerator") ||
      compact.includes("fridge")
    ) {
      return "Refrigerator"
    }
    if (
      compact.includes("washingmachine") ||
      compact.includes("washmachine") ||
      compact.includes("washer")
    ) {
      return "Washing Machine"
    }
    if (
      compact.includes("microwave") ||
      compact.includes("micowave") ||
      compact.includes("microoven")
    ) {
      return "Microwave"
    }
    if (compact.includes("deepfreezer")) return "Deep Freezer"
    if (
      compact.includes("chestfreezer") ||
      compact.includes("chestfreeze") ||
      compact.includes("chestfreez")
    ) {
      return "Chest Freezer"
    }

    const cleaned = raw.replace(/[^a-zA-Z0-9\s/-]/g, " ").replace(/\s+/g, " ").trim()
    if (!cleaned) return "Unknown"
    return cleaned
      .toLowerCase()
      .replace(/\b\w/g, (c) => c.toUpperCase())
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

const mergeRowsByDimension = (rows: Row[], dimKey: string): Row[] => {
  const merged = new Map<string, Row>()
  rows.forEach((row) => {
    const bucket = String(row[dimKey] ?? "Unknown").trim() || "Unknown"
    const existing = merged.get(bucket)
    if (!existing) {
      merged.set(bucket, { ...row, [dimKey]: bucket })
      return
    }
    Object.entries(row).forEach(([key, value]) => {
      if (key === dimKey) return
      const numeric = Number(value)
      if (Number.isFinite(numeric)) {
        existing[key] = asNumber(existing[key]) + numeric
      } else if ((existing[key] == null || existing[key] === "") && value != null) {
        existing[key] = value
      }
    })
  })
  return Array.from(merged.values())
}

const LOG_PLOT_SUFFIX = "__log_plot"

const toLogPlotKey = (key: string) => `${key}${LOG_PLOT_SUFFIX}`

const toOriginalMetricKey = (key: string) => (
  key.endsWith(LOG_PLOT_SUFFIX)
    ? key.slice(0, -LOG_PLOT_SUFFIX.length)
    : key
)

const toLogSafeValue = (value: unknown) => {
  const numeric = asNumber(value)
  return numeric > 0 ? numeric : 1
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

const monthBucketValue = (date: Date) => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, "0")
  return `${year}-${month}-01`
}

const buildZeroTemporalRow = (template: Row, dimKey: string, date: Date): Row => {
  const bucket = monthBucketValue(date)
  const next: Row = {}
  Object.entries(template).forEach(([key, value]) => {
    if (key === dimKey || key === "period_start" || key === "period_end") {
      next[key] = bucket
      return
    }
    const numeric = Number(value)
    next[key] = Number.isFinite(numeric) ? 0 : ""
  })
  return next
}

const padSingleTemporalRows = (rows: Row[], dimKey: string) => {
  if (rows.length !== 1) return rows
  const centerTs = toTimeValue(rows[0][dimKey])
  if (!Number.isFinite(centerTs)) return rows
  const center = new Date(centerTs)
  center.setDate(1)
  const prev = new Date(center)
  prev.setMonth(center.getMonth() - 1)
  const next = new Date(center)
  next.setMonth(center.getMonth() + 1)
  return [
    buildZeroTemporalRow(rows[0], dimKey, prev),
    rows[0],
    buildZeroTemporalRow(rows[0], dimKey, next),
  ]
}

/* ---------- DATA FETCH ---------- */
export type GraphFetchParams = {
  source: string
  dimension: string
  metric: string
  datasetType: "sales" | "claims"
  bucket?: "day" | "week" | "month"
  jobId?: string | null
  from_date?: string
  to_date?: string
  categoryFilters?: GraphCategoryFilter[]
}

type FetchRowsResult = {
  ts: number
  data: Row[]
  measure: string
  usedRangeFallback?: boolean
}

const GRAPH_RESULT_TTL_MS = 300000
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
  categoryFilters,
}: GraphFetchParams) => {
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
  const activeFilters = (categoryFilters || [])
    .map((filter) => ({
      dimension: toSafeKey(String(filter.dimension || "")),
      values: Array.isArray(filter.values)
        ? filter.values
            .map((value) => String(value || "").trim())
            .filter(Boolean)
        : [],
    }))
    .filter((filter) => Boolean(filter.dimension) && filter.values.length > 0)
    .slice(0, 2)
  activeFilters.forEach((filter, index) => {
    const slot = index + 1
    query.set(`filter_${slot}_dimension`, filter.dimension)
    query.set(`filter_${slot}_values`, filter.values.join(","))
  })
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

const fetchRows = async (params: GraphFetchParams): Promise<FetchRowsResult> => {
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

        let processed: Row[] = raw.map(row => {
          const out: Row = {}
          Object.entries(row).forEach(([k, v]) => {
            out[toSafeKey(k)] = v
          })
          out[dimKey] = normalizeDimValue(pickDimensionValue(out, dimKey), dimKey)
          return out
        })
        if (dimKey.includes("product_category")) {
          processed = mergeRowsByDimension(processed, dimKey)
        }

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

const fetchRowsWithRangeFallback = async (params: GraphFetchParams): Promise<FetchRowsResult> => {
  return fetchRows(params)
}

export const fetchGraphRows = async (params: GraphFetchParams) => {
  return fetchRowsWithRangeFallback(params)
}

export const prefetchGraphData = async (params: GraphFetchParams) => {
  if (!params.source || !params.dimension || !params.metric) return
  await fetchRows(params)
}

export const hasGraphData = async (params: GraphFetchParams): Promise<boolean> => {
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

const mixWithBlack = (hex: string, amount: number) => {
  const { r, g, b } = hexToRgb(hex)
  const mix = (c: number) => Math.round(c * (1 - amount))
  return `#${[mix(r), mix(g), mix(b)]
    .map(v => v.toString(16).padStart(2, "0"))
    .join("")}`
}

type IndiaMapLocation = {
  id: string
  name: string
  path: string
}

const INDIA_MAP_LOCATIONS: IndiaMapLocation[] = (
  indiaSvgMap.locations as Array<{ id: string; name: string; path: string }>
).map((location) => ({
  id: location.id,
  name: location.name,
  path: location.path,
}))

const normalizeStateLookupKey = (value: string) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z]/g, "")

const INDIA_STATE_KEY_TO_ID: Record<string, string> = INDIA_MAP_LOCATIONS.reduce(
  (acc, location) => {
    acc[normalizeStateLookupKey(location.name)] = location.id
    return acc
  },
  {} as Record<string, string>
)

const STATE_NAME_ALIASES: Record<string, string> = {
  orissa: "odisha",
  pondicherry: "puducherry",
  uttaranchal: "uttarakhand",
  nctofdelhi: "delhi",
  newdelhi: "delhi",
  andamanandnicobar: "andamanandnicobarislands",
  andamannicobar: "andamanandnicobarislands",
  jandk: "jammuandkashmir",
  jk: "jammuandkashmir",
  up: "uttarpradesh",
  wb: "westbengal",
  telengana: "telangana",
  del: "delhi",
  delhincr: "delhi",
  ncrdelhi: "delhi",
  mumbai: "maharashtra",
  navimumbai: "maharashtra",
  pune: "maharashtra",
  thane: "maharashtra",
  bengaluru: "karnataka",
  bangalore: "karnataka",
  chennai: "tamilnadu",
  hyderabad: "telangana",
  kolkata: "westbengal",
  ahmedabad: "gujarat",
  surat: "gujarat",
  jaipur: "rajasthan",
  lucknow: "uttarpradesh",
  noida: "uttarpradesh",
  ghaziabad: "uttarpradesh",
  gurgaon: "haryana",
  gurugram: "haryana",
  faridabad: "haryana",
  kochi: "kerala",
  trivandrum: "kerala",
  thiruvananthapuram: "kerala",
  bhubaneswar: "odisha",
  visakhapatnam: "andhrapradesh",
  vijayawada: "andhrapradesh",
  indore: "madhyapradesh",
  bhopal: "madhyapradesh",
  patna: "bihar",
  ranchi: "jharkhand",
  chandigarh: "chandigarh",
}

Object.entries(STATE_NAME_ALIASES).forEach(([aliasKey, canonicalKey]) => {
  const canonicalId = INDIA_STATE_KEY_TO_ID[canonicalKey]
  if (canonicalId) INDIA_STATE_KEY_TO_ID[aliasKey] = canonicalId
})

const mapStateToIndiaStateId = (stateValue: string): string | null => {
  const key = normalizeStateLookupKey(stateValue)
  if (!key) return null
  return INDIA_STATE_KEY_TO_ID[key] || null
}

const interpolateHex = (fromHex: string, toHex: string, t: number) => {
  const clamped = Math.max(0, Math.min(1, t))
  const from = hexToRgb(fromHex)
  const to = hexToRgb(toHex)
  const lerp = (a: number, b: number) => Math.round(a + (b - a) * clamped)
  return `#${[lerp(from.r, to.r), lerp(from.g, to.g), lerp(from.b, to.b)]
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("")}`
}

const INDIA_HEATMAP_LOW = "#fee2e2"
const INDIA_HEATMAP_HIGH = "#7f1d1d"

type HoverDetailMetricKey =
  | "gross_premium"
  | "earned_premium"
  | "zopper_earned_premium"
  | "quantity"

const HOVER_DETAIL_METRICS: HoverDetailMetricKey[] = [
  "gross_premium",
  "earned_premium",
  "zopper_earned_premium",
  "quantity",
]

const HOVER_DETAIL_LABELS: Record<HoverDetailMetricKey, string> = {
  gross_premium: "Gross Premium",
  earned_premium: "Earned Premium",
  zopper_earned_premium: "Zopper Earned Premium",
  quantity: "Quantity",
}

const formatAxisCompact = (value: number, measure: string) => {
  const m = measure.toLowerCase()
  if (m.includes("loss_ratio")) return `${value.toFixed(1)}%`
  return new Intl.NumberFormat("en-IN", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value)
}

/* ---------- TOOLTIP ---------- */
type TooltipEntry = {
  dataKey: string
  color?: string
  name?: string
  value?: number | string
  payload?: Row
}

type CustomTooltipProps = {
  active?: boolean
  payload?: TooltipEntry[]
  label?: string
  measure: string
  compareTooltipQuantity?: boolean
  showPeriodRange?: boolean
  periodStartLabel?: string
}

const CustomTooltip = ({
  active,
  payload,
  label,
  measure,
  compareTooltipQuantity = false,
  showPeriodRange = false,
  periodStartLabel = "",
}: CustomTooltipProps) => {
  if (!active || !payload?.length) return null
  const formattedLabel = formatMonth(label || "")
  const tooltipRow = payload[0]?.payload
  const tooltipObject = (tooltipRow || {}) as Record<string, unknown>
  const compareTooltipPartners = SAMSUNG_PARTNERS.filter((partner) => (
    `tooltip_${partner.key}` in tooltipObject
  ))
  const metricIsLossRatio = measure.toLowerCase().includes("loss_ratio")
  const rowPeriodStart = formatMonth(String(tooltipRow?.period_start ?? ""))
  const rowPeriodEnd = formatMonth(String((tooltipRow?.period_end ?? label) || ""))
  const effectivePeriodStart = rowPeriodStart || periodStartLabel
  const effectivePeriodEnd = rowPeriodEnd || formattedLabel
  const showPeriod = Boolean(
    showPeriodRange &&
    metricIsLossRatio &&
    effectivePeriodStart &&
    effectivePeriodEnd
  )
  const showQuantityOnly = compareTooltipQuantity && compareTooltipPartners.length > 0

  if (showQuantityOnly) {
    return (
      <div className="max-w-[min(84vw,280px)] rounded-lg border bg-white p-2.5 shadow sm:p-3">
        <p className="text-[11px] font-bold text-gray-400 sm:text-xs">{formattedLabel}</p>
        {showPeriod && (
          <p className="mt-1 text-[10px] font-semibold text-indigo-500">
            Period: {effectivePeriodStart} to {effectivePeriodEnd}
          </p>
        )}
        <div className="mt-2">
          <div className="grid grid-cols-[minmax(92px,1fr)_auto_auto] items-center gap-x-2 gap-y-1 text-[10px] font-semibold text-slate-500 sm:grid-cols-[minmax(120px,1fr)_auto_auto] sm:gap-x-4 sm:text-[11px]">
            <span />
            <span>Quantity</span>
            <span>{prettyLabel(measure)}</span>
          </div>
          <div className="mt-1 grid grid-cols-[minmax(92px,1fr)_auto_auto] items-center gap-x-2 gap-y-1.5 text-[11px] font-semibold sm:grid-cols-[minmax(120px,1fr)_auto_auto] sm:gap-x-4 sm:gap-y-2 sm:text-sm">
            {compareTooltipPartners.map((partner) => {
              const quantity = asNumber(tooltipObject[`tooltip_${partner.key}`])
              const metricValue = asNumber(tooltipObject[partner.key])
              return (
                <Fragment key={`tooltip-${partner.key}`}>
                  <span className="flex items-center gap-2 text-slate-700">
                    <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ backgroundColor: partner.color }} />
                    {partner.shortLabel}
                  </span>
                  <span className="text-slate-900">{quantity.toLocaleString()}</span>
                  <span className="text-slate-900">{formatValue(metricValue, measure)}</span>
                </Fragment>
              )
            })}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="max-w-[min(84vw,280px)] rounded-lg border bg-white p-2.5 shadow sm:p-3">
      <p className="text-[11px] font-bold text-gray-400 sm:text-xs">{formattedLabel}</p>
      {showPeriod && (
        <p className="mt-1 text-[10px] font-semibold text-indigo-500">
          Period: {effectivePeriodStart} to {effectivePeriodEnd}
        </p>
      )}
      <div className="space-y-0.5 sm:space-y-1">
        {payload.map((p) => (
          (() => {
            const metricKey = toOriginalMetricKey(String(p.dataKey || ""))
            const metricValue = p.payload && metricKey in p.payload
              ? asNumber(p.payload[metricKey])
              : asNumber(p.value)
            return (
              <div key={p.dataKey} className="flex items-center gap-2 text-[11px] font-semibold sm:text-sm">
                <span
                  className="inline-block h-2.5 w-2.5 rounded-full"
                  style={{ backgroundColor: p.color || "#64748b" }}
                />
                <span className="text-slate-700">
                  {p.name || p.dataKey}
                </span>
                <span className="ml-auto text-slate-900">
                  {formatValue(metricValue, measure)}
                </span>
              </div>
            )
          })()
        ))}
      </div>
      <p className="mt-1.5 text-[10px] font-semibold text-indigo-500 sm:mt-2">
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
  chartType = "bar",
  tooltipMetricOverride,
  heightClassName = "h-72",
  categoryFilters,
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
  const [isMobileViewport, setIsMobileViewport] = useState<boolean>(() => {
    if (typeof window === "undefined") return false
    return window.innerWidth < 640
  })
  const [activeMapKey, setActiveMapKey] = useState<string | null>(null)
  const [hoverDetailByStateId, setHoverDetailByStateId] = useState<
    Record<string, Partial<Record<HoverDetailMetricKey, number>>>
  >({})
  const [hoverDetailLoadingKey, setHoverDetailLoadingKey] = useState<string | null>(null)
  const [hoverCard, setHoverCard] = useState<{
    key: string
    label: string
    x: number
    y: number
    width: number
    compact: boolean
  } | null>(null)
  const mapPanelRef = useRef<HTMLDivElement | null>(null)
  const requestIdRef = useRef(0)
  const gradientId = useId()
  const gradientIdAlt = useId()
  const gradientIdThird = useId()
  const normalizedCategoryFilters = useMemo<GraphCategoryFilter[]>(() => (
    (categoryFilters || [])
      .map((filter) => ({
        dimension: toSafeKey(String(filter.dimension || "")),
        values: Array.isArray(filter.values)
          ? filter.values
              .map((value) => String(value || "").trim())
              .filter(Boolean)
          : [],
      }))
      .filter((filter) => Boolean(filter.dimension) && filter.values.length > 0)
      .slice(0, 2)
  ), [categoryFilters])
  const categoryFiltersKey = useMemo(
    () => JSON.stringify(normalizedCategoryFilters),
    [normalizedCategoryFilters]
  )

  useEffect(() => {
    setActiveMapKey(null)
    setHoverCard(null)
    setHoverDetailByStateId({})
    setHoverDetailLoadingKey(null)
  }, [source, dimension, metric, datasetType, bucket, chartType, fromDate, toDate, jobId, categoryFiltersKey])

  useEffect(() => {
    if (typeof window === "undefined") return
    const onResize = () => setIsMobileViewport(window.innerWidth < 640)
    window.addEventListener("resize", onResize)
    return () => {
      window.removeEventListener("resize", onResize)
    }
  }, [])

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
      { rootMargin: "420px 0px 420px 0px" }
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
        const tooltipMetricKey = toSafeKey(tooltipMetricOverride || "")

        if (source === "samsung") {
          setCompareMode(true)
          // Backend returns all Samsung partners in one response to avoid extra compare requests.
          const combined = await fetchRowsWithRangeFallback({
            source: "samsung",
            dimension,
            metric,
            datasetType,
            bucket,
            jobId,
            from_date: fromDate,
            to_date: toDate,
            categoryFilters: normalizedCategoryFilters,
          })

          let merged: Row[] = (combined.data || []).map((row) => {
            const next: Row = { ...row }
            SAMSUNG_PARTNERS.forEach((partner) => {
              next[partner.key] = asNumber(row[partner.key])
            })
            return next
          })

          if (tooltipMetricOverride && tooltipMetricKey && tooltipMetricKey !== metricKey) {
            const tooltipRows = await fetchRowsWithRangeFallback({
              source: "samsung",
              dimension,
              metric: tooltipMetricOverride,
              datasetType,
              bucket,
              jobId,
              from_date: fromDate,
              to_date: toDate,
              categoryFilters: normalizedCategoryFilters,
            })

            const tooltipMap = new Map<string, Record<SamsungPartnerKey, number>>()
            for (const row of tooltipRows.data || []) {
              const dimValue = String(row[dimKey] ?? "")
              const partnerValues = {} as Record<SamsungPartnerKey, number>
              SAMSUNG_PARTNERS.forEach((partner) => {
                partnerValues[partner.key] = asNumber(row[partner.key])
              })
              tooltipMap.set(dimValue, partnerValues)
            }

            merged = merged.map((row) => {
              const dimValue = String(row[dimKey] ?? "")
              const tooltip = tooltipMap.get(dimValue)
              const next: Row = { ...row }
              SAMSUNG_PARTNERS.forEach((partner) => {
                next[`tooltip_${partner.key}`] = tooltip?.[partner.key] ?? 0
              })
              return next
            })
          }

          if (dimKey.includes("month") || dimKey.includes("date")) {
            merged = padSingleTemporalRows(sortTemporalRows(merged, dimKey), dimKey)
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
          categoryFilters: normalizedCategoryFilters,
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
          const normalizedSource = normalizeSamsungSource(source)
          const partnerMetricKey =
            isSamsungPartnerSource(source)
              ? normalizedSource as SamsungPartnerKey
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
          next = padSingleTemporalRows(sortTemporalRows(next, dimKey), dimKey)
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
  }, [
    source,
    dimension,
    metric,
    datasetType,
    bucket,
    jobId,
    fromDate,
    toDate,
    fetchDelayMs,
    onDataReady,
    deferUntilVisible,
    isVisible,
    tooltipMetricOverride,
    categoryFiltersKey,
    normalizedCategoryFilters,
  ])

  const hoverDetailQueryLabel = useMemo(() => {
    if (chartType !== "india_map" || !hoverCard?.key) return ""
    const stateDimensionKey = toSafeKey(dimension || "state")
    for (const row of data) {
      const stateName = String(row[stateDimensionKey] ?? row.state ?? "").trim()
      if (!stateName) continue
      const stateId = mapStateToIndiaStateId(stateName)
      if (stateId === hoverCard.key) return stateName
    }
    return hoverCard.label || ""
  }, [chartType, data, dimension, hoverCard])

  useEffect(() => {
    if (chartType !== "india_map" || !source || !hoverCard?.key || !hoverDetailQueryLabel) return

    const cachedMetrics = hoverDetailByStateId[hoverCard.key] || {}
    const pendingMetrics = HOVER_DETAIL_METRICS.filter((detailMetric) => cachedMetrics[detailMetric] == null)
    if (!pendingMetrics.length) return

    let active = true
    const hoverFilters: GraphCategoryFilter[] = [
      ...normalizedCategoryFilters.filter((filter) => filter.dimension !== "state").slice(0, 1),
      { dimension: "state", values: [hoverDetailQueryLabel] },
    ]

    const timer = setTimeout(async () => {
      if (!active) return
      setHoverDetailLoadingKey(hoverCard.key)

      const nextMetrics: Partial<Record<HoverDetailMetricKey, number>> = { ...cachedMetrics }
      await Promise.all(
        pendingMetrics.map(async (detailMetric) => {
          try {
            const result = await fetchGraphRows({
              source,
              dimension: "state",
              metric: detailMetric,
              datasetType: "sales",
              bucket,
              jobId,
              from_date: fromDate,
              to_date: toDate,
              categoryFilters: hoverFilters,
            })
            const measureKey = toSafeKey(result.measure || detailMetric)
            nextMetrics[detailMetric] = (result.data || []).reduce((sum, row) => (
              sum + asNumber(row[measureKey] ?? row[detailMetric])
            ), 0)
          } catch {
            // ignore hover detail misses per metric and keep map interactive
          }
        })
      )

      if (!active) return
      setHoverDetailByStateId((prev) => ({
        ...prev,
        [hoverCard.key]: nextMetrics,
      }))
      setHoverDetailLoadingKey((prev) => (prev === hoverCard.key ? null : prev))
    }, 120)

    return () => {
      active = false
      clearTimeout(timer)
    }
  }, [
    chartType,
    source,
    bucket,
    jobId,
    fromDate,
    toDate,
    hoverCard?.key,
    hoverDetailByStateId,
    hoverDetailQueryLabel,
    normalizedCategoryFilters,
  ])

  if (deferUntilVisible && !isVisible) {
    return (
      <div ref={containerRef} className={`${heightClassName} flex items-center justify-center text-sm text-gray-400`}>
        Loading chart...
      </div>
    )
  }

  if (loading) {
    return (
      <div ref={containerRef} className={`${heightClassName} flex items-center justify-center text-sm text-gray-500`}>
        Loading...
      </div>
    )
  }

  if (error || !data.length || !measure) {
    return (
      <div ref={containerRef} className={`${heightClassName} flex items-center justify-center text-sm text-gray-400`}>
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
  const showCompareQuantityTooltip =
    compareMode && toSafeKey(tooltipMetricOverride || "") === "quantity"
  const clampToZero = !isLossRatio || source === "reliance"
  const useLogScale = false
  const primaryPlotKey = useLogScale ? toLogPlotKey(measure) : measure
  const ewCountPlotKey = useLogScale ? toLogPlotKey("ew_count") : "ew_count"
  const samsungCompareSeries = SAMSUNG_PARTNERS.map((partner, index) => ({
    ...partner,
    plotKey: useLogScale ? toLogPlotKey(partner.key) : partner.key,
    gradientId: index === 0 ? gradientId : index === 1 ? gradientIdAlt : gradientIdThird,
  }))
  const isSamsungSource = source === "samsung"
  const isRelianceSource = source === "reliance"
  const chartData: Row[] = data.map((row) => {
    const next: Row = { ...row }
    if (compareMode) {
      if (clampToZero) {
        SAMSUNG_PARTNERS.forEach((partner) => {
          next[partner.key] = Math.max(0, asNumber(row[partner.key]))
        })
      }
      if (useLogScale) {
        samsungCompareSeries.forEach((series) => {
          next[series.plotKey] = toLogSafeValue(next[series.key])
        })
      }
      return next
    }

    if (clampToZero) {
      next[measure] = Math.max(0, asNumber(row[measure]))
      if (showEwCounts) {
        next.ew_count = Math.max(0, asNumber(row.ew_count))
      }
    }
    if (useLogScale) {
      next[primaryPlotKey] = toLogSafeValue(next[measure])
      if (showEwCounts) {
        next[ewCountPlotKey] = toLogSafeValue(next.ew_count)
      }
    }
    return next
  })
  const shouldAnimateBars = !prefersReducedMotion && chartData.length <= 36
  const periodStartLabel = (() => {
    if (!isTemporalDimension || !chartData.length) return ""
    const firstRow = chartData[0]
    const explicitStart = String(firstRow?.period_start ?? "").trim()
    if (explicitStart) return formatMonth(explicitStart)
    return formatMonth(String(firstRow?.[dimKey] ?? ""))
  })()
  const barAnimationDuration = shouldAnimateBars ? 500 : 0
  const pieData = chartData
    .map((row) => {
      const baseValue = compareMode
        ? Math.max(0, sumSamsungPartnerValues(row as Record<string, unknown>))
        : Math.max(0, asNumber(row[measure]))
      const ewValue = !compareMode && isRelianceSource && measure.includes("quantity")
        ? Math.max(0, asNumber(row.ew_count))
        : 0
      return {
        name: String(row[dimKey] ?? "Unknown"),
        value: baseValue + ewValue,
        baseValue,
        ewValue,
      }
    })
    .filter((row) => row.value > 0)
  const pieRows = pieData.length
    ? pieData
    : [{ name: "No Data", value: 1, baseValue: 1, ewValue: 0 }]
  const pieGradientBase = (gradientId || "pie").replace(/[^a-zA-Z0-9_-]/g, "")
  const pieSlices = pieRows.map((entry, idx) => {
    const baseColor = pieData.length
      ? pickColor(`${baseKey}-${entry.name}`, palette)
      : "#dbeafe"
    return {
      ...entry,
      baseColor,
      gradientFrom: mixWithWhite(baseColor, 0.28),
      gradientTo: mixWithBlack(baseColor, 0.16),
      gradientId: `${pieGradientBase}-pie-${idx}`,
    }
  })
  const pieTotal = pieSlices.reduce((sum, slice) => sum + asNumber(slice.value), 0)
  const pieLegendItems = pieSlices.map((slice) => {
    const value = asNumber(slice.value)
    const percentage = pieTotal > 0 ? (value / pieTotal) * 100 : 0
    return {
      ...slice,
      value,
      percentage,
      formattedValue: measure.includes("quantity")
        ? value.toLocaleString()
        : formatValue(value, measure),
    }
  })
  const radarData = chartData.map((row) => {
    const next: Row = {
      name: String(row[dimKey] ?? "Unknown"),
      [measure]: Math.max(0, asNumber(row[measure])),
    }
    SAMSUNG_PARTNERS.forEach((partner) => {
      next[partner.key] = Math.max(0, asNumber(row[partner.key]))
    })
    return next
  })
  const indiaMapValuesByStateId = new Map<string, number>()
  INDIA_MAP_LOCATIONS.forEach((location) => {
    indiaMapValuesByStateId.set(location.id, 0)
  })
  chartData.forEach((row) => {
    const stateName = String(row[dimKey] ?? "").trim()
    const stateId = mapStateToIndiaStateId(stateName)
    if (!stateId) return
    const value = compareMode
      ? Math.max(0, sumSamsungPartnerValues(row as Record<string, unknown>))
      : Math.max(0, asNumber(row[measure]))
    indiaMapValuesByStateId.set(stateId, (indiaMapValuesByStateId.get(stateId) || 0) + value)
  })
  const indiaMapEntries = INDIA_MAP_LOCATIONS.map((location) => {
    const value = indiaMapValuesByStateId.get(location.id) || 0
    return {
      key: location.id,
      label: location.name,
      path: location.path,
      value,
    }
  })
  const indiaMapLegendEntries = indiaMapEntries
    .filter((entry) => entry.value > 0)
    .sort((a, b) => b.value - a.value)
  const maxIndiaMapValue = indiaMapEntries.reduce((max, entry) => Math.max(max, entry.value), 0)
  const indiaMapLegendRows = indiaMapLegendEntries.length ? indiaMapLegendEntries : indiaMapEntries
  const hoverMetricValues = hoverCard ? hoverDetailByStateId[hoverCard.key] || {} : {}

  const placeHoverCard = (
    clientX: number,
    clientY: number,
    stateKey: string,
    stateLabel: string
  ) => {
    const host = mapPanelRef.current
    if (!host) return
    const rect = host.getBoundingClientRect()
    const viewportWidth = typeof window !== "undefined" ? window.innerWidth : rect.width
    const compact = viewportWidth < 640 || rect.width < 380
    const desiredWidth = compact ? 210 : 254
    const maxAllowedWidth = Math.max(120, rect.width - 16)
    const cardWidth = Math.min(desiredWidth, maxAllowedWidth)
    const cardHeight = compact ? 142 : 168
    let x = clientX - rect.left + 12
    let y = clientY - rect.top + 12
    if (x + cardWidth > rect.width - 8) x = rect.width - cardWidth - 8
    if (y + cardHeight > rect.height - 8) y = rect.height - cardHeight - 8
    if (x < 8) x = 8
    if (y < 8) y = 8
    setHoverCard({
      key: stateKey,
      label: stateLabel,
      x,
      y,
      width: cardWidth,
      compact,
    })
  }

  const updateHoverCardPosition = (
    event: MouseEvent<SVGPathElement>,
    stateKey: string,
    stateLabel: string
  ) => {
    placeHoverCard(event.clientX, event.clientY, stateKey, stateLabel)
  }

  const updateHoverCardPositionFromTouch = (
    event: TouchEvent<SVGPathElement>,
    stateKey: string,
    stateLabel: string
  ) => {
    const touch = event.touches[0] || event.changedTouches[0]
    if (!touch) return
    placeHoverCard(touch.clientX, touch.clientY, stateKey, stateLabel)
  }

  return (
    <div ref={containerRef} className={`smooth-surface ${heightClassName}`}>
      {compareMode && chartType !== "pie" && chartType !== "india_map" && (
        <div className="mb-2 flex items-center gap-3 text-[11px] font-semibold text-slate-500">
          {samsungCompareSeries.map((series) => (
            <span key={`legend-${series.key}`} className="flex items-center gap-1.5">
              <span
                className="inline-block h-2.5 w-2.5 rounded-full"
                style={{ backgroundColor: series.color }}
              />
              {series.shortLabel}
            </span>
          ))}
        </div>
      )}
      {chartType === "india_map" ? (
        <div className="flex h-full min-h-0 flex-col gap-2">
          <div
            ref={mapPanelRef}
            className="relative min-h-0 basis-[64%] rounded-xl border border-slate-200/80 bg-slate-50/60 px-2 py-2"
          >
            <svg
              viewBox={indiaSvgMap.viewBox}
              className="h-full w-full"
              aria-label="India state heatmap"
              role="img"
            >
              {indiaMapEntries.map((entry) => {
                const ratio = maxIndiaMapValue > 0 ? entry.value / maxIndiaMapValue : 0
                const isActive = activeMapKey === entry.key
                const isDimmed = activeMapKey !== null && !isActive
                const baseFill = entry.value > 0
                  ? interpolateHex(INDIA_HEATMAP_LOW, INDIA_HEATMAP_HIGH, Math.max(0.08, ratio))
                  : "#e5e7eb"
                const fill = isActive ? mixWithBlack(baseFill, 0.14) : baseFill
                const stroke = isActive ? "#0f172a" : "#ffffff"
                const accessibleValue = measure.includes("quantity")
                  ? entry.value.toLocaleString()
                  : formatValue(entry.value, measure)
                return (
                  <path
                    key={entry.key}
                    d={entry.path}
                    fill={fill}
                    stroke={stroke}
                    strokeWidth={isActive ? 1.4 : 0.8}
                    opacity={isDimmed ? 0.35 : 1}
                    className="cursor-pointer transition-all duration-150 ease-out"
                    onClick={() => setActiveMapKey((prev) => (prev === entry.key ? null : entry.key))}
                    onMouseEnter={(event) => updateHoverCardPosition(event, entry.key, entry.label)}
                    onMouseMove={(event) => updateHoverCardPosition(event, entry.key, entry.label)}
                    onTouchStart={(event) => updateHoverCardPositionFromTouch(event, entry.key, entry.label)}
                    onTouchMove={(event) => updateHoverCardPositionFromTouch(event, entry.key, entry.label)}
                    onMouseLeave={() => setHoverCard(null)}
                    onTouchCancel={() => setHoverCard(null)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault()
                        setActiveMapKey((prev) => (prev === entry.key ? null : entry.key))
                      }
                    }}
                    onBlur={() => setHoverCard(null)}
                    tabIndex={0}
                    role="button"
                    aria-pressed={isActive}
                    aria-label={`${entry.label}: ${accessibleValue}`}
                  />
                )
              })}
            </svg>
            {hoverCard && (
              <div
                className={`pointer-events-none absolute z-20 rounded-lg border border-slate-300/90 bg-white/96 shadow-[0_16px_36px_-18px_rgba(15,23,42,0.5)] ${
                  hoverCard.compact ? "p-2.5" : "p-3"
                }`}
                style={{ left: `${hoverCard.x}px`, top: `${hoverCard.y}px`, width: `${hoverCard.width}px` }}
              >
                <div className={`mb-1 font-bold text-slate-800 ${hoverCard.compact ? "text-[11px]" : "text-xs"}`}>
                  {hoverCard.label}
                </div>
                <div className={hoverCard.compact ? "space-y-0.5" : "space-y-1"}>
                  {HOVER_DETAIL_METRICS.map((detailMetric) => {
                    const value = hoverMetricValues[detailMetric]
                    const hasValue = value != null
                    const formatted = hasValue
                      ? formatValue(asNumber(value), detailMetric)
                      : hoverDetailLoadingKey === hoverCard.key
                        ? "Loading..."
                        : "N/A"
                    return (
                      <div
                        key={`${hoverCard.key}-${detailMetric}`}
                        className={`grid grid-cols-[minmax(0,1fr)_auto] items-center ${
                          hoverCard.compact ? "gap-1.5 text-[10px]" : "gap-2 text-[11px]"
                        }`}
                      >
                        <span className="truncate text-slate-600">{HOVER_DETAIL_LABELS[detailMetric]}</span>
                        <span className="font-semibold text-slate-800">{formatted}</span>
                      </div>
                    )
                  })}
                </div>
              </div>
            )}
          </div>
          <div className="rounded-xl border border-slate-200/80 bg-white/90 px-2.5 py-2">
            <div className="mb-1 text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-500">
              Heat Scale
            </div>
            <div className="flex items-center gap-2 text-[10px] font-semibold text-slate-500">
              <span>0</span>
              <div
                className="h-2 flex-1 rounded-full border border-slate-200"
                style={{ backgroundImage: `linear-gradient(90deg, ${INDIA_HEATMAP_LOW}, ${INDIA_HEATMAP_HIGH})` }}
              />
              <span>
                {measure.includes("quantity")
                  ? maxIndiaMapValue.toLocaleString()
                  : formatValue(maxIndiaMapValue, measure)}
              </span>
            </div>
          </div>
          <div className="min-h-[138px] basis-[30%] overflow-y-auto rounded-xl border border-slate-200/80 bg-white/85 p-2">
            <div className="mb-2 text-[10px] font-bold uppercase tracking-[0.14em] text-slate-500">
              State and UT Legends
            </div>
            <ul className="space-y-1.5">
              {indiaMapLegendRows.map((entry) => {
                const ratio = maxIndiaMapValue > 0 ? entry.value / maxIndiaMapValue : 0
                const isActive = activeMapKey === entry.key
                const isDimmed = activeMapKey !== null && !isActive
                const swatch = entry.value > 0
                  ? interpolateHex(INDIA_HEATMAP_LOW, INDIA_HEATMAP_HIGH, Math.max(0.08, ratio))
                  : "#e5e7eb"
                const formatted = measure.includes("quantity")
                  ? entry.value.toLocaleString()
                  : formatValue(entry.value, measure)
                return (
                  <li key={`legend-${entry.key}`}>
                    <button
                      type="button"
                      className={`flex w-full items-center gap-2 rounded-lg border px-2.5 py-1.5 text-left text-[11px] transition ${
                        isActive
                          ? "border-slate-400 bg-slate-100/90 text-slate-900"
                          : "border-slate-200 bg-white text-slate-700 hover:border-slate-300"
                      }`}
                      style={{ opacity: isDimmed ? 0.45 : 1 }}
                      onClick={() => setActiveMapKey((prev) => (prev === entry.key ? null : entry.key))}
                      aria-pressed={isActive}
                    >
                      <span
                        className="inline-block h-2.5 w-2.5 rounded-full border border-white/80"
                        style={{ backgroundColor: isActive ? mixWithBlack(swatch, 0.14) : swatch }}
                      />
                      <span className="truncate">{entry.label}</span>
                      <span className="ml-auto whitespace-nowrap font-semibold text-slate-500">
                        {formatted}
                      </span>
                    </button>
                  </li>
                )
              })}
            </ul>
          </div>
        </div>
      ) : chartType === "pie" && !isSamsungSource ? (
        <div className="flex h-full min-h-0 flex-col">
          <div className="min-h-0 basis-[68%]">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart margin={{ top: 2, right: 4, bottom: 2, left: 4 }}>
                <defs>
                  {pieSlices.map((slice) => (
                    <linearGradient
                      key={slice.gradientId}
                      id={slice.gradientId}
                      x1="0%"
                      y1="0%"
                      x2="100%"
                      y2="100%"
                    >
                      <stop offset="0%" stopColor={slice.gradientFrom} />
                      <stop offset="100%" stopColor={slice.gradientTo} />
                    </linearGradient>
                  ))}
                </defs>
                <Tooltip
                  formatter={(value: unknown, name: unknown) => {
                    const numericValue = asNumber(value)
                    const pct = pieTotal > 0 ? (numericValue / pieTotal) * 100 : 0
                    return [`${formatValue(numericValue, measure)} (${pct.toFixed(1)}%)`, String(name || "")]
                  }}
                />
                <Pie
                  data={pieSlices}
                  dataKey="value"
                  nameKey="name"
                  innerRadius="36%"
                  outerRadius="72%"
                  paddingAngle={2}
                  stroke="#ffffff"
                  strokeWidth={1}
                  isAnimationActive={shouldAnimateBars}
                >
                  {pieSlices.map((entry, idx) => (
                    <Cell
                      key={`${entry.name}-${idx}`}
                      fill={`url(#${entry.gradientId})`}
                    />
                  ))}
                </Pie>
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-1 min-h-[78px] basis-[32%] overflow-y-auto pr-1">
            <ul className="space-y-1.5">
              {pieLegendItems.map((item) => (
                <li key={item.gradientId} className="flex items-center gap-2 text-[11px] text-slate-600">
                  <span
                    className="inline-block h-2.5 w-2.5 rounded-full border border-white/80"
                    style={{
                      backgroundImage: `linear-gradient(135deg, ${item.gradientFrom}, ${item.gradientTo})`,
                    }}
                  />
                  <span className="truncate" title={item.name}>
                    {truncateLabel(item.name, 24)}
                  </span>
                  <span className="ml-auto whitespace-nowrap font-semibold text-slate-500">
                    {item.percentage.toFixed(1)}%
                  </span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      ) : (
      <ResponsiveContainer width="100%" height="100%">
        {chartType === "line" ? (
          <AreaChart
            data={chartData}
            margin={
              isMobileViewport
                ? { top: 8, right: 6, left: 2, bottom: 4 }
                : { top: 12, right: 10, left: 14, bottom: 8 }
            }
          >
            <defs>
              <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={mixWithWhite(primaryColor, 0.15)} stopOpacity={0.55} />
                <stop offset="100%" stopColor={primaryColor} stopOpacity={0.04} />
              </linearGradient>
              <linearGradient id={gradientIdAlt} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={mixWithWhite(secondaryColor, 0.15)} stopOpacity={0.55} />
                <stop offset="100%" stopColor={secondaryColor} stopOpacity={0.04} />
              </linearGradient>
              {samsungCompareSeries[2] && (
                <linearGradient id={gradientIdThird} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={mixWithWhite(samsungCompareSeries[2].color, 0.15)} stopOpacity={0.55} />
                  <stop offset="100%" stopColor={samsungCompareSeries[2].color} stopOpacity={0.04} />
                </linearGradient>
              )}
            </defs>
            <CartesianGrid strokeDasharray="4 4" vertical={false} stroke="#e2e8f0" />
            <XAxis
              dataKey={dimKey}
              interval={isTemporalDimension ? "preserveStartEnd" : "preserveEnd"}
              minTickGap={isTemporalDimension ? (isMobileViewport ? 8 : 16) : 8}
              tick={{ fontSize: isMobileViewport ? 10 : 11 }}
              tickFormatter={(v) => (isTemporalDimension ? formatMonth(v) : String(v))}
            />
            <YAxis
              scale={useLogScale ? "log" : "auto"}
              domain={useLogScale ? [1, "auto"] : (clampToZero ? [0, "auto"] : ["auto", "auto"])}
              width={isMobileViewport ? 52 : 72}
              tickMargin={isMobileViewport ? 4 : 6}
              tick={{ fontSize: isMobileViewport ? 10 : 11 }}
              tickFormatter={(v) => formatAxisCompact(asNumber(v), measure)}
            />
            <Tooltip
              allowEscapeViewBox={{ x: false, y: false }}
              wrapperStyle={{
                maxWidth: isMobileViewport ? "calc(100vw - 40px)" : "320px",
                zIndex: 30,
              }}
              content={
                <CustomTooltip
                  measure={measure}
                  compareTooltipQuantity={showCompareQuantityTooltip}
                  showPeriodRange={isTemporalDimension}
                  periodStartLabel={periodStartLabel}
                />
              }
            />
            {compareMode ? (
              <>
                {samsungCompareSeries.map((series) => (
                  <Area
                    key={`area-${series.key}`}
                    type="monotone"
                    dataKey={series.plotKey}
                    name={series.shortLabel}
                    stroke={series.color}
                    fill={`url(#${series.gradientId})`}
                    strokeWidth={2.4}
                    fillOpacity={1}
                    isAnimationActive={shouldAnimateBars}
                    dot={false}
                  />
                ))}
              </>
            ) : (
              <>
                <Area
                  type="monotone"
                  dataKey={primaryPlotKey}
                  name={prettyLabel(measure)}
                  stroke={primaryColor}
                  fill={`url(#${gradientId})`}
                  strokeWidth={2.5}
                  fillOpacity={1}
                  isAnimationActive={shouldAnimateBars}
                  dot={false}
                />
                {showEwCounts && (
                  <Line
                    type="monotone"
                    dataKey={ewCountPlotKey}
                    name="EW Count"
                    stroke={secondaryColor}
                    strokeWidth={2}
                    dot={false}
                    isAnimationActive={shouldAnimateBars}
                  />
                )}
              </>
            )}
          </AreaChart>
        ) : chartType === "pie" ? (
          <PieChart margin={{ top: 6, right: 20, bottom: 22, left: 20 }}>
            <defs>
              {pieSlices.map((slice) => (
                <linearGradient
                  key={slice.gradientId}
                  id={slice.gradientId}
                  x1="0%"
                  y1="0%"
                  x2="100%"
                  y2="100%"
                >
                  <stop offset="0%" stopColor={slice.gradientFrom} />
                  <stop offset="100%" stopColor={slice.gradientTo} />
                </linearGradient>
              ))}
            </defs>
            <Tooltip
              formatter={(value: unknown, name: unknown) => [
                formatValue(asNumber(value), measure),
                String(name || ""),
              ]}
            />
            <Pie
              data={pieSlices}
              dataKey="value"
              nameKey="name"
              innerRadius={44}
              outerRadius={98}
              paddingAngle={2}
              stroke="#ffffff"
              strokeWidth={1}
              isAnimationActive={shouldAnimateBars}
            >
              {pieSlices.map((entry, idx) => (
                <Cell
                  key={`${entry.name}-${idx}`}
                  fill={`url(#${entry.gradientId})`}
                />
              ))}
            </Pie>
            <Legend
              verticalAlign="bottom"
              align="center"
              iconType="circle"
              wrapperStyle={{ fontSize: "11px", paddingTop: "8px", color: "#475569" }}
              formatter={(value, _entry, index) => {
                const item = pieLegendItems[index] || null
                if (!item) return String(value)
                return `${String(value)} (${item.percentage.toFixed(1)}%)`
              }}
            />
          </PieChart>
        ) : chartType === "radar" ? (
          <RadarChart
            data={radarData}
            cx="50%"
            cy="50%"
            outerRadius="72%"
            margin={{ top: 8, right: 10, bottom: 8, left: 10 }}
          >
            <PolarGrid stroke="#d6dde8" />
            <PolarAngleAxis
              dataKey="name"
              tick={{ fontSize: 10, fill: "#64748b" }}
              tickFormatter={(value) => truncateLabel(String(value || ""), 16)}
            />
            <PolarRadiusAxis tick={{ fontSize: 10, fill: "#94a3b8" }} tickFormatter={(v) => formatAxisCompact(asNumber(v), measure)} />
            <Tooltip
              formatter={(value: unknown, name: unknown) => [
                formatValue(asNumber(value), measure),
                String(name || ""),
              ]}
            />
            {compareMode ? (
              <>
                {SAMSUNG_PARTNERS.map((partner) => (
                  <Radar
                    key={`radar-${partner.key}`}
                    name={partner.shortLabel}
                    dataKey={partner.key}
                    stroke={partner.color}
                    fill={partner.color}
                    fillOpacity={0.22}
                    isAnimationActive={shouldAnimateBars}
                  />
                ))}
              </>
            ) : (
              <Radar
                name={prettyLabel(measure)}
                dataKey={measure}
                stroke={primaryColor}
                fill={primaryColor}
                fillOpacity={0.3}
                isAnimationActive={shouldAnimateBars}
              />
            )}
          </RadarChart>
        ) : (
          <BarChart data={chartData} margin={{ top: 12, right: 10, left: 14, bottom: 8 }} barCategoryGap={14}>
            <defs>
              <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={mixWithWhite(primaryColor, 0.35)} />
                <stop offset="100%" stopColor={primaryColor} />
              </linearGradient>
              <linearGradient id={gradientIdAlt} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={mixWithWhite(secondaryColor, 0.35)} />
                <stop offset="100%" stopColor={secondaryColor} />
              </linearGradient>
              {samsungCompareSeries[2] && (
                <linearGradient id={gradientIdThird} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={mixWithWhite(samsungCompareSeries[2].color, 0.35)} />
                  <stop offset="100%" stopColor={samsungCompareSeries[2].color} />
                </linearGradient>
              )}
            </defs>
            <CartesianGrid strokeDasharray="4 4" vertical={false} stroke="#e2e8f0" />
            <XAxis
              dataKey={dimKey}
              interval={isTemporalDimension ? "preserveStartEnd" : "preserveEnd"}
              minTickGap={isTemporalDimension ? 16 : 8}
              tick={{ fontSize: 11 }}
              tickFormatter={(v) => (isTemporalDimension ? formatMonth(v) : String(v))}
            />
            <YAxis
              domain={clampToZero ? [0, "auto"] : ["auto", "auto"]}
              width={72}
              tickMargin={6}
              tick={{ fontSize: 11 }}
              tickFormatter={(v) => formatValue(asNumber(v), measure)}
            />
            <Tooltip
              content={
                <CustomTooltip
                  measure={measure}
                  compareTooltipQuantity={showCompareQuantityTooltip}
                  showPeriodRange={isTemporalDimension}
                  periodStartLabel={periodStartLabel}
                />
              }
            />
            {compareMode ? (
              <>
                {samsungCompareSeries.map((series, index) => (
                  <Bar
                    key={`bar-${series.key}`}
                    dataKey={series.key}
                    name={series.shortLabel}
                    barSize={18}
                    radius={[8, 8, 2, 2]}
                    fill={`url(#${series.gradientId})`}
                    isAnimationActive={shouldAnimateBars}
                    animationDuration={barAnimationDuration}
                    animationBegin={150 + (index * 100)}
                  />
                ))}
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
        )}
      </ResponsiveContainer>
      )}
    </div>
  )
}


