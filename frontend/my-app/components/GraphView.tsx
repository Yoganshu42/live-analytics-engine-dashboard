"use client"

import { Fragment, type MouseEvent, type TouchEvent, useEffect, useId, useMemo, useRef, useState } from "react"
import { useReducedMotion } from "framer-motion"
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
import { SALES_METRIC_ORDER } from "@/lib/salesMetricOrder"
import { fetchByDimensionBatch, type FetchByDimensionBatchItem } from "@/app/lib/api"
import IndiaSvgMap from "@svg-maps/india"

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
  eagerMapHoverPrefetch?: boolean
  enableCrossDatasetHoverCompare?: boolean
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

const formatMonth = (value: string, dimKey = "month") => {
  if (!value) return value
  if (typeof value === "string") {
    const shortMatch = value.match(/^[A-Za-z]{3}[-/]\d{2}$/)
    if (shortMatch) {
      return value.replace("-", " ").replace("/", " ")
    }
  }
  const d = new Date(value)
  if (isNaN(d.getTime())) return value
  if (toSafeKey(dimKey).includes("date")) {
    return d.toLocaleDateString("en-US", {
      day: "2-digit",
      month: "short",
      year: "2-digit",
    })
  }
  return d.toLocaleString("en-US", {
    month: "short",
    year: "2-digit",
  })
}

const canonicalizePlanCategoryLabel = (value: string, source: string) => {
  const raw = String(value || "").trim()
  const text = raw.toLowerCase().replace(/\s+/g, " ").trim()
  if (!text) return "Unknown"
  if (text.includes("combo")) return "Combo"
  if (text.includes("adld") || text.includes("accidental") || text.includes("liquid")) return "ADLD"
  if (source === "reliance" && (text.includes("crack") || text.includes("screen") || /\bsp\b|\bspp\b/.test(text))) {
    return "Crack Screen"
  }
  if (text.includes("screen") || text.includes("crack") || text.includes("protect max") || /\bsp\b|\bspp\b/.test(text)) {
    return "Screen Protection"
  }
  if (text.includes("extended") || text.includes("warranty") || /\bew\b/.test(text)) {
    return "Extended Warranty"
  }
  return raw.replace(/[^a-zA-Z0-9\s/-]/g, " ").replace(/\s+/g, " ").trim() || "Unknown"
}

const normalizeDimValue = (value: unknown, dimKey: string, source = "") => {
  if (value == null) return "Unknown"
  const raw = String(value).trim()
  if (!raw) return "Unknown"
  const safeDimKey = toSafeKey(dimKey)
  if (safeDimKey.includes("date")) {
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
      const day = String(d.getDate()).padStart(2, "0")
      return `${year}-${month}-${day}`
    }
  } else if (safeDimKey.includes("month")) {
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
  if (safeDimKey === "plan_category") {
    return canonicalizePlanCategoryLabel(raw, source)
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

const FILTERED_LABEL_DIMENSIONS = new Set([
  "state",
  "region",
  "channel",
  "product_category",
  "product_subcategory",
  "plan_category",
  "device_plan_category",
  "article_brand",
  "brand",
])

const UNKNOWN_LIKE_LABELS = new Set([
  "",
  "0",
  "unknown",
  "nan",
  "none",
  "null",
  "na",
  "other",
  "others",
])

const shouldFilterDimensionLabels = (dimKey: string) => {
  const safe = toSafeKey(dimKey)
  if (FILTERED_LABEL_DIMENSIONS.has(safe)) return true
  return Array.from(FILTERED_LABEL_DIMENSIONS).some((candidate) => safe.includes(candidate))
}

const isRelatableDimensionLabel = (value: unknown) => {
  const label = String(value ?? "").trim()
  if (!label) return false
  const compact = label.toLowerCase().replace(/[^a-z0-9]/g, "")
  return !UNKNOWN_LIKE_LABELS.has(compact)
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

const filterRelatableRows = (rows: Row[], dimKey: string): Row[] => {
  if (!shouldFilterDimensionLabels(dimKey)) return rows
  return rows.filter((row) => isRelatableDimensionLabel(row[dimKey]))
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

const toTimeValue = (value: unknown, dimKey = "month") => {
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

  const normalized = normalizeDimValue(raw, dimKey)
  const normalizedTs = new Date(normalized).getTime()
  if (!Number.isNaN(normalizedTs)) return normalizedTs

  const directTs = new Date(raw).getTime()
  return directTs
}

const sortTemporalRows = (rows: Row[], dimKey: string) =>
  [...rows].sort((a, b) => {
    const at = toTimeValue(a[dimKey], dimKey)
    const bt = toTimeValue(b[dimKey], dimKey)
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

const dayBucketValue = (date: Date) => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, "0")
  const day = String(date.getDate()).padStart(2, "0")
  return `${year}-${month}-${day}`
}

const temporalBucketValue = (date: Date, dimKey: string) => (
  toSafeKey(dimKey).includes("date") ? dayBucketValue(date) : monthBucketValue(date)
)

const buildZeroTemporalRow = (template: Row, dimKey: string, date: Date): Row => {
  const bucket = temporalBucketValue(date, dimKey)
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
  const centerTs = toTimeValue(rows[0][dimKey], dimKey)
  if (!Number.isFinite(centerTs)) return rows
  const center = new Date(centerTs)
  if (toSafeKey(dimKey).includes("date")) {
    const prev = new Date(center)
    const next = new Date(center)
    prev.setDate(center.getDate() - 1)
    next.setDate(center.getDate() + 1)
    return [
      buildZeroTemporalRow(rows[0], dimKey, prev),
      rows[0],
      buildZeroTemporalRow(rows[0], dimKey, next),
    ]
  } else {
    center.setDate(1)
    const prev = new Date(center)
    const next = new Date(center)
    prev.setMonth(center.getMonth() - 1)
    next.setMonth(center.getMonth() + 1)
    return [
      buildZeroTemporalRow(rows[0], dimKey, prev),
      rows[0],
      buildZeroTemporalRow(rows[0], dimKey, next),
    ]
  }
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

type GraphFetchOptions = {
  signal?: AbortSignal
}

type FetchRowsResult = {
  ts: number
  data: Row[]
  measure: string
  usedRangeFallback?: boolean
}

const GRAPH_RESULT_TTL_MS = 30000
const graphResultCache = new Map<string, { expiresAt: number; value: FetchRowsResult }>()
const graphInFlight = new Map<string, Promise<FetchRowsResult>>()

export const clearGraphDataCache = () => {
  graphResultCache.clear()
  graphInFlight.clear()
}

const normalizeFetchedRows = (
  rawRows: Array<Record<string, unknown>>,
  params: Pick<GraphFetchParams, "dimension" | "metric" | "source">
): FetchRowsResult => {
  const dimKey = toSafeKey(params.dimension)
  const metricKey = toSafeKey(params.metric)

  if (!Array.isArray(rawRows) || rawRows.length === 0) {
    return { ts: Date.now(), data: [], measure: metricKey, usedRangeFallback: false }
  }

  let processed: Row[] = rawRows.map((row) => {
    const out: Row = {}
    Object.entries(row || {}).forEach(([key, value]) => {
      out[toSafeKey(key)] = value
    })
    out[dimKey] = normalizeDimValue(pickDimensionValue(out, dimKey), dimKey, params.source)
    return out
  })

  if (dimKey.includes("product_category") || dimKey.includes("plan_category")) {
    processed = mergeRowsByDimension(processed, dimKey)
  }

  processed = filterRelatableRows(processed, dimKey)

  return {
    ts: Date.now(),
    data: processed,
    measure: metricKey,
    usedRangeFallback: false,
  }
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

const ENV_API_BASE = normalizeApiBase(process.env.NEXT_PUBLIC_API_BASE || "")
const API_BASE = ENV_API_BASE || DEFAULT_API_BASE
const runtimeOverride =
  typeof window !== "undefined"
    ? normalizeApiBase(new URLSearchParams(window.location.search).get("api") || "")
    : ""

const browserOriginApiBases =
  typeof window !== "undefined"
    ? [
        `${window.location.origin}/api`,
        window.location.origin,
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
      ENV_API_BASE,
      ...browserHostApiBases,
      ...browserOriginApiBases,
      API_BASE,
      "http://127.0.0.1:8000",
      "http://localhost:8000",
      "http://0.0.0.0:8000",
    ]
      .map(v => normalizeApiBase(v))
      .filter(Boolean)
  )
)
const API_REQUEST_TIMEOUT_MS = Number(
  process.env.NEXT_PUBLIC_ANALYTICS_TIMEOUT_MS
  || process.env.NEXT_PUBLIC_API_TIMEOUT_MS
  || 60000
)
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

const buildGraphCacheKey = (params: GraphFetchParams) => (
  `${params.source}|${params.datasetType}|${buildQuery(params)}`
)

const toBatchRequest = (
  params: GraphFetchParams,
  requestKey: string
): FetchByDimensionBatchItem => {
  let safeFrom = params.from_date
  let safeTo = params.to_date
  if (safeFrom && safeTo && safeFrom > safeTo) {
    const swappedFrom = safeTo
    const swappedTo = safeFrom
    safeFrom = swappedFrom
    safeTo = swappedTo
  }

  const request: FetchByDimensionBatchItem = {
    request_key: requestKey,
    source: params.source,
    dataset_type: params.datasetType,
    dimension: params.dimension,
    metric: params.metric,
  }
  if (params.jobId) request.job_id = params.jobId
  if (params.bucket) request.bucket = params.bucket
  if (safeFrom) request.from_date = safeFrom
  if (safeTo) request.to_date = safeTo

  const activeFilters = (params.categoryFilters || [])
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
    request[`filter_${slot}_dimension` as "filter_1_dimension" | "filter_2_dimension"] = filter.dimension
    request[`filter_${slot}_values` as "filter_1_values" | "filter_2_values"] = filter.values.join(",")
  })

  return request
}

const buildUrl = (base: string, query: string) => `${base}/analytics/by-dimension?${query}`

const writeGraphCache = (cacheKey: string, result: FetchRowsResult) => {
  if (result.data.length > 0) {
    graphResultCache.set(cacheKey, {
      expiresAt: Date.now() + GRAPH_RESULT_TTL_MS,
      value: result,
    })
    return
  }

  graphResultCache.delete(cacheKey)
}

const mergeAbortSignals = (signals: Array<AbortSignal | null | undefined>) => {
  const activeSignals = signals.filter(Boolean) as AbortSignal[]
  if (!activeSignals.length) return undefined
  if (activeSignals.length === 1) return activeSignals[0]

  const controller = new AbortController()
  const abort = () => controller.abort()
  activeSignals.forEach((signal) => {
    if (signal.aborted) {
      abort()
      return
    }
    signal.addEventListener("abort", abort, { once: true })
  })
  return controller.signal
}

const fetchWithTimeout = async (url: string, init: RequestInit) => {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), API_REQUEST_TIMEOUT_MS)
  try {
    const signal = mergeAbortSignals([init.signal, controller.signal])
    return await fetch(url, { ...init, signal })
  } finally {
    clearTimeout(timer)
  }
}

class NoFallbackError extends Error {
  noFallback = true
}

const fetchRows = async (
  params: GraphFetchParams,
  options: GraphFetchOptions = {}
): Promise<FetchRowsResult> => {
  const query = buildQuery(params)
  const cacheKey = buildGraphCacheKey(params)
  const now = Date.now()
  const cached = graphResultCache.get(cacheKey)
  if (cached && cached.expiresAt > now) {
    return cached.value
  }
  if (!options.signal) {
    const inFlight = graphInFlight.get(cacheKey)
    if (inFlight) {
      return inFlight
    }
  }

  const headers = new Headers()
  const token = getAuthToken()
  if (token) headers.set("Authorization", `Bearer ${token}`)

  const requestPromise = (async (): Promise<FetchRowsResult> => {
    const errors: string[] = []
    let sawUnauthorized = false
    for (const base of orderedApiBases()) {
        const url = buildUrl(base, query)
        try {
        const res = await fetchWithTimeout(url, { headers, mode: "cors", signal: options.signal })
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
        return normalizeFetchedRows(
          Array.isArray(raw) ? raw : [],
          { source: params.source, dimension: params.dimension, metric: params.metric }
        )
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

  if (!options.signal) {
    graphInFlight.set(cacheKey, requestPromise)
  }
  try {
    const result = await requestPromise
    writeGraphCache(cacheKey, result)
    return result
  } finally {
    if (!options.signal) {
      graphInFlight.delete(cacheKey)
    }
  }
}

const fetchRowsWithRangeFallback = async (
  params: GraphFetchParams,
  options: GraphFetchOptions = {}
): Promise<FetchRowsResult> => {
  return fetchRows(params, options)
}

export const fetchGraphRows = async (
  params: GraphFetchParams,
  options: GraphFetchOptions = {}
) => {
  return fetchRowsWithRangeFallback(params, options)
}

export const readGraphDataCache = (params: GraphFetchParams) => {
  const cached = graphResultCache.get(buildGraphCacheKey(params))
  if (!cached || cached.expiresAt <= Date.now()) return null
  return cached.value
}

export const seedGraphDataCache = (
  params: GraphFetchParams,
  rawRows: Array<Record<string, unknown>>
) => {
  if (!params.source || !params.dimension || !params.metric) return null
  const cacheKey = buildGraphCacheKey(params)
  const result = normalizeFetchedRows(rawRows, {
    source: params.source,
    dimension: params.dimension,
    metric: params.metric,
  })
  writeGraphCache(cacheKey, result)
  return result
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

const fetchRowsBatch = async (
  requests: Array<{ requestKey: string; params: GraphFetchParams }>,
  options: GraphFetchOptions = {}
): Promise<Record<string, FetchRowsResult>> => {
  const now = Date.now()
  const resolved: Record<string, FetchRowsResult> = {}
  const pending = requests.filter(({ requestKey, params }) => {
    const cached = graphResultCache.get(buildGraphCacheKey(params))
    if (cached && cached.expiresAt > now) {
      resolved[requestKey] = cached.value
      return false
    }
    return true
  })

  if (!pending.length) return resolved

  const batchResponse = await fetchByDimensionBatch(
    pending.map(({ requestKey, params }) => toBatchRequest(params, requestKey)),
    { signal: options.signal }
  )
  const resultByKey = new Map(
    (batchResponse.results || []).map((entry) => [entry.request_key, Array.isArray(entry.rows) ? entry.rows : []])
  )

  pending.forEach(({ requestKey, params }) => {
    const normalized = normalizeFetchedRows(resultByKey.get(requestKey) || [], {
      source: params.source,
      dimension: params.dimension,
      metric: params.metric,
    })
    writeGraphCache(buildGraphCacheKey(params), normalized)
    resolved[requestKey] = normalized
  })

  return resolved
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

const hslToHex = (hue: number, saturation: number, lightness: number) => {
  const h = ((hue % 360) + 360) % 360
  const s = Math.max(0, Math.min(100, saturation)) / 100
  const l = Math.max(0, Math.min(100, lightness)) / 100
  const chroma = (1 - Math.abs((2 * l) - 1)) * s
  const segment = h / 60
  const second = chroma * (1 - Math.abs((segment % 2) - 1))
  const match = l - (chroma / 2)

  let red = 0
  let green = 0
  let blue = 0

  if (segment >= 0 && segment < 1) {
    red = chroma
    green = second
  } else if (segment < 2) {
    red = second
    green = chroma
  } else if (segment < 3) {
    green = chroma
    blue = second
  } else if (segment < 4) {
    green = second
    blue = chroma
  } else if (segment < 5) {
    red = second
    blue = chroma
  } else {
    red = chroma
    blue = second
  }

  const toHex = (value: number) =>
    Math.round((value + match) * 255)
      .toString(16)
      .padStart(2, "0")

  return `#${toHex(red)}${toHex(green)}${toHex(blue)}`
}

const buildDistinctChartColors = (count: number, palette: string[], seedKey: string) => {
  if (count <= 0) return [] as string[]

  const colors: string[] = []
  const used = new Set<string>()
  const paletteSize = palette.length
  const seed = hashString(seedKey)
  const paletteOffset = paletteSize ? seed % paletteSize : 0
  let generatedIndex = 0

  for (let index = 0; index < count; index += 1) {
    let candidate = ""

    if (index < paletteSize) {
      candidate = palette[(paletteOffset + index) % paletteSize]
    }

    while (!candidate || used.has(candidate)) {
      const hue = (seed + (generatedIndex * 137.508)) % 360
      const saturation = 68 + ((generatedIndex % 3) * 6)
      const lightness = 46 + ((generatedIndex % 4) * 5)
      candidate = hslToHex(hue, saturation, lightness)
      generatedIndex += 1
    }

    used.add(candidate)
    colors.push(candidate)
  }

  return colors
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

type IndiaMapDefinition = {
  viewBox: string
  locations: IndiaMapLocation[]
}

const normalizeStateLookupKey = (value: string) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z]/g, "")

const toIndiaMapDefinition = (value: unknown): IndiaMapDefinition | null => {
  if (!value || typeof value !== "object") return null
  const mapValue = value as {
    viewBox?: string
    locations?: Array<{ id?: string; name?: string; path?: string }>
  }
  const locations = Array.isArray(mapValue.locations)
    ? mapValue.locations
        .map((location) => ({
          id: String(location?.id || "").trim(),
          name: String(location?.name || "").trim(),
          path: String(location?.path || "").trim(),
        }))
        .filter((location) => Boolean(location.id) && Boolean(location.name) && Boolean(location.path))
    : []
  const viewBox = String(mapValue.viewBox || "").trim()
  if (!viewBox || !locations.length) return null
  return { viewBox, locations }
}

const STATIC_INDIA_MAP_DEFINITION = toIndiaMapDefinition(IndiaSvgMap)

const buildIndiaStateKeyToId = (locations: IndiaMapLocation[]): Record<string, string> => {
  const keyMap = locations.reduce((acc, location) => {
    const nameKey = normalizeStateLookupKey(location.name)
    if (nameKey) acc[nameKey] = location.id
    const idKey = normalizeStateLookupKey(location.id)
    if (idKey) acc[idKey] = location.id
    return acc
  }, {} as Record<string, string>)

  Object.entries(STATE_NAME_ALIASES).forEach(([aliasKey, canonicalKey]) => {
    const canonicalId = keyMap[canonicalKey]
    if (canonicalId) keyMap[aliasKey] = canonicalId
  })

  return keyMap
}

const STATE_NAME_ALIASES: Record<string, string> = {
  orissa: "odisha",
  pondicherry: "puducherry",
  uttaranchal: "uttarakhand",
  nctofdelhi: "delhi",
  newdelhi: "delhi",
  andamanandnicobar: "andamanandnicobarislands",
  andamannicobar: "andamanandnicobarislands",
  dadraandnagarhavelianddamananddiu: "dadraandnagarhavelianddamananddiu",
  dnhdd: "dadraandnagarhavelianddamananddiu",
  dnh: "dadraandnagarhavelianddamananddiu",
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

const COMBINED_UT_KEY = normalizeStateLookupKey("Dadra and Nagar Haveli and Daman and Diu")
const COMBINED_UT_ALIASES = new Set<string>([
  COMBINED_UT_KEY,
  "dnhdd",
  "dnh",
  "dadraandnagarhavelianddamananddiu",
])
const COMBINED_UT_COMPONENT_KEYS = [
  normalizeStateLookupKey("Dadra and Nagar Haveli"),
  normalizeStateLookupKey("Daman and Diu"),
]

const mapStateToIndiaStateIds = (stateValue: string, stateKeyToId: Record<string, string>): string[] => {
  const key = normalizeStateLookupKey(stateValue)
  if (!key) return []
  if (COMBINED_UT_ALIASES.has(key)) {
    const ids = COMBINED_UT_COMPONENT_KEYS
      .map(componentKey => stateKeyToId[componentKey])
      .filter((id): id is string => Boolean(id))
    return Array.from(new Set(ids))
  }
  const resolved = stateKeyToId[key]
  return resolved ? [resolved] : []
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
  | "claims"
  | "net_claims"
  | "loss_ratio"
  | "quantity"

const SALES_HOVER_DETAIL_METRICS: HoverDetailMetricKey[] = [...SALES_METRIC_ORDER]

const CLAIMS_HOVER_DETAIL_METRICS: HoverDetailMetricKey[] = [
  "claims",
  "net_claims",
  "loss_ratio",
  "quantity",
]

const getHoverMetricsForDataset = (datasetType: "sales" | "claims"): HoverDetailMetricKey[] => (
  datasetType === "claims" ? CLAIMS_HOVER_DETAIL_METRICS : SALES_HOVER_DETAIL_METRICS
)

const hasSamsungCompareColumns = (row: Row) => (
  SAMSUNG_PARTNERS.some((partner) => row[partner.key] != null)
)

const resolveHoverMetricValue = (
  row: Row,
  measureKey: string,
  fallbackMetric: HoverDetailMetricKey
) => {
  if (hasSamsungCompareColumns(row)) {
    return sumSamsungPartnerValues(row as Record<string, unknown>)
  }
  return asNumber(row[measureKey] ?? row[fallbackMetric])
}

const HOVER_DETAIL_LABELS: Record<HoverDetailMetricKey, string> = {
  gross_premium: "Gross Premium",
  earned_premium: "Earned Premium",
  zopper_earned_premium: "Zopper Earned Premium",
  claims: "Claims Cost",
  net_claims: "Net Claims Paid",
  loss_ratio: "Loss Ratio",
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
  dimensionKey: string
  compareTooltipQuantity?: boolean
  showPeriodRange?: boolean
  periodStartLabel?: string
}

const CustomTooltip = ({
  active,
  payload,
  label,
  measure,
  dimensionKey,
  compareTooltipQuantity = false,
  showPeriodRange = false,
  periodStartLabel = "",
}: CustomTooltipProps) => {
  if (!active || !payload?.length) return null
  const formattedLabel = formatMonth(label || "", dimensionKey)
  const tooltipRow = payload[0]?.payload
  const tooltipObject = (tooltipRow || {}) as Record<string, unknown>
  const compareTooltipPartners = SAMSUNG_PARTNERS.filter((partner) => (
    `tooltip_${partner.key}` in tooltipObject
  ))
  const metricIsLossRatio = measure.toLowerCase().includes("loss_ratio")
  const rowPeriodStart = formatMonth(String(tooltipRow?.period_start ?? ""), dimensionKey)
  const rowPeriodEnd = formatMonth(String((tooltipRow?.period_end ?? label) || ""), dimensionKey)
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
  eagerMapHoverPrefetch = false,
  enableCrossDatasetHoverCompare = false,
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
  const [indiaMapDefinition, setIndiaMapDefinition] = useState<IndiaMapDefinition | null>(STATIC_INDIA_MAP_DEFINITION)
  const [indiaMapAssetState, setIndiaMapAssetState] = useState<"idle" | "loading" | "ready" | "error">(
    STATIC_INDIA_MAP_DEFINITION ? "ready" : "error"
  )
  const [indiaMapAssetError, setIndiaMapAssetError] = useState<string | null>(
    STATIC_INDIA_MAP_DEFINITION ? null : "Map asset unavailable"
  )
  const [activeMapKey, setActiveMapKey] = useState<string | null>(null)
  const [hoverDetailByStateId, setHoverDetailByStateId] = useState<
    Record<string, Partial<Record<HoverDetailMetricKey, number>>>
  >({})
  const [crossDatasetHoverExpanded, setCrossDatasetHoverExpanded] = useState(false)
  const [crossDatasetHoverDetailByStateId, setCrossDatasetHoverDetailByStateId] = useState<
    Record<string, Partial<Record<HoverDetailMetricKey, number>>>
  >({})
  const [hoverDetailLoadingKey, setHoverDetailLoadingKey] = useState<string | null>(null)
  const [hoverDetailLoadingAll, setHoverDetailLoadingAll] = useState(false)
  const [crossDatasetHoverLoadingKey, setCrossDatasetHoverLoadingKey] = useState<string | null>(null)
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
  const compareGradientBase = (gradientId || "compare").replace(/[^a-zA-Z0-9_-]/g, "")
  const hoverDetailMetrics = useMemo<HoverDetailMetricKey[]>(() => (
    getHoverMetricsForDataset(datasetType)
  ), [datasetType])
  const crossDatasetType = datasetType === "sales" ? "claims" : "sales"
  const crossDatasetHoverMetrics = useMemo<HoverDetailMetricKey[]>(() => (
    getHoverMetricsForDataset(crossDatasetType)
  ), [crossDatasetType])
  const showCrossDatasetHover = enableCrossDatasetHoverCompare && crossDatasetHoverExpanded
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
    setCrossDatasetHoverExpanded(false)
    setCrossDatasetHoverDetailByStateId({})
    setHoverDetailLoadingKey(null)
    setHoverDetailLoadingAll(false)
    setCrossDatasetHoverLoadingKey(null)
  }, [source, dimension, metric, datasetType, bucket, chartType, fromDate, toDate, jobId, categoryFiltersKey])

  useEffect(() => {
    if (chartType !== "india_map") return
    setHoverCard(null)
  }, [chartType, showCrossDatasetHover])

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
    if (chartType !== "india_map") return
    if (deferUntilVisible && !isVisible) return
    if (!STATIC_INDIA_MAP_DEFINITION) {
      setIndiaMapAssetState("error")
      setIndiaMapAssetError("Map asset unavailable")
      return
    }
    if (indiaMapDefinition !== STATIC_INDIA_MAP_DEFINITION) {
      setIndiaMapDefinition(STATIC_INDIA_MAP_DEFINITION)
    }
    if (indiaMapAssetState !== "ready") {
      setIndiaMapAssetState("ready")
    }
    if (indiaMapAssetError) {
      setIndiaMapAssetError(null)
    }
  }, [chartType, deferUntilVisible, indiaMapAssetError, indiaMapAssetState, indiaMapDefinition, isVisible])

  const indiaMapLocations = useMemo(
    () => indiaMapDefinition?.locations ?? [],
    [indiaMapDefinition]
  )
  const indiaStateKeyToId = useMemo(
    () => buildIndiaStateKeyToId(indiaMapLocations),
    [indiaMapLocations]
  )

  useEffect(() => {
    if (!dimension || !source || !metric) return
    if (deferUntilVisible && !isVisible) return
    const requestId = ++requestIdRef.current
    const controller = new AbortController()
    const metricKey = toSafeKey(metric)
    const tooltipMetricKey = toSafeKey(tooltipMetricOverride || "")
    const baseFetchParams: GraphFetchParams = {
      source,
      dimension,
      metric,
      datasetType,
      bucket,
      jobId,
      from_date: fromDate,
      to_date: toDate,
      categoryFilters: normalizedCategoryFilters,
    }
    const tooltipFetchParams =
      tooltipMetricOverride && tooltipMetricKey && tooltipMetricKey !== metricKey
        ? {
            ...baseFetchParams,
            metric: tooltipMetricOverride,
          }
        : null
    const hasWarmCache =
      Boolean(readGraphDataCache(baseFetchParams))
      && (!tooltipFetchParams || Boolean(readGraphDataCache(tooltipFetchParams)))

    const fetchData = async () => {
      const dimKey = toSafeKey(dimension)
      if (!hasWarmCache) {
        setLoading(true)
      }
      setError(null)

      try {

        if (source === "samsung") {
          setCompareMode(true)
          // Backend returns all Samsung partners in one response to avoid extra compare requests.
          const combined = await fetchRowsWithRangeFallback({
            ...baseFetchParams,
            source: "samsung",
          }, { signal: controller.signal })

          let merged: Row[] = (combined.data || []).map((row) => {
            const next: Row = { ...row }
            SAMSUNG_PARTNERS.forEach((partner) => {
              next[partner.key] = asNumber(row[partner.key])
            })
            return next
          })

          if (tooltipFetchParams) {
            const tooltipRows = await fetchRowsWithRangeFallback({
              ...tooltipFetchParams,
              source: "samsung",
            }, { signal: controller.signal })

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
        const single = await fetchRowsWithRangeFallback(baseFetchParams, { signal: controller.signal })

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
        if (controller.signal.aborted || (e instanceof Error && e.name === "AbortError")) {
          return
        }
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
    if (!hasWarmCache && fetchDelayMs && fetchDelayMs > 0) {
      timer = setTimeout(fetchData, fetchDelayMs)
    } else {
      fetchData()
    }

    return () => {
      controller.abort()
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

  const indiaMapStateKeys = useMemo(() => {
    if (chartType !== "india_map" || !indiaMapLocations.length) return [] as string[]
    const stateDimensionKey = toSafeKey(dimension || "state")
    const keys = new Set<string>()
    for (const row of data) {
      const stateName = String(row[stateDimensionKey] ?? row.state ?? "").trim()
      if (!stateName) continue
      mapStateToIndiaStateIds(stateName, indiaStateKeyToId).forEach((stateId) => keys.add(stateId))
    }
    return Array.from(keys)
  }, [chartType, data, dimension, indiaMapLocations.length, indiaStateKeyToId])

  useEffect(() => {
    if (!eagerMapHoverPrefetch || chartType !== "india_map" || !source || !indiaMapStateKeys.length) return

    const hasAllMetrics = indiaMapStateKeys.every((stateId) => (
      hoverDetailMetrics.every((detailMetric) => hoverDetailByStateId[stateId]?.[detailMetric] != null)
    ))
    if (hasAllMetrics) return

    let active = true
    const controller = new AbortController()
    const baseFilters = normalizedCategoryFilters.filter((filter) => filter.dimension !== "state").slice(0, 2)

    const timer = setTimeout(async () => {
      if (!active) return
      setHoverDetailLoadingAll(true)

      try {
        const stateDimensionKey = toSafeKey(dimension || "state")
        const detailRowsByMetric = await fetchRowsBatch(
          hoverDetailMetrics.map((detailMetric) => ({
            requestKey: detailMetric,
            params: {
              source,
              dimension: "state",
              metric: detailMetric,
              datasetType,
              bucket,
              jobId,
              from_date: fromDate,
              to_date: toDate,
              categoryFilters: baseFilters,
            },
          })),
          { signal: controller.signal }
        )

        if (!active) return

        const nextByStateId: Record<string, Partial<Record<HoverDetailMetricKey, number>>> = {}
        indiaMapStateKeys.forEach((stateId) => {
          nextByStateId[stateId] = {}
        })

        hoverDetailMetrics.forEach((detailMetric) => {
          const result = detailRowsByMetric[detailMetric]
          const measureKey = toSafeKey(result?.measure || detailMetric)
          const rows = result?.data || []
          rows.forEach((row) => {
            const stateName = String(row[stateDimensionKey] ?? row.state ?? "").trim()
            if (!stateName) return
            const stateIds = mapStateToIndiaStateIds(stateName, indiaStateKeyToId)
            if (!stateIds.length) return
            const value = resolveHoverMetricValue(row, measureKey, detailMetric)
            const apportionedValue = stateIds.length > 1 ? value / stateIds.length : value
            stateIds.forEach((stateId) => {
              const target = nextByStateId[stateId] || {}
              target[detailMetric] = asNumber(target[detailMetric]) + apportionedValue
              nextByStateId[stateId] = target
            })
          })
          indiaMapStateKeys.forEach((stateId) => {
            const target = nextByStateId[stateId] || {}
            if (target[detailMetric] == null) {
              target[detailMetric] = 0
            }
            nextByStateId[stateId] = target
          })
        })

        setHoverDetailByStateId((prev) => ({
          ...prev,
          ...nextByStateId,
        }))
      } catch {
        // Keep the map interactive even if the eager KPI preload misses.
      } finally {
        setHoverDetailLoadingAll(false)
      }
    }, 0)

    return () => {
      active = false
      controller.abort()
      clearTimeout(timer)
    }
  }, [
    eagerMapHoverPrefetch,
    chartType,
    source,
    dimension,
    bucket,
    jobId,
    fromDate,
    toDate,
    datasetType,
    indiaMapStateKeys,
    indiaStateKeyToId,
    hoverDetailByStateId,
    hoverDetailMetrics,
    normalizedCategoryFilters,
  ])

  const hoverDetailQueryLabel = useMemo(() => {
    if (chartType !== "india_map" || !hoverCard?.key) return ""
    const stateDimensionKey = toSafeKey(dimension || "state")
    for (const row of data) {
      const stateName = String(row[stateDimensionKey] ?? row.state ?? "").trim()
      if (!stateName) continue
      const stateIds = mapStateToIndiaStateIds(stateName, indiaStateKeyToId)
      if (stateIds.includes(hoverCard.key)) return stateName
    }
    return hoverCard.label || ""
  }, [chartType, data, dimension, hoverCard, indiaStateKeyToId])

  useEffect(() => {
    if (chartType !== "india_map" || !source || !hoverCard?.key || !hoverDetailQueryLabel || hoverDetailLoadingAll) return

    const cachedMetrics = hoverDetailByStateId[hoverCard.key] || {}
    const pendingMetrics = hoverDetailMetrics.filter((detailMetric) => cachedMetrics[detailMetric] == null)
    if (!pendingMetrics.length) return

    let active = true
    const controller = new AbortController()
    const hoverKey = hoverCard.key
    const hoverFilters: GraphCategoryFilter[] = [
      ...normalizedCategoryFilters.filter((filter) => filter.dimension !== "state").slice(0, 1),
      { dimension: "state", values: [hoverDetailQueryLabel] },
    ]

    const timer = setTimeout(async () => {
      if (!active) return
      setHoverDetailLoadingKey(hoverKey)

      const nextMetrics: Partial<Record<HoverDetailMetricKey, number>> = { ...cachedMetrics }
      try {
        const detailRowsByMetric = await fetchRowsBatch(
          pendingMetrics.map((detailMetric) => ({
            requestKey: detailMetric,
            params: {
              source,
              dimension: "state",
              metric: detailMetric,
              datasetType,
              bucket,
              jobId,
              from_date: fromDate,
              to_date: toDate,
              categoryFilters: hoverFilters,
            },
          })),
          { signal: controller.signal }
        )

        pendingMetrics.forEach((detailMetric) => {
          const result = detailRowsByMetric[detailMetric]
          const measureKey = toSafeKey(result?.measure || detailMetric)
          nextMetrics[detailMetric] = (result?.data || []).reduce((sum, row) => (
            sum + resolveHoverMetricValue(row, measureKey, detailMetric)
          ), 0)
        })
      } catch {
        pendingMetrics.forEach((detailMetric) => {
          if (nextMetrics[detailMetric] == null) {
            nextMetrics[detailMetric] = 0
          }
        })
      }

      if (active) {
        setHoverDetailByStateId((prev) => ({
          ...prev,
          [hoverKey]: nextMetrics,
        }))
      }
      setHoverDetailLoadingKey((prev) => (prev === hoverKey ? null : prev))
    }, 120)

    return () => {
      active = false
      controller.abort()
      clearTimeout(timer)
      setHoverDetailLoadingKey((prev) => (prev === hoverKey ? null : prev))
    }
  }, [
    chartType,
    source,
    bucket,
    jobId,
    fromDate,
    toDate,
    datasetType,
    hoverCard?.key,
    hoverDetailByStateId,
    hoverDetailQueryLabel,
    hoverDetailLoadingAll,
    hoverDetailMetrics,
    normalizedCategoryFilters,
  ])

  useEffect(() => {
    if (
      chartType !== "india_map"
      || !showCrossDatasetHover
      || !source
      || !hoverCard?.key
      || !hoverDetailQueryLabel
    ) {
      return
    }

    const cachedMetrics = crossDatasetHoverDetailByStateId[hoverCard.key] || {}
    const pendingMetrics = crossDatasetHoverMetrics.filter((detailMetric) => cachedMetrics[detailMetric] == null)
    if (!pendingMetrics.length) return

    let active = true
    const controller = new AbortController()
    const hoverKey = hoverCard.key
    const hoverFilters: GraphCategoryFilter[] = [
      ...normalizedCategoryFilters.filter((filter) => filter.dimension !== "state").slice(0, 1),
      { dimension: "state", values: [hoverDetailQueryLabel] },
    ]

    const timer = setTimeout(async () => {
      if (!active) return
      setCrossDatasetHoverLoadingKey(hoverKey)

      const nextMetrics: Partial<Record<HoverDetailMetricKey, number>> = { ...cachedMetrics }
      try {
        const detailRowsByMetric = await fetchRowsBatch(
          pendingMetrics.map((detailMetric) => ({
            requestKey: detailMetric,
            params: {
              source,
              dimension: "state",
              metric: detailMetric,
              datasetType: crossDatasetType,
              bucket,
              jobId,
              from_date: fromDate,
              to_date: toDate,
              categoryFilters: hoverFilters,
            },
          })),
          { signal: controller.signal }
        )

        pendingMetrics.forEach((detailMetric) => {
          const result = detailRowsByMetric[detailMetric]
          const measureKey = toSafeKey(result?.measure || detailMetric)
          nextMetrics[detailMetric] = (result?.data || []).reduce((sum, row) => (
              sum + resolveHoverMetricValue(row, measureKey, detailMetric)
            ), 0)
        })
      } catch {
        pendingMetrics.forEach((detailMetric) => {
          if (nextMetrics[detailMetric] == null) {
            nextMetrics[detailMetric] = 0
          }
        })
      }

      if (active) {
        setCrossDatasetHoverDetailByStateId((prev) => ({
          ...prev,
          [hoverKey]: nextMetrics,
        }))
      }
      setCrossDatasetHoverLoadingKey((prev) => (prev === hoverKey ? null : prev))
    }, 120)

    return () => {
      active = false
      controller.abort()
      clearTimeout(timer)
      setCrossDatasetHoverLoadingKey((prev) => (prev === hoverKey ? null : prev))
    }
  }, [
    bucket,
    chartType,
    crossDatasetHoverDetailByStateId,
    crossDatasetHoverMetrics,
    crossDatasetType,
    fromDate,
    hoverCard?.key,
    hoverDetailQueryLabel,
    jobId,
    normalizedCategoryFilters,
    showCrossDatasetHover,
    source,
    toDate,
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

  if (chartType === "india_map" && (indiaMapAssetState === "idle" || indiaMapAssetState === "loading")) {
    return (
      <div ref={containerRef} className={`${heightClassName} flex items-center justify-center text-sm text-gray-500`}>
        Loading map...
      </div>
    )
  }

  if (chartType === "india_map" && indiaMapAssetState === "error") {
    return (
      <div ref={containerRef} className={`${heightClassName} flex items-center justify-center text-sm text-gray-400`}>
        {indiaMapAssetError || "Map unavailable"}
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
  const samsungCompareSeries = SAMSUNG_PARTNERS.map((partner) => ({
    ...partner,
    plotKey: useLogScale ? toLogPlotKey(partner.key) : partner.key,
    gradientId: `${compareGradientBase}-${partner.key}`,
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
    if (explicitStart) return formatMonth(explicitStart, dimKey)
    return formatMonth(String(firstRow?.[dimKey] ?? ""), dimKey)
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
  const pieBaseColors = buildDistinctChartColors(
    pieRows.length,
    palette,
    `${baseKey}-${pieRows.map((entry) => entry.name).join("|")}`
  )
  const pieSlices = pieRows.map((entry, idx) => {
    const baseColor = pieData.length ? pieBaseColors[idx] : "#dbeafe"
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
  const radarData = chartData
    .map((row) => {
      const next: Row = {
        name: String(row[dimKey] ?? "Unknown"),
        [measure]: Math.max(0, asNumber(row[measure])),
      }
      SAMSUNG_PARTNERS.forEach((partner) => {
        next[partner.key] = Math.max(0, asNumber(row[partner.key]))
      })
      return next
    })
    .sort((a, b) => {
      if (compareMode) {
        return sumSamsungPartnerValues(b as Record<string, unknown>) - sumSamsungPartnerValues(a as Record<string, unknown>)
      }
      return asNumber(b[measure]) - asNumber(a[measure])
    })
    .slice(0, 12)
  const indiaMapValuesByStateId = new Map<string, number>()
  indiaMapLocations.forEach((location) => {
    indiaMapValuesByStateId.set(location.id, 0)
  })
  chartData.forEach((row) => {
    const stateName = String(row[dimKey] ?? "").trim()
    const stateIds = mapStateToIndiaStateIds(stateName, indiaStateKeyToId)
    if (!stateIds.length) return
    const value = compareMode
      ? Math.max(0, sumSamsungPartnerValues(row as Record<string, unknown>))
      : Math.max(0, asNumber(row[measure]))
    const perStateValue = value / stateIds.length
    stateIds.forEach((stateId) => {
      indiaMapValuesByStateId.set(stateId, (indiaMapValuesByStateId.get(stateId) || 0) + perStateValue)
    })
  })
  const indiaMapEntries = indiaMapLocations.map((location) => {
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
  const crossDatasetHoverMetricValues = hoverCard ? crossDatasetHoverDetailByStateId[hoverCard.key] || {} : {}
  const hoverSectionLabel = datasetType === "sales" ? "Sales" : "Claims"
  const crossDatasetHoverSectionLabel = crossDatasetType === "sales" ? "Sales" : "Claims"

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
    const desiredWidth = showCrossDatasetHover
      ? (compact ? 228 : 292)
      : (compact ? 210 : 254)
    const maxAllowedWidth = Math.max(120, rect.width - 16)
    const cardWidth = Math.min(desiredWidth, maxAllowedWidth)
    const cardHeight = showCrossDatasetHover
      ? (compact ? 248 : 300)
      : (compact ? 142 : 168)
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

  const renderHoverMetricRows = (
    stateKey: string,
    metrics: HoverDetailMetricKey[],
    values: Partial<Record<HoverDetailMetricKey, number>>,
    loading: boolean,
    compact: boolean
  ) => (
    metrics.map((detailMetric) => {
      const value = values[detailMetric]
      const hasValue = value != null
      const formatted = hasValue
        ? formatValue(asNumber(value), detailMetric)
        : loading
          ? "Loading..."
          : "N/A"
      return (
        <div
          key={`${stateKey}-${detailMetric}`}
          className={`grid grid-cols-[minmax(0,1fr)_auto] items-center ${
            compact ? "gap-1.5 text-[10px]" : "gap-2 text-[11px]"
          }`}
        >
          <span className="truncate text-slate-600">{HOVER_DETAIL_LABELS[detailMetric]}</span>
          <span className="font-semibold text-slate-800">{formatted}</span>
        </div>
      )
    })
  )

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
            {enableCrossDatasetHoverCompare && (
              <div className="pointer-events-none absolute left-2 top-2 z-10">
                <button
                  type="button"
                  onClick={() => setCrossDatasetHoverExpanded((prev) => !prev)}
                  className="pointer-events-auto rounded-full border border-slate-200 bg-white/95 px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.12em] text-slate-600 shadow-sm transition-colors hover:border-slate-300 hover:text-slate-900"
                  aria-pressed={showCrossDatasetHover}
                >
                  {showCrossDatasetHover ? "Hide Sales + Claims" : "Show Sales + Claims"}
                </button>
              </div>
            )}
            <svg
              viewBox={indiaMapDefinition?.viewBox || "0 0 1 1"}
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
                <div className={hoverCard.compact ? "space-y-1.5" : "space-y-2"}>
                  <div>
                    <div className={`mb-1 font-black uppercase tracking-[0.14em] text-slate-400 ${
                      hoverCard.compact ? "text-[9px]" : "text-[10px]"
                    }`}>
                      {hoverSectionLabel}
                    </div>
                    <div className={hoverCard.compact ? "space-y-0.5" : "space-y-1"}>
                      {renderHoverMetricRows(
                        hoverCard.key,
                        hoverDetailMetrics,
                        hoverMetricValues,
                        hoverDetailLoadingAll || hoverDetailLoadingKey === hoverCard.key,
                        hoverCard.compact
                      )}
                    </div>
                  </div>
                  {showCrossDatasetHover && (
                    <div className={`border-t border-slate-200/80 pt-1.5 ${hoverCard.compact ? "mt-1" : "mt-1.5"}`}>
                      <div className={`mb-1 font-black uppercase tracking-[0.14em] text-slate-400 ${
                        hoverCard.compact ? "text-[9px]" : "text-[10px]"
                      }`}>
                        {crossDatasetHoverSectionLabel}
                      </div>
                      <div className={hoverCard.compact ? "space-y-0.5" : "space-y-1"}>
                        {renderHoverMetricRows(
                          `${hoverCard.key}-${crossDatasetType}`,
                          crossDatasetHoverMetrics,
                          crossDatasetHoverMetricValues,
                          crossDatasetHoverLoadingKey === hoverCard.key,
                          hoverCard.compact
                        )}
                      </div>
                    </div>
                  )}
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
              {samsungCompareSeries.map((series) => (
                <linearGradient key={`compare-grad-${series.key}`} id={series.gradientId} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={mixWithWhite(series.color, 0.15)} stopOpacity={0.55} />
                  <stop offset="100%" stopColor={series.color} stopOpacity={0.04} />
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid strokeDasharray="4 4" vertical={false} stroke="#e2e8f0" />
            <XAxis
              dataKey={dimKey}
              interval={isTemporalDimension ? "preserveStartEnd" : "preserveEnd"}
              minTickGap={isTemporalDimension ? (isMobileViewport ? 8 : 16) : 8}
              tick={{ fontSize: isMobileViewport ? 10 : 11 }}
              tickFormatter={(v) => (isTemporalDimension ? formatMonth(v, dimKey) : String(v))}
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
                  dimensionKey={dimKey}
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
              {samsungCompareSeries.map((series) => (
                <linearGradient key={`compare-grad-${series.key}`} id={series.gradientId} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={mixWithWhite(series.color, 0.35)} />
                  <stop offset="100%" stopColor={series.color} />
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid strokeDasharray="4 4" vertical={false} stroke="#e2e8f0" />
            <XAxis
              dataKey={dimKey}
              interval={isTemporalDimension ? "preserveStartEnd" : "preserveEnd"}
              minTickGap={isTemporalDimension ? 16 : 8}
              tick={{ fontSize: 11 }}
              tickFormatter={(v) => (isTemporalDimension ? formatMonth(v, dimKey) : String(v))}
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
                  dimensionKey={dimKey}
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


