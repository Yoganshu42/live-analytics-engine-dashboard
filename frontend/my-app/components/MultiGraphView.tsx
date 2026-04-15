"use client"

import dynamic from "next/dynamic"
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { ChevronDown, Maximize2 } from "lucide-react"
import { AnimatePresence, motion, useReducedMotion } from "framer-motion"
import {
  Area,
  AreaChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Sector,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from "recharts"

import GraphView, { fetchGraphRows, seedGraphDataCache } from "@/components/GraphView"
import DateRangePicker from "@/components/DateRangePicker"
import type { GraphChartType, GraphDataSnapshot } from "@/components/GraphView"
import {
  fetchAnnualComparison,
  fetchByDimensionBatch,
  fetchCategoryPercentage,
  fetchCityBreakdownByState,
  fetchGraphInsights,
  fetchSummary,
  type CategoryPercentageParams,
  type FetchByDimensionBatchItem,
  type CategoryPercentageRow,
} from "@/app/lib/api"
import {
  SAMSUNG_PARTNERS,
  isSamsungPartnerSource,
  sumSamsungPartnerValues,
} from "@/lib/samsungPartners"
import { SALES_METRIC_ORDER } from "@/lib/salesMetricOrder"
import { GRAPH_PRESETS } from "@/utils/graphPresets"

const normalizedInsightsFlag = (
  process.env.NEXT_PUBLIC_ENABLE_GRAPH_INSIGHTS || ""
).trim().toLowerCase()

// Enable insights by default; allow explicit opt-out via env.
const INSIGHTS_ENABLED = !["0", "false", "no", "off"].includes(normalizedInsightsFlag)

const YearOnYearComparisonChart = dynamic(
  () => import("@/components/YearOnYearComparisonChart"),
  {
    ssr: false,
    loading: () => (
      <div className="smooth-surface content-auto relative overflow-hidden rounded-2xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/60 p-4 shadow-sm sm:p-5">
        <div className="flex h-[340px] items-center justify-center text-sm text-slate-500">
          Loading year-on-year comparison...
        </div>
      </div>
    ),
  }
)

type Props = {
  source: string
  datasetType: "sales" | "claims"
  jobId?: string | null
  fromDate?: string
  toDate?: string
  resetFromDate?: string
  resetToDate?: string
  onDateRangeApply?: (fromDate: string, toDate: string) => void
}

type Preset = {
  group: string
  dimension: string
  metrics: readonly string[]
  bucket?: "day" | "week" | "month"
}

type FullscreenGraph = {
  metric: string
  dimension: string
  bucket?: "day" | "week" | "month"
  chartType?: GraphChartType
  tooltipMetricOverride?: string
  sectionGroup?: string
  sectionMode?: SectionMainChartMode
  isComposite?: boolean
  isYearOnYear?: boolean
} | null

type SamsungOverviewCard = {
  id: string
  title: string
  subtitle: string
  dimension: string
  metric:
    | "gross_premium"
    | "earned_premium"
    | "zopper_earned_premium"
    | "quantity"
    | "claims"
    | "net_claims"
    | "loss_ratio"
  bucket?: "day" | "week" | "month"
  chartType: GraphChartType
  size: "main" | "small"
  tooltipMetricOverride?: string
}

type PartnerSideCard = {
  id: string
  title: string
  subtitle: string
  dimension: string
  metric:
    | "gross_premium"
    | "earned_premium"
    | "zopper_earned_premium"
    | "quantity"
    | "claims"
    | "net_claims"
    | "loss_ratio"
  bucket?: "day" | "week" | "month"
  chartType: GraphChartType
  tooltipMetricOverride?: string
}

type CityBreakdownSlice = {
  city: string
  value: number
  share: number
}

type ActiveCityShapeProps = {
  cx?: number | string
  cy?: number | string
  innerRadius?: number | string
  outerRadius?: number | string
  startAngle?: number
  endAngle?: number
  midAngle?: number
  fill?: string
  payload?: {
    city?: string
  }
}

type CategoryOption = CategoryPercentageRow

type GradientTone = {
  from: string
  to: string
}

type MixSlice = {
  label: string
  value: number
  percentage: number
  gradient: GradientTone
  gradientId: string
}

type StateComparisonMix = {
  state: string
  metricValue: number
  planRows: CategoryOption[]
  deviceRows: CategoryOption[]
  planMessage?: string
  deviceMessage?: string
}

type RegionalCategoryDimension =
  | "plan_category"
  | "device_plan_category"
  | "article_brand"
  | "brand"
  | "channel"
  | "product_category"

type RegionalCategoryDescriptor = {
  dimension: RegionalCategoryDimension
  label: string
  sectionTitle: string
  missingText: string
}

type RegionalMapFilterDescriptor = {
  dimension: RegionalCategoryDimension
  label: string
  allLabel: string
}

type PieSizing = {
  height: number
  innerRadius: number
  outerRadius: number
}

type StateComparisonLayout = {
  gridClass: string
  cardMinHeightClass: string
}

type StateComparisonRow = {
  state: string
  value: number
}

type SectionMergedRow = Record<string, string | number>

type SectionMergedState = {
  loading: boolean
  error: string | null
  rows: SectionMergedRow[]
}

type FocusCompareSummary = {
  gross_premium?: number
  earned_premium?: number
  zopper_earned_premium?: number
  units_sold?: number
  claims?: number
  net_claims?: number
  loss_ratio?: number
}

type FullscreenCompareSummaryState = {
  loading: boolean
  error: string | null
  sales: FocusCompareSummary | null
  claims: FocusCompareSummary | null
}

type FullscreenCompareMatrixRow = {
  label: string
  grossPremiumValue: number
  earnedPremiumValue: number
  claimsValue: number
  ratioValue: number
  lossRatioValue: number
}

type FullscreenCompareMatrixState = {
  loading: boolean
  error: string | null
  rows: FullscreenCompareMatrixRow[]
  othersBreakdownRows: FullscreenCompareMatrixRow[]
}

const createEmptyFullscreenCompareSummaryState = (): FullscreenCompareSummaryState => ({
  loading: false,
  error: null,
  sales: null,
  claims: null,
})

const createEmptyFullscreenCompareMatrixState = (): FullscreenCompareMatrixState => ({
  loading: false,
  error: null,
  rows: [],
  othersBreakdownRows: [],
})

const normalizeFocusCompareSummary = (
  summary: FocusCompareSummary | null | undefined,
  datasetType: "sales" | "claims",
  salesReference?: FocusCompareSummary | null
): FocusCompareSummary | null => {
  if (!summary) return null

  if (datasetType === "sales") {
    return {
      gross_premium: asNumber(summary.gross_premium),
      earned_premium: asNumber(summary.earned_premium),
      zopper_earned_premium: asNumber(summary.zopper_earned_premium),
      units_sold: asNumber(summary.units_sold),
    }
  }

  const claims = asNumber(summary.claims ?? summary.gross_premium)
  const netClaims = asNumber(summary.net_claims ?? summary.earned_premium ?? summary.gross_premium)
  const quantity = asNumber(summary.units_sold)
  const zopperBase = asNumber(salesReference?.zopper_earned_premium)
  const lossRatio = summary.loss_ratio != null
    ? asNumber(summary.loss_ratio)
    : (zopperBase > 0 ? (claims / zopperBase) * 100 : 0)

  return {
    gross_premium: claims,
    earned_premium: netClaims,
    units_sold: quantity,
    claims,
    net_claims: netClaims,
    loss_ratio: lossRatio,
  }
}

const FULLSCREEN_COMPARE_MATRIX_OTHERS_GROSS_THRESHOLD = 80 * 100000
const FULLSCREEN_COMPARE_MATRIX_LOSS_RATIO_CAP = 300

const getFullscreenCompareMatrixMetricValue = (
  row: Record<string, unknown>,
  metric: string,
  source: string
) => {
  if (source === "samsung" && SAMSUNG_PARTNERS.some((partner) => row[partner.key] != null)) {
    return Math.max(0, sumSamsungPartnerValues(row))
  }
  return Math.max(0, getSourceMetricValue(row, metric, source))
}

const CITY_PIE_COLORS = [
  "#0ea5e9",
  "#22c55e",
  "#f97316",
  "#8b5cf6",
  "#ef4444",
  "#14b8a6",
  "#eab308",
  "#64748b",
  "#ec4899",
  "#84cc16",
]

const getCityColor = (index: number) =>
  CITY_PIE_COLORS[index % CITY_PIE_COLORS.length]

const PIE_GRADIENTS: GradientTone[] = [
  { from: "#0ea5e9", to: "#0284c7" },
  { from: "#22c55e", to: "#15803d" },
  { from: "#f97316", to: "#ea580c" },
  { from: "#8b5cf6", to: "#7c3aed" },
  { from: "#ec4899", to: "#db2777" },
  { from: "#14b8a6", to: "#0f766e" },
  { from: "#eab308", to: "#ca8a04" },
  { from: "#ef4444", to: "#b91c1c" },
  { from: "#3b82f6", to: "#1d4ed8" },
  { from: "#84cc16", to: "#4d7c0f" },
]

const getPieGradient = (index: number) =>
  PIE_GRADIENTS[index % PIE_GRADIENTS.length]

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

const buildDistinctSolidColors = (count: number, palette: string[], seedKey: string) => {
  if (count <= 0) return [] as string[]

  const colors: string[] = []
  const used = new Set<string>()
  const paletteSize = palette.length
  const seedBase = toDomId(seedKey).split("").reduce((acc, char) => acc + char.charCodeAt(0), 0)
  const paletteOffset = paletteSize ? seedBase % paletteSize : 0
  let generatedIndex = 0

  for (let index = 0; index < count; index += 1) {
    let candidate = ""

    if (index < paletteSize) {
      candidate = palette[(paletteOffset + index) % paletteSize]
    }

    while (!candidate || used.has(candidate)) {
      const hue = (seedBase + (generatedIndex * 137.508)) % 360
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

const buildDistinctPieGradients = (count: number, seedKey: string) => {
  if (count <= 0) return [] as GradientTone[]

  const seedBase = toDomId(seedKey).split("").reduce((acc, char) => acc + char.charCodeAt(0), 0)
  const gradients: GradientTone[] = []
  const used = new Set<string>()
  const presetOffset = PIE_GRADIENTS.length ? seedBase % PIE_GRADIENTS.length : 0
  let generatedIndex = 0

  for (let index = 0; index < count; index += 1) {
    let gradient: GradientTone | null = null

    if (index < PIE_GRADIENTS.length) {
      gradient = PIE_GRADIENTS[(presetOffset + index) % PIE_GRADIENTS.length]
    }

    while (!gradient || used.has(`${gradient.from}:${gradient.to}`)) {
      const hue = (seedBase + (generatedIndex * 137.508)) % 360
      gradient = {
        from: hslToHex(hue, 74, 64),
        to: hslToHex(hue, 72, 42),
      }
      generatedIndex += 1
    }

    used.add(`${gradient.from}:${gradient.to}`)
    gradients.push(gradient)
  }

  return gradients
}

const toDomId = (value: string) => {
  const normalized = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
  return normalized || "item"
}

const normalizeCategoryRows = (rows: CategoryPercentageRow[]) =>
  (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      label: String(row.label || "").trim(),
      value: Math.max(0, asNumber(row.value)),
      percentage: Math.max(0, asNumber(row.percentage)),
    }))
    .filter((row) => row.label)
    .sort((a, b) => b.value - a.value)

const toMixSlices = (rows: CategoryOption[], prefix: string): MixSlice[] => {
  const gradients = buildDistinctPieGradients(rows.length, prefix)
  return rows.map((row, index) => ({
    label: row.label,
    value: row.value,
    percentage: row.percentage,
    gradient: gradients[index] || getPieGradient(index),
    gradientId: `${prefix}-${index}-${toDomId(row.label)}`,
  }))
}

const getPieSizing = (stateCount: number): PieSizing => {
  if (stateCount <= 1) return { height: 250, innerRadius: 58, outerRadius: 94 }
  if (stateCount <= 3) return { height: 220, innerRadius: 50, outerRadius: 82 }
  if (stateCount <= 6) return { height: 196, innerRadius: 44, outerRadius: 74 }
  return { height: 176, innerRadius: 38, outerRadius: 64 }
}

const getStateComparisonLayout = (stateCount: number): StateComparisonLayout => {
  if (stateCount <= 1) {
    return {
      gridClass: "grid-cols-1",
      cardMinHeightClass: "min-h-[560px]",
    }
  }
  if (stateCount === 2) {
    return {
      gridClass: "grid-cols-1 xl:grid-cols-2",
      cardMinHeightClass: "min-h-[500px]",
    }
  }
  if (stateCount <= 4) {
    return {
      gridClass: "grid-cols-1 md:grid-cols-2",
      cardMinHeightClass: "min-h-[480px]",
    }
  }
  if (stateCount <= 6) {
    return {
      gridClass: "grid-cols-1 md:grid-cols-2 xl:grid-cols-3",
      cardMinHeightClass: "min-h-[440px]",
    }
  }
  return {
    gridClass: "grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4",
    cardMinHeightClass: "min-h-[420px]",
  }
}

const renderCityShape = (props: ActiveCityShapeProps, isActive: boolean) => {
  const RADIAN = Math.PI / 180
  const cx = Number(props.cx ?? 0)
  const cy = Number(props.cy ?? 0)
  const innerRadius = Number(props.innerRadius ?? 0)
  const outerRadius = Number(props.outerRadius ?? 0)
  const startAngle = Number(props.startAngle ?? 0)
  const endAngle = Number(props.endAngle ?? 0)
  const midAngle = Number(props.midAngle ?? 0)
  const fill = props.fill || "#0ea5e9"

  const popout = isActive ? 8 : 0
  const radiusBoost = isActive ? 8 : 0
  const dx = Math.cos(-midAngle * RADIAN) * popout
  const dy = Math.sin(-midAngle * RADIAN) * popout

  return (
    <Sector
      cx={cx + dx}
      cy={cy + dy}
      innerRadius={innerRadius}
      outerRadius={outerRadius + radiusBoost}
      startAngle={startAngle}
      endAngle={endAngle}
      fill={fill}
      stroke="none"
    />
  )
}

const asNumber = (value: unknown) => {
  const n = Number(value ?? 0)
  return Number.isFinite(n) ? n : 0
}

const toSafeKey = (value: string) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[()%'.]/g, "")

const formatMetricValue = (value: number, metric: string) => {
  const m = metric.toLowerCase()
  if (m.includes("loss_ratio")) return `${value.toFixed(2)}%`
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

const formatAxisCompact = (value: number, metric: string) => {
  const m = metric.toLowerCase()
  if (m.includes("loss_ratio")) return `${value.toFixed(1)}%`
  if (m.includes("quantity") || m.includes("count")) {
    return new Intl.NumberFormat("en-IN", {
      notation: "compact",
      maximumFractionDigits: 1,
    }).format(value)
  }
  const abs = Math.abs(value)
  if (abs >= 1e7) return `${(value / 1e7).toFixed(1)}Cr`
  if (abs >= 1e5) return `${(value / 1e5).toFixed(1)}L`
  if (abs >= 1e3) return `${(value / 1e3).toFixed(1)}K`
  return `${Math.round(value)}`
}

const normalizeMonthKey = (value: unknown, dimKey = "month") => {
  const raw = String(value ?? "").trim()
  if (!raw) return ""

  if (/^\d{4}-\d{2}(-\d{2})?$/.test(raw)) {
    if (toSafeKey(dimKey).includes("date")) return raw.slice(0, 10)
    return raw.length === 7 ? `${raw}-01` : raw.slice(0, 10)
  }

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
      return `${year}-${String(month).padStart(2, "0")}-01`
    }
  }

  const parsed = new Date(raw)
  if (!Number.isNaN(parsed.getTime())) {
    const year = parsed.getFullYear()
    const month = String(parsed.getMonth() + 1).padStart(2, "0")
    if (toSafeKey(dimKey).includes("date")) {
      const day = String(parsed.getDate()).padStart(2, "0")
      return `${year}-${month}-${day}`
    }
    return `${year}-${month}-01`
  }
  return raw
}

const monthSortValue = (value: string, dimKey = "month") => {
  const normalized = normalizeMonthKey(value, dimKey)
  const parsed = new Date(normalized).getTime()
  if (!Number.isNaN(parsed)) return parsed
  return Number.MAX_SAFE_INTEGER
}

const formatMonthLabel = (value: string, dimKey = "month") => {
  const normalized = normalizeMonthKey(value, dimKey)
  const parsed = new Date(normalized)
  if (Number.isNaN(parsed.getTime())) return value
  if (toSafeKey(dimKey).includes("date")) {
    return parsed.toLocaleDateString("en-US", {
      day: "2-digit",
      month: "short",
      year: "2-digit",
    })
  }
  return parsed.toLocaleString("en-US", {
    month: "short",
    year: "2-digit",
  })
}

const LOG_PLOT_SUFFIX = "__log_plot"

const isTemporalDimensionKey = (value: string) => {
  const key = (value || "").trim().toLowerCase()
  return key.includes("month") || key.includes("date")
}

const DAILY_TEMPORAL_RANGE_MAX_DAYS = 62

const getInclusiveRangeDays = (fromDate?: string, toDate?: string) => {
  if (!fromDate || !toDate) return Number.POSITIVE_INFINITY
  const from = new Date(fromDate)
  const to = new Date(toDate)
  if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) {
    return Number.POSITIVE_INFINITY
  }
  const msPerDay = 24 * 60 * 60 * 1000
  return Math.floor((to.getTime() - from.getTime()) / msPerDay) + 1
}

const shouldUseDailyTemporalSeries = (
  datasetType: "sales" | "claims",
  fromDate?: string,
  toDate?: string
) => (
  datasetType === "sales"
  && getInclusiveRangeDays(fromDate, toDate) <= DAILY_TEMPORAL_RANGE_MAX_DAYS
)

const resolveTemporalPreset = <T extends { dimension: string; bucket?: "day" | "week" | "month" }>(
  preset: T,
  datasetType: "sales" | "claims",
  fromDate?: string,
  toDate?: string
): T => {
  const dimKey = toSafeKey(preset.dimension)
  if (
    !shouldUseDailyTemporalSeries(datasetType, fromDate, toDate)
    || !dimKey.includes("month")
  ) {
    return preset
  }
  return {
    ...preset,
    dimension: "date",
    bucket: "day",
  }
}

const toOriginalMetricKey = (dataKey: string) => (
  dataKey.endsWith(LOG_PLOT_SUFFIX)
    ? dataKey.slice(0, -LOG_PLOT_SUFFIX.length)
    : dataKey
)

const buildInsightsRows = (snapshot: GraphDataSnapshot) => {
  const rows = snapshot.rows.slice(0, 80)
  const dimKey = snapshot.dimensionKey

  if (snapshot.compareMode) {
    return rows.map((row) => {
      const next: Record<string, unknown> = {
        [dimKey]: row[dimKey],
      }
      SAMSUNG_PARTNERS.forEach((partner) => {
        next[partner.key] = row[partner.key] ?? 0
      })
      return next
    })
  }

  const measureKey = snapshot.measure
  return rows.map((row) => ({
    [dimKey]: row[dimKey],
    [measureKey]: row[measureKey],
  }))
}

const SALES_METRICS = [...SALES_METRIC_ORDER]

const CLAIMS_SECTION_METRICS = [
  "claims",
  "net_claims",
]

const FULLSCREEN_COMPARE_CLAIMS_METRICS = [
  "claims",
  "net_claims",
  "loss_ratio",
] as const

const getSectionMetricPriority = (datasetType: "sales" | "claims") =>
  datasetType === "sales" ? SALES_METRICS : CLAIMS_SECTION_METRICS

const GROUP_TITLES: Record<string, string> = {
  time: "Time Trend Desk",
  region: "Regional Signal Board",
  category: "Plan Mix Pulse",
  device_category: "Device Segment Pulse",
}

const GROUP_ORDER = ["time", "region", "category", "device_category"]

const GODREJ_SALES_PRESETS: Preset[] = [
  {
    group: "time",
    dimension: "month",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
  {
    group: "region",
    dimension: "state",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
  {
    group: "channel",
    dimension: "channel",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
  {
    group: "product",
    dimension: "product_category",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
]

const GODREJ_CLAIMS_PRESETS: Preset[] = [
  {
    group: "time",
    dimension: "month",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
  {
    group: "region",
    dimension: "state",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
  {
    group: "channel",
    dimension: "channel",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
  {
    group: "product",
    dimension: "product_category",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
]

const HITACHI_SALES_PRESETS: Preset[] = [
  {
    group: "time",
    dimension: "month",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
  {
    group: "region",
    dimension: "state",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
  {
    group: "category",
    dimension: "plan_category",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
  {
    group: "product",
    dimension: "product_category",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
]

const HITACHI_CLAIMS_PRESETS: Preset[] = [
  {
    group: "time",
    dimension: "month",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
  {
    group: "region",
    dimension: "state",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
  {
    group: "category",
    dimension: "plan_category",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
  {
    group: "product",
    dimension: "product_category",
    metrics: ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity", "net_claims", "claims", "loss_ratio"],
  },
]

const GODREJ_GROUP_TITLES: Record<string, string> = {
  time: "Time Trend Desk",
  region: "Regional Signal Board",
  channel: "Channel Signal Board",
  product: "Product Category Pulse",
}

const METRIC_LINE_COLORS: Record<string, string> = {
  gross_premium: "#2563eb",
  earned_premium: "#06b6d4",
  zopper_earned_premium: "#d97706",
  quantity: "#8b5cf6",
  claims: "#ef4444",
  net_claims: "#f97316",
  loss_ratio: "#14b8a6",
}

const SOURCE_FALLBACK_METRIC_KEYS: Record<string, string[]> = {
  samsung_vs: ["samsung_vs"],
  samsung_croma: ["samsung_croma"],
  samsung_reliance_digital: ["samsung_reliance_digital"],
  reliance: ["reliance", "reliance_resq"],
  godrej: ["godrej"],
  hitachi: ["hitachi"],
}

const getMetricLabel = (metric: string) => {
  const m = metric.toLowerCase()
  if (m.includes("zopper")) return "Zopper Earned Premium"
  if (m.includes("earned")) return "Earned Premium"
  if (m.includes("gross")) return "Gross Premium"
  if (m.includes("net_claims")) return "Net Claims Paid"
  if (m === "claims") return "Claims Cost"
  if (m.includes("loss_ratio")) return "Loss Ratio"
  if (m.includes("quantity") || m.includes("count")) return "Volume"
  return metric.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
}

const getDimensionLabel = (dimension: string, source?: string) => {
  const d = dimension.toLowerCase()
  if (d.includes("date")) return "Date"
  if (d.includes("month")) return "Month"
  if (d.includes("state")) return source === "godrej" || source === "hitachi" ? "Region" : "State"
  if (d === "article_brand" || d === "brand") return "Brand Category"
  if (d.includes("channel")) return "Channel"
  if (d.includes("product_subcategory")) return "Product Subcategory"
  if (d.includes("product_category")) return "Product Category"
  if (d.includes("device_plan_category")) return source === "reliance" ? "Brand Category" : "Device Segment"
  if (d.includes("plan_category")) return "Plan Type"
  return dimension.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
}

const getGraphTitle = (
  metric: string,
  dimension: string,
  source?: string
) => {
  return `${getMetricLabel(metric)} by ${getDimensionLabel(dimension, source)}`
}

const getSourceMetricValue = (
  row: Record<string, unknown>,
  metric: string,
  source: string
) => {
  const metricValue = row[metric]
  if (metricValue != null) return asNumber(metricValue)

  const fallbackKeys = SOURCE_FALLBACK_METRIC_KEYS[source] || []
  for (const key of fallbackKeys) {
    if (row[key] != null) return asNumber(row[key])
  }
  return 0
}

const toGraphKey = (
  dimension: string,
  metric: string,
  bucket?: "day" | "week" | "month",
  chartType?: GraphChartType
) => `${dimension}|${metric}|${bucket || ""}|${chartType || "bar"}`

type SectionMainChartMode = "line" | "dense_heatmap" | "metric_strips"

const SECTION_HEATMAP_MAX_ROWS = 16
const SECTION_METRIC_STRIP_MAX_ROWS = 12

const truncateCategoryLabel = (value: string, maxLength = 18) => {
  const clean = String(value || "").trim()
  if (!clean) return "Unknown"
  if (clean.length <= maxLength) return clean
  return `${clean.slice(0, Math.max(1, maxLength - 1))}...`
}

const isUnknownLikeLabel = (value: unknown) => {
  const label = String(value ?? "").trim().toLowerCase()
  return label === "" || label === "unknown" || label === "nan" || label === "none"
}

const SECTION_DIMENSION_ALIASES: Record<string, string[]> = {
  plan_category: ["device_plan_category"],
  device_plan_category: ["plan_category"],
}

const normalizeSectionBatchRow = (
  row: Record<string, unknown>,
  dimension: string
): SectionMergedRow | null => {
  const normalized: SectionMergedRow = {}

  Object.entries(row || {}).forEach(([key, value]) => {
    const safeKey = toSafeKey(key)
    if (!safeKey) return
    normalized[safeKey] = typeof value === "number" ? value : String(value ?? "")
  })

  const dimKey = toSafeKey(dimension)
  const dimensionValue = [
    normalized[dimKey],
    ...(SECTION_DIMENSION_ALIASES[dimKey] || []).map((alias) => normalized[alias]),
  ].find((value) => String(value ?? "").trim() !== "")

  if (dimensionValue == null) return null

  const resolvedValue = isTemporalDimensionKey(dimKey)
    ? normalizeMonthKey(dimensionValue, dimKey)
    : String(dimensionValue).trim()

  if (!resolvedValue) return null
  normalized[dimKey] = resolvedValue
  return normalized
}

const toFilterOptionLabels = (rows: CategoryPercentageRow[]) => {
  const seen = new Set<string>()
  const labels: string[] = []
  for (const row of normalizeCategoryRows(rows)) {
    const label = String(row.label || "").trim()
    if (!label || isUnknownLikeLabel(label)) continue
    const normalized = label.toLowerCase()
    if (seen.has(normalized)) continue
    seen.add(normalized)
    labels.push(label)
  }
  return labels
}

type RegionalAnalyticsFilterParams = Pick<
  CategoryPercentageParams,
  "filter_1_dimension" | "filter_1_values" | "filter_2_dimension" | "filter_2_values"
>

const toRegionalAnalyticsFilterParams = (
  filters: Array<{ dimension: RegionalCategoryDimension; values: string[] }>,
  excludeDimension?: RegionalCategoryDimension
): RegionalAnalyticsFilterParams => {
  const next: RegionalAnalyticsFilterParams = {}
  filters
    .filter((filter) => (
      filter.values.length > 0
      && (!excludeDimension || filter.dimension !== excludeDimension)
    ))
    .slice(0, 2)
    .forEach((filter, index) => {
      const slot = index + 1
      next[`filter_${slot}_dimension` as "filter_1_dimension" | "filter_2_dimension"] = filter.dimension
      next[`filter_${slot}_values` as "filter_1_values" | "filter_2_values"] = filter.values.join(",")
    })
  return next
}

const hexToRgb = (hex: string) => {
  const clean = hex.replace("#", "")
  const normalized = clean.length === 3
    ? clean.split("").map((ch) => `${ch}${ch}`).join("")
    : clean
  const num = Number.parseInt(normalized, 16)
  if (Number.isNaN(num)) return { r: 37, g: 99, b: 235 }
  return {
    r: (num >> 16) & 255,
    g: (num >> 8) & 255,
    b: num & 255,
  }
}

const mixColorWithWhite = (hex: string, amount: number) => {
  const { r, g, b } = hexToRgb(hex)
  const clamped = Math.max(0, Math.min(1, amount))
  const mix = (channel: number) => Math.round(channel + (255 - channel) * clamped)
  return `#${[mix(r), mix(g), mix(b)].map((v) => v.toString(16).padStart(2, "0")).join("")}`
}

const getHeatCellColor = (metric: string, intensity: number) => {
  const base = METRIC_LINE_COLORS[metric] || "#2563eb"
  const clamped = Math.max(0, Math.min(1, intensity))
  return mixColorWithWhite(base, 0.9 - clamped * 0.68)
}

const getSectionMainChartMode = (
  source: string,
  dimension: string
): SectionMainChartMode => {
  const sourceKey = (source || "").trim().toLowerCase()
  const dimKey = (dimension || "").trim().toLowerCase()

  if (
    ((sourceKey === "godrej" || sourceKey === "hitachi") && (dimKey === "channel" || dimKey === "plan_category"))
    || (sourceKey === "reliance" && dimKey === "plan_category")
    || (isSamsungPartnerSource(sourceKey) && dimKey === "plan_category")
  ) {
    return "dense_heatmap"
  }

  if (
    (isSamsungPartnerSource(sourceKey) && dimKey === "device_plan_category")
    || (sourceKey === "reliance" && (dimKey === "article_brand" || dimKey === "brand"))
    || ((sourceKey === "godrej" || sourceKey === "hitachi") && dimKey === "product_category")
  ) {
    return "metric_strips"
  }

  return "line"
}

const resolveFullscreenCompareSalesMetric = (metric: string) => {
  const normalized = toSafeKey(metric)
  if (
    normalized === "quantity"
    || SALES_METRICS.some((item) => item === normalized)
  ) {
    return normalized
  }
  return "gross_premium"
}

const resolveFullscreenCompareClaimsMetric = (metric: string) => {
  const normalized = toSafeKey(metric)
  if (
    normalized === "quantity"
    || FULLSCREEN_COMPARE_CLAIMS_METRICS.includes(
      normalized as (typeof FULLSCREEN_COMPARE_CLAIMS_METRICS)[number]
    )
  ) {
    return normalized
  }
  return "claims"
}

export default function MultiGraphView({
  source,
  datasetType,
  jobId,
  fromDate,
  toDate,
  resetFromDate,
  resetToDate,
  onDateRangeApply,
}: Props) {
  const prefersReducedMotion = useReducedMotion()
  const [isMobileViewport, setIsMobileViewport] = useState<boolean>(() => {
    if (typeof window === "undefined") return false
    return window.innerWidth < 640
  })
  const isSamsungOverview = source === "samsung"
  const samsungOverviewCards = useMemo<SamsungOverviewCard[]>(
    () => {
      if (datasetType === "claims") {
        const cards: SamsungOverviewCard[] = [
          {
            id: "samsung-month-claims-cost",
            title: "Claims Cost Trend by Month",
            subtitle: "Month-on-month claims cost line range comparison across Vijay Sales, Croma, and Reliance Digital.",
            dimension: "month",
            metric: "claims",
            bucket: "month",
            chartType: "line",
            size: "main",
            tooltipMetricOverride: "quantity",
          },
          {
            id: "samsung-plan-claims-pie",
            title: "Plan Category Claims Distribution",
            subtitle: "Claims cost split by plan category in pie format.",
            dimension: "plan_category",
            metric: "claims",
            chartType: "pie",
            size: "small",
          },
          {
            id: "samsung-device-claims-radar",
            title: "Device Plan Category Claims Radar",
            subtitle: "Spider-web comparison of claims cost across device plan categories.",
            dimension: "device_plan_category",
            metric: "claims",
            chartType: "radar",
            size: "small",
          },
        ]
        return cards.map((card) => resolveTemporalPreset(card, datasetType, fromDate, toDate))
      }

      const cards: SamsungOverviewCard[] = [
        {
          id: "samsung-month-gross-premium",
          title: "Gross Premium Trend by Month",
          subtitle: "Month-on-month gross premium line range comparison across Vijay Sales, Croma, and Reliance Digital.",
          dimension: "month",
          metric: "gross_premium",
          bucket: "month",
          chartType: "line",
          size: "main",
          tooltipMetricOverride: "quantity",
        },
        {
          id: "samsung-plan-pie",
          title: "Plan Category Distribution",
          subtitle: "Quantity split by plan category in pie format.",
          dimension: "plan_category",
          metric: "quantity",
          chartType: "pie",
          size: "small",
        },
        {
          id: "samsung-device-radar",
          title: "Device Plan Category Radar",
          subtitle: "Spider-web comparison of quantity across device plan categories.",
          dimension: "device_plan_category",
          metric: "quantity",
          chartType: "radar",
          size: "small",
        },
      ]
      return cards.map((card) => resolveTemporalPreset(card, datasetType, fromDate, toDate))
    },
    [datasetType, fromDate, toDate]
  )
  const isApplianceSource = source === "godrej" || source === "hitachi"
  const isHitachiSource = source === "hitachi"
  const isApplianceClaims = isApplianceSource && datasetType === "claims"
  const activeGroupOrder = useMemo(
    () => (
      isHitachiSource
        ? ["time", "region", "category", "product"]
        : isApplianceSource
        ? ["time", "region", "channel", "product"]
        : GROUP_ORDER
    ),
    [isApplianceSource, isHitachiSource]
  )
  const activePresets = useMemo(
    () => (
      (isHitachiSource
        ? (isApplianceClaims ? HITACHI_CLAIMS_PRESETS : HITACHI_SALES_PRESETS)
        : isApplianceSource
        ? (isApplianceClaims ? GODREJ_CLAIMS_PRESETS : GODREJ_SALES_PRESETS)
        : Object.values(GRAPH_PRESETS))
        .map((preset) => resolveTemporalPreset({ ...preset }, datasetType, fromDate, toDate))
    ),
    [datasetType, fromDate, isApplianceSource, isApplianceClaims, isHitachiSource, toDate]
  )
  const sideCardMetric = datasetType === "sales" ? "quantity" : "claims"
  const partnerSideCards = useMemo<PartnerSideCard[]>(() => {
    if (isSamsungOverview) return []

    if (isHitachiSource) {
      return [
        {
          id: `${source}-plan-distribution`,
          title: datasetType === "sales" ? "Plan Distribution Pie" : "Plan Claims Pie",
          subtitle:
            datasetType === "sales"
              ? "Share of total volume by plan category."
              : "Share of claims cost by Care+ Plan Name.",
          dimension: "plan_category",
          metric: sideCardMetric,
          chartType: "pie",
        },
      ]
    }

    if (isApplianceSource) {
      return [
        {
          id: `${source}-channel-distribution`,
          title: datasetType === "sales" ? "Channel Distribution" : "Channel Claims Distribution",
          subtitle:
            datasetType === "sales"
              ? "Share of total volume by channel."
              : "Share of claims cost by channel.",
          dimension: "channel",
          metric: sideCardMetric,
          chartType: "pie",
        },
        {
          id: `${source}-product-distribution`,
          title: datasetType === "sales" ? "Product Distribution Radar" : "Product Claims Radar",
          subtitle:
            datasetType === "sales"
              ? "Relative distribution across product categories."
              : "Relative claims concentration by product category.",
          dimension: "product_category",
          metric: sideCardMetric,
          chartType: "radar",
        },
      ]
    }

    if (source === "reliance") {
      return [
        {
          id: "reliance-plan-distribution",
          title: datasetType === "sales" ? "Plan Category Distribution" : "Plan Category Claims Split",
          subtitle:
            datasetType === "sales"
              ? "Volume spread across plan categories."
              : "Claims spread across plan categories.",
          dimension: "plan_category",
          metric: sideCardMetric,
          chartType: "pie",
        },
        {
          id: "reliance-brand-distribution",
          title: datasetType === "sales" ? "Brand Distribution Radar" : "Brand Claims Radar",
          subtitle:
            datasetType === "sales"
              ? "Brand-level distribution based on ARTICLE_BRAND."
              : "Brand-level claims pattern.",
          dimension: "article_brand",
          metric: sideCardMetric,
          chartType: "radar",
        },
      ]
    }

    return [
      {
        id: `${source}-plan-distribution`,
        title: datasetType === "sales" ? "Plan Category Distribution" : "Plan Category Claims Split",
        subtitle:
          datasetType === "sales"
            ? "Volume split across plan categories."
            : "Claims split across plan categories.",
        dimension: "plan_category",
        metric: sideCardMetric,
        chartType: "pie",
      },
      {
        id: `${source}-device-distribution`,
        title: datasetType === "sales" ? "Device Plan Category Radar" : "Device Plan Claims Radar",
        subtitle:
          datasetType === "sales"
            ? "Relative distribution across device plan categories."
            : "Relative claims concentration across device plan categories.",
        dimension: "device_plan_category",
        metric: sideCardMetric,
        chartType: "radar",
      },
    ]
  }, [datasetType, isSamsungOverview, isApplianceSource, isHitachiSource, sideCardMetric, source])

  const [fullscreen, setFullscreen] = useState<FullscreenGraph>(null)
  const [fullscreenFromDate, setFullscreenFromDate] = useState(fromDate || "")
  const [fullscreenToDate, setFullscreenToDate] = useState(toDate || "")
  const [openedGraphData, setOpenedGraphData] = useState<GraphDataSnapshot | null>(null)
  const [isFullscreenCompareMode, setIsFullscreenCompareMode] = useState(false)
  const [fullscreenCompareSummaries, setFullscreenCompareSummaries] = useState<FullscreenCompareSummaryState>(
    () => createEmptyFullscreenCompareSummaryState()
  )
  const [fullscreenCompareMatrixState, setFullscreenCompareMatrixState] = useState<FullscreenCompareMatrixState>(
    () => createEmptyFullscreenCompareMatrixState()
  )
  const [isFullscreenCompareMatrixOthersExpanded, setIsFullscreenCompareMatrixOthersExpanded] = useState(false)
  const [insights, setInsights] = useState<string[]>([])
  const [insightsModel, setInsightsModel] = useState<string>("")
  const [insightsLoading, setInsightsLoading] = useState(false)
  const [insightsError, setInsightsError] = useState<string | null>(null)
  const [selectedState, setSelectedState] = useState("")
  const [cityBreakdownRows, setCityBreakdownRows] = useState<CityBreakdownSlice[]>([])
  const [cityBreakdownLoading, setCityBreakdownLoading] = useState(false)
  const [cityBreakdownError, setCityBreakdownError] = useState<string | null>(null)
  const [activeCityName, setActiveCityName] = useState("")
  const [selectedComparisonStates, setSelectedComparisonStates] = useState<string[]>([])
  const [stateComparisonMixRows, setStateComparisonMixRows] = useState<StateComparisonMix[]>([])
  const [stateComparisonMixLoading, setStateComparisonMixLoading] = useState(false)
  const [stateComparisonMixError, setStateComparisonMixError] = useState<string | null>(null)
  const [regionalMapPrimaryValue, setRegionalMapPrimaryValue] = useState("")
  const [regionalMapSecondaryValue, setRegionalMapSecondaryValue] = useState("")
  const [regionalMapPrimaryOptions, setRegionalMapPrimaryOptions] = useState<string[]>([])
  const [regionalMapSecondaryOptions, setRegionalMapSecondaryOptions] = useState<string[]>([])
  const [regionalMapFiltersLoading, setRegionalMapFiltersLoading] = useState(false)
  const [isRegionFilterCardCollapsed, setIsRegionFilterCardCollapsed] = useState(false)
  const [isPartnerGraphScrollEnabled, setIsPartnerGraphScrollEnabled] = useState(false)
  const [sectionMergedMap, setSectionMergedMap] = useState<Record<string, SectionMergedState>>({})
  const lastInsightsKeyRef = useRef("")
  const clearRegionalMapFilters = () => {
    setRegionalMapPrimaryValue("")
    setRegionalMapSecondaryValue("")
    setRegionalMapPrimaryOptions([])
    setRegionalMapSecondaryOptions([])
    setRegionalMapFiltersLoading(false)
    setIsRegionFilterCardCollapsed(false)
  }
  const resetFullscreenCompareSummaries = () => {
    setFullscreenCompareSummaries(createEmptyFullscreenCompareSummaryState())
  }
  const resetFullscreenCompareMatrix = () => {
    setIsFullscreenCompareMatrixOthersExpanded(false)
    setFullscreenCompareMatrixState(createEmptyFullscreenCompareMatrixState())
  }
  const isYearOnYearFullscreen = Boolean(fullscreen?.isYearOnYear)
  const isStateFullscreen = Boolean(fullscreen && fullscreen.dimension.toLowerCase().includes("state"))
  const fullscreenCompareSalesMetric = useMemo(
    () => resolveFullscreenCompareSalesMetric(fullscreen?.metric || ""),
    [fullscreen?.metric]
  )
  const fullscreenCompareClaimsMetric = useMemo(
    () => resolveFullscreenCompareClaimsMetric(fullscreen?.metric || ""),
    [fullscreen?.metric]
  )
  const geographyLabel = "Region"
  const geographyLabelPlural = "Regions"
  useEffect(() => {
    if (typeof window === "undefined") return
    const onResize = () => setIsMobileViewport(window.innerWidth < 640)
    window.addEventListener("resize", onResize)
    return () => {
      window.removeEventListener("resize", onResize)
    }
  }, [])
  const isBreakdownMetricSupported = useMemo(() => {
    const metricKey = (fullscreen?.metric || "").trim().toLowerCase()
    return metricKey !== "loss_ratio"
  }, [fullscreen])
  const regionalAnalysisMetric = useMemo(() => {
    const metricKey = (fullscreen?.metric || "").trim().toLowerCase()
    if (metricKey && metricKey !== "loss_ratio") return metricKey
    return datasetType === "claims" ? "claims" : "gross_premium"
  }, [datasetType, fullscreen?.metric])
  const regionalCategoryConfig = useMemo(() => {
    if (source === "reliance") {
      return {
        primary: {
          dimension: "plan_category",
          label: "Plan Category",
          sectionTitle: "Plan Category Division",
          missingText: "No plan-category data found.",
        } as RegionalCategoryDescriptor,
        secondary: {
          dimension: "article_brand",
          label: "Brand Category",
          sectionTitle: "Brand Category Division",
          missingText: "No brand-category data found.",
        } as RegionalCategoryDescriptor,
      }
    }

    if (isHitachiSource) {
      return {
        primary: {
          dimension: "product_category",
          label: "Product Distribution",
          sectionTitle: "Product Distribution",
          missingText: "No product-distribution data found.",
        } as RegionalCategoryDescriptor,
        secondary: {
          dimension: "channel",
          label: "Channel Distribution",
          sectionTitle: "Channel Distribution",
          missingText: "No channel-distribution data found.",
        } as RegionalCategoryDescriptor,
      }
    }

    if (isApplianceSource) {
      return {
        primary: {
          dimension: "channel",
          label: "Channel Distribution",
          sectionTitle: "Channel Distribution",
          missingText: "No channel-distribution data found.",
        } as RegionalCategoryDescriptor,
        secondary: {
          dimension: "product_category",
          label: "Product Distribution",
          sectionTitle: "Product Distribution",
          missingText: "No product-distribution data found.",
        } as RegionalCategoryDescriptor,
      }
    }

    return {
      primary: {
        dimension: "plan_category",
        label: "Plan Category",
        sectionTitle: "Plan Category Division",
        missingText: "No plan-category data found.",
      } as RegionalCategoryDescriptor,
      secondary: {
        dimension: "device_plan_category",
        label: "Device Plan Category",
        sectionTitle: "Device Plan Category Division",
        missingText: "No device-category data found.",
      } as RegionalCategoryDescriptor,
    }
  }, [isApplianceSource, isHitachiSource, source])
  const activeRegionalPrimaryDescriptor = regionalCategoryConfig.primary
  const activeRegionalSecondaryDescriptor = regionalCategoryConfig.secondary
  const regionalMapFilterConfig = useMemo(() => {
    if (isHitachiSource) {
      return {
        primary: {
          dimension: activeRegionalPrimaryDescriptor.dimension,
          label: "Product",
          allLabel: "All Products",
        } as RegionalMapFilterDescriptor,
        secondary: {
          dimension: activeRegionalSecondaryDescriptor.dimension,
          label: "Channel",
          allLabel: "All Channels",
        } as RegionalMapFilterDescriptor,
      }
    }
    if (isApplianceSource) {
      return {
        primary: {
          dimension: activeRegionalPrimaryDescriptor.dimension,
          label: "Channel",
          allLabel: "All Channels",
        } as RegionalMapFilterDescriptor,
        secondary: {
          dimension: activeRegionalSecondaryDescriptor.dimension,
          label: "Product",
          allLabel: "All Products",
        } as RegionalMapFilterDescriptor,
      }
    }
    if (source === "reliance") {
      return {
        primary: {
          dimension: activeRegionalPrimaryDescriptor.dimension,
          label: "Plan Category",
          allLabel: "All Plan Categories",
        } as RegionalMapFilterDescriptor,
        secondary: {
          dimension: activeRegionalSecondaryDescriptor.dimension,
          label: "Brand Category",
          allLabel: "All Brand Categories",
        } as RegionalMapFilterDescriptor,
      }
    }
    return {
      primary: {
        dimension: activeRegionalPrimaryDescriptor.dimension,
        label: "Plan Category",
        allLabel: "All Plan Categories",
      } as RegionalMapFilterDescriptor,
      secondary: {
        dimension: activeRegionalSecondaryDescriptor.dimension,
        label: "Device Plan Category",
        allLabel: "All Device Plan Categories",
      } as RegionalMapFilterDescriptor,
    }
  }, [
    activeRegionalPrimaryDescriptor.dimension,
    activeRegionalSecondaryDescriptor.dimension,
    isApplianceSource,
    isHitachiSource,
    source,
  ])
  const activeRegionalMapFilters = useMemo(() => {
    const filters: Array<{ dimension: RegionalCategoryDimension; values: string[] }> = []
    if (regionalMapPrimaryValue) {
      filters.push({
        dimension: regionalMapFilterConfig.primary.dimension,
        values: [regionalMapPrimaryValue],
      })
    }
    if (regionalMapSecondaryValue) {
      filters.push({
        dimension: regionalMapFilterConfig.secondary.dimension,
        values: [regionalMapSecondaryValue],
      })
    }
    return filters
  }, [
    regionalMapPrimaryValue,
    regionalMapSecondaryValue,
    regionalMapFilterConfig.primary.dimension,
    regionalMapFilterConfig.secondary.dimension,
  ])
  useEffect(() => {
    if (!isStateFullscreen) return

    let active = true
    const controller = new AbortController()
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setRegionalMapFiltersLoading(true)

    const baseParams = {
      source,
      dataset_type: datasetType,
      metric: regionalAnalysisMetric,
      job_id: jobId || undefined,
      from_date: fullscreenFromDate || fromDate || undefined,
      to_date: fullscreenToDate || toDate || undefined,
      limit: 200,
    } as const

    const primaryFilterParams = toRegionalAnalyticsFilterParams(
      activeRegionalMapFilters,
      regionalMapFilterConfig.primary.dimension
    )
    const secondaryFilterParams = toRegionalAnalyticsFilterParams(
      activeRegionalMapFilters,
      regionalMapFilterConfig.secondary.dimension
    )

    Promise.all([
      fetchCategoryPercentage({
        ...baseParams,
        dimension: regionalMapFilterConfig.primary.dimension,
        ...primaryFilterParams,
      }, { signal: controller.signal }),
      fetchCategoryPercentage({
        ...baseParams,
        dimension: regionalMapFilterConfig.secondary.dimension,
        ...secondaryFilterParams,
      }, { signal: controller.signal }),
    ])
      .then(([primaryRes, secondaryRes]) => {
        if (!active) return
        const nextPrimary = toFilterOptionLabels((primaryRes.rows as CategoryOption[]) || [])
        const nextSecondary = toFilterOptionLabels((secondaryRes.rows as CategoryOption[]) || [])
        setRegionalMapPrimaryOptions(nextPrimary)
        setRegionalMapSecondaryOptions(nextSecondary)
        setRegionalMapPrimaryValue((prev) => (
          prev && nextPrimary.includes(prev) ? prev : ""
        ))
        setRegionalMapSecondaryValue((prev) => (
          prev && nextSecondary.includes(prev) ? prev : ""
        ))
      })
      .catch(() => {
        if (!active) return
        setRegionalMapPrimaryOptions([])
        setRegionalMapSecondaryOptions([])
        setRegionalMapPrimaryValue("")
        setRegionalMapSecondaryValue("")
      })
      .finally(() => {
        if (!active) return
        setRegionalMapFiltersLoading(false)
      })

    return () => {
      active = false
      controller.abort()
    }
  }, [
    isStateFullscreen,
    source,
    datasetType,
    jobId,
    fullscreenFromDate,
    fullscreenToDate,
    fromDate,
    toDate,
    regionalAnalysisMetric,
    activeRegionalMapFilters,
    regionalMapFilterConfig.primary.dimension,
    regionalMapFilterConfig.secondary.dimension,
  ])
  const showRegionalPrimaryFilter = regionalMapPrimaryOptions.length > 0
  const showRegionalSecondaryFilter = regionalMapSecondaryOptions.length > 0
  const regionalMixLabels = useMemo(() => {
    const labels: string[] = []
    if (showRegionalPrimaryFilter) {
      labels.push(activeRegionalPrimaryDescriptor.label)
    }
    if (
      showRegionalSecondaryFilter
      && activeRegionalSecondaryDescriptor.label !== activeRegionalPrimaryDescriptor.label
    ) {
      labels.push(activeRegionalSecondaryDescriptor.label)
    }
    if (!labels.length) {
      labels.push(activeRegionalPrimaryDescriptor.label)
    }
    return labels
  }, [
    activeRegionalPrimaryDescriptor.label,
    activeRegionalSecondaryDescriptor.label,
    showRegionalPrimaryFilter,
    showRegionalSecondaryFilter,
  ])
  const getSectionTitle = useCallback((group: string) => {
    if (isApplianceSource) return GODREJ_GROUP_TITLES[group] || group
    if (group === "device_category" && source === "reliance") return "Brand Segment Pulse"
    return GROUP_TITLES[group] || group
  }, [isApplianceSource, source])
  const sectionConfigs = useMemo(() => {
    if (isSamsungOverview) return []
    return activeGroupOrder
      .map(group => {
        const presets = activePresets.filter((p: Preset) => p.group === group)
        const entries = presets
          .map((preset: Preset) => {
            const resolvedDimension =
              source === "reliance" && group === "device_category"
                ? "article_brand"
                : preset.dimension
            const metricPriority = getSectionMetricPriority(datasetType)
            const visibleMetrics = metricPriority.filter((metric) =>
              preset.metrics.includes(metric)
            )
            return {
              preset: { ...preset, dimension: resolvedDimension },
              visibleMetrics,
              mergedMetrics: visibleMetrics.slice(0, datasetType === "sales" ? 4 : 2),
            }
          })
          .filter(entry => entry.visibleMetrics.length > 0)
        return { group, entries }
      })
      .filter(section => section.entries.length > 0)
  }, [isSamsungOverview, activeGroupOrder, activePresets, datasetType, source])
  const visibleSectionConfigs = useMemo(
    () => sectionConfigs.filter(({ group }) => {
      const sectionData = sectionMergedMap[group]
      if (!sectionData) return true
      if (sectionData.loading || sectionData.error) return true
      return sectionData.rows.length > 0
    }),
    [sectionConfigs, sectionMergedMap]
  )

  useEffect(() => {
    if (!isSamsungOverview || !samsungOverviewCards.length) return

    let active = true
    const controller = new AbortController()
    const requestKeys = new Set<string>()
    const batchRequests: FetchByDimensionBatchItem[] = []

    samsungOverviewCards.forEach((card) => {
      const metrics = [
        card.metric,
        card.tooltipMetricOverride,
      ].filter((value): value is string => Boolean(value))

      metrics.forEach((metricKey) => {
        const requestKey = `${card.dimension}::${metricKey}::${card.bucket || ""}`
        if (requestKeys.has(requestKey)) return
        requestKeys.add(requestKey)
        batchRequests.push({
          request_key: requestKey,
          source,
          dataset_type: datasetType,
          dimension: card.dimension,
          metric: metricKey,
          bucket: card.bucket,
          job_id: jobId || undefined,
          from_date: fromDate || undefined,
          to_date: toDate || undefined,
        })
      })
    })

    if (!batchRequests.length) return

    const timer = window.setTimeout(async () => {
      try {
        const response = await fetchByDimensionBatch(batchRequests, { signal: controller.signal })
        if (!active) return

        const rowsByRequest = new Map<string, Array<Record<string, unknown>>>()
        ;(response.results || []).forEach((item) => {
          rowsByRequest.set(item.request_key, Array.isArray(item.rows) ? item.rows : [])
        })

        batchRequests.forEach((request) => {
          const rows = rowsByRequest.get(request.request_key)
          if (!rows?.length) return
          seedGraphDataCache({
            source: request.source,
            dimension: request.dimension,
            metric: request.metric,
            datasetType,
            bucket: request.bucket || undefined,
            jobId: request.job_id || null,
            from_date: request.from_date || undefined,
            to_date: request.to_date || undefined,
          }, rows)
        })
      } catch {
        // Embedded samsung cards fall back to their own cached fetch path.
      }
    }, 0)

    return () => {
      active = false
      controller.abort()
      window.clearTimeout(timer)
    }
  }, [isSamsungOverview, samsungOverviewCards, source, datasetType, jobId, fromDate, toDate])

  useEffect(() => {
    if (!source || datasetType === "claims") return

    const controller = new AbortController()
    const timer = window.setTimeout(() => {
      void fetchAnnualComparison({
        source,
        dataset_type: datasetType,
        job_id: jobId || undefined,
        from_date: fromDate || undefined,
        to_date: toDate || undefined,
      }, {
        signal: controller.signal,
      }).catch(() => {
        // The chart falls back to its normal path if this warm request misses.
      })
    }, 0)

    return () => {
      controller.abort()
      window.clearTimeout(timer)
    }
  }, [source, datasetType, jobId, fromDate, toDate])

  const navigableGraphs = useMemo(() => {
    if (isSamsungOverview) {
      return samsungOverviewCards.map((card) => ({
        group: "samsung_overview",
        sectionTitle: datasetType === "claims" ? "Samsung Claims Focus" : "Samsung Gross Premium Focus",
        metric: card.metric,
        dimension: card.dimension,
        bucket: card.bucket,
        chartType: card.chartType,
        tooltipMetricOverride: card.tooltipMetricOverride,
      }))
    }
    const fallbackSection = visibleSectionConfigs[0]?.group || sectionConfigs[0]?.group || "time"
    const sectionTitle = getSectionTitle(fallbackSection)
    return partnerSideCards.map((card) => ({
      group: fallbackSection,
      sectionTitle,
      metric: card.metric,
      dimension: card.dimension,
      bucket: card.bucket,
      chartType: card.chartType,
      tooltipMetricOverride: card.tooltipMetricOverride,
    }))
  }, [isSamsungOverview, samsungOverviewCards, visibleSectionConfigs, sectionConfigs, getSectionTitle, partnerSideCards, datasetType])

  const fullscreenGraphIndex = useMemo(() => {
    if (!fullscreen) return -1
    const currentKey = toGraphKey(fullscreen.dimension, fullscreen.metric, fullscreen.bucket, fullscreen.chartType)
    return navigableGraphs.findIndex((item) =>
      toGraphKey(item.dimension, item.metric, item.bucket, item.chartType) === currentKey
    )
  }, [fullscreen, navigableGraphs])

  const currentNavigableGraph =
    fullscreenGraphIndex >= 0 ? navigableGraphs[fullscreenGraphIndex] : null
  const canGoToPreviousGraph = fullscreenGraphIndex > 0
  const canGoToNextGraph =
    fullscreenGraphIndex >= 0 && fullscreenGraphIndex < navigableGraphs.length - 1
  const fullscreenGraphHeightClass = isStateFullscreen
    ? "h-[72vh] sm:h-[74vh] lg:h-[76vh]"
    : "h-[62vh] sm:h-[66vh] lg:h-[70vh]"

  useEffect(() => {
    if (isSamsungOverview || !sectionConfigs.length) return

    let active = true
    const controller = new AbortController()
    const run = async () => {
      if (!active) return

      const loadingState: Record<string, SectionMergedState> = {}
      sectionConfigs.forEach(({ group }) => {
        loadingState[group] = { loading: true, error: null, rows: [] }
      })
      setSectionMergedMap((prev) => ({ ...prev, ...loadingState }))

      const buildSectionState = (
        entries: Array<{ preset: Preset; visibleMetrics: string[]; mergedMetrics: string[] }>,
        rowsByMetric: Map<string, Array<Record<string, unknown>>>
      ): SectionMergedState => {
        const firstEntry = entries[0]
        if (!firstEntry) {
          return { loading: false, error: "No section preset available.", rows: [] }
        }

        const dimension = firstEntry.preset.dimension
        const mergedMetrics = firstEntry.mergedMetrics
        const dimKey = toSafeKey(dimension)
        const merged = new Map<string, SectionMergedRow>()
        let successCount = 0

        mergedMetrics.forEach((metric) => {
          const metricRows = rowsByMetric.get(metric)
          if (!metricRows) return
          successCount += 1

          metricRows.forEach((rawRow) => {
            const row = normalizeSectionBatchRow(rawRow, dimension)
            if (!row) return

            const rawDim = row[dimKey]
            const dimValue = String(rawDim ?? "").trim()
            if (!dimValue) return

            if (!merged.has(dimValue)) {
              const seed: SectionMergedRow = { [dimKey]: dimValue }
              mergedMetrics.forEach((m) => {
                seed[m] = 0
              })
              merged.set(dimValue, seed)
            }

            const target = merged.get(dimValue)
            if (!target) return
            target[metric] = Math.max(0, getSourceMetricValue(row, metric, source))
          })
        })

        if (successCount === 0) {
          return { loading: false, error: "Unable to load section graph data.", rows: [] }
        }

        const rows = Array.from(merged.values())
        if (dimKey.includes("month") || dimKey.includes("date")) {
          rows.sort((a, b) => (
            monthSortValue(String(a[dimKey] || ""), dimKey) - monthSortValue(String(b[dimKey] || ""), dimKey)
          ))
        } else {
          rows.sort((a, b) => String(a[dimKey] || "").localeCompare(String(b[dimKey] || "")))
        }

        return { loading: false, error: rows.length ? null : "No data available in selected range.", rows }
      }

      const fetchSectionState = async (
        entries: Array<{ preset: Preset; visibleMetrics: string[]; mergedMetrics: string[] }>
      ): Promise<SectionMergedState> => {
        const firstEntry = entries[0]
        if (!firstEntry) {
          return { loading: false, error: "No section preset available.", rows: [] }
        }

        const dimension = firstEntry.preset.dimension
        const bucket = firstEntry.preset.bucket
        const mergedMetrics = firstEntry.mergedMetrics

        try {
          const metricResponses = await Promise.allSettled(
            mergedMetrics.map((metric) =>
              fetchGraphRows({
                source,
                dimension,
                metric,
                datasetType,
                bucket,
                jobId,
                from_date: fromDate,
                to_date: toDate,
              }, { signal: controller.signal })
            )
          )

          const rowsByMetric = new Map<string, Array<Record<string, unknown>>>()

          metricResponses.forEach((response, idx) => {
            const metric = mergedMetrics[idx]
            if (response.status !== "fulfilled") return
            rowsByMetric.set(metric, response.value.data || [])
          })

          return buildSectionState(entries, rowsByMetric)
        } catch {
          return { loading: false, error: "Unable to load section graph data.", rows: [] }
        }
      }

      const batchRequests: FetchByDimensionBatchItem[] = sectionConfigs.flatMap(({ group, entries }) => {
        const firstEntry = entries[0]
        if (!firstEntry) return []
        const { preset, mergedMetrics } = firstEntry
        return mergedMetrics.map((metric) => ({
          request_key: `${group}::${metric}`,
          source,
          dataset_type: datasetType,
          dimension: preset.dimension,
          metric,
          bucket: preset.bucket,
          job_id: jobId || undefined,
          from_date: fromDate || undefined,
          to_date: toDate || undefined,
        }))
      })

      try {
        if (batchRequests.length) {
          const response = await fetchByDimensionBatch(batchRequests, { signal: controller.signal })
          if (!active) return

          const rowsByRequest = new Map<string, Array<Record<string, unknown>>>()
          ;(response.results || []).forEach((item) => {
            rowsByRequest.set(item.request_key, Array.isArray(item.rows) ? item.rows : [])
          })

          batchRequests.forEach((request) => {
            const rows = rowsByRequest.get(request.request_key)
            if (!rows?.length) return
            seedGraphDataCache({
              source: request.source,
              dimension: request.dimension,
              metric: request.metric,
              datasetType,
              bucket: request.bucket || undefined,
              jobId: request.job_id || null,
              from_date: request.from_date || undefined,
              to_date: request.to_date || undefined,
            }, rows)
          })

          const nextState: Record<string, SectionMergedState> = {}
          sectionConfigs.forEach(({ group, entries }) => {
            const firstEntry = entries[0]
            const mergedMetrics = firstEntry?.mergedMetrics || []
            const rowsByMetric = new Map<string, Array<Record<string, unknown>>>()
            mergedMetrics.forEach((metric) => {
              rowsByMetric.set(metric, rowsByRequest.get(`${group}::${metric}`) || [])
            })
            nextState[group] = buildSectionState(entries, rowsByMetric)
          })

          setSectionMergedMap((prev) => ({ ...prev, ...nextState }))
          return
        }
      } catch {
        // Fall back to individual graph fetches when the batch endpoint is not yet deployed.
      }

      const sectionStates = await Promise.all(
        sectionConfigs.map(async ({ group, entries }) => (
          [group, await fetchSectionState(entries)] as const
        ))
      )
      if (!active) return
      setSectionMergedMap((prev) => ({
        ...prev,
        ...Object.fromEntries(sectionStates),
      }))
    }

    const timer = window.setTimeout(() => { void run() }, 60)

    return () => {
      active = false
      controller.abort()
      window.clearTimeout(timer)
    }
  }, [isSamsungOverview, sectionConfigs, source, datasetType, jobId, fromDate, toDate])

  const fullscreenCompositeData = useMemo(() => {
    if (!fullscreen?.isComposite || !fullscreen.sectionGroup) return null
    const section = sectionConfigs.find((item) => item.group === fullscreen.sectionGroup)
    const entry = section?.entries[0]
    if (!section || !entry) return null

    const dimKey = entry.preset.dimension || fullscreen.dimension
    const mergedMetrics = entry.mergedMetrics || []
    const sectionData = sectionMergedMap[section.group] || { loading: false, error: null, rows: [] }
    const sectionChartMode = fullscreen.sectionMode || getSectionMainChartMode(source, dimKey)
    const isRelianceBrandSection =
      source === "reliance" && (dimKey === "article_brand" || dimKey === "brand")
    const sectionRows = isRelianceBrandSection
      ? sectionData.rows.filter((row) => !isUnknownLikeLabel(row[dimKey]))
      : sectionData.rows
    const dominantMetric = mergedMetrics[0] || (datasetType === "sales" ? "gross_premium" : "claims")
    const rankedRows = sectionRows.length
      ? [...sectionRows].sort((a, b) => asNumber(b[dominantMetric]) - asNumber(a[dominantMetric]))
      : []
    const displayRows = sectionChartMode === "line"
      ? sectionRows
      : rankedRows.slice(
          0,
          sectionChartMode === "dense_heatmap"
            ? SECTION_HEATMAP_MAX_ROWS
            : SECTION_METRIC_STRIP_MAX_ROWS
        )
    const metricMaxima = mergedMetrics.reduce((acc, metric) => {
      acc[metric] = displayRows.reduce((max, row) => Math.max(max, asNumber(row[metric])), 0)
      return acc
    }, {} as Record<string, number>)

    return {
      group: section.group,
      dimKey,
      mergedMetrics,
      sectionData,
      sectionChartMode,
      displayRows,
      metricMaxima,
    }
  }, [fullscreen, sectionConfigs, sectionMergedMap, source, datasetType])

  const fullscreenCompositeChartRows = useMemo(() => (
    fullscreenCompositeData?.displayRows || []
  ), [fullscreenCompositeData])
  const activeOpenedGraphData = useMemo<GraphDataSnapshot | null>(() => {
    if (!fullscreen?.isComposite) return openedGraphData
    if (
      !fullscreenCompositeData
      || fullscreenCompositeData.sectionData.loading
      || Boolean(fullscreenCompositeData.sectionData.error)
      || !fullscreenCompositeData.displayRows.length
    ) {
      return null
    }

    const fallbackMetric = datasetType === "sales" ? "gross_premium" : "claims"
    const insightMetric = String(
      fullscreen.metric || fullscreenCompositeData.mergedMetrics[0] || fallbackMetric
    )

    return {
      rows: fullscreenCompositeData.displayRows,
      measure: insightMetric,
      dimensionKey: fullscreenCompositeData.dimKey,
      compareMode: false,
    }
  }, [datasetType, fullscreen, fullscreenCompositeData, openedGraphData])
  const stateOptions = useMemo(() => {
    if (!isStateFullscreen || !activeOpenedGraphData?.rows.length || !activeOpenedGraphData.dimensionKey) {
      return []
    }

    const seen = new Set<string>()
    const out: string[] = []
    const dimKey = activeOpenedGraphData.dimensionKey
    for (const row of activeOpenedGraphData.rows) {
      const rawValue = row[dimKey]
      const label = rawValue == null ? "" : String(rawValue).trim()
      if (!label || seen.has(label)) continue
      seen.add(label)
      out.push(label)
    }
    return out
  }, [activeOpenedGraphData, isStateFullscreen])
  const activeSelectedState =
    isStateFullscreen && stateOptions.includes(selectedState) ? selectedState : ""
  const activeComparisonStates = useMemo(
    () => selectedComparisonStates.filter((state) => stateOptions.includes(state)),
    [selectedComparisonStates, stateOptions]
  )
  const activeCityKey = useMemo(() => {
    if (!cityBreakdownRows.length) return ""
    if (activeCityName && cityBreakdownRows.some((row) => row.city === activeCityName)) {
      return activeCityName
    }
    return cityBreakdownRows[0]?.city || ""
  }, [cityBreakdownRows, activeCityName])
  const cityBreakdownColors = useMemo(
    () => buildDistinctSolidColors(
      cityBreakdownRows.length,
      CITY_PIE_COLORS,
      `${selectedState}-${fullscreen?.metric || ""}-city-breakdown`
    ),
    [cityBreakdownRows.length, fullscreen?.metric, selectedState]
  )
  const stateMetricMap = useMemo(() => {
    const map = new Map<string, number>()
    if (!isStateFullscreen || !activeOpenedGraphData?.rows.length || !activeOpenedGraphData.dimensionKey) {
      return map
    }

    const dimKey = activeOpenedGraphData.dimensionKey
    const measureKey = activeOpenedGraphData.measure
    for (const row of activeOpenedGraphData.rows) {
      const raw = row[dimKey]
      const label = raw == null ? "" : String(raw).trim()
      if (!label) continue

      const value = activeOpenedGraphData.compareMode
        ? sumSamsungPartnerValues(row as Record<string, unknown>)
        : asNumber(row[measureKey])
      map.set(label, (map.get(label) ?? 0) + Math.max(0, value))
    }
    return map
  }, [activeOpenedGraphData, isStateFullscreen])
  const comparisonMetricRows = useMemo(() => {
    const rows: StateComparisonRow[] = activeComparisonStates.map((state) => ({
      state,
      value: asNumber(stateMetricMap.get(state)),
    }))

    rows.sort((a, b) => b.value - a.value)
    return rows
  }, [activeComparisonStates, stateMetricMap])
  const stateComparisonLayout = useMemo(
    () => getStateComparisonLayout(activeComparisonStates.length),
    [activeComparisonStates.length]
  )
  const comparisonPieSizing = useMemo(
    () => getPieSizing(activeComparisonStates.length),
    [activeComparisonStates.length]
  )

  useEffect(() => {
    if (!fullscreen || !isFullscreenCompareMode) return

    let active = true
    const controller = new AbortController()

    // eslint-disable-next-line react-hooks/set-state-in-effect
    setFullscreenCompareSummaries({
      loading: true,
      error: null,
      sales: null,
      claims: null,
    })

    Promise.all([
      fetchSummary({
        source,
        dataset_type: "sales",
        job_id: jobId || undefined,
        from_date: fullscreenFromDate || fromDate || undefined,
        to_date: fullscreenToDate || toDate || undefined,
      }, { signal: controller.signal }),
      fetchSummary({
        source,
        dataset_type: "claims",
        job_id: jobId || undefined,
        from_date: fullscreenFromDate || fromDate || undefined,
        to_date: fullscreenToDate || toDate || undefined,
      }, { signal: controller.signal }),
    ])
      .then(([sales, claims]) => {
        if (!active) return
        const normalizedSales = normalizeFocusCompareSummary(
          (sales || null) as FocusCompareSummary | null,
          "sales"
        )
        const normalizedClaims = normalizeFocusCompareSummary(
          (claims || null) as FocusCompareSummary | null,
          "claims",
          normalizedSales
        )
        setFullscreenCompareSummaries({
          loading: false,
          error: null,
          sales: normalizedSales,
          claims: normalizedClaims,
        })
      })
      .catch((err) => {
        if (!active) return
        const message = err instanceof Error ? err.message : "Unable to load comparison metrics."
        setFullscreenCompareSummaries({
          loading: false,
          error: message,
          sales: null,
          claims: null,
        })
      })

    return () => {
      active = false
      controller.abort()
    }
  }, [
    fullscreen,
    isFullscreenCompareMode,
    source,
    jobId,
    fullscreenFromDate,
    fullscreenToDate,
    fromDate,
    toDate,
  ])

  const fullscreenCompareMetricCards = useMemo(() => {
    const sales = fullscreenCompareSummaries.sales
    const claims = fullscreenCompareSummaries.claims
    return [
      {
        key: "gross_premium",
        family: "Sales",
        label: "Gross Premium",
        metric: "gross_premium",
        value: sales?.gross_premium,
      },
      {
        key: "earned_premium",
        family: "Sales",
        label: "Earned Premium",
        metric: "earned_premium",
        value: sales?.earned_premium,
      },
      {
        key: "zopper_earned_premium",
        family: "Sales",
        label: "Zopper Earned Premium",
        metric: "zopper_earned_premium",
        value: sales?.zopper_earned_premium,
      },
      {
        key: "quantity",
        family: "Sales",
        label: "Volume",
        metric: "quantity",
        value: sales?.units_sold,
      },
      {
        key: "claims",
        family: "Claims",
        label: "Claims Cost",
        metric: "claims",
        value: claims?.claims,
      },
      {
        key: "net_claims",
        family: "Claims",
        label: "Net Claims Paid",
        metric: "net_claims",
        value: claims?.net_claims,
      },
      {
        key: "loss_ratio",
        family: "Claims",
        label: "Loss Ratio",
        metric: "loss_ratio",
        value: claims?.loss_ratio,
      },
    ]
  }, [fullscreenCompareSummaries.claims, fullscreenCompareSummaries.sales])

  useEffect(() => {
    if (!fullscreen || !isFullscreenCompareMode || fullscreen.chartType !== "india_map") return

    let active = true
    const controller = new AbortController()
    const dimension = fullscreen.dimension || "state"
    const batchRequests: FetchByDimensionBatchItem[] = [
      {
        request_key: "gross_premium",
        source,
        dataset_type: "sales",
        dimension,
        metric: "gross_premium",
        bucket: fullscreen.bucket,
        job_id: jobId || undefined,
        from_date: fullscreenFromDate || fromDate || undefined,
        to_date: fullscreenToDate || toDate || undefined,
      },
      {
        request_key: "earned_premium",
        source,
        dataset_type: "sales",
        dimension,
        metric: "earned_premium",
        bucket: fullscreen.bucket,
        job_id: jobId || undefined,
        from_date: fullscreenFromDate || fromDate || undefined,
        to_date: fullscreenToDate || toDate || undefined,
      },
      {
        request_key: "zopper_earned_premium",
        source,
        dataset_type: "sales",
        dimension,
        metric: "zopper_earned_premium",
        bucket: fullscreen.bucket,
        job_id: jobId || undefined,
        from_date: fullscreenFromDate || fromDate || undefined,
        to_date: fullscreenToDate || toDate || undefined,
      },
      {
        request_key: "claims",
        source,
        dataset_type: "claims",
        dimension,
        metric: "claims",
        bucket: fullscreen.bucket,
        job_id: jobId || undefined,
        from_date: fullscreenFromDate || fromDate || undefined,
        to_date: fullscreenToDate || toDate || undefined,
      },
      {
        request_key: "net_claims",
        source,
        dataset_type: "claims",
        dimension,
        metric: "net_claims",
        bucket: fullscreen.bucket,
        job_id: jobId || undefined,
        from_date: fullscreenFromDate || fromDate || undefined,
        to_date: fullscreenToDate || toDate || undefined,
      },
    ]

    activeRegionalMapFilters.slice(0, 2).forEach((filter, index) => {
      const slot = index + 1
      batchRequests.forEach((request) => {
        request[`filter_${slot}_dimension` as "filter_1_dimension" | "filter_2_dimension"] = filter.dimension
        request[`filter_${slot}_values` as "filter_1_values" | "filter_2_values"] = filter.values.join(",")
      })
    })

    // eslint-disable-next-line react-hooks/set-state-in-effect
    setIsFullscreenCompareMatrixOthersExpanded(false)
    setFullscreenCompareMatrixState((prev) => ({
      loading: true,
      error: null,
      rows: prev.rows.length ? prev.rows : [],
      othersBreakdownRows: prev.othersBreakdownRows.length ? prev.othersBreakdownRows : [],
    }))

    fetchByDimensionBatch(batchRequests, { signal: controller.signal })
      .then((response) => {
        if (!active) return

        const rowsByRequest = new Map<string, Array<Record<string, unknown>>>()
        ;(response.results || []).forEach((result) => {
          rowsByRequest.set(result.request_key, Array.isArray(result.rows) ? result.rows : [])
        })

        const dimKey = toSafeKey(dimension)
        const grossMap = new Map<string, number>()
        const earnedMap = new Map<string, number>()
        const zopperMap = new Map<string, number>()
        const claimsMap = new Map<string, number>()
        const netClaimsMap = new Map<string, number>()

        const ingestRows = (
          rows: Array<Record<string, unknown>>,
          metric:
            | "gross_premium"
            | "earned_premium"
            | "zopper_earned_premium"
            | "claims"
            | "net_claims",
          target: Map<string, number>
        ) => {
          rows.forEach((rawRow) => {
            const row = normalizeSectionBatchRow(rawRow, dimension)
            if (!row) return
            const label = String(row[dimKey] ?? "").trim()
            if (!label) return
            const value = getFullscreenCompareMatrixMetricValue(row, metric, source)
            target.set(label, (target.get(label) ?? 0) + value)
          })
        }

        ingestRows(rowsByRequest.get("gross_premium") || [], "gross_premium", grossMap)
        ingestRows(rowsByRequest.get("earned_premium") || [], "earned_premium", earnedMap)
        ingestRows(rowsByRequest.get("zopper_earned_premium") || [], "zopper_earned_premium", zopperMap)
        ingestRows(rowsByRequest.get("claims") || [], "claims", claimsMap)
        ingestRows(rowsByRequest.get("net_claims") || [], "net_claims", netClaimsMap)

        const labels = Array.from(
          new Set([
            ...grossMap.keys(),
            ...earnedMap.keys(),
            ...zopperMap.keys(),
            ...claimsMap.keys(),
            ...netClaimsMap.keys(),
          ])
        )

        const othersSeed: FullscreenCompareMatrixRow = {
          label: "Others",
          grossPremiumValue: 0,
          earnedPremiumValue: 0,
          claimsValue: 0,
          ratioValue: 0,
          lossRatioValue: 0,
        }
        const othersBreakdownRows: FullscreenCompareMatrixRow[] = []
        let othersZopperEarnedPremiumValue = 0
        let othersNetClaimsValue = 0

        const nextRows = labels
          .map((label) => {
            const grossPremiumValue = asNumber(grossMap.get(label))
            const earnedPremiumValue = asNumber(earnedMap.get(label))
            const zopperEarnedPremiumValue = asNumber(zopperMap.get(label))
            const claimsValue = asNumber(claimsMap.get(label))
            const netClaimsValue = asNumber(netClaimsMap.get(label))
            const ratioValue = grossPremiumValue > 0 ? (claimsValue / grossPremiumValue) * 100 : 0
            const lossRatioValue = zopperEarnedPremiumValue > 0
              ? Math.min(
                FULLSCREEN_COMPARE_MATRIX_LOSS_RATIO_CAP,
                Math.max(0, (netClaimsValue / zopperEarnedPremiumValue) * 100)
              )
              : 0

            return {
              label,
              grossPremiumValue,
              earnedPremiumValue,
              claimsValue,
              ratioValue,
              lossRatioValue,
              zopperEarnedPremiumValue,
              netClaimsValue,
            }
          })
          .filter((row) => row.grossPremiumValue > 0 || row.earnedPremiumValue > 0 || row.claimsValue > 0)
          .reduce<FullscreenCompareMatrixRow[]>((acc, row) => {
            if (row.grossPremiumValue < FULLSCREEN_COMPARE_MATRIX_OTHERS_GROSS_THRESHOLD) {
              othersSeed.grossPremiumValue += row.grossPremiumValue
              othersSeed.earnedPremiumValue += row.earnedPremiumValue
              othersSeed.claimsValue += row.claimsValue
              othersZopperEarnedPremiumValue += row.zopperEarnedPremiumValue
              othersNetClaimsValue += row.netClaimsValue
              othersBreakdownRows.push({
                label: row.label,
                grossPremiumValue: row.grossPremiumValue,
                earnedPremiumValue: row.earnedPremiumValue,
                claimsValue: row.claimsValue,
                ratioValue: row.ratioValue,
                lossRatioValue: row.lossRatioValue,
              })
              return acc
            }

            acc.push({
              label: row.label,
              grossPremiumValue: row.grossPremiumValue,
              earnedPremiumValue: row.earnedPremiumValue,
              claimsValue: row.claimsValue,
              ratioValue: row.ratioValue,
              lossRatioValue: row.lossRatioValue,
            })
            return acc
          }, [])

        if (othersSeed.grossPremiumValue > 0 || othersSeed.earnedPremiumValue > 0 || othersSeed.claimsValue > 0) {
          othersSeed.ratioValue =
            othersSeed.grossPremiumValue > 0
              ? (othersSeed.claimsValue / othersSeed.grossPremiumValue) * 100
              : 0
          othersSeed.lossRatioValue =
            othersZopperEarnedPremiumValue > 0
              ? Math.min(
                FULLSCREEN_COMPARE_MATRIX_LOSS_RATIO_CAP,
                Math.max(0, (othersNetClaimsValue / othersZopperEarnedPremiumValue) * 100)
              )
              : 0
          nextRows.push(othersSeed)
        }

        othersBreakdownRows.sort((a, b) => b.grossPremiumValue - a.grossPremiumValue)
        nextRows.sort((a, b) => {
          if (a.label === "Others") return 1
          if (b.label === "Others") return -1
          return b.grossPremiumValue - a.grossPremiumValue
        })

        setFullscreenCompareMatrixState({
          loading: false,
          error: null,
          rows: nextRows,
          othersBreakdownRows,
        })
      })
      .catch((err) => {
        if (!active || controller.signal.aborted) return
        const message = err instanceof Error ? err.message : "Unable to load comparison matrix."
        setFullscreenCompareMatrixState({
          loading: false,
          error: message,
          rows: [],
          othersBreakdownRows: [],
        })
      })

    return () => {
      active = false
      controller.abort()
    }
  }, [
    fullscreen,
    isFullscreenCompareMode,
    source,
    jobId,
    fullscreenFromDate,
    fullscreenToDate,
    fromDate,
    toDate,
    activeRegionalMapFilters,
  ])

  const fullscreenCompareMatrixRows = fullscreenCompareMatrixState.rows
  const fullscreenCompareMatrixOthersBreakdownRows = fullscreenCompareMatrixState.othersBreakdownRows
  const hasFullscreenCompareMatrixOthersBreakdown = fullscreenCompareMatrixOthersBreakdownRows.length > 0

  const fullscreenCompareMatrixMaxima = useMemo(() => ({
    grossPremiumValue: fullscreenCompareMatrixRows.reduce((max, row) => Math.max(max, row.grossPremiumValue), 0),
    earnedPremiumValue: fullscreenCompareMatrixRows.reduce((max, row) => Math.max(max, row.earnedPremiumValue), 0),
    claimsValue: fullscreenCompareMatrixRows.reduce((max, row) => Math.max(max, row.claimsValue), 0),
    ratioValue: fullscreenCompareMatrixRows.reduce((max, row) => Math.max(max, row.ratioValue), 0),
    lossRatioValue: fullscreenCompareMatrixRows.reduce((max, row) => Math.max(max, row.lossRatioValue), 0),
  }), [fullscreenCompareMatrixRows])

  useEffect(() => {
    if (!INSIGHTS_ENABLED) return
    if (!fullscreen || !activeOpenedGraphData) return
    if (!activeOpenedGraphData.rows.length) return
    if (!activeOpenedGraphData.measure || !activeOpenedGraphData.dimensionKey) return

    const insightsRows = buildInsightsRows(activeOpenedGraphData)
    if (!insightsRows.length) return

    const insightsRequestKey = JSON.stringify({
      source,
      datasetType,
      jobId: jobId || "",
      fromDate: fromDate || "",
      toDate: toDate || "",
      dimension: fullscreen.dimension,
      metric: fullscreen.metric,
      bucket: fullscreen.bucket || "",
      compareMode: activeOpenedGraphData.compareMode,
      rows: insightsRows,
    })

    if (lastInsightsKeyRef.current === insightsRequestKey) return

    let active = true
    const timer = setTimeout(() => {
      if (!active) return
      setInsightsLoading(true)
      setInsightsError(null)

      fetchGraphInsights({
        source,
        dataset_type: datasetType,
        dimension: fullscreen.dimension,
        metric: fullscreen.metric,
        bucket: fullscreen.bucket,
        job_id: jobId || undefined,
        from_date: fromDate || undefined,
        to_date: toDate || undefined,
        compare_mode: activeOpenedGraphData.compareMode,
        rows: insightsRows,
      })
        .then((res) => {
          if (!active) return
          // Only lock the request key after a response so temporary failures
          // (e.g., backend toggles, network) can be retried.
          lastInsightsKeyRef.current = insightsRequestKey
          setInsights(Array.isArray(res.insights) ? res.insights : [])
          setInsightsModel(res.model || "")
          if (!res.insights?.length) {
            setInsightsError(res.message || "No insights returned for this graph.")
          }
        })
        .catch((err) => {
          if (!active) return
          lastInsightsKeyRef.current = ""
          const rawMessage = err instanceof Error ? err.message : "Failed to generate insights."
          const safeMessage = /invalid token|jwt|unauthorized|forbidden|not authenticated|authentication required/i.test(rawMessage)
            ? "Insights are unavailable for this session right now."
            : rawMessage
          setInsights([])
          setInsightsModel("")
          setInsightsError(safeMessage)
        })
        .finally(() => {
          if (!active) return
          setInsightsLoading(false)
        })
    }, 0)

    return () => {
      active = false
      clearTimeout(timer)
    }
  }, [source, datasetType, jobId, fromDate, toDate, fullscreen, activeOpenedGraphData])

  useEffect(() => {
    if (!fullscreen || !isStateFullscreen || !activeSelectedState) return
    if (!isBreakdownMetricSupported) return

    let active = true
    const controller = new AbortController()
    const timer = setTimeout(() => {
      if (!active) return
      setCityBreakdownLoading(true)
      setCityBreakdownError(null)
      setCityBreakdownRows([])

      fetchCityBreakdownByState({
        state: activeSelectedState,
        metric: fullscreen.metric,
        source,
        dataset_type: datasetType,
        job_id: jobId || undefined,
        from_date: fullscreenFromDate || fromDate || undefined,
        to_date: fullscreenToDate || toDate || undefined,
        limit: 150,
        ...toRegionalAnalyticsFilterParams(activeRegionalMapFilters),
      }, { signal: controller.signal })
        .then((res) => {
          if (!active) return

          const rows = Array.isArray(res.rows) ? res.rows : []
          const normalizedRows = rows
            .map((row) => ({
              city: String(row.city || "").trim(),
              value: Math.max(0, asNumber(row.value)),
            }))
            .filter((row) => row.city)
            .sort((a, b) => b.value - a.value)

          const total = normalizedRows.reduce((sum, row) => sum + row.value, 0)
          const next = normalizedRows.map((row) => ({
            ...row,
            share: total > 0 ? (row.value / total) * 100 : 0,
          }))

          setCityBreakdownRows(next)
          if (!next.length) {
            setCityBreakdownError(
              res.message || `No city-level data is available for ${activeSelectedState}.`
            )
          }
        })
        .catch((err) => {
          if (!active) return
          const message =
            err instanceof Error
              ? err.message
              : `Unable to load city breakdown for ${activeSelectedState}.`
          setCityBreakdownRows([])
          setCityBreakdownError(message)
        })
        .finally(() => {
          if (!active) return
          setCityBreakdownLoading(false)
        })
    }, 0)

    return () => {
      active = false
      controller.abort()
      clearTimeout(timer)
    }
  }, [
    fullscreen,
    isStateFullscreen,
    activeSelectedState,
    source,
    datasetType,
    jobId,
    fullscreenFromDate,
    fullscreenToDate,
    fromDate,
    toDate,
    isBreakdownMetricSupported,
    activeRegionalMapFilters,
  ])

  useEffect(() => {
    if (!fullscreen || !isStateFullscreen) return
    if (!isBreakdownMetricSupported) return

    let active = true
    const controller = new AbortController()
    const timer = setTimeout(() => {
      if (!active) return
      if (!activeComparisonStates.length) {
        setStateComparisonMixRows([])
        setStateComparisonMixError(null)
        setStateComparisonMixLoading(false)
        return
      }
      setStateComparisonMixLoading(true)
      setStateComparisonMixError(null)
      setStateComparisonMixRows([])

      const baseParams = {
        source,
        dataset_type: datasetType,
        metric: regionalAnalysisMetric,
        job_id: jobId || undefined,
        from_date: fullscreenFromDate || fromDate || undefined,
        to_date: fullscreenToDate || toDate || undefined,
        limit: 200,
      } as const
      const regionalFilterParams = toRegionalAnalyticsFilterParams(activeRegionalMapFilters)

      Promise.all(
        activeComparisonStates.map(async (stateLabel) => {
          const metricValue = asNumber(stateMetricMap.get(stateLabel))
          try {
            const [planRes, deviceRes] = await Promise.all([
              fetchCategoryPercentage({
                ...baseParams,
                dimension: activeRegionalPrimaryDescriptor.dimension,
                state: stateLabel,
                ...regionalFilterParams,
              }, { signal: controller.signal }),
              fetchCategoryPercentage({
                ...baseParams,
                dimension: activeRegionalSecondaryDescriptor.dimension,
                state: stateLabel,
                ...regionalFilterParams,
              }, { signal: controller.signal }),
            ])

            const planRows = normalizeCategoryRows((planRes.rows as CategoryOption[]) || [])
            const deviceRows = normalizeCategoryRows((deviceRes.rows as CategoryOption[]) || [])

            return {
              state: stateLabel,
              metricValue,
              planRows,
              deviceRows,
              planMessage: planRows.length
                ? undefined
                : ((planRes.message as string) || `No ${activeRegionalPrimaryDescriptor.label.toLowerCase()} data for ${stateLabel}.`),
              deviceMessage: deviceRows.length
                ? undefined
                : ((deviceRes.message as string) || `No ${activeRegionalSecondaryDescriptor.label.toLowerCase()} data for ${stateLabel}.`),
            } satisfies StateComparisonMix
          } catch (err) {
            const message =
              err instanceof Error
                ? err.message
                : `Unable to load category mix for ${stateLabel}.`
            return {
              state: stateLabel,
              metricValue,
              planRows: [],
              deviceRows: [],
              planMessage: message,
              deviceMessage: message,
            } satisfies StateComparisonMix
          }
        })
      )
        .then((rows) => {
          if (!active) return
          const sortedRows = [...rows].sort((a, b) => b.metricValue - a.metricValue)
          setStateComparisonMixRows(sortedRows)

          const noDataStates = sortedRows
            .filter((row) => {
              return !row.planRows.length && !row.deviceRows.length
            })
            .map((row) => row.state)
          if (noDataStates.length) {
            setStateComparisonMixError(
              `Category mix unavailable for: ${noDataStates.join(", ")}.`
            )
          }
        })
        .catch((err) => {
          if (!active) return
          const message =
            err instanceof Error
              ? err.message
              : "Unable to load category mix distribution."
          setStateComparisonMixError(message)
          setStateComparisonMixRows([])
        })
        .finally(() => {
          if (!active) return
          setStateComparisonMixLoading(false)
        })
    }, 0)

    return () => {
      active = false
      controller.abort()
      clearTimeout(timer)
    }
  }, [
    fullscreen,
    isStateFullscreen,
    source,
    datasetType,
    jobId,
    fullscreenFromDate,
    fullscreenToDate,
    fromDate,
    toDate,
    activeComparisonStates,
    stateMetricMap,
    isBreakdownMetricSupported,
    activeRegionalPrimaryDescriptor.dimension,
    activeRegionalPrimaryDescriptor.label,
    activeRegionalSecondaryDescriptor.dimension,
    activeRegionalSecondaryDescriptor.label,
    regionalAnalysisMetric,
    activeRegionalMapFilters,
  ])

  const pickerMaxDate = useMemo(() => (
    (resetToDate || toDate || fullscreenToDate || "").trim() || undefined
  ), [resetToDate, toDate, fullscreenToDate])

  const handleApplyFullscreenDateRange = (nextFromRaw: string, nextToRaw: string) => {
    if (!onDateRangeApply) return
    const nextFrom = (nextFromRaw || "").trim()
    const nextTo = (nextToRaw || "").trim()
    if (!nextFrom && !nextTo) return
    const orderedFrom = nextFrom && nextTo && nextFrom > nextTo ? nextTo : nextFrom
    const orderedTo = nextFrom && nextTo && nextFrom > nextTo ? nextFrom : nextTo
    setFullscreenFromDate(orderedFrom)
    setFullscreenToDate(orderedTo)
    onDateRangeApply(orderedFrom, orderedTo)
  }

  const handleResetFullscreenDateRange = () => {
    if (!onDateRangeApply) return
    const hasResetAnchor = Boolean(
      (resetFromDate || fromDate || fullscreenFromDate || "").trim()
      || (resetToDate || toDate || fullscreenToDate || "").trim()
    )
    if (!hasResetAnchor) {
      setFullscreenFromDate("")
      setFullscreenToDate("")
      onDateRangeApply("", "")
      return
    }
    const nextFrom = (resetFromDate || fromDate || fullscreenFromDate || "").trim()
    const nextTo = (resetToDate || toDate || fullscreenToDate || "").trim()
    if (!nextFrom && !nextTo) return
    const orderedFrom = nextFrom && nextTo && nextFrom > nextTo ? nextTo : nextFrom
    const orderedTo = nextFrom && nextTo && nextFrom > nextTo ? nextFrom : nextTo
    setFullscreenFromDate(orderedFrom)
    setFullscreenToDate(orderedTo)
    onDateRangeApply(orderedFrom, orderedTo)
  }

  const handleOpenFullscreen = (item: NonNullable<FullscreenGraph>) => {
    const shouldCollapseFocusFilters = item.dimension.toLowerCase().includes("state")
    setFullscreenFromDate(fromDate || "")
    setFullscreenToDate(toDate || "")
    setFullscreen(item)
    setOpenedGraphData(null)
    setIsFullscreenCompareMode(false)
    resetFullscreenCompareSummaries()
    resetFullscreenCompareMatrix()
    setInsights([])
    setInsightsModel("")
    setInsightsError(null)
    setInsightsLoading(false)
    setSelectedState("")
    setCityBreakdownRows([])
    setCityBreakdownLoading(false)
    setCityBreakdownError(null)
    setActiveCityName("")
    setSelectedComparisonStates([])
    setStateComparisonMixRows([])
    setStateComparisonMixLoading(false)
    setStateComparisonMixError(null)
    clearRegionalMapFilters()
    setIsRegionFilterCardCollapsed(shouldCollapseFocusFilters)
    lastInsightsKeyRef.current = ""
  }

  const handleCloseFullscreen = () => {
    setFullscreen(null)
    setOpenedGraphData(null)
    setIsFullscreenCompareMode(false)
    resetFullscreenCompareSummaries()
    resetFullscreenCompareMatrix()
    setInsights([])
    setInsightsModel("")
    setInsightsError(null)
    setInsightsLoading(false)
    setSelectedState("")
    setCityBreakdownRows([])
    setCityBreakdownLoading(false)
    setCityBreakdownError(null)
    setActiveCityName("")
    setSelectedComparisonStates([])
    setStateComparisonMixRows([])
    setStateComparisonMixLoading(false)
    setStateComparisonMixError(null)
    clearRegionalMapFilters()
    lastInsightsKeyRef.current = ""
  }
  const handleToggleFullscreenCompareMode = () => {
    setIsFullscreenCompareMode((prev) => {
      const next = !prev
      if (!next) {
        resetFullscreenCompareSummaries()
        resetFullscreenCompareMatrix()
      }
      return next
    })
  }

  const handleTraverseGraph = (direction: -1 | 1) => {
    if (!fullscreen || fullscreenGraphIndex < 0) return
    handleTraverseGraphFromIndex(fullscreenGraphIndex, direction)
  }

  const handleTraverseGraphFromIndex = (currentIndex: number, direction: -1 | 1) => {
    const targetIndex = currentIndex + direction
    if (targetIndex < 0 || targetIndex >= navigableGraphs.length) return

    const current = navigableGraphs[currentIndex]
    const target = navigableGraphs[targetIndex]
    if (!target) return

    if (current && current.group !== target.group) {
      const verb = direction > 0 ? "next" : "previous"
      const ok = window.confirm(
        `The ${verb} graph is in "${target.sectionTitle}". Do you want to continue?`
      )
      if (!ok) return
    }

    handleOpenFullscreen({
      metric: target.metric,
      dimension: target.dimension,
      bucket: target.bucket,
      chartType: target.chartType,
      tooltipMetricOverride: target.tooltipMetricOverride,
    })
  }

  const renderSamsungCard = (
    card: SamsungOverviewCard,
    layout: "main" | "small",
    index: number
  ) => {
    return (
      <motion.div
        key={card.id}
        initial={prefersReducedMotion ? false : { opacity: 0, y: 16 }}
        animate={prefersReducedMotion ? undefined : { opacity: 1, y: 0 }}
        transition={prefersReducedMotion ? undefined : { duration: 0.35, ease: "easeOut" }}
        whileHover={prefersReducedMotion ? undefined : { y: -4 }}
        className="smooth-surface content-auto relative overflow-hidden rounded-2xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/60 p-4 shadow-sm transition-shadow hover:shadow-[0_18px_40px_-26px_rgba(15,23,42,0.5)] sm:p-5"
      >
        <div className="pointer-events-none absolute -top-16 right-[-58px] h-32 w-32 rounded-full bg-cyan-100/60 blur-2xl" />
        <div className="relative">
          <div className="mb-3 flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="text-sm font-bold leading-snug text-slate-800 sm:text-base">
                {card.title}
              </div>
              <div className="mt-1 text-[11px] text-slate-500">
                {card.subtitle}
              </div>
            </div>
            <div className="ml-auto flex items-center gap-2">
              <button
                className="rounded-lg border border-slate-200 bg-white p-1.5 text-slate-600 transition-colors hover:border-slate-300 hover:text-slate-900"
                onClick={() => {
                  handleOpenFullscreen({
                    metric: card.metric,
                    dimension: card.dimension,
                    bucket: card.bucket,
                    chartType: card.chartType,
                    tooltipMetricOverride: card.tooltipMetricOverride,
                  })
                }}
              >
                <Maximize2 size={16} />
              </button>
            </div>
          </div>

          <div>
            <GraphView
              source={source}
              dimension={card.dimension}
              metric={card.metric}
              datasetType={datasetType}
              bucket={card.bucket}
              jobId={jobId}
              fromDate={fromDate}
              toDate={toDate}
              chartType={card.chartType}
              tooltipMetricOverride={card.tooltipMetricOverride}
              deferUntilVisible
              fetchDelayMs={layout === "main" ? index * 40 : 80 + index * 60}
              heightClassName={layout === "main" ? "h-[360px] sm:h-[430px]" : "h-[300px] sm:h-[340px]"}
            />
          </div>
        </div>
      </motion.div>
    )
  }

  const renderPartnerSideCard = (
    card: PartnerSideCard,
    keyPrefix: string,
    index: number
  ) => {
    return (
      <motion.div
        key={`${keyPrefix}-${card.id}`}
        initial={prefersReducedMotion ? false : { opacity: 0, y: 16 }}
        animate={prefersReducedMotion ? undefined : { opacity: 1, y: 0 }}
        transition={prefersReducedMotion ? undefined : { duration: 0.35, ease: "easeOut" }}
        whileHover={prefersReducedMotion ? undefined : { y: -4 }}
        className="smooth-surface content-auto relative overflow-hidden rounded-2xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/60 p-3 shadow-sm transition-shadow hover:shadow-[0_18px_40px_-26px_rgba(15,23,42,0.5)] sm:p-4"
      >
        <div className="pointer-events-none absolute -top-16 right-[-58px] h-28 w-28 rounded-full bg-cyan-100/60 blur-2xl" />
        <div className="relative">
          <div className="mb-2 flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="text-sm font-bold leading-snug text-slate-800">
                {card.title}
              </div>
            </div>
            <div className="ml-auto flex items-center gap-2">
              <button
                className="rounded-lg border border-slate-200 bg-white p-1.5 text-slate-600 transition-colors hover:border-slate-300 hover:text-slate-900"
                onClick={() => {
                  handleOpenFullscreen({
                    metric: card.metric,
                    dimension: card.dimension,
                    bucket: card.bucket,
                    chartType: card.chartType,
                    tooltipMetricOverride: card.tooltipMetricOverride,
                  })
                }}
              >
                <Maximize2 size={16} />
              </button>
            </div>
          </div>

          <div>
            <GraphView
              source={source}
              dimension={card.dimension}
              metric={card.metric}
              datasetType={datasetType}
              bucket={card.bucket}
              jobId={jobId}
              fromDate={fromDate}
              toDate={toDate}
              chartType={card.chartType}
              tooltipMetricOverride={card.tooltipMetricOverride}
              deferUntilVisible
              fetchDelayMs={60 + index * 60}
              heightClassName={
                card.chartType === "pie" || card.chartType === "bar"
                  ? "h-[230px] sm:h-[260px]"
                  : "h-[180px] sm:h-[210px]"
              }
            />
          </div>
        </div>
      </motion.div>
    )
  }

  const yearOnYearTitle = datasetType === "sales"
    ? "Financial Year Plan-Wise Quantity"
    : "Financial Year Plan-Wise Claims"

  const yearOnYearSubtitle = datasetType === "sales"
    ? "Financial-year buckets stacked by plan. Hover for quantity, gross, earned, and zopper earned premium."
    : "Financial-year buckets stacked by plan category with year-wise claims comparison."
  const yearOnYearInitialDelay = datasetType === "claims" ? 900 : 0

  const handleOpenYearOnYearFullscreen = () => {
    handleOpenFullscreen({
      metric: datasetType === "sales" ? "quantity" : "claims",
      dimension: "year_on_year",
      chartType: "bar",
      isYearOnYear: true,
    })
  }

  const renderYearOnYearCard = (
    layout: "partner-side" | "samsung-small",
    key: string,
    initialDelayMs: number
  ) => (
    <motion.div
      key={key}
      initial={prefersReducedMotion ? false : { opacity: 0, y: 16 }}
      animate={prefersReducedMotion ? undefined : { opacity: 1, y: 0 }}
      transition={prefersReducedMotion ? undefined : { duration: 0.35, ease: "easeOut" }}
      whileHover={prefersReducedMotion ? undefined : { y: -4 }}
      className={
        layout === "samsung-small"
          ? "smooth-surface content-auto relative overflow-hidden rounded-2xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/60 p-4 shadow-sm transition-shadow hover:shadow-[0_18px_40px_-26px_rgba(15,23,42,0.5)] sm:p-5"
          : "smooth-surface content-auto relative overflow-hidden rounded-2xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/60 p-3 shadow-sm transition-shadow hover:shadow-[0_18px_40px_-26px_rgba(15,23,42,0.5)] sm:p-4"
      }
    >
      <div className={`pointer-events-none absolute -top-16 right-[-58px] rounded-full bg-cyan-100/60 blur-2xl ${layout === "samsung-small" ? "h-32 w-32" : "h-28 w-28"}`} />
      <div className="relative">
        <div className={`flex items-start justify-between gap-3 ${layout === "samsung-small" ? "mb-3" : "mb-2"}`}>
          <div className="min-w-0">
            <div className={`font-bold leading-snug text-slate-800 ${layout === "samsung-small" ? "text-sm sm:text-base" : "text-sm"}`}>
              {yearOnYearTitle}
            </div>
            {layout === "samsung-small" && (
              <div className="mt-1 text-[11px] text-slate-500">
                {yearOnYearSubtitle}
              </div>
            )}
          </div>
          <div className="ml-auto flex items-center gap-2">
            <button
              className="rounded-lg border border-slate-200 bg-white p-1.5 text-slate-600 transition-colors hover:border-slate-300 hover:text-slate-900"
              onClick={handleOpenYearOnYearFullscreen}
            >
              <Maximize2 size={16} />
            </button>
          </div>
        </div>

        <YearOnYearComparisonChart
          source={source}
          datasetType={datasetType}
          jobId={jobId}
          fromDate={fromDate}
          toDate={toDate}
          initialDelayMs={initialDelayMs}
          embedded
          compact
          heightClassName={
            layout === "samsung-small"
              ? "h-[300px] sm:h-[340px]"
              : "h-[230px] sm:h-[260px]"
          }
        />
      </div>
    </motion.div>
  )

  return (
    <>
      {isSamsungOverview ? (
        <div className="space-y-4">
          {samsungOverviewCards
            .filter((card) => card.size === "main")
            .map((card, index) => renderSamsungCard(card, "main", index))}
          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            {renderYearOnYearCard("samsung-small", "samsung-yoy-comparison", yearOnYearInitialDelay)}
            {samsungOverviewCards
            .filter((card) => card.size === "small")
            .map((card, index) => renderSamsungCard(card, "small", index))}
          </div>
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,2fr)_minmax(300px,1fr)] xl:items-start">
          <div className="space-y-3">
            <div className="flex items-center justify-end">
              <button
                type="button"
                onClick={() => setIsPartnerGraphScrollEnabled((current) => !current)}
                className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white/90 px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-600 shadow-sm transition-colors hover:border-slate-300 hover:text-slate-900"
                aria-pressed={isPartnerGraphScrollEnabled}
              >
                <ChevronDown
                  size={14}
                  className={`transition-transform ${isPartnerGraphScrollEnabled ? "-rotate-90" : "rotate-0"}`}
                />
                {isPartnerGraphScrollEnabled ? "Graph Scroll On" : "Graph Scroll Off"}
              </button>
            </div>
            <div
              className={`space-y-2 ${isPartnerGraphScrollEnabled ? "custom-scrollbar max-h-[72vh] overflow-y-auto pr-1 sm:max-h-[78vh] sm:pr-2 xl:max-h-[calc(100vh-2.5rem)]" : ""}`}
            >
            {visibleSectionConfigs.map(({ group, entries }) => {
        const sectionTitle = getSectionTitle(group)
        const entry = entries[0]
        const dimKey = entry?.preset.dimension || "month"
        const mergedMetrics = entry?.mergedMetrics || []
        const sectionData = sectionMergedMap[group] || { loading: false, error: null, rows: [] }
        const isRegionalSection = group === "region"
        const sectionChartMode = isRegionalSection
          ? "line"
          : getSectionMainChartMode(source, dimKey)
        const isRelianceBrandSection =
          source === "reliance" && (dimKey === "article_brand" || dimKey === "brand")
        const sectionRows = isRelianceBrandSection
          ? sectionData.rows.filter((row) => !isUnknownLikeLabel(row[dimKey]))
          : sectionData.rows
        const dominantMetric = mergedMetrics[0] || (datasetType === "sales" ? "gross_premium" : "claims")
        const rankedSectionRows = sectionRows.length
          ? [...sectionRows].sort((a, b) => (
              asNumber(b[dominantMetric]) - asNumber(a[dominantMetric])
            ))
          : []
        const sectionDisplayRows = sectionChartMode === "line"
          ? sectionRows
          : rankedSectionRows.slice(
              0,
              sectionChartMode === "dense_heatmap"
                ? SECTION_HEATMAP_MAX_ROWS
                : SECTION_METRIC_STRIP_MAX_ROWS
            )
        const sectionChartRows = sectionDisplayRows
        const sectionMetricMaxima = mergedMetrics.reduce((acc, metric) => {
          acc[metric] = sectionDisplayRows.reduce(
            (max, row) => Math.max(max, asNumber(row[metric])),
            0
          )
          return acc
        }, {} as Record<string, number>)
        const mainChartTitle = isRegionalSection
          ? `${sectionTitle} Geographic Heatmap`
          : sectionChartMode === "dense_heatmap"
            ? `${sectionTitle} Dense Heatmap`
            : sectionChartMode === "metric_strips"
              ? `${sectionTitle} 1D Heatmaps`
              : `${sectionTitle} Combined Trend`
        const mainChartSubtitle = isRegionalSection
          ? "Click on a legend to highlight the state on map, and click a state on map to highlight it in legends."
          : sectionChartMode === "dense_heatmap"
            ? `Dense heatmap of ${mergedMetrics.map(getMetricLabel).join(", ")} by ${getDimensionLabel(dimKey, source)}. Darker cells indicate stronger contribution within each metric.`
            : sectionChartMode === "metric_strips"
              ? `One-dimensional heatmaps by metric (${mergedMetrics.map(getMetricLabel).join(", ")}). Each strip shows category intensity for that metric.`
              : `Merged view of ${mergedMetrics.map(getMetricLabel).join(", ")} by ${getDimensionLabel(dimKey, source)}.`
        const expandedChartType = isRegionalSection
          ? "india_map"
          : sectionChartMode === "line"
            ? "line"
            : "bar"
        return (
          <div
            key={group}
            className="content-auto relative mb-6 overflow-hidden rounded-[24px] border border-slate-200/80 bg-white shadow-[0_22px_60px_-38px_rgba(15,23,42,0.45)] sm:mb-10 sm:rounded-[28px]"
          >
            <div className="pointer-events-none absolute -top-24 right-[-140px] h-64 w-64 rounded-full bg-cyan-100/70 blur-3xl" />
            <div className="pointer-events-none absolute -bottom-24 left-[-120px] h-56 w-56 rounded-full bg-amber-100/70 blur-3xl" />

            <div className="relative p-3.5 sm:p-5 md:p-6">
              <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
                <div>
                  <h2 className="text-[13px] font-black uppercase tracking-[0.16em] text-slate-700">
                    {sectionTitle}
                  </h2>
                  <p className="mt-1 text-xs text-slate-500">
                    {isRegionalSection
                      ? `State and UT heatmap based on ${getMetricLabel(sideCardMetric)}.`
                      : datasetType === "sales"
                        ? "Combined 4-metric trend: Gross, Earned, Zopper Earned Premium, and Quantity."
                        : "Combined 4-metric trend: Claims, Net Claims, Loss Ratio, and Quantity."}
                  </p>
                </div>
                <div className="rounded-full border border-slate-200 bg-white/90 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">
                  {datasetType === "sales" ? "Sales Lens" : "Claims Lens"}
                </div>
              </div>

              <div className="grid grid-cols-1 gap-4">
                <motion.div
                  initial={prefersReducedMotion ? false : { opacity: 0, y: 16 }}
                  animate={prefersReducedMotion ? undefined : { opacity: 1, y: 0 }}
                  transition={prefersReducedMotion ? undefined : { duration: 0.35, ease: "easeOut" }}
                  className="smooth-surface content-auto relative overflow-hidden rounded-2xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/60 p-4 shadow-sm sm:p-5"
                >
                  <div className="pointer-events-none absolute -top-16 right-[-58px] h-32 w-32 rounded-full bg-cyan-100/60 blur-2xl" />
                  <div className="relative">
                    <div className="mb-3 flex flex-wrap items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className="text-sm font-bold leading-snug text-slate-800 sm:text-base">
                          {mainChartTitle}
                        </div>
                        <div className="mt-1 text-[11px] text-slate-500">
                          {mainChartSubtitle}
                        </div>
                      </div>
                      <div className="ml-auto flex flex-wrap items-center justify-end gap-2">
                        <button
                          className="rounded-lg border border-slate-200 bg-white p-1.5 text-slate-600 transition-colors hover:border-slate-300 hover:text-slate-900"
                          onClick={() => {
                            const expandedMetric = (
                              isRegionalSection
                                ? sideCardMetric
                                : (mergedMetrics[0] || sideCardMetric)
                            ) as NonNullable<FullscreenGraph>["metric"]
                            handleOpenFullscreen({
                              metric: expandedMetric,
                              dimension: isRegionalSection ? "state" : dimKey,
                              bucket: entry?.preset.bucket,
                              chartType: expandedChartType,
                              sectionGroup: group,
                              sectionMode: sectionChartMode,
                              isComposite: !isRegionalSection,
                            })
                          }}
                        >
                          <Maximize2 size={16} />
                        </button>
                      </div>
                    </div>

                    <div className="h-[340px] sm:h-[410px]">
                      {isRegionalSection ? (
                        <GraphView
                          source={source}
                          dimension="state"
                          metric={sideCardMetric}
                          datasetType={datasetType}
                          bucket={entry?.preset.bucket}
                          jobId={jobId}
                          fromDate={fromDate}
                          toDate={toDate}
                          chartType="india_map"
                        deferUntilVisible
                        heightClassName="h-full"
                        />
                      ) : sectionData.loading ? (
                        <div className="flex h-full items-center justify-center text-sm text-slate-500">
                          Loading section view...
                        </div>
                      ) : sectionData.error ? (
                        <div className="flex h-full items-center justify-center text-sm text-slate-500">
                          {sectionData.error}
                        </div>
                      ) : !sectionDisplayRows.length ? (
                        <div className="flex h-full items-center justify-center text-sm text-slate-500">
                          No data available for this section.
                        </div>
                      ) : sectionChartMode === "dense_heatmap" ? (
                        <div className="h-full overflow-auto pr-1">
                          <div className="mb-2 flex items-center justify-between text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                            <span>{`Top ${sectionDisplayRows.length} ${getDimensionLabel(dimKey, source)}`}</span>
                            <span>Darker = Higher Within Metric</span>
                          </div>
                          <table className="w-full min-w-[620px] border-separate border-spacing-y-1.5 text-[11px]">
                            <thead>
                              <tr>
                                <th className="px-2 py-1 text-left font-bold uppercase tracking-[0.12em] text-slate-500">
                                  {getDimensionLabel(dimKey, source)}
                                </th>
                                {mergedMetrics.map((metric) => (
                                  <th
                                    key={`${group}-heat-head-${metric}`}
                                    className="px-2 py-1 text-right font-bold uppercase tracking-[0.12em] text-slate-500"
                                  >
                                    {getMetricLabel(metric)}
                                  </th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {sectionDisplayRows.map((row, rowIndex) => {
                                const label = String(row[dimKey] ?? "Unknown").trim() || "Unknown"
                                return (
                                  <tr key={`${group}-heat-${label}-${rowIndex}`}>
                                    <td className="rounded-md bg-slate-100/70 px-2 py-2 font-semibold text-slate-700">
                                      <span title={label}>{truncateCategoryLabel(label, 30)}</span>
                                    </td>
                                    {mergedMetrics.map((metric) => {
                                      const value = asNumber(row[metric])
                                      const maxValue = sectionMetricMaxima[metric] || 0
                                      const intensity = maxValue > 0 ? value / maxValue : 0
                                      return (
                                        <td
                                          key={`${group}-heat-${label}-${metric}`}
                                          className="rounded-md px-2 py-2 text-right font-semibold text-slate-800"
                                          style={{
                                            backgroundColor: getHeatCellColor(metric, intensity),
                                          }}
                                          title={`${getMetricLabel(metric)}: ${formatMetricValue(value, metric)}`}
                                        >
                                          {formatMetricValue(value, metric)}
                                        </td>
                                      )
                                    })}
                                  </tr>
                                )
                              })}
                            </tbody>
                          </table>
                        </div>
                      ) : sectionChartMode === "metric_strips" ? (
                        <div className="h-full overflow-auto pr-1">
                          <div className="mb-2 flex items-center justify-between text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                            <span>{`Top ${sectionDisplayRows.length} ${getDimensionLabel(dimKey, source)}`}</span>
                            <span>Darker = Higher Within Metric</span>
                          </div>
                          <div className="space-y-2.5">
                            {mergedMetrics.map((metric) => {
                              const metricMax = sectionMetricMaxima[metric] || 0
                              return (
                                <div key={`${group}-strip-${metric}`} className="rounded-lg border border-slate-200/80 bg-white/80 p-2">
                                  <div className="mb-1 flex items-center justify-between text-[11px] font-semibold text-slate-700">
                                    <span>{getMetricLabel(metric)}</span>
                                    <span className="text-[10px] text-slate-500">
                                      Peak: {formatMetricValue(metricMax, metric)}
                                    </span>
                                  </div>
                                  <div className="flex gap-1 overflow-x-auto pb-1">
                                    {sectionDisplayRows.map((row, rowIndex) => {
                                      const label = String(row[dimKey] ?? "Unknown").trim() || "Unknown"
                                      const value = asNumber(row[metric])
                                      const intensity = metricMax > 0 ? value / metricMax : 0
                                      return (
                                        <div
                                          key={`${group}-strip-${metric}-${label}-${rowIndex}`}
                                          className="min-w-[112px] rounded-md px-2 py-1.5 text-slate-800"
                                          style={{ backgroundColor: getHeatCellColor(metric, intensity) }}
                                          title={`${label} | ${getMetricLabel(metric)}: ${formatMetricValue(value, metric)}`}
                                        >
                                          <div className="truncate text-[10px] font-semibold">
                                            {truncateCategoryLabel(label, 18)}
                                          </div>
                                          <div className="mt-0.5 text-[10px] font-medium text-slate-700">
                                            {formatAxisCompact(value, metric)}
                                          </div>
                                        </div>
                                      )
                                    })}
                                  </div>
                                </div>
                              )
                            })}
                          </div>
                        </div>
                      ) : (
                        <ResponsiveContainer width="100%" height="100%">
                          <AreaChart
                            data={sectionChartRows}
                            margin={
                              isMobileViewport
                                ? { top: 8, right: 6, left: 2, bottom: 4 }
                                : { top: 12, right: 12, left: 18, bottom: 8 }
                            }
                          >
                            <defs>
                              {mergedMetrics.map((metric) => {
                                const color = METRIC_LINE_COLORS[metric] || "#2563eb"
                                const gid = `section-${group}-${metric}`
                                return (
                                  <linearGradient key={gid} id={gid} x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="0%" stopColor={color} stopOpacity={0.35} />
                                    <stop offset="100%" stopColor={color} stopOpacity={0.02} />
                                  </linearGradient>
                                )
                              })}
                            </defs>
                            <CartesianGrid strokeDasharray="4 4" vertical={false} stroke="#e2e8f0" />
                            <XAxis
                              dataKey={dimKey}
                              tick={{ fontSize: isMobileViewport ? 10 : 11 }}
                              minTickGap={isMobileViewport ? 8 : 12}
                              tickFormatter={(value) =>
                                isTemporalDimensionKey(dimKey)
                                  ? formatMonthLabel(String(value || ""), dimKey)
                                  : String(value || "")
                              }
                            />
                            <YAxis
                              scale="auto"
                              domain={[0, "auto"]}
                              width={isMobileViewport ? 52 : 76}
                              tickMargin={isMobileViewport ? 4 : 6}
                              tick={{ fontSize: isMobileViewport ? 10 : 11 }}
                              tickFormatter={(value) => formatAxisCompact(asNumber(value), mergedMetrics[0] || "quantity")}
                            />
                            <RechartsTooltip
                              allowEscapeViewBox={{ x: false, y: false }}
                              wrapperStyle={{
                                maxWidth: isMobileViewport ? "calc(100vw - 40px)" : "320px",
                                zIndex: 30,
                              }}
                              content={({ active, payload, label }) => {
                                if (!active || !payload?.length) return null
                                return (
                                  <div className={`rounded-lg border bg-white shadow ${isMobileViewport ? "p-2.5" : "p-3"}`}>
                                    <p className={`${isMobileViewport ? "text-[11px]" : "text-xs"} font-bold text-gray-400`}>
                                      {isTemporalDimensionKey(dimKey)
                                        ? formatMonthLabel(String(label || ""), dimKey)
                                        : String(label || "")}
                                    </p>
                                    <div className={`${isMobileViewport ? "mt-1 space-y-0.5" : "mt-1 space-y-1"}`}>
                                      {payload.map((entry) => {
                                        const rawDataKey = String(entry.dataKey || "")
                                        const metric = toOriginalMetricKey(rawDataKey)
                                        const rawValue = asNumber(entry.value)
                                        return (
                                          <div
                                            key={rawDataKey}
                                            className={`flex items-center gap-2 font-semibold ${isMobileViewport ? "text-[11px]" : "text-sm"}`}
                                          >
                                            <span
                                              className="inline-block h-2.5 w-2.5 rounded-full"
                                              style={{ backgroundColor: entry.color || "#64748b" }}
                                            />
                                            <span className="text-slate-700">
                                              {getMetricLabel(metric)}
                                            </span>
                                            <span className="ml-auto text-slate-900">
                                              {formatMetricValue(rawValue, metric)}
                                            </span>
                                          </div>
                                        )
                                      })}
                                    </div>
                                  </div>
                                )
                              }}
                            />
                            <Legend
                              verticalAlign="top"
                              align="left"
                              wrapperStyle={{ fontSize: "11px", paddingBottom: "8px", color: "#475569" }}
                              formatter={(value) => getMetricLabel(String(value || ""))}
                            />
                            {mergedMetrics.map((metric) => {
                              const color = METRIC_LINE_COLORS[metric] || "#2563eb"
                              const gid = `section-${group}-${metric}`
                              return (
                                <Area
                                  key={`${group}-${metric}`}
                                  type="monotone"
                                  dataKey={metric}
                                  name={metric}
                                  stroke={color}
                                  fill={`url(#${gid})`}
                                  strokeWidth={2.2}
                                  fillOpacity={1}
                                  isAnimationActive={!prefersReducedMotion}
                                  dot={false}
                                />
                              )
                            })}
                          </AreaChart>
                        </ResponsiveContainer>
                      )}
                    </div>
                  </div>
                </motion.div>
              </div>
            </div>
          </div>
        )
      })}
            </div>
          </div>
          <div className="xl:sticky xl:top-4">
            <div className="grid grid-cols-1 gap-3">
              {renderYearOnYearCard("partner-side", `${source}-${datasetType}-yoy-comparison`, yearOnYearInitialDelay)}
              {partnerSideCards.map((card, index) => renderPartnerSideCard(card, "frozen-side-rail", index))}
            </div>
          </div>
        </div>
      )}

      {/* FULLSCREEN */}
      <AnimatePresence>
        {fullscreen && (
          <motion.div className="fixed inset-0 z-50 bg-slate-950/35 p-1.5 sm:p-2 md:p-4">
            <div className="h-full w-full overflow-hidden rounded-[20px] border border-slate-200 bg-gradient-to-b from-slate-50 via-white to-slate-100 shadow-2xl sm:rounded-[28px]">
              <div className="h-full w-full overflow-auto">
                <div className="sticky top-0 z-20 border-b border-slate-200/80 bg-white/92 px-3 py-2 backdrop-blur sm:px-4 sm:py-2.5 md:px-5">
                  <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                  <div>
                    <div className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-400">
                      Expanded View
                    </div>
                    <div className="text-sm font-semibold leading-tight text-slate-800">
                      {isYearOnYearFullscreen
                        ? yearOnYearTitle
                        : getGraphTitle(fullscreen.metric, fullscreen.dimension, source)}
                    </div>
                    {!isYearOnYearFullscreen && currentNavigableGraph && (
                      <div className="mt-1 text-[11px] text-slate-500">
                        {currentNavigableGraph.sectionTitle} | Graph {fullscreenGraphIndex + 1} of {navigableGraphs.length}
                      </div>
                    )}
                  </div>
                  <div className="flex w-full flex-wrap items-center gap-2 sm:w-auto sm:justify-end">
                    <button
                      type="button"
                      onClick={handleToggleFullscreenCompareMode}
                      className={`inline-flex items-center gap-2 rounded-full border px-2.5 py-1.5 text-[10px] font-black uppercase tracking-[0.12em] transition-colors sm:text-[11px] ${
                        isFullscreenCompareMode
                          ? "border-cyan-300 bg-cyan-50 text-cyan-800"
                          : "border-slate-300 bg-white text-slate-600 hover:bg-slate-50"
                      }`}
                      aria-pressed={isFullscreenCompareMode}
                    >
                      <span
                        className={`relative inline-flex h-[18px] w-8 items-center rounded-full transition-colors ${
                          isFullscreenCompareMode ? "bg-cyan-500" : "bg-slate-300"
                        }`}
                      >
                        <span
                          className={`inline-block h-[14px] w-[14px] rounded-full bg-white shadow transition-transform ${
                            isFullscreenCompareMode ? "translate-x-[17px]" : "translate-x-0.5"
                          }`}
                        />
                      </span>
                      {isFullscreenCompareMode ? "Comparison Mode" : "Normal Mode"}
                    </button>
                    <button
                      className="rounded-lg border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
                      onClick={() => handleTraverseGraph(-1)}
                      disabled={!canGoToPreviousGraph}
                    >
                      <span className="sm:hidden">Prev</span>
                      <span className="hidden sm:inline">Previous Graph</span>
                    </button>
                    <button
                      className="rounded-lg border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
                      onClick={() => handleTraverseGraph(1)}
                      disabled={!canGoToNextGraph}
                    >
                      <span className="sm:hidden">Next</span>
                      <span className="hidden sm:inline">Next Graph</span>
                    </button>
                    <button
                      className="ml-auto rounded-lg border border-slate-300 bg-white px-2.5 py-1.5 text-sm font-semibold text-slate-700 hover:bg-slate-50 sm:ml-0"
                      onClick={handleCloseFullscreen}
                    >
                      Close
                    </button>
                  </div>
                  </div>
                </div>

                <div className="sticky top-[96px] z-30 border-b border-slate-200/80 bg-white/92 px-3 py-2 backdrop-blur sm:top-[64px] sm:px-4 md:px-5">
                  <div className="flex items-center justify-between gap-3">
                    <div className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-400">
                      Date Window
                    </div>
                    {isStateFullscreen && (
                      <button
                        type="button"
                        onClick={() => setIsRegionFilterCardCollapsed((prev) => !prev)}
                        className="inline-flex items-center gap-1.5 rounded-full border border-slate-200 bg-white px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.12em] text-slate-600 hover:bg-slate-50"
                        aria-expanded={!isRegionFilterCardCollapsed}
                        aria-label={isRegionFilterCardCollapsed ? "Expand region filters" : "Collapse region filters"}
                      >
                        <ChevronDown
                          size={13}
                          className={`transition-transform ${isRegionFilterCardCollapsed ? "-rotate-90" : ""}`}
                        />
                        {isRegionFilterCardCollapsed ? "Expand Filters" : "Collapse Filters"}
                      </button>
                    )}
                  </div>
                  {(!isStateFullscreen || !isRegionFilterCardCollapsed) && (
                    <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-5">
                      <div className="w-full max-w-[360px] xl:max-w-none">
                        <DateRangePicker
                          draftFromDate={fullscreenFromDate}
                          draftToDate={fullscreenToDate}
                          minDate={resetFromDate || fromDate || undefined}
                          maxDate={pickerMaxDate}
                          compact
                          align="left"
                          onDraftChange={(from, to) => {
                            setFullscreenFromDate(from)
                            setFullscreenToDate(to)
                          }}
                          onApply={handleApplyFullscreenDateRange}
                          onReset={handleResetFullscreenDateRange}
                        />
                      </div>

                      {isStateFullscreen && (
                        <>
                          <div className="rounded-xl border border-slate-200 bg-white px-3 py-2">
                            <div className="mb-1 text-[9px] font-black uppercase tracking-[0.16em] text-slate-400">
                              {`Focus ${geographyLabel}`}
                            </div>
                            <select
                              value={activeSelectedState}
                              onChange={(e) => {
                                setSelectedState(e.target.value)
                                setActiveCityName("")
                              }}
                              className="w-full rounded-lg border border-slate-200 px-2 py-1 text-xs font-semibold text-slate-700"
                            >
                              <option value="">{`Choose ${geographyLabel}`}</option>
                              {stateOptions.map((stateOption) => (
                                <option key={stateOption} value={stateOption}>
                                  {stateOption}
                                </option>
                              ))}
                            </select>
                          </div>

                          {showRegionalPrimaryFilter && (
                            <div className="rounded-xl border border-slate-200 bg-white px-3 py-2">
                              <div className="mb-1 text-[9px] font-black uppercase tracking-[0.16em] text-slate-400">
                                {regionalMapFilterConfig.primary.label}
                              </div>
                              <select
                                value={regionalMapPrimaryValue}
                                onChange={(e) => setRegionalMapPrimaryValue(e.target.value)}
                                className="w-full rounded-lg border border-slate-200 px-2 py-1 text-xs font-semibold text-slate-700"
                              >
                                <option value="">{regionalMapFilterConfig.primary.allLabel}</option>
                                {regionalMapPrimaryOptions.map((option) => (
                                  <option
                                    key={`regional-map-primary-value-${option}`}
                                    value={option}
                                  >
                                    {option}
                                  </option>
                                ))}
                              </select>
                            </div>
                          )}

                          {showRegionalSecondaryFilter && (
                            <div className="rounded-xl border border-slate-200 bg-white px-3 py-2">
                              <div className="mb-1 text-[9px] font-black uppercase tracking-[0.16em] text-slate-400">
                                {regionalMapFilterConfig.secondary.label}
                              </div>
                              <select
                                value={regionalMapSecondaryValue}
                                onChange={(e) => setRegionalMapSecondaryValue(e.target.value)}
                                className="w-full rounded-lg border border-slate-200 px-2 py-1 text-xs font-semibold text-slate-700"
                              >
                                <option value="">{regionalMapFilterConfig.secondary.allLabel}</option>
                                {regionalMapSecondaryOptions.map((option) => (
                                  <option
                                    key={`regional-map-secondary-value-${option}`}
                                    value={option}
                                  >
                                    {option}
                                  </option>
                                ))}
                              </select>
                            </div>
                          )}

                          <div className="rounded-xl border border-slate-200 bg-white px-3 py-2">
                            <div className="mb-1 text-[9px] font-black uppercase tracking-[0.16em] text-slate-400">
                              {`Compare ${geographyLabelPlural}`}
                            </div>
                            <div className="flex items-center gap-2">
                              <select
                                value=""
                                onChange={(event) => {
                                  const picked = event.target.value
                                  if (!picked) return
                                  setSelectedComparisonStates((prev) => (
                                    prev.includes(picked)
                                      ? prev.filter((state) => state !== picked)
                                      : [...prev, picked]
                                  ))
                                }}
                                className="min-w-0 flex-1 rounded-lg border border-slate-200 px-2 py-1 text-xs font-semibold text-slate-700"
                              >
                                <option value="">
                                  {`Select ${geographyLabel} (${activeComparisonStates.length})`}
                                </option>
                                {stateOptions.map((stateLabel) => (
                                  <option key={`state-compare-option-${stateLabel}`} value={stateLabel}>
                                    {activeComparisonStates.includes(stateLabel)
                                      ? `${stateLabel} (Selected)`
                                      : stateLabel}
                                  </option>
                                ))}
                              </select>
                              {!!activeComparisonStates.length && (
                                <button
                                  className="rounded-lg border border-slate-200 px-2 py-1 text-[10px] font-semibold text-slate-500 hover:bg-slate-50 hover:text-slate-700"
                                  onClick={() => setSelectedComparisonStates([])}
                                >
                                  Clear
                                </button>
                              )}
                            </div>
                          </div>

                          {regionalMapFiltersLoading && (
                            <div className="text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-400 sm:col-span-2 xl:col-span-5">
                              Loading Filters...
                            </div>
                          )}

                          {!!activeComparisonStates.length && (
                            <div className="flex flex-wrap items-center gap-1.5 sm:col-span-2 xl:col-span-5">
                              {activeComparisonStates.map((stateLabel) => (
                                <button
                                  key={`selected-compare-state-${stateLabel}`}
                                  className="rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[10px] font-semibold text-slate-600 hover:bg-slate-100"
                                  onClick={() => setSelectedComparisonStates((prev) => prev.filter((state) => state !== stateLabel))}
                                  title={`Remove ${stateLabel}`}
                                >
                                  {`${stateLabel} x`}
                                </button>
                              ))}
                            </div>
                          )}
                        </>
                      )}
                    </div>
                  )}
                </div>

              <div
                className={`flex min-h-[70vh] justify-center px-3 py-4 sm:min-h-[74vh] sm:px-6 sm:py-6 ${
                  isStateFullscreen ? "items-start" : "items-center"
                }`}
              >
                <div className="w-full max-w-6xl">
                  {isFullscreenCompareMode ? (
                    <div className="space-y-6">
                      <div className="rounded-3xl border border-slate-200/80 bg-white/90 p-4 shadow-[0_18px_40px_-28px_rgba(15,23,42,0.45)] sm:p-5">
                        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                          <div>
                            <h4 className="text-xs font-black uppercase tracking-[0.16em] text-slate-500">
                              Comparative Metrics
                            </h4>
                            <div className="mt-1 text-[11px] text-slate-500">
                              Compare sales and claims metrics together for this focused date range.
                            </div>
                          </div>
                          <div className="rounded-full border border-cyan-200 bg-cyan-50 px-3 py-1 text-[10px] font-black uppercase tracking-[0.14em] text-cyan-700">
                            Comparison Mode
                          </div>
                        </div>
                        {fullscreenCompareSummaries.error ? (
                          <div className="text-sm text-rose-600">
                            {fullscreenCompareSummaries.error}
                          </div>
                        ) : (
                          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
                            {fullscreenCompareMetricCards.map((card) => (
                              <div
                                key={`fullscreen-compare-card-${card.key}`}
                                className="rounded-2xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50 to-slate-100 p-3 shadow-sm"
                              >
                                <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.14em] text-slate-400">
                                  <span
                                    className="inline-block h-2.5 w-2.5 rounded-full"
                                    style={{ backgroundColor: METRIC_LINE_COLORS[card.metric] || "#2563eb" }}
                                  />
                                  {card.family}
                                </div>
                                <div className="mt-2 text-[11px] font-semibold text-slate-500">
                                  {card.label}
                                </div>
                                <div className="mt-1 text-base font-black text-slate-800 sm:text-lg">
                                  {fullscreenCompareSummaries.loading
                                    ? "Loading..."
                                    : card.value == null
                                      ? "N/A"
                                      : formatMetricValue(asNumber(card.value), card.metric)}
                                </div>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>

                      {isYearOnYearFullscreen ? (
                        <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
                          <div className="rounded-3xl border border-slate-200/80 bg-white/90 p-4 shadow-[0_18px_40px_-28px_rgba(15,23,42,0.45)] sm:p-5">
                            <div className="mb-3">
                              <div className="text-xs font-black uppercase tracking-[0.16em] text-slate-500">
                                Sales Lens
                              </div>
                              <div className="mt-1 text-[11px] text-slate-500">
                                Financial-year comparison for sales metrics.
                              </div>
                            </div>
                            <YearOnYearComparisonChart
                              source={source}
                              datasetType="sales"
                              jobId={jobId}
                              fromDate={fullscreenFromDate || fromDate}
                              toDate={fullscreenToDate || toDate}
                              initialDelayMs={0}
                              heightClassName="h-[48vh] min-h-[360px]"
                            />
                          </div>
                          <div className="rounded-3xl border border-slate-200/80 bg-white/90 p-4 shadow-[0_18px_40px_-28px_rgba(15,23,42,0.45)] sm:p-5">
                            <div className="mb-3">
                              <div className="text-xs font-black uppercase tracking-[0.16em] text-slate-500">
                                Claims Lens
                              </div>
                              <div className="mt-1 text-[11px] text-slate-500">
                                Financial-year comparison for claims metrics.
                              </div>
                            </div>
                            <YearOnYearComparisonChart
                              source={source}
                              datasetType="claims"
                              jobId={jobId}
                              fromDate={fullscreenFromDate || fromDate}
                              toDate={fullscreenToDate || toDate}
                              initialDelayMs={0}
                              heightClassName="h-[48vh] min-h-[360px]"
                            />
                          </div>
                        </div>
                      ) : (
                        <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
                          <div className="rounded-3xl border border-slate-200/80 bg-white/90 p-4 shadow-[0_18px_40px_-28px_rgba(15,23,42,0.45)] sm:p-5">
                            <div className="mb-3">
                              <div className="text-xs font-black uppercase tracking-[0.16em] text-slate-500">
                                Sales Lens
                              </div>
                              <div className="mt-1 text-[11px] text-slate-500">
                                {`${getMetricLabel(fullscreenCompareSalesMetric)} by ${getDimensionLabel(fullscreen.dimension, source).toLowerCase()}.`}
                              </div>
                            </div>
                            <div className="h-[52vh] min-h-[360px]">
                              <GraphView
                                source={source}
                                dimension={fullscreen.dimension}
                                metric={fullscreenCompareSalesMetric}
                                datasetType="sales"
                                bucket={fullscreen.bucket}
                                jobId={jobId}
                                fromDate={fullscreenFromDate || fromDate}
                                toDate={fullscreenToDate || toDate}
                                chartType={fullscreen.chartType}
                                tooltipMetricOverride={fullscreenCompareSalesMetric}
                                categoryFilters={isStateFullscreen ? activeRegionalMapFilters : undefined}
                                enableCrossDatasetHoverCompare={false}
                                eagerMapHoverPrefetch={false}
                                heightClassName="h-full"
                              />
                            </div>
                          </div>
                          <div className="rounded-3xl border border-slate-200/80 bg-white/90 p-4 shadow-[0_18px_40px_-28px_rgba(15,23,42,0.45)] sm:p-5">
                            <div className="mb-3">
                              <div className="text-xs font-black uppercase tracking-[0.16em] text-slate-500">
                                Claims Lens
                              </div>
                              <div className="mt-1 text-[11px] text-slate-500">
                                {`${getMetricLabel(fullscreenCompareClaimsMetric)} by ${getDimensionLabel(fullscreen.dimension, source).toLowerCase()}.`}
                              </div>
                            </div>
                            <div className="h-[52vh] min-h-[360px]">
                              <GraphView
                                source={source}
                                dimension={fullscreen.dimension}
                                metric={fullscreenCompareClaimsMetric}
                                datasetType="claims"
                                bucket={fullscreen.bucket}
                                jobId={jobId}
                                fromDate={fullscreenFromDate || fromDate}
                                toDate={fullscreenToDate || toDate}
                                chartType={fullscreen.chartType}
                                tooltipMetricOverride={fullscreenCompareClaimsMetric}
                                categoryFilters={isStateFullscreen ? activeRegionalMapFilters : undefined}
                                enableCrossDatasetHoverCompare={false}
                                eagerMapHoverPrefetch={false}
                                heightClassName="h-full"
                              />
                            </div>
                          </div>
                        </div>
                      )}
                      {fullscreen.chartType === "india_map" && (
                        <div className="rounded-3xl border border-slate-200/80 bg-white/95 p-4 shadow-[0_18px_40px_-28px_rgba(15,23,42,0.45)] sm:p-5">
                          <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                            <div>
                              <div className="text-xs font-black uppercase tracking-[0.16em] text-slate-500">
                                Sales Vs Claims Matrix
                              </div>
                              <div className="mt-1 text-[11px] text-slate-500">
                                States below Rs 80 L gross premium are merged into Others. Click Others to expand its state-wise bifurcation. Darker cells indicate stronger contribution within that column.
                              </div>
                            </div>
                            <div className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-[10px] font-black uppercase tracking-[0.14em] text-slate-600">
                              Ratio = Claims / Gross
                            </div>
                          </div>
                          {fullscreenCompareMatrixState.loading ? (
                            <div className="flex min-h-[220px] items-center justify-center text-sm text-slate-500">
                              Loading comparison matrix...
                            </div>
                          ) : fullscreenCompareMatrixState.error ? (
                            <div className="flex min-h-[220px] items-center justify-center text-sm text-rose-600">
                              {fullscreenCompareMatrixState.error}
                            </div>
                          ) : !fullscreenCompareMatrixRows.length ? (
                            <div className="flex min-h-[220px] items-center justify-center text-sm text-slate-500">
                              No regional comparison data available for this selection.
                            </div>
                          ) : (
                            <div className="overflow-auto">
                              <table className="w-full min-w-[1040px] border-separate border-spacing-y-2 text-[11px]">
                                <thead>
                                  <tr>
                                    <th className="px-3 py-2 text-left text-[10px] font-black uppercase tracking-[0.14em] text-slate-500">
                                      {getDimensionLabel(fullscreen.dimension, source)}
                                    </th>
                                    <th className="px-3 py-2 text-right text-[10px] font-black uppercase tracking-[0.14em] text-slate-500">
                                      Gross Premium
                                    </th>
                                    <th className="px-3 py-2 text-right text-[10px] font-black uppercase tracking-[0.14em] text-slate-500">
                                      Earned Premium
                                    </th>
                                    <th className="px-3 py-2 text-right text-[10px] font-black uppercase tracking-[0.14em] text-slate-500">
                                      Claims Cost
                                    </th>
                                    <th className="px-3 py-2 text-right text-[10px] font-black uppercase tracking-[0.14em] text-slate-500">
                                      Claims Vs Sales
                                    </th>
                                    <th className="px-3 py-2 text-right text-[10px] font-black uppercase tracking-[0.14em] text-slate-500">
                                      Loss Ratio
                                    </th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {fullscreenCompareMatrixRows.map((row) => {
                                    const isOthersRow =
                                      row.label === "Others" && hasFullscreenCompareMatrixOthersBreakdown
                                    const isOthersExpanded =
                                      isOthersRow && isFullscreenCompareMatrixOthersExpanded
                                    const grossIntensity = fullscreenCompareMatrixMaxima.grossPremiumValue > 0
                                      ? row.grossPremiumValue / fullscreenCompareMatrixMaxima.grossPremiumValue
                                      : 0
                                    const earnedIntensity = fullscreenCompareMatrixMaxima.earnedPremiumValue > 0
                                      ? row.earnedPremiumValue / fullscreenCompareMatrixMaxima.earnedPremiumValue
                                      : 0
                                    const claimsIntensity = fullscreenCompareMatrixMaxima.claimsValue > 0
                                      ? row.claimsValue / fullscreenCompareMatrixMaxima.claimsValue
                                      : 0
                                    const ratioIntensity = fullscreenCompareMatrixMaxima.ratioValue > 0
                                      ? row.ratioValue / fullscreenCompareMatrixMaxima.ratioValue
                                      : 0
                                    const lossRatioIntensity = fullscreenCompareMatrixMaxima.lossRatioValue > 0
                                      ? row.lossRatioValue / fullscreenCompareMatrixMaxima.lossRatioValue
                                      : 0

                                    const grossCellStyle = {
                                      backgroundColor: `rgba(37, 99, 235, ${0.1 + (grossIntensity * 0.7)})`,
                                      color: grossIntensity > 0.55 ? "#eff6ff" : "#1e3a8a",
                                    }
                                    const earnedCellStyle = {
                                      backgroundColor: `rgba(8, 145, 178, ${0.1 + (earnedIntensity * 0.68)})`,
                                      color: earnedIntensity > 0.55 ? "#ecfeff" : "#155e75",
                                    }
                                    const claimsCellStyle = {
                                      backgroundColor: `rgba(249, 115, 22, ${0.1 + (claimsIntensity * 0.68)})`,
                                      color: claimsIntensity > 0.55 ? "#fff7ed" : "#9a3412",
                                    }
                                    const ratioCellStyle = {
                                      backgroundColor: `rgba(109, 40, 217, ${0.1 + (ratioIntensity * 0.68)})`,
                                      color: ratioIntensity > 0.55 ? "#f5f3ff" : "#5b21b6",
                                    }
                                    const lossRatioCellStyle = {
                                      backgroundColor: `rgba(13, 148, 136, ${0.1 + (lossRatioIntensity * 0.68)})`,
                                      color: lossRatioIntensity > 0.55 ? "#f0fdfa" : "#115e59",
                                    }

                                    const renderedRows = [
                                      <tr
                                        key={`fullscreen-compare-matrix-${row.label}`}
                                        className={isOthersRow ? "cursor-pointer" : undefined}
                                        onClick={isOthersRow ? () => {
                                          setIsFullscreenCompareMatrixOthersExpanded((prev) => !prev)
                                        } : undefined}
                                        onKeyDown={isOthersRow ? (event) => {
                                          if (event.key === "Enter" || event.key === " ") {
                                            event.preventDefault()
                                            setIsFullscreenCompareMatrixOthersExpanded((prev) => !prev)
                                          }
                                        } : undefined}
                                        role={isOthersRow ? "button" : undefined}
                                        tabIndex={isOthersRow ? 0 : undefined}
                                        aria-expanded={isOthersRow ? isOthersExpanded : undefined}
                                        title={isOthersRow ? "Click to expand Others breakdown" : undefined}
                                      >
                                        <td className="rounded-l-2xl border border-r-0 border-slate-200/80 bg-white px-3 py-2.5">
                                          <div className="font-semibold text-slate-800">{row.label}</div>
                                        </td>
                                        <td
                                          className="border border-slate-200/70 px-3 py-2.5 text-right align-middle"
                                          style={grossCellStyle}
                                          title={`${row.label} | Gross Premium: ${formatMetricValue(row.grossPremiumValue, "gross_premium")}`}
                                        >
                                          <div className="text-[10px] font-black uppercase tracking-[0.14em] opacity-75">
                                            Gross
                                          </div>
                                          <div className="mt-1 text-sm font-black">
                                            {formatMetricValue(row.grossPremiumValue, "gross_premium")}
                                          </div>
                                        </td>
                                        <td
                                          className="border border-slate-200/70 px-3 py-2.5 text-right align-middle"
                                          style={earnedCellStyle}
                                          title={`${row.label} | Earned Premium: ${formatMetricValue(row.earnedPremiumValue, "earned_premium")}`}
                                        >
                                          <div className="text-[10px] font-black uppercase tracking-[0.14em] opacity-75">
                                            Earned
                                          </div>
                                          <div className="mt-1 text-sm font-black">
                                            {formatMetricValue(row.earnedPremiumValue, "earned_premium")}
                                          </div>
                                        </td>
                                        <td
                                          className="border border-slate-200/70 px-3 py-2.5 text-right align-middle"
                                          style={claimsCellStyle}
                                          title={`${row.label} | Claims Cost: ${formatMetricValue(row.claimsValue, "claims")}`}
                                        >
                                          <div className="text-[10px] font-black uppercase tracking-[0.14em] opacity-75">
                                            Claims
                                          </div>
                                          <div className="mt-1 text-sm font-black">
                                            {formatMetricValue(row.claimsValue, "claims")}
                                          </div>
                                        </td>
                                        <td
                                          className="border border-slate-200/70 px-3 py-2.5 text-right align-middle"
                                          style={ratioCellStyle}
                                          title={`${row.label} | Claims Vs Sales: ${formatMetricValue(row.ratioValue, "loss_ratio")}`}
                                        >
                                          <div className="text-[10px] font-black uppercase tracking-[0.14em] opacity-75">
                                            Ratio
                                          </div>
                                          <div className="mt-1 text-sm font-black">
                                            {formatMetricValue(row.ratioValue, "loss_ratio")}
                                          </div>
                                        </td>
                                        <td
                                          className="rounded-r-2xl border border-l-0 border-slate-200/70 px-3 py-2.5 text-right align-middle"
                                          style={lossRatioCellStyle}
                                          title={`${row.label} | Loss Ratio: ${formatMetricValue(row.lossRatioValue, "loss_ratio")}`}
                                        >
                                          <div className="text-[10px] font-black uppercase tracking-[0.14em] opacity-75">
                                            Loss Ratio
                                          </div>
                                          <div className="mt-1 text-sm font-black">
                                            {formatMetricValue(row.lossRatioValue, "loss_ratio")}
                                          </div>
                                        </td>
                                      </tr>,
                                    ]

                                    if (isOthersExpanded) {
                                      fullscreenCompareMatrixOthersBreakdownRows.forEach((breakdownRow) => {
                                        const breakdownGrossIntensity = fullscreenCompareMatrixMaxima.grossPremiumValue > 0
                                          ? breakdownRow.grossPremiumValue / fullscreenCompareMatrixMaxima.grossPremiumValue
                                          : 0
                                        const breakdownEarnedIntensity = fullscreenCompareMatrixMaxima.earnedPremiumValue > 0
                                          ? breakdownRow.earnedPremiumValue / fullscreenCompareMatrixMaxima.earnedPremiumValue
                                          : 0
                                        const breakdownClaimsIntensity = fullscreenCompareMatrixMaxima.claimsValue > 0
                                          ? breakdownRow.claimsValue / fullscreenCompareMatrixMaxima.claimsValue
                                          : 0
                                        const breakdownRatioIntensity = fullscreenCompareMatrixMaxima.ratioValue > 0
                                          ? breakdownRow.ratioValue / fullscreenCompareMatrixMaxima.ratioValue
                                          : 0
                                        const breakdownLossRatioIntensity = fullscreenCompareMatrixMaxima.lossRatioValue > 0
                                          ? breakdownRow.lossRatioValue / fullscreenCompareMatrixMaxima.lossRatioValue
                                          : 0

                                        const breakdownGrossCellStyle = {
                                          backgroundColor: `rgba(37, 99, 235, ${0.08 + (breakdownGrossIntensity * 0.55)})`,
                                          color: breakdownGrossIntensity > 0.58 ? "#eff6ff" : "#1e3a8a",
                                        }
                                        const breakdownEarnedCellStyle = {
                                          backgroundColor: `rgba(8, 145, 178, ${0.08 + (breakdownEarnedIntensity * 0.53)})`,
                                          color: breakdownEarnedIntensity > 0.58 ? "#ecfeff" : "#155e75",
                                        }
                                        const breakdownClaimsCellStyle = {
                                          backgroundColor: `rgba(249, 115, 22, ${0.08 + (breakdownClaimsIntensity * 0.53)})`,
                                          color: breakdownClaimsIntensity > 0.58 ? "#fff7ed" : "#9a3412",
                                        }
                                        const breakdownRatioCellStyle = {
                                          backgroundColor: `rgba(109, 40, 217, ${0.08 + (breakdownRatioIntensity * 0.53)})`,
                                          color: breakdownRatioIntensity > 0.58 ? "#f5f3ff" : "#5b21b6",
                                        }
                                        const breakdownLossRatioCellStyle = {
                                          backgroundColor: `rgba(13, 148, 136, ${0.08 + (breakdownLossRatioIntensity * 0.53)})`,
                                          color: breakdownLossRatioIntensity > 0.58 ? "#f0fdfa" : "#115e59",
                                        }

                                        renderedRows.push(
                                          <tr key={`fullscreen-compare-matrix-breakdown-${breakdownRow.label}`}>
                                            <td className="rounded-l-2xl border border-r-0 border-slate-200/70 bg-slate-50/90 px-3 py-2.5">
                                              <div className="pl-4 font-medium text-slate-700">{breakdownRow.label}</div>
                                            </td>
                                            <td
                                              className="border border-slate-200/60 px-3 py-2.5 text-right align-middle"
                                              style={breakdownGrossCellStyle}
                                              title={`${breakdownRow.label} | Gross Premium: ${formatMetricValue(breakdownRow.grossPremiumValue, "gross_premium")}`}
                                            >
                                              <div className="text-[10px] font-black uppercase tracking-[0.14em] opacity-75">
                                                Gross
                                              </div>
                                              <div className="mt-1 text-sm font-black">
                                                {formatMetricValue(breakdownRow.grossPremiumValue, "gross_premium")}
                                              </div>
                                            </td>
                                            <td
                                              className="border border-slate-200/60 px-3 py-2.5 text-right align-middle"
                                              style={breakdownEarnedCellStyle}
                                              title={`${breakdownRow.label} | Earned Premium: ${formatMetricValue(breakdownRow.earnedPremiumValue, "earned_premium")}`}
                                            >
                                              <div className="text-[10px] font-black uppercase tracking-[0.14em] opacity-75">
                                                Earned
                                              </div>
                                              <div className="mt-1 text-sm font-black">
                                                {formatMetricValue(breakdownRow.earnedPremiumValue, "earned_premium")}
                                              </div>
                                            </td>
                                            <td
                                              className="border border-slate-200/60 px-3 py-2.5 text-right align-middle"
                                              style={breakdownClaimsCellStyle}
                                              title={`${breakdownRow.label} | Claims Cost: ${formatMetricValue(breakdownRow.claimsValue, "claims")}`}
                                            >
                                              <div className="text-[10px] font-black uppercase tracking-[0.14em] opacity-75">
                                                Claims
                                              </div>
                                              <div className="mt-1 text-sm font-black">
                                                {formatMetricValue(breakdownRow.claimsValue, "claims")}
                                              </div>
                                            </td>
                                            <td
                                              className="border border-slate-200/60 px-3 py-2.5 text-right align-middle"
                                              style={breakdownRatioCellStyle}
                                              title={`${breakdownRow.label} | Claims Vs Sales: ${formatMetricValue(breakdownRow.ratioValue, "loss_ratio")}`}
                                            >
                                              <div className="text-[10px] font-black uppercase tracking-[0.14em] opacity-75">
                                                Ratio
                                              </div>
                                              <div className="mt-1 text-sm font-black">
                                                {formatMetricValue(breakdownRow.ratioValue, "loss_ratio")}
                                              </div>
                                            </td>
                                            <td
                                              className="rounded-r-2xl border border-l-0 border-slate-200/60 px-3 py-2.5 text-right align-middle"
                                              style={breakdownLossRatioCellStyle}
                                              title={`${breakdownRow.label} | Loss Ratio: ${formatMetricValue(breakdownRow.lossRatioValue, "loss_ratio")}`}
                                            >
                                              <div className="text-[10px] font-black uppercase tracking-[0.14em] opacity-75">
                                                Loss Ratio
                                              </div>
                                              <div className="mt-1 text-sm font-black">
                                                {formatMetricValue(breakdownRow.lossRatioValue, "loss_ratio")}
                                              </div>
                                            </td>
                                          </tr>
                                        )
                                      })
                                    }

                                    return renderedRows
                                  })}
                                </tbody>
                              </table>
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  ) : fullscreenCompositeData ? (
                    <div className={fullscreenGraphHeightClass}>
                      {fullscreenCompositeData.sectionData.loading ? (
                        <div className="flex h-full items-center justify-center text-sm text-slate-500">
                          Loading section view...
                        </div>
                      ) : fullscreenCompositeData.sectionData.error ? (
                        <div className="flex h-full items-center justify-center text-sm text-slate-500">
                          {fullscreenCompositeData.sectionData.error}
                        </div>
                      ) : !fullscreenCompositeData.displayRows.length ? (
                        <div className="flex h-full items-center justify-center text-sm text-slate-500">
                          No data available for this section.
                        </div>
                      ) : fullscreenCompositeData.sectionChartMode === "dense_heatmap" ? (
                        <div className="h-full overflow-auto pr-1">
                          <div className="mb-2 flex items-center justify-between text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                            <span>{`Top ${fullscreenCompositeData.displayRows.length} ${getDimensionLabel(fullscreenCompositeData.dimKey, source)}`}</span>
                            <span>Darker = Higher Within Metric</span>
                          </div>
                          <table className="w-full min-w-[620px] border-separate border-spacing-y-1.5 text-[11px]">
                            <thead>
                              <tr>
                                <th className="px-2 py-1 text-left font-bold uppercase tracking-[0.12em] text-slate-500">
                                  {getDimensionLabel(fullscreenCompositeData.dimKey, source)}
                                </th>
                                {fullscreenCompositeData.mergedMetrics.map((metric) => (
                                  <th
                                    key={`fullscreen-heat-head-${metric}`}
                                    className="px-2 py-1 text-right font-bold uppercase tracking-[0.12em] text-slate-500"
                                  >
                                    {getMetricLabel(metric)}
                                  </th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {fullscreenCompositeData.displayRows.map((row, rowIndex) => {
                                const label = String(row[fullscreenCompositeData.dimKey] ?? "Unknown").trim() || "Unknown"
                                return (
                                  <tr key={`fullscreen-heat-${label}-${rowIndex}`}>
                                    <td className="rounded-md bg-slate-100/70 px-2 py-2 font-semibold text-slate-700">
                                      <span title={label}>{truncateCategoryLabel(label, 30)}</span>
                                    </td>
                                    {fullscreenCompositeData.mergedMetrics.map((metric) => {
                                      const value = asNumber(row[metric])
                                      const maxValue = fullscreenCompositeData.metricMaxima[metric] || 0
                                      const intensity = maxValue > 0 ? value / maxValue : 0
                                      return (
                                        <td
                                          key={`fullscreen-heat-${label}-${metric}`}
                                          className="rounded-md px-2 py-2 text-right font-semibold text-slate-800"
                                          style={{
                                            backgroundColor: getHeatCellColor(metric, intensity),
                                          }}
                                          title={`${getMetricLabel(metric)}: ${formatMetricValue(value, metric)}`}
                                        >
                                          {formatMetricValue(value, metric)}
                                        </td>
                                      )
                                    })}
                                  </tr>
                                )
                              })}
                            </tbody>
                          </table>
                        </div>
                      ) : fullscreenCompositeData.sectionChartMode === "metric_strips" ? (
                        <div className="h-full overflow-auto pr-1">
                          <div className="mb-2 flex items-center justify-between text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                            <span>{`Top ${fullscreenCompositeData.displayRows.length} ${getDimensionLabel(fullscreenCompositeData.dimKey, source)}`}</span>
                            <span>Darker = Higher Within Metric</span>
                          </div>
                          <div className="space-y-2.5">
                            {fullscreenCompositeData.mergedMetrics.map((metric) => {
                              const metricMax = fullscreenCompositeData.metricMaxima[metric] || 0
                              return (
                                <div key={`fullscreen-strip-${metric}`} className="rounded-lg border border-slate-200/80 bg-white/80 p-2">
                                  <div className="mb-1 flex items-center justify-between text-[11px] font-semibold text-slate-700">
                                    <span>{getMetricLabel(metric)}</span>
                                    <span className="text-[10px] text-slate-500">
                                      Peak: {formatMetricValue(metricMax, metric)}
                                    </span>
                                  </div>
                                  <div className="flex gap-1 overflow-x-auto pb-1">
                                    {fullscreenCompositeData.displayRows.map((row, rowIndex) => {
                                      const label = String(row[fullscreenCompositeData.dimKey] ?? "Unknown").trim() || "Unknown"
                                      const value = asNumber(row[metric])
                                      const intensity = metricMax > 0 ? value / metricMax : 0
                                      return (
                                        <div
                                          key={`fullscreen-strip-${metric}-${label}-${rowIndex}`}
                                          className="min-w-[112px] rounded-md px-2 py-1.5 text-slate-800"
                                          style={{ backgroundColor: getHeatCellColor(metric, intensity) }}
                                          title={`${label} | ${getMetricLabel(metric)}: ${formatMetricValue(value, metric)}`}
                                        >
                                          <div className="truncate text-[10px] font-semibold">
                                            {truncateCategoryLabel(label, 18)}
                                          </div>
                                          <div className="mt-0.5 text-[10px] font-medium text-slate-700">
                                            {formatAxisCompact(value, metric)}
                                          </div>
                                        </div>
                                      )
                                    })}
                                  </div>
                                </div>
                              )
                            })}
                          </div>
                        </div>
                      ) : (
                        <ResponsiveContainer width="100%" height="100%">
                          <AreaChart
                            data={fullscreenCompositeChartRows}
                            margin={
                              isMobileViewport
                                ? { top: 8, right: 6, left: 2, bottom: 4 }
                                : { top: 12, right: 12, left: 18, bottom: 8 }
                            }
                          >
                            <defs>
                              {fullscreenCompositeData.mergedMetrics.map((metric) => {
                                const color = METRIC_LINE_COLORS[metric] || "#2563eb"
                                const gid = `fullscreen-section-${fullscreenCompositeData.group}-${metric}`
                                return (
                                  <linearGradient key={gid} id={gid} x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="0%" stopColor={color} stopOpacity={0.35} />
                                    <stop offset="100%" stopColor={color} stopOpacity={0.02} />
                                  </linearGradient>
                                )
                              })}
                            </defs>
                            <CartesianGrid strokeDasharray="4 4" vertical={false} stroke="#e2e8f0" />
                            <XAxis
                              dataKey={fullscreenCompositeData.dimKey}
                              tick={{ fontSize: isMobileViewport ? 10 : 11 }}
                              minTickGap={isMobileViewport ? 8 : 12}
                              tickFormatter={(value) =>
                                isTemporalDimensionKey(fullscreenCompositeData.dimKey)
                                  ? formatMonthLabel(String(value || ""), fullscreenCompositeData.dimKey)
                                  : String(value || "")
                              }
                            />
                            <YAxis
                              scale="auto"
                              domain={[0, "auto"]}
                              width={isMobileViewport ? 52 : 76}
                              tickMargin={isMobileViewport ? 4 : 6}
                              tick={{ fontSize: isMobileViewport ? 10 : 11 }}
                              tickFormatter={(value) => formatAxisCompact(asNumber(value), fullscreenCompositeData.mergedMetrics[0] || "quantity")}
                            />
                            <RechartsTooltip
                              allowEscapeViewBox={{ x: false, y: false }}
                              wrapperStyle={{
                                maxWidth: isMobileViewport ? "calc(100vw - 40px)" : "320px",
                                zIndex: 30,
                              }}
                              content={({ active, payload, label }) => {
                                if (!active || !payload?.length) return null
                                return (
                                  <div className={`rounded-lg border bg-white shadow ${isMobileViewport ? "p-2.5" : "p-3"}`}>
                                    <p className={`${isMobileViewport ? "text-[11px]" : "text-xs"} font-bold text-gray-400`}>
                                      {isTemporalDimensionKey(fullscreenCompositeData.dimKey)
                                        ? formatMonthLabel(String(label || ""), fullscreenCompositeData.dimKey)
                                        : String(label || "")}
                                    </p>
                                    <div className={`${isMobileViewport ? "mt-1 space-y-0.5" : "mt-1 space-y-1"}`}>
                                      {payload.map((entry) => {
                                        const rawDataKey = String(entry.dataKey || "")
                                        const metric = toOriginalMetricKey(rawDataKey)
                                        const rawValue = asNumber(entry.value)
                                        return (
                                          <div
                                            key={rawDataKey}
                                            className={`flex items-center gap-2 font-semibold ${isMobileViewport ? "text-[11px]" : "text-sm"}`}
                                          >
                                            <span
                                              className="inline-block h-2.5 w-2.5 rounded-full"
                                              style={{ backgroundColor: entry.color || "#64748b" }}
                                            />
                                            <span className="text-slate-700">
                                              {getMetricLabel(metric)}
                                            </span>
                                            <span className="ml-auto text-slate-900">
                                              {formatMetricValue(rawValue, metric)}
                                            </span>
                                          </div>
                                        )
                                      })}
                                    </div>
                                  </div>
                                )
                              }}
                            />
                            <Legend
                              verticalAlign="top"
                              align="left"
                              wrapperStyle={{ fontSize: "11px", paddingBottom: "8px", color: "#475569" }}
                              formatter={(value) => getMetricLabel(String(value || ""))}
                            />
                            {fullscreenCompositeData.mergedMetrics.map((metric) => {
                              const color = METRIC_LINE_COLORS[metric] || "#2563eb"
                              const gid = `fullscreen-section-${fullscreenCompositeData.group}-${metric}`
                              return (
                                <Area
                                  key={`fullscreen-section-${fullscreenCompositeData.group}-${metric}`}
                                  type="monotone"
                                  dataKey={metric}
                                  name={metric}
                                  stroke={color}
                                  fill={`url(#${gid})`}
                                  strokeWidth={2.2}
                                  fillOpacity={1}
                                  isAnimationActive={!prefersReducedMotion}
                                  dot={false}
                                />
                              )
                            })}
                          </AreaChart>
                        </ResponsiveContainer>
                      )}
                    </div>
                  ) : isYearOnYearFullscreen ? (
                    <div className="w-full max-w-6xl">
                      <YearOnYearComparisonChart
                        source={source}
                        datasetType={datasetType}
                        jobId={jobId}
                        fromDate={fullscreenFromDate || fromDate}
                        toDate={fullscreenToDate || toDate}
                        initialDelayMs={0}
                        heightClassName="h-[58vh] min-h-[420px]"
                      />
                    </div>
                  ) : (
                    <div className={fullscreenGraphHeightClass}>
                      <GraphView
                        source={source}
                        dimension={fullscreen.dimension}
                        metric={fullscreen.metric}
                        datasetType={datasetType}
                        bucket={fullscreen.bucket}
                        jobId={jobId}
                        fromDate={fullscreenFromDate || fromDate}
                        toDate={fullscreenToDate || toDate}
                        chartType={fullscreen.chartType}
                        tooltipMetricOverride={fullscreen.tooltipMetricOverride}
                        categoryFilters={isStateFullscreen ? activeRegionalMapFilters : undefined}
                        enableCrossDatasetHoverCompare={fullscreen.chartType === "india_map"}
                        eagerMapHoverPrefetch={fullscreen.chartType === "india_map"}
                        heightClassName="h-full"
                        onDataReady={setOpenedGraphData}
                      />
                    </div>
                  )}
                  {!isFullscreenCompareMode && isStateFullscreen && (
                    <div className="mt-6 rounded-3xl border border-slate-200/80 bg-white/90 p-4 shadow-[0_18px_40px_-28px_rgba(15,23,42,0.45)] sm:p-5">
                      <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
                        <div>
                          <h4 className="text-xs font-black uppercase tracking-[0.16em] text-slate-500">
                            {`${geographyLabel} To City Contribution`}
                          </h4>
                          <div className="mt-1 text-[11px] text-slate-500">
                            {`View how cities drive ${getMetricLabel(fullscreen.metric).toLowerCase()} inside a selected ${geographyLabel.toLowerCase()}.`}
                          </div>
                        </div>
                        {activeSelectedState && (
                          <span className="text-xs font-semibold text-slate-600">
                            {activeSelectedState}
                          </span>
                        )}
                      </div>

                      {!activeSelectedState ? (
                        <div className="text-sm text-slate-500">
                          {`Choose a ${geographyLabel.toLowerCase()} from Focus ${geographyLabel} to unlock city-level contribution.`}
                        </div>
                      ) : !isBreakdownMetricSupported ? (
                        <div className="text-sm text-slate-500">
                          {`City breakdown is unavailable for ${getMetricLabel(fullscreen.metric)}.`}
                        </div>
                      ) : cityBreakdownLoading ? (
                        <div className="text-sm text-slate-500">Loading city contribution...</div>
                      ) : cityBreakdownError ? (
                        <div className="text-sm text-rose-600">{cityBreakdownError}</div>
                      ) : cityBreakdownRows.length ? (
                        <>
                          <div className="grid grid-cols-1 lg:grid-cols-[minmax(320px,1fr)_minmax(260px,1fr)] gap-6">
                            <div className="h-72">
                              <ResponsiveContainer width="100%" height="100%">
                                <PieChart>
                                  <Pie
                                    data={cityBreakdownRows}
                                    dataKey="value"
                                    nameKey="city"
                                    innerRadius={54}
                                    outerRadius={110}
                                    paddingAngle={2}
                                    stroke="none"
                                    shape={(props) => {
                                      const city = String(props?.payload?.city || "").trim()
                                      return renderCityShape(props, city !== "" && city === activeCityKey)
                                    }}
                                    onMouseEnter={(_, index) => {
                                      const row = cityBreakdownRows[index]
                                      if (row) setActiveCityName(row.city)
                                    }}
                                    onClick={(_, index) => {
                                      const row = cityBreakdownRows[index]
                                      if (row) setActiveCityName(row.city)
                                    }}
                                    isAnimationActive={!prefersReducedMotion}
                                    animationDuration={prefersReducedMotion ? 0 : 450}
                                  >
                                    {cityBreakdownRows.map((entry, idx) => (
                                      <Cell
                                        key={`${entry.city}-${idx}`}
                                        fill={cityBreakdownColors[idx] || getCityColor(idx)}
                                      />
                                    ))}
                                  </Pie>
                                  <RechartsTooltip
                                    formatter={(value) => {
                                      const raw = Array.isArray(value) ? value[0] : value
                                      return formatMetricValue(asNumber(raw), fullscreen.metric)
                                    }}
                                  />
                                </PieChart>
                              </ResponsiveContainer>
                            </div>
                            <div className="max-h-72 overflow-y-auto pr-1">
                              <ul className="space-y-2">
                                {cityBreakdownRows.map((row, idx) => (
                                  <li
                                    key={`${row.city}-${idx}`}
                                    onMouseEnter={() => setActiveCityName(row.city)}
                                    onClick={() => setActiveCityName(row.city)}
                                    className={`cursor-pointer rounded-xl border bg-white px-3 py-2 transition-all ${
                                      activeCityKey === row.city
                                        ? "border-slate-400 shadow-[0_12px_28px_-24px_rgba(15,23,42,0.8)]"
                                        : "border-slate-200 hover:border-slate-300"
                                    }`}
                                  >
                                    <div className="flex items-center gap-2 text-sm">
                                      <span
                                        className="inline-block h-2.5 w-2.5 rounded-full"
                                        style={{ backgroundColor: cityBreakdownColors[idx] || getCityColor(idx) }}
                                      />
                                      <span className="font-semibold text-slate-700">
                                        {row.city}
                                      </span>
                                      <span className="ml-auto text-slate-500">
                                        {row.share.toFixed(1)}%
                                      </span>
                                    </div>
                                    <div className="text-xs font-semibold text-slate-500 mt-0.5">
                                      {formatMetricValue(row.value, fullscreen.metric)}
                                    </div>
                                  </li>
                                ))}
                              </ul>
                            </div>
                          </div>
                        </>
                      ) : (
                        <div className="text-sm text-slate-500">
                          No city-level data available for {activeSelectedState}.
                        </div>
                      )}
                    </div>
                  )}
                  {!isFullscreenCompareMode && isStateFullscreen && (
                    <div className="mt-6 rounded-3xl border border-slate-200/80 bg-white/90 p-4 shadow-[0_18px_40px_-28px_rgba(15,23,42,0.45)] sm:p-5">
                      <div className="flex flex-wrap items-start justify-between gap-3 mb-4">
                        <div>
                          <h4 className="text-xs font-black uppercase tracking-[0.16em] text-slate-500">
                            {`${geographyLabel} Compare Mix`}
                          </h4>
                          <div className="text-[11px] text-slate-500 mt-1">
                            {`For every selected ${geographyLabel.toLowerCase()} in Compare ${geographyLabelPlural}, view distribution charts for ${regionalMixLabels.join(" and ")}.`}
                          </div>
                        </div>
                      </div>

                      {!isBreakdownMetricSupported ? (
                        <div className="text-sm text-slate-500">
                          {`Mix breakdown is unavailable for ${getMetricLabel(fullscreen.metric)}.`}
                        </div>
                      ) : !activeComparisonStates.length ? (
                        <div className="text-sm text-slate-500">
                          {`Select at least 1 ${geographyLabel.toLowerCase()} in Compare ${geographyLabelPlural} to render comparison charts.`}
                        </div>
                      ) : stateComparisonMixLoading ? (
                        <div className="text-sm text-slate-500">Loading mix distribution...</div>
                      ) : (
                        <div className="space-y-4">
                          {stateComparisonMixError && (
                            <div className="text-sm text-amber-700">
                              {stateComparisonMixError}
                            </div>
                          )}
                            <div className={`grid ${stateComparisonLayout.gridClass} gap-4`}>
                              {stateComparisonMixRows.map((mixRow) => {
                                const stateSlug = toDomId(mixRow.state)
                                const planSlices = toMixSlices(mixRow.planRows, `plan-${stateSlug}`)
                                const deviceSlices = toMixSlices(mixRow.deviceRows, `device-${stateSlug}`)
                                const mixSections = [
                                  {
                                    key: `primary-${stateSlug}`,
                                    title: activeRegionalPrimaryDescriptor.sectionTitle,
                                    slices: planSlices,
                                    emptyText: mixRow.planMessage || activeRegionalPrimaryDescriptor.missingText,
                                  },
                                  {
                                    key: `secondary-${stateSlug}`,
                                    title: activeRegionalSecondaryDescriptor.sectionTitle,
                                    slices: deviceSlices,
                                    emptyText: mixRow.deviceMessage || activeRegionalSecondaryDescriptor.missingText,
                                  },
                                ].filter((section, index, items) => (
                                  index === 0 || section.title !== items[0].title
                                ))
                                const visibleMixSections = mixSections.filter((section) => section.slices.length > 0)
                                return (
                                  <div
                                    key={mixRow.state}
                                    className={`rounded-2xl border border-slate-200 bg-gradient-to-br from-white via-slate-50/80 to-cyan-50/40 p-4 ${stateComparisonLayout.cardMinHeightClass}`}
                                  >
                                  <div className="mb-3 flex flex-wrap items-start justify-between gap-2">
                                    <div>
                                      <div className="text-sm font-bold text-slate-800">
                                        {mixRow.state}
                                      </div>
                                      <div className="text-[11px] text-slate-500">
                                        {`${getMetricLabel(fullscreen.metric)}: ${formatMetricValue(mixRow.metricValue, fullscreen.metric)}`}
                                      </div>
                                    </div>
                                    <span className="rounded-full border border-slate-200 bg-white px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-500">
                                      {`${visibleMixSections.length || 1} ${visibleMixSections.length === 1 ? "Chart" : "Charts"}`}
                                    </span>
                                  </div>

                                  <div className="grid grid-cols-1 gap-3">
                                    {visibleMixSections.length ? visibleMixSections.map((section) => (
                                      <div key={section.key} className="rounded-xl border border-slate-200 bg-white p-3">
                                        <div className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-400">
                                          {section.title}
                                        </div>
                                        <div className="mt-2 grid grid-cols-1 sm:grid-cols-[minmax(0,1fr)_minmax(170px,1fr)] gap-3 items-center">
                                          <div style={{ height: `${comparisonPieSizing.height}px` }}>
                                            <ResponsiveContainer width="100%" height="100%">
                                              <PieChart>
                                                <defs>
                                                  {section.slices.map((slice) => (
                                                    <linearGradient
                                                      key={slice.gradientId}
                                                      id={slice.gradientId}
                                                      x1="0%"
                                                      y1="0%"
                                                      x2="100%"
                                                      y2="100%"
                                                    >
                                                      <stop offset="0%" stopColor={slice.gradient.from} />
                                                      <stop offset="100%" stopColor={slice.gradient.to} />
                                                    </linearGradient>
                                                  ))}
                                                </defs>
                                                <Pie
                                                  data={section.slices}
                                                  dataKey="value"
                                                  nameKey="label"
                                                  innerRadius={comparisonPieSizing.innerRadius}
                                                  outerRadius={comparisonPieSizing.outerRadius}
                                                  paddingAngle={1.8}
                                                  stroke="none"
                                                  isAnimationActive={!prefersReducedMotion}
                                                  animationDuration={prefersReducedMotion ? 0 : 450}
                                                >
                                                  {section.slices.map((slice) => (
                                                    <Cell
                                                      key={slice.gradientId}
                                                      fill={`url(#${slice.gradientId})`}
                                                      stroke="#ffffff"
                                                      strokeWidth={1}
                                                    />
                                                  ))}
                                                </Pie>
                                                <RechartsTooltip
                                                  formatter={(value, _name, entry) => {
                                                    const payload = (entry?.payload || {}) as MixSlice
                                                    const rawValue = asNumber(Array.isArray(value) ? value[0] : value)
                                                    return [
                                                      formatMetricValue(rawValue, regionalAnalysisMetric),
                                                      `${payload.label} (${payload.percentage.toFixed(1)}%)`,
                                                    ]
                                                  }}
                                                />
                                              </PieChart>
                                            </ResponsiveContainer>
                                          </div>
                                          <ul className="max-h-44 overflow-y-auto space-y-1.5 pr-1">
                                            {section.slices.map((slice) => (
                                              <li key={slice.gradientId} className="flex items-center gap-2 text-[11px] text-slate-600">
                                                <span
                                                  className="inline-block h-2.5 w-2.5 rounded-full border border-white/80"
                                                  style={{
                                                    backgroundImage: `linear-gradient(135deg, ${slice.gradient.from}, ${slice.gradient.to})`,
                                                  }}
                                                />
                                                <span className="truncate">{slice.label}</span>
                                                <span className="ml-auto font-semibold text-slate-500">
                                                  {slice.percentage.toFixed(1)}%
                                                </span>
                                              </li>
                                            ))}
                                          </ul>
                                        </div>
                                      </div>
                                    )) : (
                                      <div className="rounded-xl border border-slate-200 bg-white p-3 text-xs text-slate-500">
                                        {mixSections.find((section) => section.emptyText)?.emptyText || `No relatable mix data found for ${mixRow.state}.`}
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )
                            })}
                          </div>
                          {!!comparisonMetricRows.length && (
                            <div className="rounded-xl border border-slate-200 bg-white p-3">
                              <div className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-400">
                                {`${geographyLabel} Metric Ranking`}
                              </div>
                              <ul className="mt-2 space-y-1.5">
                                {comparisonMetricRows.map((row, index) => (
                                  <li key={row.state} className="flex items-center gap-2 text-xs text-slate-600">
                                    <span className="w-5 text-slate-400">{index + 1}.</span>
                                    <span className="font-semibold text-slate-700">{row.state}</span>
                                    <span className="ml-auto text-slate-500">
                                      {formatMetricValue(row.value, fullscreen.metric)}
                                    </span>
                                  </li>
                                ))}
                              </ul>
                            </div>
                          )}
                        </div>
                      )}
                      {!stateComparisonMixLoading && !stateComparisonMixRows.length && activeComparisonStates.length > 0 && !stateComparisonMixError && (
                        <div className="text-sm text-slate-500">
                          No comparison mix data available for the selected states.
                        </div>
                      )}
                    </div>
                  )}
                  {!isFullscreenCompareMode && (
                    <div
                      className={`rounded-3xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/50 p-4 shadow-[0_18px_40px_-28px_rgba(15,23,42,0.45)] sm:p-5 ${
                        isStateFullscreen ? "mt-6" : "mt-4"
                      }`}
                    >
                      <div className="flex items-center justify-between gap-3 mb-3">
                        <h4 className="text-xs font-black uppercase tracking-[0.16em] text-slate-500">
                          Sahyogi Insights
                        </h4>
                        {insightsModel && (
                          <span className="text-[10px] font-semibold text-slate-400">
                            Model: {insightsModel}
                          </span>
                        )}
                      </div>
                      {!INSIGHTS_ENABLED ? (
                        <div className="text-sm text-slate-500">
                          Insights are disabled in this deployment.
                        </div>
                      ) : insightsLoading ? (
                        <div className="text-sm text-slate-500">Generating insights...</div>
                      ) : insightsError ? (
                        <div className="text-sm text-rose-600">{insightsError}</div>
                      ) : insights.length ? (
                        <ul className="space-y-2">
                          {insights.map((line, idx) => (
                            <li key={`${idx}-${line.slice(0, 24)}`} className="text-sm text-slate-700 leading-relaxed">
                              {line}
                            </li>
                          ))}
                        </ul>
                      ) : (
                        <div className="text-sm text-slate-500">Open graph data to view insights.</div>
                      )}
                    </div>
                  )}
                  </div>
                </div>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  )
}
