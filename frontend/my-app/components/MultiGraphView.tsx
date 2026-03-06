"use client"

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

import GraphView, { fetchGraphRows } from "@/components/GraphView"
import DateRangePicker from "@/components/DateRangePicker"
import type { GraphChartType, GraphDataSnapshot } from "@/components/GraphView"
import {
  fetchCategoryPercentage,
  fetchCityBreakdownByState,
  fetchGraphInsights,
  type CategoryPercentageRow,
} from "@/app/lib/api"
import { GRAPH_PRESETS } from "@/utils/graphPresets"

const normalizedInsightsFlag = (
  process.env.NEXT_PUBLIC_ENABLE_GRAPH_INSIGHTS || ""
).trim().toLowerCase()

// Enable insights by default; allow explicit opt-out via env.
const INSIGHTS_ENABLED = !["0", "false", "no", "off"].includes(normalizedInsightsFlag)

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

const toMixSlices = (rows: CategoryOption[], prefix: string): MixSlice[] =>
  rows.map((row, index) => ({
    label: row.label,
    value: row.value,
    percentage: row.percentage,
    gradient: getPieGradient(index),
    gradientId: `${prefix}-${index}-${toDomId(row.label)}`,
  }))

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

const normalizeMonthKey = (value: unknown) => {
  const raw = String(value ?? "").trim()
  if (!raw) return ""

  if (/^\d{4}-\d{2}(-\d{2})?$/.test(raw)) {
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
    return `${year}-${month}-01`
  }
  return raw
}

const monthSortValue = (value: string) => {
  const normalized = normalizeMonthKey(value)
  const parsed = new Date(normalized).getTime()
  if (!Number.isNaN(parsed)) return parsed
  return Number.MAX_SAFE_INTEGER
}

const formatMonthLabel = (value: string) => {
  const normalized = normalizeMonthKey(value)
  const parsed = new Date(normalized)
  if (Number.isNaN(parsed.getTime())) return value
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

const toLogPlotKey = (metric: string) => `${metric}${LOG_PLOT_SUFFIX}`

const toOriginalMetricKey = (dataKey: string) => (
  dataKey.endsWith(LOG_PLOT_SUFFIX)
    ? dataKey.slice(0, -LOG_PLOT_SUFFIX.length)
    : dataKey
)

const toLogSafeValue = (value: unknown) => {
  const numeric = asNumber(value)
  return numeric > 0 ? numeric : 1
}

const buildLogScaledRows = (
  rows: SectionMergedRow[],
  metrics: readonly string[]
): SectionMergedRow[] => rows.map((row) => {
  const next: SectionMergedRow = { ...row }
  metrics.forEach((metric) => {
    next[toLogPlotKey(metric)] = toLogSafeValue(row[metric])
  })
  return next
})

const buildInsightsRows = (snapshot: GraphDataSnapshot) => {
  const rows = snapshot.rows.slice(0, 80)
  const dimKey = snapshot.dimensionKey

  if (snapshot.compareMode) {
    return rows.map((row) => ({
      [dimKey]: row[dimKey],
      samsung_vs: row.samsung_vs ?? 0,
      samsung_croma: row.samsung_croma ?? 0,
    }))
  }

  const measureKey = snapshot.measure
  return rows.map((row) => ({
    [dimKey]: row[dimKey],
    [measureKey]: row[measureKey],
  }))
}

const SALES_METRICS = [
  "earned_premium",
  "gross_premium",
  "zopper_earned_premium",
  "quantity",
]

const CLAIMS_METRICS = [
  "net_claims",
  "claims",
  "loss_ratio",
  "quantity",
]

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
  reliance: ["reliance", "reliance_resq"],
  godrej: ["godrej"],
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
  if (d.includes("month") || d.includes("date")) return "Month"
  if (d.includes("state")) return source === "godrej" ? "Region" : "State"
  if (d === "article_brand" || d === "brand") return "Brand Category"
  if (d.includes("channel")) return "Channel"
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
    (sourceKey === "godrej" && (dimKey === "channel" || dimKey === "plan_category"))
    || (sourceKey === "reliance" && dimKey === "plan_category")
    || ((sourceKey === "samsung_vs" || sourceKey === "samsung_croma") && dimKey === "plan_category")
  ) {
    return "dense_heatmap"
  }

  if (
    ((sourceKey === "samsung_vs" || sourceKey === "samsung_croma") && dimKey === "device_plan_category")
    || (sourceKey === "reliance" && (dimKey === "article_brand" || dimKey === "brand"))
    || (sourceKey === "godrej" && dimKey === "product_category")
  ) {
    return "metric_strips"
  }

  return "line"
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
        return [
          {
            id: "samsung-month-claims-cost",
            title: "Claims Cost Trend by Month",
            subtitle: "Month-on-month claims cost line range comparison between Vijay Sales and Croma.",
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
      }

      return [
        {
          id: "samsung-month-gross-premium",
          title: "Gross Premium Trend by Month",
          subtitle: "Month-on-month gross premium line range comparison between Vijay Sales and Croma.",
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
    },
    [datasetType]
  )
  const isGodrej = source === "godrej"
  const isGodrejClaims = isGodrej && datasetType === "claims"
  const activeGroupOrder = useMemo(
    () => (
      isGodrej
        ? ["time", "region", "channel", "product"]
        : GROUP_ORDER
    ),
    [isGodrej]
  )
  const activePresets = useMemo(
    () => (
      isGodrej
        ? (isGodrejClaims ? GODREJ_CLAIMS_PRESETS : GODREJ_SALES_PRESETS)
        : Object.values(GRAPH_PRESETS)
    ),
    [isGodrej, isGodrejClaims]
  )
  const sideCardMetric = datasetType === "sales" ? "quantity" : "claims"
  const partnerSideCards = useMemo<PartnerSideCard[]>(() => {
    if (isSamsungOverview) return []

    if (source === "godrej") {
      return [
        {
          id: "godrej-channel-distribution",
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
          id: "godrej-product-distribution",
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
  }, [datasetType, isSamsungOverview, sideCardMetric, source])

  const [fullscreen, setFullscreen] = useState<FullscreenGraph>(null)
  const [fullscreenFromDate, setFullscreenFromDate] = useState(fromDate || "")
  const [fullscreenToDate, setFullscreenToDate] = useState(toDate || "")
  const [openedGraphData, setOpenedGraphData] = useState<GraphDataSnapshot | null>(null)
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
  const [sectionMergedMap, setSectionMergedMap] = useState<Record<string, SectionMergedState>>({})
  const lastInsightsKeyRef = useRef("")
  const isStateFullscreen = Boolean(fullscreen && fullscreen.dimension.toLowerCase().includes("state"))
  const geographyLabel = "Region"
  const geographyLabelPlural = "Regions"
  const stateOptions = useMemo(() => {
    if (!isStateFullscreen || !openedGraphData?.rows.length || !openedGraphData.dimensionKey) {
      return []
    }

    const seen = new Set<string>()
    const out: string[] = []
    const dimKey = openedGraphData.dimensionKey
    for (const row of openedGraphData.rows) {
      const rawValue = row[dimKey]
      const label = rawValue == null ? "" : String(rawValue).trim()
      if (!label || seen.has(label)) continue
      seen.add(label)
      out.push(label)
    }
    return out
  }, [isStateFullscreen, openedGraphData])
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
  const stateMetricMap = useMemo(() => {
    const map = new Map<string, number>()
    if (!isStateFullscreen || !openedGraphData?.rows.length || !openedGraphData.dimensionKey) {
      return map
    }

    const dimKey = openedGraphData.dimensionKey
    const measureKey = openedGraphData.measure
    for (const row of openedGraphData.rows) {
      const raw = row[dimKey]
      const label = raw == null ? "" : String(raw).trim()
      if (!label) continue

      const value = openedGraphData.compareMode
        ? asNumber(row.samsung_vs) + asNumber(row.samsung_croma)
        : asNumber(row[measureKey])
      map.set(label, (map.get(label) ?? 0) + Math.max(0, value))
    }
    return map
  }, [isStateFullscreen, openedGraphData])
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
  useEffect(() => {
    if (!isStateFullscreen) {
      setIsRegionFilterCardCollapsed(false)
    }
  }, [isStateFullscreen])
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

    if (source === "godrej") {
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
  }, [source])
  const activeRegionalPrimaryDescriptor = regionalCategoryConfig.primary
  const activeRegionalSecondaryDescriptor = regionalCategoryConfig.secondary
  const regionalMapFilterConfig = useMemo(() => {
    if (source === "godrej") {
      return {
        primary: {
          dimension: "channel",
          label: "Channel",
          allLabel: "All Channels",
        } as RegionalMapFilterDescriptor,
        secondary: {
          dimension: "product_category",
          label: "Product",
          allLabel: "All Products",
        } as RegionalMapFilterDescriptor,
      }
    }
    if (source === "reliance") {
      return {
        primary: {
          dimension: "plan_category",
          label: "Plan Category",
          allLabel: "All Plan Categories",
        } as RegionalMapFilterDescriptor,
        secondary: {
          dimension: "product_category",
          label: "Product Category",
          allLabel: "All Product Categories",
        } as RegionalMapFilterDescriptor,
      }
    }
    return {
      primary: {
        dimension: "plan_category",
        label: "Plan Category",
        allLabel: "All Plan Categories",
      } as RegionalMapFilterDescriptor,
      secondary: {
        dimension: "device_plan_category",
        label: "Device Plan Category",
        allLabel: "All Device Plan Categories",
      } as RegionalMapFilterDescriptor,
    }
  }, [source])
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
    if (!isStateFullscreen) {
      setRegionalMapPrimaryValue("")
      setRegionalMapSecondaryValue("")
      return
    }

    let active = true
    setRegionalMapFiltersLoading(true)

    const baseParams = {
      source,
      dataset_type: datasetType,
      metric: "quantity",
      job_id: jobId || undefined,
      from_date: fromDate || undefined,
      to_date: toDate || undefined,
      limit: 200,
    } as const

    Promise.all([
      fetchCategoryPercentage({
        ...baseParams,
        dimension: regionalMapFilterConfig.primary.dimension,
      }),
      fetchCategoryPercentage({
        ...baseParams,
        dimension: regionalMapFilterConfig.secondary.dimension,
      }),
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
    }
  }, [
    isStateFullscreen,
    source,
    datasetType,
    jobId,
    fromDate,
    toDate,
    regionalMapFilterConfig.primary.dimension,
    regionalMapFilterConfig.secondary.dimension,
  ])
  const getSectionTitle = useCallback((group: string) => {
    if (isGodrej) return GODREJ_GROUP_TITLES[group] || group
    if (group === "device_category" && source === "reliance") return "Brand Segment Pulse"
    return GROUP_TITLES[group] || group
  }, [isGodrej, source])
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
            const visibleMetrics =
              datasetType === "sales"
                ? preset.metrics.filter((m: string) => SALES_METRICS.includes(m))
                : preset.metrics.filter((m: string) => CLAIMS_METRICS.includes(m))
            return {
              preset: { ...preset, dimension: resolvedDimension },
              visibleMetrics,
              mergedMetrics: visibleMetrics.slice(0, 4),
            }
          })
          .filter(entry => entry.visibleMetrics.length > 0)
        return { group, entries }
      })
      .filter(section => section.entries.length > 0)
  }, [isSamsungOverview, activeGroupOrder, activePresets, datasetType, source])

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
    const fallbackSection = sectionConfigs[0]?.group || "time"
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
  }, [isSamsungOverview, samsungOverviewCards, sectionConfigs, getSectionTitle, partnerSideCards, datasetType])

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
    const timer = setTimeout(async () => {
      if (!active) return

      const loadingState: Record<string, SectionMergedState> = {}
      sectionConfigs.forEach(({ group }) => {
        loadingState[group] = { loading: true, error: null, rows: [] }
      })
      setSectionMergedMap((prev) => ({ ...prev, ...loadingState }))

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
        const dimKey = dimension.toLowerCase()

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
              })
            )
          )

          const merged = new Map<string, SectionMergedRow>()
          let successCount = 0

          metricResponses.forEach((response, idx) => {
            const metric = mergedMetrics[idx]
            if (response.status !== "fulfilled") return
            successCount += 1

            for (const row of response.value.data || []) {
              const rawDim = row[dimKey]
              const dimValue = String(rawDim ?? "").trim()
              if (!dimValue) continue

              if (!merged.has(dimValue)) {
                const seed: SectionMergedRow = { [dimKey]: dimValue }
                mergedMetrics.forEach((m) => {
                  seed[m] = 0
                })
                merged.set(dimValue, seed)
              }

              const target = merged.get(dimValue)
              if (!target) continue
              target[metric] = Math.max(0, getSourceMetricValue(row, metric, source))
            }
          })

          if (successCount === 0) {
            return { loading: false, error: "Unable to load section graph data.", rows: [] }
          }

          const rows = Array.from(merged.values())
          if (dimKey.includes("month") || dimKey.includes("date")) {
            rows.sort((a, b) => monthSortValue(String(a[dimKey] || "")) - monthSortValue(String(b[dimKey] || "")))
          } else {
            rows.sort((a, b) => String(a[dimKey] || "").localeCompare(String(b[dimKey] || "")))
          }

          return { loading: false, error: rows.length ? null : "No data available in selected range.", rows }
        } catch {
          return { loading: false, error: "Unable to load section graph data.", rows: [] }
        }
      }

      // Progressive section loading reduces backend request bursts and improves
      // time-to-first-chart rendering.
      for (const section of sectionConfigs) {
        if (!active) break
        const state = await fetchSectionState(section.entries)
        if (!active) break
        setSectionMergedMap((prev) => ({
          ...prev,
          [section.group]: state,
        }))
      }
    }, 0)

    return () => {
      active = false
      clearTimeout(timer)
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

  const fullscreenCompositeUseLogScale = false

  const fullscreenCompositeChartRows = useMemo(() => (
    fullscreenCompositeData?.displayRows || []
  ), [fullscreenCompositeData])

  useEffect(() => {
    if (!fullscreen?.isComposite) return
    if (
      !fullscreenCompositeData
      || fullscreenCompositeData.sectionData.loading
      || Boolean(fullscreenCompositeData.sectionData.error)
      || !fullscreenCompositeData.displayRows.length
    ) {
      setOpenedGraphData(null)
      return
    }

    const fallbackMetric = datasetType === "sales" ? "gross_premium" : "claims"
    const insightMetric = String(
      fullscreen.metric || fullscreenCompositeData.mergedMetrics[0] || fallbackMetric
    )

    setOpenedGraphData({
      rows: fullscreenCompositeData.displayRows,
      measure: insightMetric,
      dimensionKey: fullscreenCompositeData.dimKey,
      compareMode: false,
    })
  }, [fullscreen, fullscreenCompositeData, datasetType])

  useEffect(() => {
    if (!INSIGHTS_ENABLED) return
    if (!fullscreen || !openedGraphData) return
    if (!openedGraphData.rows.length) return
    if (!openedGraphData.measure || !openedGraphData.dimensionKey) return

    const insightsRows = buildInsightsRows(openedGraphData)
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
      compareMode: openedGraphData.compareMode,
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
        compare_mode: openedGraphData.compareMode,
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
  }, [source, datasetType, jobId, fromDate, toDate, fullscreen, openedGraphData])

  useEffect(() => {
    if (!fullscreen || !isStateFullscreen || !activeSelectedState) return
    if (!isBreakdownMetricSupported) return

    let active = true
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
        from_date: fromDate || undefined,
        to_date: toDate || undefined,
        limit: 150,
      })
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
      clearTimeout(timer)
    }
  }, [
    fullscreen,
    isStateFullscreen,
    activeSelectedState,
    source,
    datasetType,
    jobId,
    fromDate,
    toDate,
    isBreakdownMetricSupported,
  ])

  useEffect(() => {
    if (!fullscreen || !isStateFullscreen) return
    if (!isBreakdownMetricSupported) return

    let active = true
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
        metric: "quantity",
        job_id: jobId || undefined,
        from_date: fromDate || undefined,
        to_date: toDate || undefined,
        limit: 200,
      } as const

      Promise.all(
        activeComparisonStates.map(async (stateLabel) => {
          const metricValue = asNumber(stateMetricMap.get(stateLabel))
          try {
            const [planRes, deviceRes] = await Promise.all([
              fetchCategoryPercentage({
                ...baseParams,
                dimension: activeRegionalPrimaryDescriptor.dimension,
                state: stateLabel,
              }),
              fetchCategoryPercentage({
                ...baseParams,
                dimension: activeRegionalSecondaryDescriptor.dimension,
                state: stateLabel,
              }),
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
      clearTimeout(timer)
    }
  }, [
    fullscreen,
    isStateFullscreen,
    source,
    datasetType,
    jobId,
    fromDate,
    toDate,
    activeComparisonStates,
    stateMetricMap,
    isBreakdownMetricSupported,
    activeRegionalPrimaryDescriptor.dimension,
    activeRegionalPrimaryDescriptor.label,
    activeRegionalSecondaryDescriptor.dimension,
    activeRegionalSecondaryDescriptor.label,
  ])

  const todayIso = useMemo(() => new Date().toISOString().slice(0, 10), [])
  const pickerMaxDate = useMemo(() => todayIso, [todayIso])

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
    const nextTo = todayIso
    if (!nextFrom && !nextTo) return
    const orderedFrom = nextFrom && nextTo && nextFrom > nextTo ? nextTo : nextFrom
    const orderedTo = nextFrom && nextTo && nextFrom > nextTo ? nextFrom : nextTo
    setFullscreenFromDate(orderedFrom)
    setFullscreenToDate(orderedTo)
    onDateRangeApply(orderedFrom, orderedTo)
  }

  const handleOpenFullscreen = (item: NonNullable<FullscreenGraph>) => {
    setFullscreenFromDate(fromDate || "")
    setFullscreenToDate(toDate || "")
    setFullscreen(item)
    setOpenedGraphData(null)
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
    setIsRegionFilterCardCollapsed(false)
    lastInsightsKeyRef.current = ""
  }

  const handleCloseFullscreen = () => {
    setFullscreen(null)
    setOpenedGraphData(null)
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
    lastInsightsKeyRef.current = ""
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
    layout: "main" | "small"
  ) => {
    return (
      <motion.div
        key={card.id}
        initial={prefersReducedMotion ? false : { opacity: 0, y: 16 }}
        animate={prefersReducedMotion ? undefined : { opacity: 1, y: 0 }}
        transition={prefersReducedMotion ? undefined : { duration: 0.35, ease: "easeOut" }}
        whileHover={prefersReducedMotion ? undefined : { y: -4 }}
        className="smooth-surface relative overflow-hidden rounded-2xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/60 p-4 shadow-sm transition-shadow hover:shadow-[0_18px_40px_-26px_rgba(15,23,42,0.5)] sm:p-5"
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
              heightClassName={layout === "main" ? "h-[360px] sm:h-[430px]" : "h-[300px] sm:h-[340px]"}
            />
          </div>
        </div>
      </motion.div>
    )
  }

  const renderPartnerSideCard = (
    card: PartnerSideCard,
    keyPrefix: string
  ) => {
    return (
      <motion.div
        key={`${keyPrefix}-${card.id}`}
        initial={prefersReducedMotion ? false : { opacity: 0, y: 16 }}
        animate={prefersReducedMotion ? undefined : { opacity: 1, y: 0 }}
        transition={prefersReducedMotion ? undefined : { duration: 0.35, ease: "easeOut" }}
        whileHover={prefersReducedMotion ? undefined : { y: -4 }}
        className="smooth-surface relative overflow-hidden rounded-2xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/60 p-3 shadow-sm transition-shadow hover:shadow-[0_18px_40px_-26px_rgba(15,23,42,0.5)] sm:p-4"
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
              heightClassName={
                card.chartType === "pie"
                  ? "h-[230px] sm:h-[260px]"
                  : "h-[180px] sm:h-[210px]"
              }
            />
          </div>
        </div>
      </motion.div>
    )
  }

  return (
    <>
      {isSamsungOverview ? (
        <div className="space-y-4">
          {samsungOverviewCards
            .filter((card) => card.size === "main")
            .map((card) => renderSamsungCard(card, "main"))}
          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            {samsungOverviewCards
            .filter((card) => card.size === "small")
            .map((card) => renderSamsungCard(card, "small"))}
          </div>
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,2fr)_minmax(300px,1fr)] xl:items-start">
          <div className="space-y-2">
            {sectionConfigs.map(({ group, entries }) => {
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
        const sectionUseLogScale = false
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
            className="relative mb-6 overflow-hidden rounded-[24px] border border-slate-200/80 bg-white shadow-[0_22px_60px_-38px_rgba(15,23,42,0.45)] sm:mb-10 sm:rounded-[28px]"
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
                  className="smooth-surface relative overflow-hidden rounded-2xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/60 p-4 shadow-sm sm:p-5"
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
                                  ? formatMonthLabel(String(value || ""))
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
                                        ? formatMonthLabel(String(label || ""))
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
          {!!partnerSideCards.length && (
            <div className="xl:sticky xl:top-4">
              <div className="grid grid-cols-1 gap-3">
                {partnerSideCards.map((card) => renderPartnerSideCard(card, "frozen-side-rail"))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* FULLSCREEN */}
      <AnimatePresence>
        {fullscreen && (
          <motion.div className="fixed inset-0 z-50 bg-slate-950/35 p-1.5 sm:p-2 md:p-4">
            <div className="h-full w-full overflow-hidden rounded-[20px] border border-slate-200 bg-gradient-to-b from-slate-50 via-white to-slate-100 shadow-2xl sm:rounded-[28px]">
              <div className="h-full w-full overflow-auto">
                <div className="sticky top-0 z-20 border-b border-slate-200/80 bg-white/90 px-3 py-3 backdrop-blur sm:px-4 sm:py-4 md:px-6">
                  <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                  <div>
                    <div className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-400">
                      Expanded View
                    </div>
                    <div className="text-sm font-semibold text-slate-800">
                      {getGraphTitle(fullscreen.metric, fullscreen.dimension, source)}
                    </div>
                    {currentNavigableGraph && (
                      <div className="mt-1 text-[11px] text-slate-500">
                        {currentNavigableGraph.sectionTitle} | Graph {fullscreenGraphIndex + 1} of {navigableGraphs.length}
                      </div>
                    )}
                  </div>
                  <div className="flex w-full flex-wrap items-center gap-2 sm:w-auto sm:justify-end">
                    <button
                      className="rounded-lg border border-slate-300 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
                      onClick={() => handleTraverseGraph(-1)}
                      disabled={!canGoToPreviousGraph}
                    >
                      <span className="sm:hidden">Prev</span>
                      <span className="hidden sm:inline">Previous Graph</span>
                    </button>
                    <button
                      className="rounded-lg border border-slate-300 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
                      onClick={() => handleTraverseGraph(1)}
                      disabled={!canGoToNextGraph}
                    >
                      <span className="sm:hidden">Next</span>
                      <span className="hidden sm:inline">Next Graph</span>
                    </button>
                    <button
                      className="ml-auto rounded-lg border border-slate-300 bg-white px-3 py-1.5 text-sm font-semibold text-slate-700 hover:bg-slate-50 sm:ml-0"
                      onClick={handleCloseFullscreen}
                    >
                      Close
                    </button>
                  </div>
                  </div>
                </div>

                <div className="sticky top-[116px] z-30 border-b border-slate-200/80 bg-white/90 px-3 py-3 backdrop-blur sm:top-[73px] sm:px-4 md:px-6">
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
                  {fullscreenCompositeData ? (
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
                                  ? formatMonthLabel(String(value || ""))
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
                                        ? formatMonthLabel(String(label || ""))
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
                  ) : (
                    <div className={fullscreenGraphHeightClass}>
                      <GraphView
                        source={source}
                        dimension={fullscreen.dimension}
                        metric={fullscreen.metric}
                        datasetType={datasetType}
                        bucket={fullscreen.bucket}
                        jobId={jobId}
                        fromDate={fromDate}
                        toDate={toDate}
                        chartType={fullscreen.chartType}
                        tooltipMetricOverride={fullscreen.tooltipMetricOverride}
                        categoryFilters={isStateFullscreen ? activeRegionalMapFilters : undefined}
                        heightClassName="h-full"
                        onDataReady={setOpenedGraphData}
                      />
                    </div>
                  )}
                  {isStateFullscreen && (
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
                                        fill={getCityColor(idx)}
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
                                        style={{ backgroundColor: getCityColor(idx) }}
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
                  {isStateFullscreen && (
                    <div className="mt-6 rounded-3xl border border-slate-200/80 bg-white/90 p-4 shadow-[0_18px_40px_-28px_rgba(15,23,42,0.45)] sm:p-5">
                      <div className="flex flex-wrap items-start justify-between gap-3 mb-4">
                        <div>
                          <h4 className="text-xs font-black uppercase tracking-[0.16em] text-slate-500">
                            {`${geographyLabel} Compare Mix`}
                          </h4>
                          <div className="text-[11px] text-slate-500 mt-1">
                            {`For every selected ${geographyLabel.toLowerCase()} in Compare ${geographyLabelPlural}, view gradient pie splits for ${activeRegionalPrimaryDescriptor.label} and ${activeRegionalSecondaryDescriptor.label}.`}
                          </div>
                        </div>
                      </div>

                      {!isBreakdownMetricSupported ? (
                        <div className="text-sm text-slate-500">
                          {`Mix breakdown is unavailable for ${getMetricLabel(fullscreen.metric)}.`}
                        </div>
                      ) : !activeComparisonStates.length ? (
                        <div className="text-sm text-slate-500">
                          {`Select at least 1 ${geographyLabel.toLowerCase()} in Compare ${geographyLabelPlural} to render comparison pies.`}
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
                                      2 Pie Charts
                                    </span>
                                  </div>

                                  <div className="grid grid-cols-1 gap-3">
                                    <div className="rounded-xl border border-slate-200 bg-white p-3">
                                      <div className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-400">
                                        {activeRegionalPrimaryDescriptor.sectionTitle}
                                      </div>
                                      {planSlices.length ? (
                                        <div className="mt-2 grid grid-cols-1 sm:grid-cols-[minmax(0,1fr)_minmax(170px,1fr)] gap-3 items-center">
                                          <div style={{ height: `${comparisonPieSizing.height}px` }}>
                                            <ResponsiveContainer width="100%" height="100%">
                                              <PieChart>
                                                <defs>
                                                  {planSlices.map((slice) => (
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
                                                  data={planSlices}
                                                  dataKey="value"
                                                  nameKey="label"
                                                  innerRadius={comparisonPieSizing.innerRadius}
                                                  outerRadius={comparisonPieSizing.outerRadius}
                                                  paddingAngle={1.8}
                                                  stroke="none"
                                                  isAnimationActive={!prefersReducedMotion}
                                                  animationDuration={prefersReducedMotion ? 0 : 450}
                                                >
                                                  {planSlices.map((slice) => (
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
                                                      `${rawValue.toLocaleString()} plans`,
                                                      `${payload.label} (${payload.percentage.toFixed(1)}%)`,
                                                    ]
                                                  }}
                                                />
                                              </PieChart>
                                            </ResponsiveContainer>
                                          </div>
                                          <ul className="max-h-44 overflow-y-auto space-y-1.5 pr-1">
                                            {planSlices.map((slice) => (
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
                                      ) : (
                                        <div className="mt-2 text-xs text-slate-500">
                                          {mixRow.planMessage || activeRegionalPrimaryDescriptor.missingText}
                                        </div>
                                      )}
                                    </div>

                                    <div className="rounded-xl border border-slate-200 bg-white p-3">
                                      <div className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-400">
                                        {activeRegionalSecondaryDescriptor.sectionTitle}
                                      </div>
                                      {deviceSlices.length ? (
                                        <div className="mt-2 grid grid-cols-1 sm:grid-cols-[minmax(0,1fr)_minmax(170px,1fr)] gap-3 items-center">
                                          <div style={{ height: `${comparisonPieSizing.height}px` }}>
                                            <ResponsiveContainer width="100%" height="100%">
                                              <PieChart>
                                                <defs>
                                                  {deviceSlices.map((slice) => (
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
                                                  data={deviceSlices}
                                                  dataKey="value"
                                                  nameKey="label"
                                                  innerRadius={comparisonPieSizing.innerRadius}
                                                  outerRadius={comparisonPieSizing.outerRadius}
                                                  paddingAngle={1.8}
                                                  stroke="none"
                                                  isAnimationActive={!prefersReducedMotion}
                                                  animationDuration={prefersReducedMotion ? 0 : 450}
                                                >
                                                  {deviceSlices.map((slice) => (
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
                                                      `${rawValue.toLocaleString()} plans`,
                                                      `${payload.label} (${payload.percentage.toFixed(1)}%)`,
                                                    ]
                                                  }}
                                                />
                                              </PieChart>
                                            </ResponsiveContainer>
                                          </div>
                                          <ul className="max-h-44 overflow-y-auto space-y-1.5 pr-1">
                                            {deviceSlices.map((slice) => (
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
                                      ) : (
                                        <div className="mt-2 text-xs text-slate-500">
                                          {mixRow.deviceMessage || activeRegionalSecondaryDescriptor.missingText}
                                        </div>
                                      )}
                                    </div>
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
