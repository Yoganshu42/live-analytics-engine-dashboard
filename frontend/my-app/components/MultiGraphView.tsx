"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Maximize2 } from "lucide-react"
import { AnimatePresence, motion, useReducedMotion } from "framer-motion"
import {
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Sector,
  Tooltip as RechartsTooltip,
} from "recharts"

import GraphView, { prefetchGraphData } from "@/components/GraphView"
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
} | null

type NavigableGraph = {
  group: string
  sectionTitle: string
  metric: string
  dimension: string
  bucket?: "day" | "week" | "month"
  chartType?: GraphChartType
  tooltipMetricOverride?: string
}

type GraphQueueItem = {
  dimension: string
  metric: string
  bucket?: "day" | "week" | "month"
  chartType?: GraphChartType
}

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

/* ---- metrics ---- */
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

/* ---- section titles ---- */
const GROUP_TITLES: Record<string, string> = {
  time: "Time Trend Desk",
  region: "Regional Signal Board",
  category: "Plan Mix Pulse",
  device_category: "Device Segment Pulse",
}

/* ---- order of sections ---- */
const GROUP_ORDER = ["time", "region", "category", "device_category"]
const FAST_LOAD_COUNT = 4
const DEFER_STEP_MS = 120

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

const toGraphKey = (
  dimension: string,
  metric: string,
  bucket?: "day" | "week" | "month",
  chartType?: GraphChartType
) => `${dimension}|${metric}|${bucket || ""}|${chartType || "bar"}`

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
  const isGodrej = source === "godrej"
  const isSamsungOverview = source === "samsung"
  const isGodrejClaims = isGodrej && datasetType === "claims"
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

  const [fullscreen, setFullscreen] = useState<FullscreenGraph>(null)
  const [zoom, setZoom] = useState(1)
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
  const [compareDropdownOpen, setCompareDropdownOpen] = useState(false)
  const [selectedComparisonStates, setSelectedComparisonStates] = useState<string[]>([])
  const [stateComparisonMixRows, setStateComparisonMixRows] = useState<StateComparisonMix[]>([])
  const [stateComparisonMixLoading, setStateComparisonMixLoading] = useState(false)
  const [stateComparisonMixError, setStateComparisonMixError] = useState<string | null>(null)
  const lastInsightsKeyRef = useRef("")
  const compareDropdownRef = useRef<HTMLDivElement | null>(null)
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
  const isBreakdownMetricSupported = useMemo(() => {
    const metricKey = (fullscreen?.metric || "").trim().toLowerCase()
    return metricKey !== "loss_ratio"
  }, [fullscreen])
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
            const visibleMetrics =
              datasetType === "sales"
                ? preset.metrics.filter((m: string) => SALES_METRICS.includes(m))
                : preset.metrics.filter((m: string) => CLAIMS_METRICS.includes(m))
            return { preset, visibleMetrics }
          })
          .filter(entry => entry.visibleMetrics.length > 0)
        return { group, entries }
      })
      .filter(section => section.entries.length > 0)
  }, [isSamsungOverview, activeGroupOrder, activePresets, datasetType])

  const graphQueue = useMemo<GraphQueueItem[]>(() => {
    if (isSamsungOverview) {
      return samsungOverviewCards.map((card) => ({
        dimension: card.dimension,
        metric: card.metric,
        bucket: card.bucket,
        chartType: card.chartType,
      }))
    }
    return sectionConfigs.flatMap(section =>
      section.entries.flatMap(entry =>
        entry.visibleMetrics.map(metric => ({
          dimension: entry.preset.dimension,
          metric,
          bucket: entry.preset.bucket,
        }))
      )
    )
  }, [isSamsungOverview, samsungOverviewCards, sectionConfigs])

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
    const out: NavigableGraph[] = []
    sectionConfigs.forEach(({ group, entries }) => {
      const sectionTitle = getSectionTitle(group)
      entries.forEach(({ preset, visibleMetrics }) => {
        visibleMetrics.forEach((metric: string) => {
          out.push({
            group,
            sectionTitle,
            metric,
            dimension: preset.dimension,
            bucket: preset.bucket,
            chartType: "bar",
          })
        })
      })
    })
    return out
  }, [isSamsungOverview, samsungOverviewCards, sectionConfigs, getSectionTitle, datasetType])

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

  const graphOrderIndex = useMemo(() => {
    const map = new Map<string, number>()
    graphQueue.forEach((item, idx) => {
      map.set(toGraphKey(item.dimension, item.metric, item.bucket, item.chartType), idx)
    })
    return map
  }, [graphQueue])

  useEffect(() => {
    const topGraphs = graphQueue.slice(0, FAST_LOAD_COUNT)
    if (!topGraphs.length) return
    Promise.allSettled(
      topGraphs.map(item =>
        prefetchGraphData({
          source,
          dimension: item.dimension,
          metric: item.metric,
          datasetType,
          bucket: item.bucket,
          jobId,
          from_date: fromDate,
          to_date: toDate,
        })
      )
    ).catch(() => {
      // Prefetch failures should not block rendering.
    })
  }, [graphQueue, source, datasetType, jobId, fromDate, toDate])

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
                dimension: "plan_category",
                state: stateLabel,
              }),
              fetchCategoryPercentage({
                ...baseParams,
                dimension: "device_plan_category",
                state: stateLabel,
              }),
            ])

            const planRows = normalizeCategoryRows(planRes.rows || [])
            const deviceRows = normalizeCategoryRows(deviceRes.rows || [])

            return {
              state: stateLabel,
              metricValue,
              planRows,
              deviceRows,
              planMessage: planRows.length
                ? undefined
                : (planRes.message || `No plan-category data for ${stateLabel}.`),
              deviceMessage: deviceRows.length
                ? undefined
                : (deviceRes.message || `No device-category data for ${stateLabel}.`),
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
            .filter((row) => !row.planRows.length && !row.deviceRows.length)
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
  ])

  useEffect(() => {
    if (!compareDropdownOpen) return
    const onPointerDown = (event: MouseEvent) => {
      const target = event.target as Node | null
      if (!target) return
      if (compareDropdownRef.current && !compareDropdownRef.current.contains(target)) {
        setCompareDropdownOpen(false)
      }
    }
    document.addEventListener("mousedown", onPointerDown)
    return () => {
      document.removeEventListener("mousedown", onPointerDown)
    }
  }, [compareDropdownOpen])

  const toggleComparisonState = (stateLabel: string) => {
    setSelectedComparisonStates((prev) => (
      prev.includes(stateLabel)
        ? prev.filter((state) => state !== stateLabel)
        : [...prev, stateLabel]
    ))
  }

  const todayIso = useMemo(() => new Date().toISOString().slice(0, 10), [])
  const pickerMaxDate = useMemo(() => {
    const candidates = [resetToDate, toDate, fullscreenToDate]
      .map((value) => (value || "").trim())
      .filter(Boolean)
    const futureCandidate = candidates
      .filter((value) => value > todayIso)
      .sort()
      .pop()
    return futureCandidate || todayIso
  }, [resetToDate, toDate, fullscreenToDate, todayIso])

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
    setZoom(1)
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
    setCompareDropdownOpen(false)
    setSelectedComparisonStates([])
    setStateComparisonMixRows([])
    setStateComparisonMixLoading(false)
    setStateComparisonMixError(null)
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
    setCompareDropdownOpen(false)
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
    const queueKey = toGraphKey(card.dimension, card.metric, card.bucket, card.chartType)
    const queueIndex = graphOrderIndex.get(queueKey) ?? 0
    const fetchDelayMs =
      queueIndex < FAST_LOAD_COUNT
        ? 0
        : Math.min((queueIndex - FAST_LOAD_COUNT + 1) * DEFER_STEP_MS, 1400)

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

          <GraphView
            source={source}
            dimension={card.dimension}
            metric={card.metric}
            datasetType={datasetType}
            bucket={card.bucket}
            jobId={jobId}
            fromDate={fromDate}
            toDate={toDate}
            fetchDelayMs={fetchDelayMs}
            deferUntilVisible={queueIndex >= FAST_LOAD_COUNT}
            chartType={card.chartType}
            tooltipMetricOverride={card.tooltipMetricOverride}
            heightClassName={layout === "main" ? "h-[360px] sm:h-[430px]" : "h-[300px] sm:h-[340px]"}
          />
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
      ) : sectionConfigs.map(({ group, entries }) => {
        const sectionTitle = getSectionTitle(group)
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
                    {datasetType === "sales"
                      ? "Track premium movement and volume contribution across this lens."
                      : "Track claims cost, ratio pressure, and volume contribution across this lens."}
                  </p>
                </div>
                <div className="rounded-full border border-slate-200 bg-white/90 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">
                  {datasetType === "sales" ? "Sales Lens" : "Claims Lens"}
                </div>
              </div>

              <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                {entries.map(({ preset, visibleMetrics }) => {
                  return visibleMetrics.map((metric: string) => {
                    const queueKey = toGraphKey(preset.dimension, metric, preset.bucket, "bar")
                    const queueIndex = graphOrderIndex.get(queueKey) ?? 0
                    const fetchDelayMs =
                      queueIndex < FAST_LOAD_COUNT
                        ? 0
                        : Math.min((queueIndex - FAST_LOAD_COUNT + 1) * DEFER_STEP_MS, 1400)
                    const hasPrevGraph = queueIndex > 0
                    const hasNextGraph = queueIndex < navigableGraphs.length - 1
                    return (
                    <motion.div
                      key={`${preset.dimension}-${metric}`}
                      initial={prefersReducedMotion ? false : { opacity: 0, y: 16 }}
                      animate={prefersReducedMotion ? undefined : { opacity: 1, y: 0 }}
                      transition={prefersReducedMotion ? undefined : { duration: 0.35, ease: "easeOut" }}
                      whileHover={prefersReducedMotion ? undefined : { y: -6, scale: 1.01 }}
                      className="smooth-surface group relative overflow-hidden rounded-2xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/60 p-3 shadow-sm transition-shadow hover:shadow-[0_18px_40px_-26px_rgba(15,23,42,0.5)] sm:p-4"
                    >
                      <div className="pointer-events-none absolute -top-14 right-[-52px] h-32 w-32 rounded-full bg-cyan-100/60 blur-2xl" />
                      <div className="relative">
                        <div className="mb-3 flex items-start justify-between gap-3">
                          <div className="min-w-0">
                            <div className="text-sm font-bold leading-snug text-slate-800">
                              {getGraphTitle(metric, preset.dimension, source)}
                            </div>
                            <div className="mt-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                              {getDimensionLabel(preset.dimension, source)}
                            </div>
                          </div>

                          <button
                            className="rounded-lg border border-slate-200 bg-white p-1.5 text-slate-600 transition-colors hover:border-slate-300 hover:text-slate-900"
                            onClick={() => {
                              handleOpenFullscreen({
                                metric,
                                dimension: preset.dimension,
                                bucket: preset.bucket,
                                chartType: "bar",
                              })
                            }}
                          >
                            <Maximize2 size={16} />
                          </button>
                        </div>
                        <div className="mb-3 flex items-center gap-2">
                          <button
                            className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
                            disabled={!hasPrevGraph}
                            onClick={() => handleTraverseGraphFromIndex(queueIndex, -1)}
                          >
                            Previous
                          </button>
                          <button
                            className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
                            disabled={!hasNextGraph}
                            onClick={() => handleTraverseGraphFromIndex(queueIndex, 1)}
                          >
                            Next
                          </button>
                        </div>

                    <GraphView
                      source={source}
                      dimension={preset.dimension}
                      metric={metric}
                      datasetType={datasetType}
                      bucket={preset.bucket}
                      jobId={jobId}
                      fromDate={fromDate}
                      toDate={toDate}
                      fetchDelayMs={fetchDelayMs}
                      deferUntilVisible={queueIndex >= FAST_LOAD_COUNT}
                    />
                      </div>
                    </motion.div>
                    )
                  })
                })}
              </div>
            </div>
          </div>
        )
      })}

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

                <div className="sticky top-[116px] z-10 border-b border-slate-200/80 bg-white/90 px-3 py-3 backdrop-blur sm:top-[73px] sm:px-4 md:px-6">
                  <div className="flex flex-wrap items-center gap-2.5 sm:gap-3">
                    <div className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-400">
                      Date Window
                    </div>
                    <div className="w-full max-w-[360px]">
                      <DateRangePicker
                        draftFromDate={fullscreenFromDate}
                        draftToDate={fullscreenToDate}
                        minDate={resetFromDate || fromDate || undefined}
                        maxDate={pickerMaxDate}
                        compact
                        onDraftChange={(from, to) => {
                          setFullscreenFromDate(from)
                          setFullscreenToDate(to)
                        }}
                        onApply={handleApplyFullscreenDateRange}
                        onReset={handleResetFullscreenDateRange}
                      />
                    </div>
                    <div className="flex w-full flex-wrap items-center gap-2 sm:ml-auto sm:w-auto sm:justify-end">
                      {isStateFullscreen && (
                        <div className="flex w-full items-center gap-2 rounded-xl border border-slate-200 bg-white px-3 py-2 sm:w-auto">
                          <span className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-400">
                            {`Focus ${geographyLabel}`}
                          </span>
                          <select
                            value={activeSelectedState}
                            onChange={(e) => {
                              setSelectedState(e.target.value)
                              setActiveCityName("")
                            }}
                            className="w-full min-w-0 rounded-lg border border-slate-200 px-2 py-1 text-xs font-semibold text-slate-700 sm:min-w-[180px]"
                          >
                            <option value="">{`Choose ${geographyLabel}`}</option>
                            {stateOptions.map((stateOption) => (
                              <option key={stateOption} value={stateOption}>
                                {stateOption}
                              </option>
                            ))}
                          </select>
                        </div>
                      )}
                    {isStateFullscreen && (
                      <div ref={compareDropdownRef} className="relative">
                        <button
                          className="text-xs font-bold px-3 py-2 rounded-xl border border-slate-200 bg-white hover:bg-slate-50 transition-colors"
                          onClick={() => setCompareDropdownOpen((open) => !open)}
                        >
                          {`Compare ${geographyLabelPlural} (${activeComparisonStates.length})`}
                        </button>
                        {compareDropdownOpen && (
                          <div className="absolute right-0 z-30 mt-2 w-[min(18rem,calc(100vw-2.5rem))] rounded-xl border border-slate-200 bg-white p-3 shadow-lg">
                            <div className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-400 mb-2">
                              {`Pick ${geographyLabelPlural} To Compare`}
                            </div>
                            <div className="max-h-56 overflow-y-auto space-y-2 pr-1">
                              {stateOptions.map((stateLabel) => (
                                <label key={stateLabel} className="flex items-center gap-2 text-xs text-slate-700">
                                  <input
                                    type="checkbox"
                                    checked={activeComparisonStates.includes(stateLabel)}
                                    onChange={() => toggleComparisonState(stateLabel)}
                                  />
                                  <span>{stateLabel}</span>
                                </label>
                              ))}
                            </div>
                            <div className="mt-3 flex items-center justify-between gap-2">
                              <button
                                className="text-[11px] font-semibold text-slate-500 hover:text-slate-700"
                                onClick={() => setSelectedComparisonStates([])}
                              >
                                Clear
                              </button>
                              <button
                                className="text-[11px] font-semibold text-slate-500 hover:text-slate-700"
                                onClick={() => setCompareDropdownOpen(false)}
                              >
                                Done
                              </button>
                            </div>
                          </div>
                        )}
                      </div>
                    )}
                    <button
                      className="text-xs font-bold px-3 py-2 rounded-xl border border-slate-200 bg-white hover:bg-slate-50 transition-colors"
                      onClick={() => setZoom(z => Math.max(0.7, Number((z - 0.1).toFixed(2))))}
                    >
                      Zoom Out
                    </button>
                    <button
                      className="text-xs font-bold px-3 py-2 rounded-xl bg-slate-900 text-white hover:bg-slate-800 transition-colors"
                      onClick={() => setZoom(z => Math.min(1.6, Number((z + 0.1).toFixed(2))))}
                    >
                      Zoom In
                    </button>
                  </div>
                </div>
              </div>

              <div className="flex min-h-[64vh] items-center justify-center px-3 py-4 sm:min-h-[70vh] sm:px-6 sm:py-6">
                <div
                  className="w-full max-w-6xl origin-center transition-transform"
                  style={{ transform: `scale(${zoom})` }}
                >
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
                    heightClassName={isSamsungOverview ? "h-[56vh]" : undefined}
                    onDataReady={setOpenedGraphData}
                  />
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
                            {`For every selected ${geographyLabel.toLowerCase()} in Compare ${geographyLabelPlural}, view gradient pie splits for Plan Category and Device Plan Category.`}
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
                                        Plan Category Division
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
                                          {mixRow.planMessage || "No plan-category data found."}
                                        </div>
                                      )}
                                    </div>

                                    <div className="rounded-xl border border-slate-200 bg-white p-3">
                                      <div className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-400">
                                        Device Plan Category Division
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
                                          {mixRow.deviceMessage || "No device-category data found."}
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
                  <div className="mt-6 rounded-3xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/50 p-4 shadow-[0_18px_40px_-28px_rgba(15,23,42,0.45)] sm:p-5">
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

