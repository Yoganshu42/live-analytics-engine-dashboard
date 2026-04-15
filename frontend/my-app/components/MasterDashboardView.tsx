"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { useReducedMotion } from "framer-motion"
import {
  BarChart3,
  CircleDollarSign,
  Layers,
  Maximize2,
  Percent,
  Shield,
  X,
  type LucideIcon,
} from "lucide-react"
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import DateRangePicker from "@/components/DateRangePicker"
import {
  clearMasterDashboardCache,
  fetchMasterDashboard,
  type MasterDashboardResponse,
} from "@/app/lib/api"
import { VISIBLE_SAMSUNG_PARTNERS, type SamsungPartnerKey } from "@/lib/samsungPartners"

type Props = {
  jobId?: string | null
  fromDate?: string
  toDate?: string
  refreshTick?: number
  onDateRangeApply?: (nextFrom: string, nextTo: string) => void
}

type Summary = {
  gross_premium?: number
  earned_premium?: number
  zopper_earned_premium?: number
}

type KpiValues = {
  gross: number
  earned: number
  zopper: number
  claims: number
}

type MasterData = {
  totals: KpiValues
  samsung: {
    kpis: KpiValues
    salesRows: Array<Record<string, number | string>>
    claimsRows: Array<Record<string, number | string>>
  }
  reliance: {
    kpis: KpiValues
    salesRows: Array<Record<string, number | string>>
    claimsRows: Array<Record<string, number | string>>
  }
  godrej: {
    kpis: KpiValues
    salesRows: Array<Record<string, number | string>>
    claimsRows: Array<Record<string, number | string>>
  }
  hitachi: {
    kpis: KpiValues
    salesRows: Array<Record<string, number | string>>
    claimsRows: Array<Record<string, number | string>>
  }
}

type ExpandableChartId =
  | "samsung-premium"
  | "samsung-claims"
  | "reliance-premium"
  | "reliance-claims"
  | "godrej-premium"
  | "godrej-claims"
  | "hitachi-premium"
  | "hitachi-claims"

type KpiCardMeta = {
  key: keyof KpiValues
  label: string
  icon: LucideIcon
  tone: string
}

type ExtraKpiMeta = {
  label: string
  icon: LucideIcon
  tone: string
}

type MonthPoint = {
  label: string
  value: number
}

const KPI_META: KpiCardMeta[] = [
  {
    key: "gross",
    label: "Gross Premium",
    icon: CircleDollarSign,
    tone: "from-[#1f7de2] to-[#1c66c8]",
  },
  {
    key: "earned",
    label: "Earned Premium",
    icon: BarChart3,
    tone: "from-[#13a1bf] to-[#0e859f]",
  },
  {
    key: "zopper",
    label: "Zopper Earned Premium",
    icon: Layers,
    tone: "from-[#d6a03b] to-[#b9852b]",
  },
  {
    key: "claims",
    label: "Claims Cost",
    icon: Shield,
    tone: "from-[#df7a5c] to-[#c45b47]",
  },
]

const LOSS_RATIO_META: ExtraKpiMeta = {
  label: "Overall Loss Ratio",
  icon: Percent,
  tone: "from-[#16a34a] to-[#15803d]",
}

const CHART_TITLES: Record<ExpandableChartId, string> = {
  "samsung-premium": "Samsung Premium Trend (Gross Scale)",
  "samsung-claims": "Samsung Claims Cost Trend",
  "reliance-premium": "Reliance Premium Trend",
  "reliance-claims": "Reliance Claims Cost Trend",
  "godrej-premium": "Godrej Premium Trend",
  "godrej-claims": "Godrej Claims Cost Trend",
  "hitachi-premium": "Hitachi Premium Trend",
  "hitachi-claims": "Hitachi Claims Cost Trend",
}

const toSafeKey = (value: string) =>
  value
    .toLowerCase()
    .trim()
    .replace(/\s+/g, "_")
    .replace(/[()%'.]/g, "")

const asNumber = (value: unknown) => {
  const parsed = Number(value ?? 0)
  return Number.isFinite(parsed) ? parsed : 0
}

const money = (value: number) => {
  const abs = Math.abs(value)
  if (abs >= 1e7) return `Rs ${(value / 1e7).toFixed(2)} Cr`
  if (abs >= 1e5) return `Rs ${(value / 1e5).toFixed(2)} L`
  if (abs >= 1e3) return `Rs ${(value / 1e3).toFixed(1)} K`
  return `Rs ${new Intl.NumberFormat("en-IN").format(Math.round(value))}`
}

const axisMoney = (value: number) => {
  const abs = Math.abs(value)
  if (abs >= 1e7) return `${(value / 1e7).toFixed(1)}Cr`
  if (abs >= 1e5) return `${(value / 1e5).toFixed(1)}L`
  if (abs >= 1e3) return `${(value / 1e3).toFixed(1)}K`
  return `${Math.round(value)}`
}

const percent = (value: number) => `${value.toFixed(2)}%`

const toRequestKey = (jobId: string | null | undefined, fromDate?: string, toDate?: string) =>
  `${jobId || ""}|${fromDate || ""}|${toDate || ""}`

const monthToLabel = (date: Date) =>
  date.toLocaleString("en-US", { month: "short" }) + "-" + String(date.getFullYear()).slice(2)

const monthToBucket = (date: Date) => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, "0")
  return `${year}-${month}-01`
}

const parseMonthDate = (raw: unknown): Date | null => {
  const text = String(raw ?? "").trim()
  if (!text) return null

  const shortMatch = text.match(/^([A-Za-z]{3})[-/\s](\d{2}|\d{4})$/)
  if (shortMatch) {
    const monthMap: Record<string, number> = {
      jan: 0,
      feb: 1,
      mar: 2,
      apr: 3,
      may: 4,
      jun: 5,
      jul: 6,
      aug: 7,
      sep: 8,
      oct: 9,
      nov: 10,
      dec: 11,
    }
    const month = monthMap[shortMatch[1].toLowerCase()]
    if (month !== undefined) {
      const rawYear = Number(shortMatch[2])
      const year = shortMatch[2].length === 2 ? 2000 + rawYear : rawYear
      return new Date(year, month, 1)
    }
  }

  const yearMonth = text.match(/^(\d{4})-(\d{2})$/)
  if (yearMonth) {
    return new Date(Number(yearMonth[1]), Number(yearMonth[2]) - 1, 1)
  }

  const parsed = new Date(text)
  if (Number.isNaN(parsed.getTime())) return null
  return new Date(parsed.getFullYear(), parsed.getMonth(), 1)
}

const monthFromRow = (row: Record<string, unknown>) => {
  const monthKey = Object.keys(row).find((key) => {
    const safe = toSafeKey(key)
    return safe === "month" || safe === "date" || safe.includes("month")
  })
  if (!monthKey) return null
  return parseMonthDate(row[monthKey])
}

const metricFromRow = (row: Record<string, unknown>, metric: string) => {
  const wanted = toSafeKey(metric)
  const key = Object.keys(row).find((candidate) => toSafeKey(candidate) === wanted)
  if (key) return asNumber(row[key])

  if (metric === "claims") {
    const fallbackKey = Object.keys(row).find((candidate) => toSafeKey(candidate) === "net_claims")
    if (fallbackKey) return asNumber(row[fallbackKey])
  }

  const monthKey = Object.keys(row).find((candidate) => {
    const safe = toSafeKey(candidate)
    return safe === "month" || safe === "date" || safe.includes("month")
  })
  const numericFallback = Object.keys(row).find((candidate) => {
    if (candidate === monthKey) return false
    return Number.isFinite(Number(row[candidate]))
  })
  return numericFallback ? asNumber(row[numericFallback]) : 0
}

const toMonthlySeries = (rows: Array<Record<string, unknown>>, metric: string) => {
  const map = new Map<string, MonthPoint>()
  rows.forEach((row) => {
    const monthDate = monthFromRow(row)
    if (!monthDate) return
    const bucket = monthToBucket(monthDate)
    const existing = map.get(bucket)
    const current = metricFromRow(row, metric)
    map.set(bucket, {
      label: monthToLabel(monthDate),
      value: (existing?.value || 0) + current,
    })
  })
  return map
}

const mergeSeries = (seriesByKey: Record<string, Map<string, MonthPoint>>) => {
  const monthKeys = new Set<string>()
  Object.values(seriesByKey).forEach((series) => {
    series.forEach((_value, key) => monthKeys.add(key))
  })

  const merged = Array.from(monthKeys)
    .sort((a, b) => new Date(a).getTime() - new Date(b).getTime())
    .map((monthKey) => {
      const base: Record<string, number | string> = {
        month: monthToLabel(new Date(monthKey)),
      }
      Object.entries(seriesByKey).forEach(([key, series]) => {
        base[key] = series.get(monthKey)?.value || 0
      })
      return base
    })

  if (merged.length !== 1) return merged

  const center = parseMonthDate(merged[0].month)
  if (!center) return merged

  const buildZeroRow = (date: Date) => {
    const row: Record<string, number | string> = { month: monthToLabel(date) }
    Object.keys(merged[0]).forEach((key) => {
      if (key === "month") return
      row[key] = 0
    })
    return row
  }

  const prev = new Date(center)
  prev.setMonth(center.getMonth() - 1)
  const next = new Date(center)
  next.setMonth(center.getMonth() + 1)

  return [buildZeroRow(prev), merged[0], buildZeroRow(next)]
}

const toKpis = (sales: Summary | null, claims: Summary | null): KpiValues => ({
  gross: asNumber(sales?.gross_premium),
  earned: asNumber(sales?.earned_premium),
  zopper: asNumber(sales?.zopper_earned_premium),
  claims: asNumber(claims?.gross_premium),
})

const sumMetricRows = (
  rows: Array<Record<string, unknown>>,
  metric: string
) => rows.reduce((total, row) => total + metricFromRow(row, metric), 0)

const toLiveKpis = ({
  salesSummary,
  claimsSummary,
  grossRows,
  earnedRows,
  zopperRows,
  claimsRows,
}: {
  salesSummary: Summary | null
  claimsSummary: Summary | null
  grossRows: Array<Record<string, unknown>>
  earnedRows: Array<Record<string, unknown>>
  zopperRows: Array<Record<string, unknown>>
  claimsRows: Array<Record<string, unknown>>
}): KpiValues => ({
  gross: grossRows.length ? sumMetricRows(grossRows, "gross_premium") : asNumber(salesSummary?.gross_premium),
  earned: earnedRows.length ? sumMetricRows(earnedRows, "earned_premium") : asNumber(salesSummary?.earned_premium),
  zopper: zopperRows.length ? sumMetricRows(zopperRows, "zopper_earned_premium") : asNumber(salesSummary?.zopper_earned_premium),
  claims: claimsRows.length ? sumMetricRows(claimsRows, "claims") : asNumber(claimsSummary?.gross_premium),
})

const toSummaryFirstKpis = ({
  salesSummary,
  claimsSummary,
  grossRows,
  earnedRows,
  zopperRows,
  claimsRows,
}: {
  salesSummary: Summary | null
  claimsSummary: Summary | null
  grossRows: Array<Record<string, unknown>>
  earnedRows: Array<Record<string, unknown>>
  zopperRows: Array<Record<string, unknown>>
  claimsRows: Array<Record<string, unknown>>
}): KpiValues => (
  hasSummaryPayload(salesSummary) || hasSummaryPayload(claimsSummary)
    ? toKpis(salesSummary, claimsSummary)
    : toLiveKpis({
        salesSummary,
        claimsSummary,
        grossRows,
        earnedRows,
        zopperRows,
        claimsRows,
      })
)

const addKpis = (a: KpiValues, b: KpiValues): KpiValues => ({
  gross: a.gross + b.gross,
  earned: a.earned + b.earned,
  zopper: a.zopper + b.zopper,
  claims: a.claims + b.claims,
})

const hasSummaryPayload = (summary: Summary | null | undefined) =>
  Boolean(summary) && Object.values(summary || {}).some((value) => Math.abs(asNumber(value)) > 0)

const hasChartSignal = (
  rows: Array<Record<string, number | string>>,
  keys: string[],
) => rows.some((row) => keys.some((key) => Math.abs(asNumber(row[key])) > 0))

const SAMSUNG_MASTER_PREFIX: Record<SamsungPartnerKey, string> = {
  samsung_vs: "vs",
  samsung_croma: "croma",
  samsung_reliance_digital: "reliance_digital",
}

const toMasterData = (payload: MasterDashboardResponse): MasterData => {
  const summaries = payload?.summaries || {}
  const rows = payload?.rows || {}

  const samsungSalesSummary = summaries.samsung_sales || {}
  const samsungClaimsSummary = summaries.samsung_claims || {}
  const relianceSalesSummary = summaries.reliance_sales || {}
  const godrejSalesSummary = summaries.godrej_sales || {}
  const hitachiSalesSummary = summaries.hitachi_sales || {}

  const relianceClaimsSummary = summaries.reliance_claims || {}
  const godrejClaimsSummary = summaries.godrej_claims || {}
  const hitachiClaimsSummary = summaries.hitachi_claims || {}

  const relianceGrossRows = Array.isArray(rows.reliance_gross) ? rows.reliance_gross : []
  const relianceEarnedRows = Array.isArray(rows.reliance_earned) ? rows.reliance_earned : []
  const relianceZopperRows = Array.isArray(rows.reliance_zopper) ? rows.reliance_zopper : []
  const godrejGrossRows = Array.isArray(rows.godrej_gross) ? rows.godrej_gross : []
  const godrejEarnedRows = Array.isArray(rows.godrej_earned) ? rows.godrej_earned : []
  const godrejZopperRows = Array.isArray(rows.godrej_zopper) ? rows.godrej_zopper : []
  const hitachiGrossRows = Array.isArray(rows.hitachi_gross) ? rows.hitachi_gross : []
  const hitachiEarnedRows = Array.isArray(rows.hitachi_earned) ? rows.hitachi_earned : []
  const hitachiZopperRows = Array.isArray(rows.hitachi_zopper) ? rows.hitachi_zopper : []

  const relianceClaimsRows = Array.isArray(rows.reliance_claims) ? rows.reliance_claims : []
  const godrejClaimsRows = Array.isArray(rows.godrej_claims) ? rows.godrej_claims : []
  const hitachiClaimsRows = Array.isArray(rows.hitachi_claims) ? rows.hitachi_claims : []

  const samsungPartnerSnapshots = VISIBLE_SAMSUNG_PARTNERS.map((partner) => {
    const prefix = SAMSUNG_MASTER_PREFIX[partner.key]
    return {
      partner,
      prefix,
      salesSummary: summaries[`${partner.key}_sales`] || {},
      claimsSummary: summaries[`${partner.key}_claims`] || {},
      grossRows: Array.isArray(rows[`${partner.key}_gross`]) ? rows[`${partner.key}_gross`] : [],
      earnedRows: Array.isArray(rows[`${partner.key}_earned`]) ? rows[`${partner.key}_earned`] : [],
      zopperRows: Array.isArray(rows[`${partner.key}_zopper`]) ? rows[`${partner.key}_zopper`] : [],
      claimsRows: Array.isArray(rows[`${partner.key}_claims`]) ? rows[`${partner.key}_claims`] : [],
    }
  })

  const samsungSalesSeries: Record<string, Map<string, MonthPoint>> = {}
  const samsungClaimsSeries: Record<string, Map<string, MonthPoint>> = {}
  samsungPartnerSnapshots.forEach((snapshot) => {
    samsungSalesSeries[`${snapshot.prefix}_gross`] = toMonthlySeries(snapshot.grossRows, "gross_premium")
    samsungSalesSeries[`${snapshot.prefix}_earned`] = toMonthlySeries(snapshot.earnedRows, "earned_premium")
    samsungSalesSeries[`${snapshot.prefix}_zopper`] = toMonthlySeries(snapshot.zopperRows, "zopper_earned_premium")
    samsungClaimsSeries[`${snapshot.prefix}_claims`] = toMonthlySeries(snapshot.claimsRows, "claims")
  })

  const samsungSalesRows = mergeSeries(samsungSalesSeries)
  const samsungClaimsChartRows = mergeSeries(samsungClaimsSeries)
  const samsungGrossRows = samsungPartnerSnapshots.flatMap((snapshot) => snapshot.grossRows)
  const samsungEarnedRows = samsungPartnerSnapshots.flatMap((snapshot) => snapshot.earnedRows)
  const samsungZopperRows = samsungPartnerSnapshots.flatMap((snapshot) => snapshot.zopperRows)
  const samsungClaimsRows = samsungPartnerSnapshots.flatMap((snapshot) => snapshot.claimsRows)

  const relianceSalesRows = mergeSeries({
    gross: toMonthlySeries(relianceGrossRows, "gross_premium"),
    earned: toMonthlySeries(relianceEarnedRows, "earned_premium"),
    zopper: toMonthlySeries(relianceZopperRows, "zopper_earned_premium"),
  })

  const relianceClaimsChartRows = mergeSeries({
    claims: toMonthlySeries(relianceClaimsRows, "claims"),
  })

  const godrejSalesRows = mergeSeries({
    gross: toMonthlySeries(godrejGrossRows, "gross_premium"),
    earned: toMonthlySeries(godrejEarnedRows, "earned_premium"),
    zopper: toMonthlySeries(godrejZopperRows, "zopper_earned_premium"),
  })

  const godrejClaimsChartRows = mergeSeries({
    claims: toMonthlySeries(godrejClaimsRows, "claims"),
  })

  const hitachiSalesRows = mergeSeries({
    gross: toMonthlySeries(hitachiGrossRows, "gross_premium"),
    earned: toMonthlySeries(hitachiEarnedRows, "earned_premium"),
    zopper: toMonthlySeries(hitachiZopperRows, "zopper_earned_premium"),
  })

  const hitachiClaimsChartRows = mergeSeries({
    claims: toMonthlySeries(hitachiClaimsRows, "claims"),
  })

  const samsungKpis = toSummaryFirstKpis({
    salesSummary: samsungSalesSummary as Summary,
    claimsSummary: samsungClaimsSummary as Summary,
    grossRows: samsungGrossRows,
    earnedRows: samsungEarnedRows,
    zopperRows: samsungZopperRows,
    claimsRows: samsungClaimsRows,
  })

  const relianceKpis = toSummaryFirstKpis({
    salesSummary: relianceSalesSummary as Summary,
    claimsSummary: relianceClaimsSummary as Summary,
    grossRows: relianceGrossRows,
    earnedRows: relianceEarnedRows,
    zopperRows: relianceZopperRows,
    claimsRows: relianceClaimsRows,
  })
  const godrejKpis = toSummaryFirstKpis({
    salesSummary: godrejSalesSummary as Summary,
    claimsSummary: godrejClaimsSummary as Summary,
    grossRows: godrejGrossRows,
    earnedRows: godrejEarnedRows,
    zopperRows: godrejZopperRows,
    claimsRows: godrejClaimsRows,
  })
  const hitachiKpis = toSummaryFirstKpis({
    salesSummary: hitachiSalesSummary as Summary,
    claimsSummary: hitachiClaimsSummary as Summary,
    grossRows: hitachiGrossRows,
    earnedRows: hitachiEarnedRows,
    zopperRows: hitachiZopperRows,
    claimsRows: hitachiClaimsRows,
  })
  const totals = addKpis(addKpis(addKpis(samsungKpis, relianceKpis), godrejKpis), hitachiKpis)

  return {
    totals,
    samsung: { kpis: samsungKpis, salesRows: samsungSalesRows, claimsRows: samsungClaimsChartRows },
    reliance: { kpis: relianceKpis, salesRows: relianceSalesRows, claimsRows: relianceClaimsChartRows },
    godrej: { kpis: godrejKpis, salesRows: godrejSalesRows, claimsRows: godrejClaimsChartRows },
    hitachi: { kpis: hitachiKpis, salesRows: hitachiSalesRows, claimsRows: hitachiClaimsChartRows },
  }
}

const ArrowPointDot = ({ cx, cy, stroke }: { cx?: number; cy?: number; stroke?: string }) => {
  if (cx == null || cy == null) return null
  return (
    <path
      d={`M ${cx} ${cy - 5} L ${cx + 4} ${cy + 4} L ${cx - 4} ${cy + 4} Z`}
      fill={stroke || "#1f6fe5"}
      stroke="#ffffff"
      strokeWidth={1}
    />
  )
}

function KpiStrip({
  title,
  values,
  showTotalLossRatio = false,
  lossRatioLabel = "Loss Ratio",
}: {
  title: string
  values: KpiValues
  showTotalLossRatio?: boolean
  lossRatioLabel?: string
}) {
  const totalLossRatio = values.zopper > 0 ? (values.claims / values.zopper) * 100 : 0
  return (
    <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="mb-3 text-[11px] font-black uppercase tracking-[0.14em] text-slate-500">{title}</div>
      <div className={`grid grid-cols-1 gap-2 sm:grid-cols-2 ${showTotalLossRatio ? "xl:grid-cols-5" : "xl:grid-cols-4"}`}>
        {KPI_META.map((meta) => {
          const Icon = meta.icon
          return (
            <div
              key={`${title}-${meta.key}`}
              className={`relative overflow-hidden rounded-xl bg-gradient-to-br ${meta.tone} px-3 py-2.5 text-white`}
            >
              <div className="absolute -right-5 -top-5 h-14 w-14 rounded-full bg-white/10" />
              <div className="relative flex items-start justify-between gap-2">
                <div className="min-w-0">
                  <div className="truncate text-[14px] font-black">{money(values[meta.key])}</div>
                  <div className="text-[9px] font-bold uppercase tracking-[0.12em] text-white/85">
                    {meta.label}
                  </div>
                </div>
                <div className="rounded-lg bg-white/20 p-1.5">
                  <Icon className="h-3.5 w-3.5" />
                </div>
              </div>
            </div>
          )
        })}
        {showTotalLossRatio && (
          <div className={`relative overflow-hidden rounded-xl bg-gradient-to-br ${LOSS_RATIO_META.tone} px-3 py-2.5 text-white`}>
            <div className="absolute -right-5 -top-5 h-14 w-14 rounded-full bg-white/10" />
            <div className="relative flex items-start justify-between gap-2">
              <div className="min-w-0">
                <div className="truncate text-[14px] font-black">{percent(totalLossRatio)}</div>
                <div className="text-[9px] font-bold uppercase tracking-[0.12em] text-white/85">
                  {lossRatioLabel}
                </div>
              </div>
              <div className="rounded-lg bg-white/20 p-1.5">
                <LOSS_RATIO_META.icon className="h-3.5 w-3.5" />
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default function MasterDashboardView({
  jobId,
  fromDate = "",
  toDate = "",
  refreshTick = 0,
  onDateRangeApply,
}: Props) {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<MasterData | null>(null)
  const [expandedChart, setExpandedChart] = useState<ExpandableChartId | null>(null)

  const [defaultFromDate, setDefaultFromDate] = useState("")
  const [defaultToDate, setDefaultToDate] = useState("")
  const [draftFromDate, setDraftFromDate] = useState("")
  const [draftToDate, setDraftToDate] = useState("")
  const [localRefreshTick, setLocalRefreshTick] = useState(0)
  const lastAutoSyncKeyRef = useRef("")
  const requestSequenceRef = useRef(0)
  const activeRequestRef = useRef(0)
  const requestAbortRef = useRef<AbortController | null>(null)
  const prefersReducedMotion = useReducedMotion()

  const todayIso = useMemo(() => new Date().toISOString().slice(0, 10), [])
  const maxPickerDate = useMemo(
    () => (defaultToDate || todayIso),
    [defaultToDate, todayIso]
  )
  const orderedExternalRange = useMemo(() => {
    const nextFrom = (fromDate || "").trim()
    const nextTo = (toDate || "").trim()
    if (nextFrom && nextTo && nextFrom > nextTo) {
      return { from: nextTo, to: nextFrom }
    }
    return { from: nextFrom, to: nextTo }
  }, [fromDate, toDate])

  const loadMasterData = useCallback(
    async (
      nextFromDate?: string,
      nextToDate?: string,
      options?: { silent?: boolean; forceFresh?: boolean }
    ) => {
      const silent = Boolean(options?.silent)
      const forceFresh = Boolean(options?.forceFresh)
      const requestId = requestSequenceRef.current + 1
      requestSequenceRef.current = requestId
      activeRequestRef.current = requestId
      requestAbortRef.current?.abort()
      const controller = new AbortController()
      requestAbortRef.current = controller
      if (!silent) {
        setLoading(true)
        setError(null)
      }
      try {
        const params: Parameters<typeof fetchMasterDashboard>[0] = {}
        if (jobId) params.job_id = jobId
        if (nextFromDate) params.from_date = nextFromDate
        if (nextToDate) params.to_date = nextToDate
        const payload = await fetchMasterDashboard(params, {
          signal: controller.signal,
          forceFresh,
        })
        if (controller.signal.aborted || requestId !== activeRequestRef.current) {
          return null
        }
        setData(toMasterData(payload))
        setError(null)
        return payload
      } catch (err) {
        const aborted =
          controller.signal.aborted
          || (err instanceof Error && err.name === "AbortError")
          || requestId !== activeRequestRef.current
        if (aborted) {
          return null
        }
        console.error("Master dashboard load failed:", err)
        if (!silent) {
          setError("Unable to load Master Dashboard data.")
        }
        return null
      } finally {
        if (requestId === activeRequestRef.current && !controller.signal.aborted) {
          setLoading(false)
        }
      }
    },
    [jobId]
  )

  useEffect(() => {
    return () => {
      requestAbortRef.current?.abort()
    }
  }, [])

  useEffect(() => {
    let isCancelled = false

    const bootstrap = async () => {
      const payload = await loadMasterData()
      if (isCancelled || !payload) {
        return
      }
      const min = String(payload.date_bounds?.min_date || "").trim()
      const rawMax = String(payload.date_bounds?.max_date || "").trim()
      const max = rawMax || todayIso
      const effectiveMin = min && min <= max ? min : ""

      setDefaultFromDate(effectiveMin)
      setDefaultToDate(max)
      const nextFrom = orderedExternalRange.from || effectiveMin
      const nextTo = orderedExternalRange.to || max
      const defaultRequestKey = toRequestKey(jobId, nextFrom || undefined, nextTo || undefined)
      lastAutoSyncKeyRef.current = `${defaultRequestKey}|${refreshTick}|${localRefreshTick}`
      setDraftFromDate(nextFrom)
      setDraftToDate(nextTo)

      if ((orderedExternalRange.from || orderedExternalRange.to) && (nextFrom !== effectiveMin || nextTo !== max)) {
        void loadMasterData(nextFrom || undefined, nextTo || undefined, {
          silent: true,
          forceFresh: true,
        })
      }
    }

    bootstrap().catch(() => {
      // noop; error state is handled in loadMasterData
    })

    return () => {
      isCancelled = true
    }
  }, [jobId, todayIso, loadMasterData, orderedExternalRange.from, orderedExternalRange.to, refreshTick, localRefreshTick])

  useEffect(() => {
    if (typeof window === "undefined") return
    const handleRefresh = () => {
      clearMasterDashboardCache()
      setLocalRefreshTick((prev) => prev + 1)
    }
    const handleStorage = (event: StorageEvent) => {
      if (event.key === "dashboard_data_refresh_at") {
        handleRefresh()
      }
    }
    window.addEventListener("dashboard-data-refreshed", handleRefresh as EventListener)
    window.addEventListener("storage", handleStorage)
    return () => {
      window.removeEventListener("dashboard-data-refreshed", handleRefresh as EventListener)
      window.removeEventListener("storage", handleStorage)
    }
  }, [])

  useEffect(() => {
    const hasResolvedBaseRange = Boolean(defaultFromDate || defaultToDate)
    const hasExternalRange = Boolean(orderedExternalRange.from || orderedExternalRange.to)
    if (!hasResolvedBaseRange && !hasExternalRange) return

    const nextFrom = orderedExternalRange.from || defaultFromDate
    const nextTo = orderedExternalRange.to || defaultToDate
    if (!nextFrom && !nextTo) return

    const requestKey = toRequestKey(jobId, nextFrom || undefined, nextTo || undefined)
    const syncKey = `${requestKey}|${refreshTick}|${localRefreshTick}`
    if (syncKey === lastAutoSyncKeyRef.current) return
    setDraftFromDate(nextFrom)
    setDraftToDate(nextTo)
    lastAutoSyncKeyRef.current = syncKey
    void loadMasterData(nextFrom || undefined, nextTo || undefined, {
      silent: true,
      forceFresh: localRefreshTick > 0,
    })
  }, [
    jobId,
    orderedExternalRange.from,
    orderedExternalRange.to,
    defaultFromDate,
    defaultToDate,
    refreshTick,
    localRefreshTick,
    loadMasterData,
  ])

  const handleApplyDateRange = async (nextFromRaw: string, nextToRaw: string) => {
    const nextFrom = (nextFromRaw || "").trim()
    const nextTo = (nextToRaw || "").trim()
    const orderedFrom = nextFrom && nextTo && nextFrom > nextTo ? nextTo : nextFrom
    const orderedTo = nextFrom && nextTo && nextFrom > nextTo ? nextFrom : nextTo
    setDraftFromDate(orderedFrom)
    setDraftToDate(orderedTo)
    if (onDateRangeApply) {
      onDateRangeApply(orderedFrom, orderedTo)
    }
    await loadMasterData(orderedFrom || undefined, orderedTo || undefined, { forceFresh: true })
  }

  const handleResetDateRange = async () => {
    setDraftFromDate(defaultFromDate)
    setDraftToDate(defaultToDate)
    if (onDateRangeApply) {
      onDateRangeApply(defaultFromDate, defaultToDate)
    }
    await loadMasterData(defaultFromDate || undefined, defaultToDate || undefined, { forceFresh: true })
  }

  const samsungSeriesMeta = VISIBLE_SAMSUNG_PARTNERS.map((partner) => ({
    ...partner,
    prefix: SAMSUNG_MASTER_PREFIX[partner.key],
  }))
  const showSamsungClaimsChart = Boolean(
    data && hasChartSignal(data.samsung.claimsRows, samsungSeriesMeta.map((series) => `${series.prefix}_claims`))
  )
  const showRelianceClaimsChart = Boolean(
    data && hasChartSignal(data.reliance.claimsRows, ["claims"])
  )
  const showGodrejClaimsChart = Boolean(
    data && hasChartSignal(data.godrej.claimsRows, ["claims"])
  )
  const showHitachiPremiumChart = Boolean(
    data && hasChartSignal(data.hitachi.salesRows, ["gross", "earned", "zopper"])
  )
  const showHitachiClaimsChart = Boolean(
    data && hasChartSignal(data.hitachi.claimsRows, ["claims"])
  )

  const renderChart = useCallback(
    (chartId: ExpandableChartId, expanded = false) => {
      if (!data) return null
      const chartHeight = expanded ? 540 : 320
      const animateCharts = !prefersReducedMotion && expanded

      if (chartId === "samsung-premium") {
        return (
          <div style={{ height: chartHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.samsung.salesRows}>
                <CartesianGrid strokeDasharray="3 3" stroke="#dbe5f2" />
                <XAxis dataKey="month" tick={{ fontSize: 11 }} />
                <YAxis tickFormatter={axisMoney} tick={{ fontSize: 11 }} />
                <Tooltip
                  content={({ active, payload, label }) => {
                    if (!active || !payload?.length) return null
                    const row = payload[0]?.payload as Record<string, number | string> | undefined
                    if (!row) return null
                    return (
                      <div className="rounded-lg border border-slate-200 bg-white p-3 text-xs shadow-lg">
                        <div className="mb-2 font-bold text-slate-700">{label}</div>
                        <div className="space-y-2">
                          {samsungSeriesMeta.map((series) => (
                            <div key={`tooltip-${series.key}`}>
                              <div className="font-semibold text-slate-700">{series.shortLabel}</div>
                              <div className="text-slate-600">{`Gross: ${money(asNumber(row[`${series.prefix}_gross`]))}`}</div>
                              <div className="text-slate-600">{`Earned: ${money(asNumber(row[`${series.prefix}_earned`]))}`}</div>
                              <div className="text-slate-600">{`Zopper: ${money(asNumber(row[`${series.prefix}_zopper`]))}`}</div>
                            </div>
                          ))}
                        </div>
                      </div>
                    )
                  }}
                />
                <Legend />
                {samsungSeriesMeta.map((series) => (
                  <Line
                    key={`gross-${series.key}`}
                    type="monotone"
                    dataKey={`${series.prefix}_gross`}
                    name={`${series.shortLabel} Gross Premium`}
                    stroke={series.color}
                    strokeWidth={2.4}
                    isAnimationActive={animateCharts}
                    dot={<ArrowPointDot />}
                    activeDot={{ r: 5 }}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        )
      }

      if (chartId === "samsung-claims") {
        return (
          <div style={{ height: chartHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.samsung.claimsRows}>
                <CartesianGrid strokeDasharray="3 3" stroke="#dbe5f2" />
                <XAxis dataKey="month" tick={{ fontSize: 11 }} />
                <YAxis tickFormatter={axisMoney} tick={{ fontSize: 11 }} />
                <Tooltip formatter={(value: unknown) => money(asNumber(value))} />
                <Legend />
                {samsungSeriesMeta.map((series) => (
                  <Line
                    key={`claims-${series.key}`}
                    type="monotone"
                    dataKey={`${series.prefix}_claims`}
                    name={`${series.shortLabel} Claims Cost`}
                    stroke={series.color}
                    strokeWidth={2.4}
                    isAnimationActive={animateCharts}
                    dot={<ArrowPointDot />}
                    activeDot={{ r: 5 }}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        )
      }

      if (chartId === "reliance-premium") {
        return (
          <div style={{ height: chartHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.reliance.salesRows}>
                <CartesianGrid strokeDasharray="3 3" stroke="#dbe5f2" />
                <XAxis dataKey="month" tick={{ fontSize: 11 }} />
                <YAxis tickFormatter={axisMoney} tick={{ fontSize: 11 }} />
                <Tooltip formatter={(value: unknown) => money(asNumber(value))} />
                <Legend />
                <Line type="monotone" dataKey="gross" name="Gross Premium" stroke="#2563eb" strokeWidth={2.3} isAnimationActive={animateCharts} dot={<ArrowPointDot />} activeDot={{ r: 5 }} />
                <Line type="monotone" dataKey="earned" name="Earned Premium" stroke="#0ea5a4" strokeWidth={2.3} isAnimationActive={animateCharts} dot={<ArrowPointDot />} activeDot={{ r: 5 }} />
                <Line type="monotone" dataKey="zopper" name="Zopper Earned Premium" stroke="#8b5cf6" strokeWidth={2.3} isAnimationActive={animateCharts} dot={<ArrowPointDot />} activeDot={{ r: 5 }} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )
      }

      if (chartId === "reliance-claims") {
        return (
          <div style={{ height: chartHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.reliance.claimsRows}>
                <CartesianGrid strokeDasharray="3 3" stroke="#dbe5f2" />
                <XAxis dataKey="month" tick={{ fontSize: 11 }} />
                <YAxis tickFormatter={axisMoney} tick={{ fontSize: 11 }} />
                <Tooltip formatter={(value: unknown) => money(asNumber(value))} />
                <Legend />
                <Line type="monotone" dataKey="claims" name="Claims Cost" stroke="#ef4444" strokeWidth={2.4} isAnimationActive={animateCharts} dot={<ArrowPointDot />} activeDot={{ r: 5 }} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )
      }

      if (chartId === "godrej-premium") {
        return (
          <div style={{ height: chartHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.godrej.salesRows}>
                <CartesianGrid strokeDasharray="3 3" stroke="#dbe5f2" />
                <XAxis dataKey="month" tick={{ fontSize: 11 }} />
                <YAxis tickFormatter={axisMoney} tick={{ fontSize: 11 }} />
                <Tooltip formatter={(value: unknown) => money(asNumber(value))} />
                <Legend />
                <Line type="monotone" dataKey="gross" name="Gross Premium" stroke="#2563eb" strokeWidth={2.3} isAnimationActive={animateCharts} dot={<ArrowPointDot />} activeDot={{ r: 5 }} />
                <Line type="monotone" dataKey="earned" name="Earned Premium" stroke="#0ea5a4" strokeWidth={2.3} isAnimationActive={animateCharts} dot={<ArrowPointDot />} activeDot={{ r: 5 }} />
                <Line type="monotone" dataKey="zopper" name="Zopper Earned Premium" stroke="#8b5cf6" strokeWidth={2.3} isAnimationActive={animateCharts} dot={<ArrowPointDot />} activeDot={{ r: 5 }} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )
      }

      if (chartId === "godrej-claims") {
        return (
          <div style={{ height: chartHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.godrej.claimsRows}>
                <CartesianGrid strokeDasharray="3 3" stroke="#dbe5f2" />
                <XAxis dataKey="month" tick={{ fontSize: 11 }} />
                <YAxis tickFormatter={axisMoney} tick={{ fontSize: 11 }} />
                <Tooltip formatter={(value: unknown) => money(asNumber(value))} />
                <Legend />
                <Line type="monotone" dataKey="claims" name="Claims Cost" stroke="#ef4444" strokeWidth={2.4} isAnimationActive={animateCharts} dot={<ArrowPointDot />} activeDot={{ r: 5 }} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )
      }

      if (chartId === "hitachi-premium") {
        return (
          <div style={{ height: chartHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.hitachi.salesRows}>
                <CartesianGrid strokeDasharray="3 3" stroke="#dbe5f2" />
                <XAxis dataKey="month" tick={{ fontSize: 11 }} />
                <YAxis tickFormatter={axisMoney} tick={{ fontSize: 11 }} />
                <Tooltip formatter={(value: unknown) => money(asNumber(value))} />
                <Legend />
                <Line type="monotone" dataKey="gross" name="Gross Premium" stroke="#2563eb" strokeWidth={2.3} isAnimationActive={animateCharts} dot={<ArrowPointDot />} activeDot={{ r: 5 }} />
                <Line type="monotone" dataKey="earned" name="Earned Premium" stroke="#0ea5a4" strokeWidth={2.3} isAnimationActive={animateCharts} dot={<ArrowPointDot />} activeDot={{ r: 5 }} />
                <Line type="monotone" dataKey="zopper" name="Zopper Earned Premium" stroke="#8b5cf6" strokeWidth={2.3} isAnimationActive={animateCharts} dot={<ArrowPointDot />} activeDot={{ r: 5 }} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )
      }

      if (chartId === "hitachi-claims") {
        return (
          <div style={{ height: chartHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.hitachi.claimsRows}>
                <CartesianGrid strokeDasharray="3 3" stroke="#dbe5f2" />
                <XAxis dataKey="month" tick={{ fontSize: 11 }} />
                <YAxis tickFormatter={axisMoney} tick={{ fontSize: 11 }} />
                <Tooltip formatter={(value: unknown) => money(asNumber(value))} />
                <Legend />
                <Line type="monotone" dataKey="claims" name="Claims Cost" stroke="#ef4444" strokeWidth={2.4} isAnimationActive={animateCharts} dot={<ArrowPointDot />} activeDot={{ r: 5 }} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )
      }

      return null
    },
    [data, samsungSeriesMeta, prefersReducedMotion]
  )

  return (
    <div className="space-y-4">
      <div className="pt-0">
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-12 xl:items-start">
          <div className="xl:col-span-9">
            {data && (
              <KpiStrip
                title="Total KPI Across All Partners"
                values={data.totals}
                showTotalLossRatio
                lossRatioLabel={LOSS_RATIO_META.label}
              />
            )}
          </div>
          <div className="xl:col-span-3">
            <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
              <div className="mb-2 text-[10px] font-black uppercase tracking-[0.14em] text-slate-500">
                Date Filter
              </div>
              <DateRangePicker
                draftFromDate={draftFromDate}
                draftToDate={draftToDate}
                minDate={defaultFromDate || undefined}
                maxDate={maxPickerDate}
                compact
                onDraftChange={(from, to) => {
                  setDraftFromDate(from)
                  setDraftToDate(to)
                }}
                onApply={handleApplyDateRange}
                onReset={handleResetDateRange}
              />
            </div>
          </div>
        </div>
      </div>

      <div className="space-y-4">
        {loading && (
          <div className="rounded-2xl border border-slate-200 bg-white p-10 text-center text-sm text-slate-500 shadow-sm">
            Loading master trends...
          </div>
        )}

        {!loading && error && (
          <div className="rounded-2xl border border-rose-200 bg-rose-50 p-6 text-sm text-rose-700">
            {error}
          </div>
        )}

        {!loading && !error && data && (
          <div className="space-y-6">
            <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5">
              <KpiStrip title="Samsung KPI (Vijay Sales + Croma + Reliance Digital)" values={data.samsung.kpis} showTotalLossRatio />
              <div className={`mt-4 grid grid-cols-1 gap-4 ${showSamsungClaimsChart ? "lg:grid-cols-2" : ""}`}>
                <div className="rounded-xl border border-slate-200 bg-slate-50/60 p-3">
                  <div className="mb-2 flex items-center justify-between gap-3">
                    <div className="text-xs font-bold text-slate-700">Samsung Premium Trend (Gross Scale)</div>
                    <button
                      type="button"
                      onClick={() => setExpandedChart("samsung-premium")}
                      className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600 transition hover:bg-slate-100"
                    >
                      <span className="inline-flex items-center gap-1">
                        <Maximize2 className="h-3.5 w-3.5" />
                        Expand
                      </span>
                    </button>
                  </div>
                  {renderChart("samsung-premium")}
                </div>

                {showSamsungClaimsChart && (
                  <div className="rounded-xl border border-slate-200 bg-slate-50/60 p-3">
                    <div className="mb-2 flex items-center justify-between gap-3">
                      <div className="text-xs font-bold text-slate-700">Samsung Claims Cost Trend</div>
                      <button
                        type="button"
                        onClick={() => setExpandedChart("samsung-claims")}
                        className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600 transition hover:bg-slate-100"
                      >
                        <span className="inline-flex items-center gap-1">
                          <Maximize2 className="h-3.5 w-3.5" />
                          Expand
                        </span>
                      </button>
                    </div>
                    {renderChart("samsung-claims")}
                  </div>
                )}
              </div>
            </section>

            <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5">
              <KpiStrip title="Reliance ResQ KPI" values={data.reliance.kpis} showTotalLossRatio />
              <div className={`mt-4 grid grid-cols-1 gap-4 ${showRelianceClaimsChart ? "lg:grid-cols-2" : ""}`}>
                <div className="rounded-xl border border-slate-200 bg-slate-50/60 p-3">
                  <div className="mb-2 flex items-center justify-between gap-3">
                    <div className="text-xs font-bold text-slate-700">Reliance Premium Trend</div>
                    <button
                      type="button"
                      onClick={() => setExpandedChart("reliance-premium")}
                      className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600 transition hover:bg-slate-100"
                    >
                      <span className="inline-flex items-center gap-1">
                        <Maximize2 className="h-3.5 w-3.5" />
                        Expand
                      </span>
                    </button>
                  </div>
                  {renderChart("reliance-premium")}
                </div>

                {showRelianceClaimsChart && (
                  <div className="rounded-xl border border-slate-200 bg-slate-50/60 p-3">
                    <div className="mb-2 flex items-center justify-between gap-3">
                      <div className="text-xs font-bold text-slate-700">Reliance Claims Cost Trend</div>
                      <button
                        type="button"
                        onClick={() => setExpandedChart("reliance-claims")}
                        className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600 transition hover:bg-slate-100"
                      >
                        <span className="inline-flex items-center gap-1">
                          <Maximize2 className="h-3.5 w-3.5" />
                          Expand
                        </span>
                      </button>
                    </div>
                    {renderChart("reliance-claims")}
                  </div>
                )}
              </div>
            </section>

            <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5">
              <KpiStrip title="Godrej KPI" values={data.godrej.kpis} showTotalLossRatio />
              <div className={`mt-4 grid grid-cols-1 gap-4 ${showGodrejClaimsChart ? "lg:grid-cols-2" : ""}`}>
                <div className="rounded-xl border border-slate-200 bg-slate-50/60 p-3">
                  <div className="mb-2 flex items-center justify-between gap-3">
                    <div className="text-xs font-bold text-slate-700">Godrej Premium Trend</div>
                    <button
                      type="button"
                      onClick={() => setExpandedChart("godrej-premium")}
                      className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600 transition hover:bg-slate-100"
                    >
                      <span className="inline-flex items-center gap-1">
                        <Maximize2 className="h-3.5 w-3.5" />
                        Expand
                      </span>
                    </button>
                  </div>
                  {renderChart("godrej-premium")}
                </div>

                {showGodrejClaimsChart && (
                  <div className="rounded-xl border border-slate-200 bg-slate-50/60 p-3">
                    <div className="mb-2 flex items-center justify-between gap-3">
                      <div className="text-xs font-bold text-slate-700">Godrej Claims Cost Trend</div>
                      <button
                        type="button"
                        onClick={() => setExpandedChart("godrej-claims")}
                        className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600 transition hover:bg-slate-100"
                      >
                        <span className="inline-flex items-center gap-1">
                          <Maximize2 className="h-3.5 w-3.5" />
                          Expand
                        </span>
                      </button>
                    </div>
                    {renderChart("godrej-claims")}
                  </div>
                )}
              </div>
            </section>

            <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5">
              <KpiStrip title="Hitachi KPI" values={data.hitachi.kpis} showTotalLossRatio />
              <div className={`mt-4 grid grid-cols-1 gap-4 ${showHitachiClaimsChart ? "lg:grid-cols-2" : ""}`}>
                {showHitachiPremiumChart && (
                  <div className="rounded-xl border border-slate-200 bg-slate-50/60 p-3">
                    <div className="mb-2 flex items-center justify-between gap-3">
                      <div className="text-xs font-bold text-slate-700">Hitachi Premium Trend</div>
                      <button
                        type="button"
                        onClick={() => setExpandedChart("hitachi-premium")}
                        className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600 transition hover:bg-slate-100"
                      >
                        <span className="inline-flex items-center gap-1">
                          <Maximize2 className="h-3.5 w-3.5" />
                          Expand
                        </span>
                      </button>
                    </div>
                    {renderChart("hitachi-premium")}
                  </div>
                )}

                {showHitachiClaimsChart && (
                  <div className="rounded-xl border border-slate-200 bg-slate-50/60 p-3">
                    <div className="mb-2 flex items-center justify-between gap-3">
                      <div className="text-xs font-bold text-slate-700">Hitachi Claims Cost Trend</div>
                      <button
                        type="button"
                        onClick={() => setExpandedChart("hitachi-claims")}
                        className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600 transition hover:bg-slate-100"
                      >
                        <span className="inline-flex items-center gap-1">
                          <Maximize2 className="h-3.5 w-3.5" />
                          Expand
                        </span>
                      </button>
                    </div>
                    {renderChart("hitachi-claims")}
                  </div>
                )}
              </div>
            </section>
          </div>
        )}
      </div>

      {expandedChart && data && (
        <div className="fixed inset-0 z-[140] flex items-start justify-center bg-slate-900/60 p-3 pt-20 sm:p-6 sm:pt-24">
          <div className="flex h-auto max-h-[calc(100vh-6.5rem)] w-full max-w-6xl flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl">
            <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3 sm:px-6">
              <div className="text-sm font-bold text-slate-700 sm:text-base">{CHART_TITLES[expandedChart]}</div>
              <button
                type="button"
                onClick={() => setExpandedChart(null)}
                className="rounded-md border border-slate-200 bg-white p-1.5 text-slate-600 transition hover:bg-slate-100"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
            <div className="flex-1 overflow-auto p-3 sm:p-6">{renderChart(expandedChart, true)}</div>
          </div>
        </div>
      )}
    </div>
  )
}
