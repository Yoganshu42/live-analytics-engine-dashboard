"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { useReducedMotion } from "framer-motion"
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import {
  fetchAnnualComparison,
  fetchByDimensionBatch,
  fetchByDimensionRows,
  type AnnualComparisonMetricPayload,
  type FetchByDimensionBatchItem,
} from "@/app/lib/api"
import { SALES_METRIC_ORDER } from "@/lib/salesMetricOrder"
import { isSamsungPartnerSource } from "@/lib/samsungPartners"

type Props = {
  source: string
  datasetType: "sales" | "claims"
  jobId?: string | null
  fromDate?: string
  toDate?: string
  initialDelayMs?: number
  embedded?: boolean
  compact?: boolean
  heightClassName?: string
}

type MetricKey =
  | "gross_premium"
  | "earned_premium"
  | "zopper_earned_premium"
  | "quantity"
  | "claims"
  | "net_claims"
  | "loss_ratio"

type ChartRow = Record<string, string | number>

type YearBucket = {
  label: string
  from: string
  to: string
}

type PlanSeries = {
  plan: string
  dataKey: string
  color: string
}

type MetricPayload = {
  rows: ChartRow[]
  series: PlanSeries[]
}

const SALES_METRICS: MetricKey[] = [...SALES_METRIC_ORDER]

const CLAIMS_METRICS: MetricKey[] = [
  "claims",
  "net_claims",
  "loss_ratio",
  "quantity",
]

const ALL_METRICS: MetricKey[] = [
  "gross_premium",
  "earned_premium",
  "zopper_earned_premium",
  "quantity",
  "claims",
  "net_claims",
  "loss_ratio",
]

const METRIC_LABELS: Record<MetricKey, string> = {
  gross_premium: "Gross Premium",
  earned_premium: "Earned Premium",
  zopper_earned_premium: "Zopper Earned Premium",
  quantity: "Quantity",
  claims: "Claims Cost",
  net_claims: "Net Claims Paid",
  loss_ratio: "Loss Ratio",
}

const PLAN_COLORS = [
  "#2563eb",
  "#06b6d4",
  "#14b8a6",
  "#f97316",
  "#8b5cf6",
  "#ec4899",
  "#84cc16",
  "#f59e0b",
]

const BATCH_CHUNK_SIZE = 72

const toSafeKey = (value: string) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[()%'.]/g, "")

const asNumber = (value: unknown) => {
  const numeric = Number(value ?? 0)
  return Number.isFinite(numeric) ? numeric : 0
}

const getRowValue = (row: Record<string, unknown>, ...candidates: string[]) => {
  for (const candidate of candidates) {
    if (candidate in row) return row[candidate]
  }

  const entryMap = new Map<string, unknown>()
  Object.entries(row || {}).forEach(([key, value]) => {
    entryMap.set(toSafeKey(key), value)
  })

  for (const candidate of candidates) {
    const normalized = toSafeKey(candidate)
    if (entryMap.has(normalized)) return entryMap.get(normalized)
  }

  return undefined
}

const formatMetricValue = (value: number, metric: MetricKey) => {
  if (metric === "loss_ratio") return `${value.toFixed(2)}%`
  if (metric === "quantity") return value.toLocaleString("en-IN")

  const absValue = Math.abs(value)
  const sign = value < 0 ? "-" : ""
  if (absValue >= 1e7) return `Rs ${sign}${(absValue / 1e7).toFixed(2)} Cr`
  if (absValue >= 1e5) return `Rs ${sign}${(absValue / 1e5).toFixed(2)} L`
  if (absValue >= 1e3) return `Rs ${sign}${(absValue / 1e3).toFixed(1)} K`
  return `Rs ${value.toLocaleString("en-IN")}`
}

const formatAxisValue = (value: number, metric: MetricKey) => {
  if (metric === "loss_ratio") return `${value.toFixed(1)}%`
  if (metric === "quantity") {
    return new Intl.NumberFormat("en-IN", {
      notation: "compact",
      maximumFractionDigits: 1,
    }).format(value)
  }

  const absValue = Math.abs(value)
  if (absValue >= 1e7) return `${(value / 1e7).toFixed(1)}Cr`
  if (absValue >= 1e5) return `${(value / 1e5).toFixed(1)}L`
  if (absValue >= 1e3) return `${(value / 1e3).toFixed(1)}K`
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
  if (Number.isNaN(parsed.getTime())) return ""
  return `${parsed.getFullYear()}-${String(parsed.getMonth() + 1).padStart(2, "0")}-01`
}

const parseIsoDate = (value: string | undefined, fallback?: Date) => {
  const raw = String(value || "").trim()
  if (!raw) return fallback ? new Date(fallback) : null
  const parsed = new Date(raw)
  if (Number.isNaN(parsed.getTime())) return fallback ? new Date(fallback) : null
  return parsed
}

const shiftYear = (value: string, delta: number) => {
  const parsed = parseIsoDate(value)
  if (!parsed) return value
  const shifted = new Date(parsed)
  shifted.setFullYear(parsed.getFullYear() + delta)
  return shifted.toISOString().slice(0, 10)
}

const isWithinIsoRange = (value: string, fromIso: string, toIso: string) =>
  Boolean(value) && value >= fromIso && value <= toIso

const canonicalizePlanLabel = (source: string, value: string) => {
  const raw = String(value || "").trim()
  const text = raw.toLowerCase()
  if (!text) return ""

  if (/^plan\s*\d+$/i.test(raw)) {
    const numberMatch = raw.match(/\d+/)
    return numberMatch ? `Plan ${numberMatch[0]}` : raw
  }

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

  return raw
}

const getPlanOrder = (source: string) => {
  if (source === "reliance") return ["ADLD", "Crack Screen", "Extended Warranty"]
  if (source === "godrej") return []
  if (source === "hitachi") return []
  if (source === "samsung" || isSamsungPartnerSource(source)) {
    return ["Combo", "ADLD", "Screen Protection", "Extended Warranty"]
  }
  return []
}

const getPlanSortTuple = (source: string, value: string): [number, number, string] => {
  const canonical = canonicalizePlanLabel(source, value)
  const order = getPlanOrder(source)
  const normalized = canonical.toLowerCase()
  const fixedIndex = order.findIndex((candidate) => candidate.toLowerCase() === normalized)
  if (fixedIndex >= 0) {
    return [0, fixedIndex, canonical]
  }

  const planNumberMatch = canonical.match(/^Plan\s+(\d+)$/i)
  if (planNumberMatch) {
    return [1, Number(planNumberMatch[1]), canonical]
  }

  return [2, 999, canonical]
}

const collectCanonicalPlans = (source: string, labels: string[]) => {
  const seen = new Set<string>()
  return labels
    .map((label) => canonicalizePlanLabel(source, label))
    .filter(Boolean)
    .filter((label) => {
      const key = label.toLowerCase()
      if (seen.has(key)) return false
      seen.add(key)
      return true
    })
    .sort((a, b) => {
      const [groupA, rankA, labelA] = getPlanSortTuple(source, a)
      const [groupB, rankB, labelB] = getPlanSortTuple(source, b)
      if (groupA !== groupB) return groupA - groupB
      if (rankA !== rankB) return rankA - rankB
      return labelA.localeCompare(labelB)
    })
}

const normalizeAnnualPlanValues = (source: string, values: Record<string, unknown>) => {
  const normalized: Record<string, number> = {}
  Object.entries(values || {}).forEach(([plan, rawValue]) => {
    const canonical = canonicalizePlanLabel(source, plan)
    if (!canonical) return
    normalized[canonical] = asNumber(normalized[canonical]) + asNumber(rawValue)
  })
  return normalized
}

const hasRenderableMetricPayload = (payload: MetricPayload | null | undefined) =>
  Boolean(payload && payload.rows.length && payload.series.length)

const getPlanColor = (index: number) => PLAN_COLORS[index % PLAN_COLORS.length]

const chunkBatchRequests = (requests: FetchByDimensionBatchItem[]) => {
  const chunks: FetchByDimensionBatchItem[][] = []
  for (let index = 0; index < requests.length; index += BATCH_CHUNK_SIZE) {
    chunks.push(requests.slice(index, index + BATCH_CHUNK_SIZE))
  }
  return chunks
}

const buildCurrentRange = (fromDate?: string, toDate?: string) => {
  const fallbackEnd = parseIsoDate(toDate) || new Date()
  const fallbackStart = new Date(fallbackEnd.getFullYear(), 0, 1)
  const currentFrom = (parseIsoDate(fromDate, fallbackStart) || fallbackStart).toISOString().slice(0, 10)
  const currentTo = (parseIsoDate(toDate, fallbackEnd) || fallbackEnd).toISOString().slice(0, 10)
  return currentFrom <= currentTo
    ? { currentFrom, currentTo }
    : { currentFrom: currentTo, currentTo: currentFrom }
}

const getFinancialYearStart = (value: string) => {
  const parsed = parseIsoDate(value)
  if (!parsed) return 0
  return parsed.getMonth() >= 3 ? parsed.getFullYear() : parsed.getFullYear() - 1
}

const formatFinancialYearLabel = (financialYearStart: number) =>
  `${financialYearStart} - ${financialYearStart + 1}`

const buildFinancialYearBuckets = (currentFrom: string, currentTo: string): YearBucket[] => {
  const startFinancialYear = getFinancialYearStart(currentFrom)
  const endFinancialYear = getFinancialYearStart(currentTo)
  if (!startFinancialYear || !endFinancialYear) return []

  if (startFinancialYear === endFinancialYear) {
    return [
      {
        label: formatFinancialYearLabel(startFinancialYear - 1),
        from: shiftYear(currentFrom, -1),
        to: shiftYear(currentTo, -1),
      },
      {
        label: formatFinancialYearLabel(startFinancialYear),
        from: currentFrom,
        to: currentTo,
      },
    ]
  }

  const buckets: YearBucket[] = []
  for (let financialYear = startFinancialYear; financialYear <= endFinancialYear; financialYear += 1) {
    buckets.push({
      label: formatFinancialYearLabel(financialYear),
      from: financialYear === startFinancialYear ? currentFrom : `${financialYear}-04-01`,
      to: financialYear === endFinancialYear ? currentTo : `${financialYear + 1}-03-31`,
    })
  }
  return buckets
}

const buildPlanSeries = (plans: string[]): PlanSeries[] => {
  const seen = new Set<string>()

  return plans.map((plan, index) => {
    const baseKey = `plan__${toSafeKey(plan) || `item_${index + 1}`}`
    let dataKey = baseKey
    let suffix = 2

    while (seen.has(dataKey)) {
      dataKey = `${baseKey}_${suffix}`
      suffix += 1
    }
    seen.add(dataKey)

    return {
      plan,
      dataKey,
      color: getPlanColor(index),
    }
  })
}

const buildMetricPayload = (
  metric: MetricKey,
  plans: string[],
  rowsByRequest: Map<string, Array<Record<string, unknown>>>,
  yearBuckets: YearBucket[],
): MetricPayload => {
  const series = buildPlanSeries(plans)
  const chartRows = yearBuckets.map((bucket) => {
    const row: ChartRow = {
      year_key: bucket.label,
      label: bucket.label,
      total: 0,
    }

    series.forEach((item) => {
      row[item.dataKey] = 0
    })

    return row
  })

  const rowMap = new Map<string, ChartRow>()
  chartRows.forEach((row) => {
    rowMap.set(String(row.year_key || ""), row)
  })

  plans.forEach((plan, index) => {
    const requestKey = `${metric}::${plan}`
    const rows = rowsByRequest.get(requestKey) || []
    const targetSeries = series[index]
    if (!targetSeries) return

    rows.forEach((row) => {
      const monthValue = normalizeMonthKey(
        getRowValue(row, "month", "Month", "date", "Date", "period_start", "period_end")
      )
      if (!monthValue) return

      const rawValue = getRowValue(row, metric, metric.toUpperCase(), metric.replace(/_/g, " "))
      const numeric = asNumber(rawValue)
      const targetBucket = yearBuckets.find((bucket) => isWithinIsoRange(monthValue, bucket.from, bucket.to))
      if (!targetBucket) return

      const targetRow = rowMap.get(targetBucket.label)
      if (!targetRow) return

      targetRow[targetSeries.dataKey] = asNumber(targetRow[targetSeries.dataKey]) + numeric
      targetRow.total = asNumber(targetRow.total) + numeric
    })
  })

  return {
    rows: chartRows,
    series,
  }
}

const buildTotalMetricPayload = (
  metric: MetricKey,
  rows: Array<Record<string, unknown>>,
  yearBuckets: YearBucket[],
): MetricPayload => {
  const chartRows = yearBuckets.map((bucket) => ({
    year_key: bucket.label,
    label: bucket.label,
    total: 0,
  }))

  const rowMap = new Map<string, ChartRow>()
  chartRows.forEach((row) => {
    rowMap.set(String(row.year_key || ""), row)
  })

  rows.forEach((row) => {
    const monthValue = normalizeMonthKey(
      getRowValue(row, "month", "Month", "date", "Date", "period_start", "period_end")
    )
    if (!monthValue) return

    const rawValue = getRowValue(row, metric, metric.toUpperCase(), metric.replace(/_/g, " "))
    const numeric = asNumber(rawValue)
    const targetBucket = yearBuckets.find((bucket) => isWithinIsoRange(monthValue, bucket.from, bucket.to))
    if (!targetBucket) return

    const targetRow = rowMap.get(targetBucket.label)
    if (!targetRow) return

    targetRow.total = asNumber(targetRow.total) + numeric
  })

  return {
    rows: chartRows,
    series: [],
  }
}

const isMetricKey = (value: string): value is MetricKey =>
  ALL_METRICS.includes(value as MetricKey)

const inflateAnnualMetricPayload = (
  source: string,
  payload: AnnualComparisonMetricPayload | undefined
): MetricPayload | null => {
  if (!payload) return null

  const normalizedRows = (Array.isArray(payload.rows) ? payload.rows : []).map((row) => {
    const rawValues = row && typeof row === "object" && row.values && typeof row.values === "object"
      ? row.values as Record<string, unknown>
      : {}
    const values = normalizeAnnualPlanValues(source, rawValues)
    const derivedTotal = Object.values(values).reduce((sum, value) => sum + asNumber(value), 0)

    return {
      label: String(row?.label || ""),
      total: asNumber(row?.total) || derivedTotal,
      values,
    }
  })

  const plans = collectCanonicalPlans(source, [
    ...(Array.isArray(payload.plans) ? payload.plans.map((plan) => String(plan || "")) : []),
    ...normalizedRows.flatMap((row) => Object.keys(row.values)),
  ])
  const series = buildPlanSeries(plans)
  const rows = normalizedRows.map((row) => {
    const nextRow: ChartRow = {
      year_key: row.label,
      label: row.label,
      total: row.total,
    }

    series.forEach((item) => {
      nextRow[item.dataKey] = asNumber(row.values[item.plan])
    })

    return nextRow
  })

  return {
    rows,
    series,
  }
}

const buildPayloadFromAnnualResponse = (
  source: string,
  payloadByMetric: Record<string, AnnualComparisonMetricPayload> | undefined
): Partial<Record<MetricKey, MetricPayload>> => {
  const nextPayload: Partial<Record<MetricKey, MetricPayload>> = {}

  Object.entries(payloadByMetric || {}).forEach(([metric, payload]) => {
    if (!isMetricKey(metric)) return
    const inflated = inflateAnnualMetricPayload(source, payload)
    if (!inflated) return
    nextPayload[metric] = inflated
  })

  return nextPayload
}

const extractPlansFromRows = (source: string, planRows: Array<Record<string, unknown>>) => {
  const seen = new Set<string>()

  return (Array.isArray(planRows) ? planRows : [])
    .map((row) => {
      const rawPlan = getRowValue(
        row,
        "plan_category",
        "Plan_Category",
        "Plan Category",
        "device_plan_category",
        "Device_Plan_Category",
        "Device Plan Category"
      )
      return canonicalizePlanLabel(source, String(rawPlan ?? "").trim())
    })
    .filter(Boolean)
    .filter((plan) => {
      const key = plan.toLowerCase()
      if (seen.has(key)) return false
      seen.add(key)
      return true
    })
    .sort((a, b) => {
      const [groupA, rankA, labelA] = getPlanSortTuple(source, a)
      const [groupB, rankB, labelB] = getPlanSortTuple(source, b)
      if (groupA !== groupB) return groupA - groupB
      if (rankA !== rankB) return rankA - rankB
      return labelA.localeCompare(labelB)
    })
}

const formatDisplayDate = (value: string) => {
  const parsed = parseIsoDate(value)
  if (!parsed) return value
  return parsed.toLocaleDateString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  })
}

const getMetricTotalForYear = (
  payloadByMetric: Partial<Record<MetricKey, MetricPayload>>,
  metric: MetricKey,
  label: string
) => {
  const row = payloadByMetric[metric]?.rows.find((item) => String(item.label || "") === label)
  return asNumber(row?.total)
}

export default function YearOnYearComparisonChart({
  source,
  datasetType,
  jobId,
  fromDate,
  toDate,
  initialDelayMs = 0,
  embedded = false,
  compact = false,
  heightClassName,
}: Props) {
  const prefersReducedMotion = useReducedMotion()
  const shouldAnimateBars = !prefersReducedMotion && !embedded
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [isVisible, setIsVisible] = useState(false)
  const [selectedMetric, setSelectedMetric] = useState<MetricKey>(
    datasetType === "sales" ? "quantity" : "claims"
  )
  const [payloadByMetric, setPayloadByMetric] = useState<Partial<Record<MetricKey, MetricPayload>>>({})
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const activeRange = useMemo(
    () => buildCurrentRange(fromDate, toDate),
    [fromDate, toDate]
  )

  const yearBuckets = useMemo(
    () => buildFinancialYearBuckets(activeRange.currentFrom, activeRange.currentTo),
    [activeRange]
  )

  const isSingleYearSelection = useMemo(
    () => getFinancialYearStart(activeRange.currentFrom) === getFinancialYearStart(activeRange.currentTo),
    [activeRange]
  )

  const chartMetric: MetricKey = datasetType === "sales" ? "quantity" : selectedMetric
  const activePayload = payloadByMetric[chartMetric]
  const activeRows = activePayload?.rows || []
  const activeSeries = activePayload?.series || []
  const requestedMetrics = useMemo<MetricKey[]>(
    () => (datasetType === "sales" ? ["quantity"] : [selectedMetric]),
    [datasetType, selectedMetric]
  )
  const salesSummaryMetrics = useMemo<MetricKey[]>(
    () => (datasetType === "sales"
      ? SALES_METRICS.filter((metric): metric is MetricKey => metric !== "quantity")
      : []),
    [datasetType]
  )
  const showMetricControls = datasetType === "claims" && !embedded
  const showLegend = !compact
  const resolvedHeightClassName = heightClassName || (
    compact ? "h-[230px] sm:h-[260px]" : "h-[340px] sm:h-[360px]"
  )
  const dataScopeKey = useMemo(
    () => `${source}|${datasetType}|${jobId || ""}|${activeRange.currentFrom}|${activeRange.currentTo}`,
    [activeRange.currentFrom, activeRange.currentTo, datasetType, jobId, source]
  )

  useEffect(() => {
    setSelectedMetric(datasetType === "sales" ? "quantity" : "claims")
  }, [datasetType])

  useEffect(() => {
    setPayloadByMetric({})
    setError(null)
    setLoading(false)
  }, [dataScopeKey])

  useEffect(() => {
    if (isVisible) return
    const node = containerRef.current
    if (!node || typeof IntersectionObserver === "undefined") {
      setIsVisible(true)
      return
    }

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          setIsVisible(true)
          observer.disconnect()
        }
      },
      { rootMargin: "320px 0px 320px 0px" }
    )

    observer.observe(node)
    return () => observer.disconnect()
  }, [isVisible])

  useEffect(() => {
    if (!isVisible) return

    if (datasetType === "sales") {
      const hasSalesPayload = (
        hasRenderableMetricPayload(payloadByMetric.quantity)
        && SALES_METRICS
          .filter((metric) => metric !== "quantity")
          .every((metric) => Boolean(payloadByMetric[metric]))
      )
      if (hasSalesPayload) return
    } else if (hasRenderableMetricPayload(payloadByMetric[selectedMetric])) {
      return
    }

    let active = true
    let timer: ReturnType<typeof setTimeout> | null = null
    const controller = new AbortController()

    const run = async () => {
      const fetchFrom = yearBuckets[0]?.from || activeRange.currentFrom
      const fetchTo = yearBuckets[yearBuckets.length - 1]?.to || activeRange.currentTo
      const baseMetric = datasetType === "sales" ? "quantity" : "claims"

      setLoading(true)
      setError(null)

      try {
        try {
          const annualResponse = await fetchAnnualComparison({
            source,
            dataset_type: datasetType,
            metric: datasetType === "claims" ? selectedMetric : undefined,
            job_id: jobId || undefined,
            from_date: activeRange.currentFrom,
            to_date: activeRange.currentTo,
          }, {
            signal: controller.signal,
          })

          const compactPayload = buildPayloadFromAnnualResponse(source, annualResponse.payload_by_metric)
          const canUseCompactPayload = datasetType === "sales"
            ? hasRenderableMetricPayload(compactPayload.quantity)
            : hasRenderableMetricPayload(compactPayload[selectedMetric])
          if (canUseCompactPayload) {
            if (!active) return
            setPayloadByMetric((prev) => (
              datasetType === "sales"
                ? compactPayload
                : { ...prev, ...compactPayload }
            ))
            return
          }
        } catch (annualError) {
          if (
            controller.signal.aborted
            || (annualError instanceof Error && annualError.name === "AbortError")
          ) {
            throw annualError
          }
        }

        const planRows = await fetchByDimensionRows({
          source,
          dataset_type: datasetType,
          dimension: "plan_category",
          metric: baseMetric,
          bucket: "month",
          job_id: jobId || undefined,
          from_date: fetchFrom,
          to_date: fetchTo,
        }, {
          signal: controller.signal,
        })

        const plans = extractPlansFromRows(source, planRows)

        if (!plans.length) {
          if (!active) return
          setPayloadByMetric({})
          setError("No plan-category data available for annual comparison.")
          return
        }

        const batchRequests: FetchByDimensionBatchItem[] = requestedMetrics.flatMap((metric) =>
          plans.map((plan) => ({
            request_key: `${metric}::${plan}`,
            source,
            dataset_type: datasetType,
            dimension: "month",
            metric,
            bucket: "month",
            job_id: jobId || undefined,
            from_date: fetchFrom,
            to_date: fetchTo,
            filter_1_dimension: "plan_category",
            filter_1_values: plan,
          }))
        )

        salesSummaryMetrics.forEach((metric) => {
          batchRequests.push({
            request_key: `summary::${metric}`,
            source,
            dataset_type: datasetType,
            dimension: "month",
            metric,
            bucket: "month",
            job_id: jobId || undefined,
            from_date: fetchFrom,
            to_date: fetchTo,
          })
        })

        const rowsByRequest = new Map<string, Array<Record<string, unknown>>>()

        try {
          const batchResponses = await Promise.all(
            chunkBatchRequests(batchRequests).map((chunk) => fetchByDimensionBatch(chunk, { signal: controller.signal }))
          )
          batchResponses.forEach((batchResponse) => {
            ;(batchResponse.results || []).forEach((item) => {
              rowsByRequest.set(item.request_key, Array.isArray(item.rows) ? item.rows : [])
            })
          })
        } catch {
          await Promise.all(batchRequests.map(async (request) => {
            const rows = await fetchByDimensionRows(request, { signal: controller.signal })
            rowsByRequest.set(request.request_key, rows)
          }))
        }

        const nextPayload: Partial<Record<MetricKey, MetricPayload>> = {}

        requestedMetrics.forEach((metric) => {
          nextPayload[metric] = buildMetricPayload(metric, plans, rowsByRequest, yearBuckets)
        })

        salesSummaryMetrics.forEach((metric) => {
          nextPayload[metric] = buildTotalMetricPayload(
            metric,
            rowsByRequest.get(`summary::${metric}`) || [],
            yearBuckets
          )
        })

        if (!active) return
        setPayloadByMetric((prev) => datasetType === "sales" ? nextPayload : { ...prev, ...nextPayload })
      } catch (err) {
        if (!active) return
        const message = err instanceof Error ? err.message : "Unable to load annual comparison."
        setError(message)
        if (datasetType === "sales") {
          setPayloadByMetric({})
        }
      } finally {
        if (active) setLoading(false)
      }
    }

    timer = setTimeout(() => { void run() }, initialDelayMs)
    return () => {
      active = false
      controller.abort()
      if (timer) clearTimeout(timer)
    }
  }, [
    activeRange,
    datasetType,
    initialDelayMs,
    isVisible,
    jobId,
    requestedMetrics,
    salesSummaryMetrics,
    source,
    yearBuckets,
    payloadByMetric,
    selectedMetric,
  ])

  const chartBody = (
    <>
      {!embedded && (
        <div className="mb-4 flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div className="min-w-0">
            <div className="text-sm font-bold leading-snug text-slate-800 sm:text-base">
              {datasetType === "sales" ? "Financial Year Plan-Wise Quantity Comparison" : "Financial Year Plan-Wise Comparison"}
            </div>
            <div className="mt-1 text-[11px] text-slate-500">
              {datasetType === "sales"
                ? "Financial-year buckets are stacked by plan, the Y-axis shows quantity counts, and hover reveals quantity plus premium metrics."
                : "Financial-year buckets are stacked by plan category in partner-specific order."}
            </div>
            <div className="mt-1 text-[11px] text-slate-500">
              {formatDisplayDate(activeRange.currentFrom)} to {formatDisplayDate(activeRange.currentTo)}
              {isSingleYearSelection ? " with the previous comparable financial year included for comparison." : "."}
            </div>
          </div>

          {showMetricControls && (
            <div className="flex flex-wrap items-center gap-2">
              {CLAIMS_METRICS.map((metric) => {
                const isActive = metric === selectedMetric
                return (
                  <button
                    key={metric}
                    type="button"
                    onClick={() => setSelectedMetric(metric)}
                    className={`rounded-full border px-3 py-1.5 text-[11px] font-semibold transition ${
                      isActive
                        ? "border-slate-900 bg-slate-900 text-white"
                        : "border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-900"
                    }`}
                  >
                    {METRIC_LABELS[metric]}
                  </button>
                )
              })}
            </div>
          )}
        </div>
      )}

      {!isVisible || loading ? (
        <div className={`flex ${resolvedHeightClassName} items-center justify-center text-sm text-slate-500`}>
          Preparing annual comparison...
        </div>
      ) : error ? (
        <div className={`flex ${resolvedHeightClassName} items-center justify-center text-sm text-slate-500`}>
          {error}
        </div>
      ) : !activePayload || !activeRows.length || !activeSeries.length ? (
        <div className={`flex ${resolvedHeightClassName} items-center justify-center text-sm text-slate-500`}>
          No annual comparison data available.
        </div>
      ) : (
        <>
          {showLegend && (
            <div className="mb-3 flex flex-wrap items-center gap-2.5 text-[11px] text-slate-600">
              {activeSeries.map((item) => (
                <span
                  key={item.plan}
                  className="inline-flex items-center gap-1.5 rounded-full border border-slate-200 bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-600"
                >
                  <span
                    className="h-2.5 w-2.5 rounded-full"
                    style={{ backgroundColor: item.color }}
                  />
                  {item.plan}
                </span>
              ))}
            </div>
          )}

          <div className={resolvedHeightClassName}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={activeRows}
                margin={{ top: 8, right: 8, left: 8, bottom: 4 }}
                barCategoryGap="28%"
              >
                <CartesianGrid strokeDasharray="4 4" vertical={false} stroke="#e2e8f0" />
                <XAxis
                  dataKey="label"
                  tick={{ fontSize: compact ? 10 : 11 }}
                  minTickGap={10}
                />
                <YAxis
                  tick={{ fontSize: compact ? 10 : 11 }}
                  width={compact ? 56 : 72}
                  tickFormatter={(value) => formatAxisValue(asNumber(value), chartMetric)}
                />
                <Tooltip
                  cursor={{ fill: "rgba(148, 163, 184, 0.08)" }}
                  content={({ active, payload, label }) => {
                    if (!active) return null
                    const yearLabel = String(label || "")
                    const rows = Array.isArray(payload) ? payload : []
                    const planItems = activeSeries
                      .map((item) => ({
                        ...item,
                        value: asNumber(rows.find((entry) => entry.dataKey === item.dataKey)?.value),
                      }))
                      .filter((item) => item.value !== 0)

                    if (datasetType === "sales") {
                      const summaryMetrics = SALES_METRICS.map((metric) => ({
                        metric,
                        value: getMetricTotalForYear(payloadByMetric, metric, yearLabel),
                      }))

                      return (
                        <div className="max-w-[340px] rounded-lg border border-slate-200 bg-white p-3 shadow-lg">
                          <div className="text-xs font-bold text-slate-500">{yearLabel}</div>
                          <div className="mt-2 grid grid-cols-2 gap-2">
                            {summaryMetrics.map((item) => (
                              <div
                                key={`summary-${yearLabel}-${item.metric}`}
                                className="rounded-md bg-slate-50 px-2.5 py-2"
                              >
                                <div className="text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-400">
                                  {METRIC_LABELS[item.metric]}
                                </div>
                                <div className="mt-1 text-xs font-bold text-slate-900">
                                  {formatMetricValue(item.value, item.metric)}
                                </div>
                              </div>
                            ))}
                          </div>

                          <div className="mt-3">
                            <div className="mb-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400">
                              Plan Split By Quantity
                            </div>
                            <div className="space-y-1.5">
                              {planItems.length ? planItems.map((item) => (
                                <div key={`tooltip-${yearLabel}-${item.plan}`} className="flex items-center gap-2 text-xs">
                                  <span
                                    className="h-2.5 w-2.5 rounded-full"
                                    style={{ backgroundColor: item.color }}
                                  />
                                  <span className="text-slate-700">{item.plan}</span>
                                  <span className="ml-auto font-semibold text-slate-900">
                                    {formatMetricValue(item.value, "quantity")}
                                  </span>
                                </div>
                              )) : (
                                <div className="text-xs text-slate-400">No data</div>
                              )}
                            </div>
                          </div>
                        </div>
                      )
                    }

                    const total = planItems.reduce((sum, item) => sum + item.value, 0)
                    return (
                      <div className="max-w-[320px] rounded-lg border border-slate-200 bg-white p-3 shadow-lg">
                        <div className="text-xs font-bold text-slate-500">{yearLabel}</div>
                        <div className="mt-2 rounded-md bg-slate-50 px-2.5 py-2">
                          <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400">
                            Total {METRIC_LABELS[selectedMetric]}
                          </div>
                          <div className="mt-1 text-sm font-bold text-slate-900">
                            {formatMetricValue(total, selectedMetric)}
                          </div>
                        </div>
                        <div className="mt-2 space-y-1.5">
                          {planItems.length ? planItems.map((item) => (
                            <div key={`tooltip-${yearLabel}-${item.plan}`} className="flex items-center gap-2 text-xs">
                              <span
                                className="h-2.5 w-2.5 rounded-full"
                                style={{ backgroundColor: item.color }}
                              />
                              <span className="text-slate-700">{item.plan}</span>
                              <span className="ml-auto font-semibold text-slate-900">
                                {formatMetricValue(item.value, selectedMetric)}
                              </span>
                            </div>
                          )) : (
                            <div className="text-xs text-slate-400">No data</div>
                          )}
                        </div>
                      </div>
                    )
                  }}
                />
                {activeSeries.map((item, index) => (
                  <Bar
                    key={item.dataKey}
                    dataKey={item.dataKey}
                    stackId="year"
                    fill={item.color}
                    isAnimationActive={shouldAnimateBars}
                    radius={index === activeSeries.length - 1 ? [4, 4, 0, 0] : undefined}
                  />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>
        </>
      )}
    </>
  )

  if (embedded) {
    return (
      <div ref={containerRef}>
        {chartBody}
      </div>
    )
  }

  return (
    <div
      ref={containerRef}
      className="smooth-surface content-auto relative overflow-hidden rounded-2xl border border-slate-200/80 bg-gradient-to-br from-white via-slate-50/90 to-cyan-50/60 p-4 shadow-sm sm:p-5"
    >
      <div className="pointer-events-none absolute -top-16 right-[-58px] h-32 w-32 rounded-full bg-cyan-100/60 blur-2xl" />
      <div className="relative">
        {chartBody}
      </div>
    </div>
  )
}
