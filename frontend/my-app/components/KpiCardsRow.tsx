"use client"

import { useEffect, useMemo, useState } from "react"
import {
  BarChart3,
  CalendarClock,
  CircleDollarSign,
  Layers,
  type LucideIcon,
} from "lucide-react"

import { fetchLastUpdated, fetchSummary } from "@/app/lib/api"

type Props = {
  source: string
  datasetType: "sales" | "claims"
  jobId?: string
  fromDate?: string
  toDate?: string
  refreshTick?: number
  layout?: "auto" | "vertical"
}

type Summary = {
  gross_premium?: number
  earned_premium?: number
  zopper_earned_premium?: number
  units_sold?: number
}

type LastUpdated = {
  data_upto: string | null
}

type MetricCard = {
  key: string
  label: string
  value: string
  caption: string
  tone: string
  icon: LucideIcon
}

type KpiCacheValue = {
  summary: Summary | null
  claimsSummary: Summary | null
  lastUpdated: string | null
}

const KPI_CACHE_TTL_MS = 15000
const kpiCache = new Map<string, { expiresAt: number; value: KpiCacheValue }>()

const money = (value: number) => {
  const abs = Math.abs(value)
  if (abs >= 1e7) return `Rs ${(value / 1e7).toFixed(2)} Cr`
  if (abs >= 1e5) return `Rs ${(value / 1e5).toFixed(2)} L`
  if (abs >= 1e3) return `Rs ${(value / 1e3).toFixed(1)} K`
  return `Rs ${new Intl.NumberFormat("en-IN").format(Math.round(value))}`
}

const formatDate = (value: string | null) => {
  if (!value) return "Unknown"
  const raw = String(value).trim()
  if (!raw) return "Unknown"

  const shortMonthYear = raw.match(/^([A-Za-z]{3,9})[-/\s](\d{2}|\d{4})$/)
  if (shortMonthYear) {
    const monthShort = shortMonthYear[1].slice(0, 3)
    const yearText = shortMonthYear[2]
    const year2 = yearText.length === 4 ? yearText.slice(2) : yearText
    return `${monthShort[0].toUpperCase()}${monthShort.slice(1).toLowerCase()} ${year2}`
  }

  const isoMonth = raw.match(/^(\d{4})-(\d{2})$/)
  if (isoMonth) {
    const [, year, month] = isoMonth
    const monthIndex = Number(month) - 1
    if (monthIndex >= 0 && monthIndex <= 11) {
      const label = new Date(Number(year), monthIndex, 1).toLocaleString("en-US", { month: "short" })
      return `${label} ${year.slice(2)}`
    }
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat("en-US", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  }).format(date)
}

export default function KpiCardsRow({
  source,
  datasetType,
  jobId,
  fromDate,
  toDate,
  refreshTick,
  layout = "auto",
}: Props) {
  const [summary, setSummary] = useState<Summary | null>(null)
  const [claimsSummary, setClaimsSummary] = useState<Summary | null>(null)
  const [lastUpdated, setLastUpdated] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [claimsLoading, setClaimsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let mounted = true
    const controller = new AbortController()
    const cacheKey = JSON.stringify({
      source,
      datasetType,
      jobId: jobId || "",
      fromDate: fromDate || "",
      toDate: toDate || "",
      refreshTick: refreshTick || 0,
    })

    const summaryParams: Parameters<typeof fetchSummary>[0] = {
      job_id: jobId,
      source,
      dataset_type: datasetType,
      from_date: fromDate,
      to_date: toDate,
    }

    const freshnessParams: Parameters<typeof fetchLastUpdated>[0] = {
      job_id: jobId,
      source,
      dataset_type: datasetType,
      from_date: fromDate,
      to_date: toDate,
    }
    const claimsSummaryParams: Parameters<typeof fetchSummary>[0] = {
      job_id: jobId,
      source,
      dataset_type: "claims",
      from_date: fromDate,
      to_date: toDate,
    }
    const load = async () => {
      const cached = kpiCache.get(cacheKey)
      if (cached && cached.expiresAt > Date.now()) {
        if (!mounted) return
        setSummary(cached.value.summary)
        setClaimsSummary(cached.value.claimsSummary)
        setLastUpdated(cached.value.lastUpdated)
        setClaimsLoading(false)
        setLoading(false)
        setError(null)
        return
      }

      if (mounted) {
        setLoading(true)
        setClaimsLoading(datasetType === "sales")
        setClaimsSummary(null)
        setLastUpdated(null)
        setError(null)
      }

      try {
        const freshnessPromise = fetchLastUpdated(freshnessParams, { signal: controller.signal }).catch(() => null)
        const claimsPromise =
          datasetType === "sales"
            ? fetchSummary(claimsSummaryParams, { signal: controller.signal }).catch(() => null)
            : Promise.resolve(null)

        const summaryRes = await fetchSummary(summaryParams, { signal: controller.signal })
        if (!mounted) return

        setSummary(summaryRes || null)
        setLoading(false)

        const [resolvedLastUpdated, claimsRes] = await Promise.all([
          freshnessPromise.then((freshnessRes) => (
            (freshnessRes as LastUpdated | null)?.data_upto
            ?? toDate
            ?? null
          )),
          claimsPromise,
        ])
        if (!mounted) return

        const nextValue: KpiCacheValue = {
          summary: summaryRes || null,
          claimsSummary: claimsRes || null,
          lastUpdated: resolvedLastUpdated,
        }

        kpiCache.set(cacheKey, {
          expiresAt: Date.now() + KPI_CACHE_TTL_MS,
          value: nextValue,
        })

        setLastUpdated(nextValue.lastUpdated)
        setClaimsSummary(nextValue.claimsSummary)
        setClaimsLoading(false)
      } catch (err: unknown) {
        const isAbort =
          controller.signal.aborted
          || (err instanceof Error && err.name === "AbortError")
        if (isAbort) {
          return
        }
        const msg = err instanceof Error ? err.message.toLowerCase() : String(err).toLowerCase()
        const isAuthError = msg.includes("not authenticated") || msg.includes("invalid token")
        if (!isAuthError) {
          console.error("KPI cards fetch failed:", err)
        }
        if (mounted) {
          setSummary(null)
          setClaimsSummary(null)
          setLastUpdated(null)
          setClaimsLoading(false)
          setError("No data")
        }
      } finally {
        if (mounted) {
          setLoading(false)
          if (datasetType !== "sales") {
            setClaimsLoading(false)
          }
        }
      }
    }

    load()

    return () => {
      mounted = false
      controller.abort()
    }
  }, [jobId, source, datasetType, fromDate, toDate, refreshTick])

  const cards = useMemo<MetricCard[]>(() => {
    const gross = summary?.gross_premium ?? 0
    const earned = summary?.earned_premium ?? 0
    const zopper = summary?.zopper_earned_premium ?? 0
    const units = summary?.units_sold ?? 0
    const claimsCount = claimsSummary?.units_sold ?? 0
    const updated = formatDate(lastUpdated)

    if (datasetType === "claims") {
      return [
        {
          key: "claims-total",
          label: "Total Claims Cost",
          value: money(gross),
          caption: `Data upto ${updated}`,
          tone: "from-[#1f7de2] to-[#1c66c8]",
          icon: CircleDollarSign,
        },
        {
          key: "claims-net",
          label: "Net Claims Paid",
          value: money(earned),
          caption: "Settled against reported claims",
          tone: "from-[#13a1bf] to-[#0e859f]",
          icon: BarChart3,
        },
        {
          key: "claims-volume",
          label: "Claim Volume",
          value: units.toLocaleString(),
          caption: "Total claim count in range",
          tone: "from-[#d6a03b] to-[#b9852b]",
          icon: Layers,
        },
      ]
    }

    return [
      {
        key: "sales-gross",
        label: "Gross Premium",
        value: money(gross),
        caption: `Data upto ${updated}`,
        tone: "from-[#1f7de2] to-[#1c66c8]",
        icon: CircleDollarSign,
      },
      {
        key: "sales-earned",
        label: "Earned Premium",
        value: money(earned),
        caption: "Net recognized premium",
        tone: "from-[#13a1bf] to-[#0e859f]",
        icon: BarChart3,
      },
      {
        key: "sales-zopper",
        label: "Zopper Earned Premium",
        value: money(zopper),
        caption: "Basis Zopper COGS",
        tone: "from-[#d6a03b] to-[#b9852b]",
        icon: Layers,
      },
      {
        key: "sales-units",
        label: "Units Sold",
        value: units.toLocaleString(),
        caption: "Total units in selection",
        tone: "from-[#df7a5c] to-[#c45b47]",
        icon: CalendarClock,
      },
      {
        key: "sales-claims-count",
        label: "No. of Claims",
        value: claimsCount.toLocaleString(),
        caption: "Claims count in same date range",
        tone: "from-[#7c5cff] to-[#6142dc]",
        icon: BarChart3,
      },
    ]
  }, [datasetType, summary, claimsSummary, lastUpdated])

  const gridClassName =
    layout === "vertical"
      ? "grid grid-cols-1 gap-2.5 sm:gap-3"
      : "grid grid-cols-1 gap-2.5 min-[420px]:grid-cols-2 sm:gap-3 xl:grid-cols-4"

  return (
    <div className={gridClassName}>
      {cards.map((card) => {
        const Icon = card.icon
        const isClaimsCountCard = card.key === "sales-claims-count"
        const value = loading
          ? "--"
          : error
            ? "N/A"
            : isClaimsCountCard && claimsLoading
              ? "--"
              : card.value
        const caption = loading
          ? "Loading..."
          : error
            ? "Unable to fetch data"
            : isClaimsCountCard && claimsLoading
              ? "Loading claim count..."
              : card.caption

        return (
          <div
            key={card.key}
            className={`relative overflow-hidden rounded-2xl bg-gradient-to-br ${card.tone} px-3 py-2.5 text-white shadow-[0_10px_24px_-16px_rgba(15,23,42,0.9)] sm:rounded-[18px] sm:px-4 sm:py-3`}
          >
            <div className="absolute -right-5 -top-5 h-16 w-16 rounded-full bg-white/10 sm:-right-6 sm:-top-6 sm:h-20 sm:w-20" />
            <div className="relative flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-[17px] font-black leading-tight tracking-tight sm:text-[19px]">
                  {value}
                </div>
                <div className="mt-1 text-[9px] font-bold uppercase tracking-[0.12em] text-white/85 sm:text-[10px]">
                  {card.label}
                </div>
              </div>
              <div className="rounded-xl bg-white/20 p-1.5 sm:p-2">
                <Icon className="h-3.5 w-3.5 sm:h-4 sm:w-4" />
              </div>
            </div>
            <div className="relative mt-2 text-[9px] text-white/75 sm:text-[10px]">
              {caption}
            </div>
          </div>
        )
      })}
    </div>
  )
}
