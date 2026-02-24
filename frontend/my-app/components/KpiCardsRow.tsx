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

const money = (value: number) => {
  const abs = Math.abs(value)
  if (abs >= 1e7) return `Rs ${(value / 1e7).toFixed(2)} Cr`
  if (abs >= 1e5) return `Rs ${(value / 1e5).toFixed(2)} L`
  if (abs >= 1e3) return `Rs ${(value / 1e3).toFixed(1)} K`
  return `Rs ${new Intl.NumberFormat("en-IN").format(Math.round(value))}`
}

const formatDate = (value: string | null) => {
  if (!value) return "Unknown"
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
  const [lastUpdated, setLastUpdated] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let mounted = true

    const sourceKey = (source || "").trim().toLowerCase()
    const isGodrejSales =
      datasetType === "sales" &&
      (sourceKey === "godrej" || sourceKey === "goodrej" || sourceKey === "goddrej")

    const summaryParams: Parameters<typeof fetchSummary>[0] = isGodrejSales
      ? {
          source,
          dataset_type: datasetType,
        }
      : {
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

    const load = async () => {
      if (mounted) {
        setLoading(true)
        setError(null)
      }

      try {
        const [summaryRes, freshnessRes] = await Promise.all([
          fetchSummary(summaryParams),
          fetchLastUpdated(freshnessParams),
        ])
        if (!mounted) return
        setSummary(summaryRes || null)
        setLastUpdated((freshnessRes as LastUpdated | null)?.data_upto ?? null)
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message.toLowerCase() : String(err).toLowerCase()
        const isAuthError = msg.includes("not authenticated") || msg.includes("invalid token")
        if (!isAuthError) {
          console.error("KPI cards fetch failed:", err)
        }
        if (mounted) {
          setSummary(null)
          setLastUpdated(null)
          setError("No data")
        }
      } finally {
        if (mounted) setLoading(false)
      }
    }

    load()

    return () => {
      mounted = false
    }
  }, [jobId, source, datasetType, fromDate, toDate, refreshTick])

  const cards = useMemo<MetricCard[]>(() => {
    const gross = summary?.gross_premium ?? 0
    const earned = summary?.earned_premium ?? 0
    const zopper = summary?.zopper_earned_premium ?? 0
    const units = summary?.units_sold ?? 0
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
        {
          key: "claims-updated",
          label: "Last Updated",
          value: updated,
          caption: "Latest available source date",
          tone: "from-[#df7a5c] to-[#c45b47]",
          icon: CalendarClock,
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
        label: "Zopper Earned",
        value: money(zopper),
        caption: "Revenue contribution",
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
    ]
  }, [datasetType, summary, lastUpdated])

  const gridClassName =
    layout === "vertical"
      ? "grid grid-cols-1 gap-2.5 sm:gap-3"
      : "grid grid-cols-1 gap-2.5 min-[420px]:grid-cols-2 sm:gap-3 xl:grid-cols-4"

  return (
    <div className={gridClassName}>
      {cards.map((card) => {
        const Icon = card.icon
        const value = loading ? "--" : error ? "N/A" : card.value
        const caption = loading ? "Loading..." : error ? "Unable to fetch data" : card.caption

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
