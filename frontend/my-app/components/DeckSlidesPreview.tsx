"use client"

import { useEffect, useMemo, useState } from "react"
import Image from "next/image"
import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import { fetchDeckPreview, type DeckPreviewPartnerItem } from "@/app/lib/api"

type Props = {
  selectedPartners: string[]
  datasetType: "sales" | "claims"
  jobId?: string
  fromDate?: string
  toDate?: string
  weekWindow: 2 | 3 | 4 | 6
}

const currencyCompact = new Intl.NumberFormat("en-IN", {
  notation: "compact",
  maximumFractionDigits: 1,
})
const PREVIEW_CACHE_TTL_MS = 120000
const previewCache = new Map<string, { expiresAt: number; items: DeckPreviewPartnerItem[] }>()

function formatCurrency(value: number) {
  return `Rs ${currencyCompact.format(Number.isFinite(value) ? value : 0)}`
}

function formatQty(value: number) {
  const numeric = Number.isFinite(value) ? value : 0
  const absValue = Math.abs(numeric)
  if (absValue >= 1e7) return `${(numeric / 1e7).toFixed(2)} Cr`
  if (absValue >= 1e5) return `${(numeric / 1e5).toFixed(2)} L`
  return `${Math.round(numeric)}`
}

export default function DeckSlidesPreview({
  selectedPartners,
  datasetType,
  jobId,
  fromDate,
  toDate,
  weekWindow,
}: Props) {
  const [result, setResult] = useState<{ key: string; items: DeckPreviewPartnerItem[] }>({
    key: "",
    items: [],
  })
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState("")

  const partnerQuery = useMemo(
    () => selectedPartners.map((value) => value.trim()).filter(Boolean),
    [selectedPartners]
  )
  const requestKey = useMemo(
    () =>
      JSON.stringify({
        partners: partnerQuery,
        datasetType,
        jobId: jobId || "",
        fromDate: fromDate || "",
        toDate: toDate || "",
        weekWindow,
      }),
    [datasetType, fromDate, jobId, partnerQuery, toDate, weekWindow]
  )
  const cachedItems = useMemo(() => {
    return previewCache.get(requestKey)?.items || []
  }, [requestKey])
  const items = result.key === requestKey ? result.items : cachedItems

  useEffect(() => {
    if (!partnerQuery.length) {
      return
    }

    const cached = previewCache.get(requestKey)
    if (cached?.items?.length && cached.expiresAt > Date.now()) {
      return
    }

    let cancelled = false
    const timer = window.setTimeout(() => {
      setIsLoading(true)
      setError("")

      fetchDeckPreview({
        partners: partnerQuery,
        dataset_type: datasetType,
        job_id: jobId || undefined,
        from_date: fromDate || undefined,
        to_date: toDate || undefined,
        week_window: weekWindow,
      })
        .then((response) => {
          if (cancelled) return
          const nextItems = Array.isArray(response?.items) ? response.items : []
          setResult({ key: requestKey, items: nextItems })
          previewCache.set(requestKey, {
            expiresAt: Date.now() + PREVIEW_CACHE_TTL_MS,
            items: nextItems,
          })
        })
        .catch((err: unknown) => {
          if (cancelled) return
          setError(err instanceof Error ? err.message : "Failed to load deck preview.")
          setResult({ key: requestKey, items: [] })
        })
        .finally(() => {
          if (cancelled) return
          setIsLoading(false)
        })
    }, 260)

    return () => {
      cancelled = true
      window.clearTimeout(timer)
    }
  }, [cachedItems.length, datasetType, fromDate, jobId, partnerQuery, requestKey, toDate, weekWindow])

  if (!partnerQuery.length) {
    return (
      <div className="rounded-2xl border border-slate-200 bg-white p-4 text-sm text-slate-500 shadow-sm">
        Select at least one partner to preview slides.
      </div>
    )
  }

  if (isLoading) {
    return (
      <div className="rounded-2xl border border-slate-200 bg-white p-4 text-sm text-slate-500 shadow-sm">
        Building live slide preview...
      </div>
    )
  }

  if (error && !items.length) {
    return (
      <div className="rounded-2xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700 shadow-sm">
        {error}
      </div>
    )
  }

  if (!items.length) {
    return (
      <div className="rounded-2xl border border-slate-200 bg-white p-4 text-sm text-slate-500 shadow-sm">
        No preview data available for the current filter combination.
      </div>
    )
  }

  return (
    <div className="space-y-5">
      {items.map((item) => {
        const primaryMetricLabel = datasetType === "claims" ? "Claims" : "Gross Premium"
        const trendRows = item.trend_points.slice(0, 12)
        const stateRows = item.state_points.slice(0, 6)
        const productRows = Array.isArray(item.product_points) ? item.product_points.slice(0, 6) : []
        const weekLabel = item.trend_dimension === "week" ? ` (last ${weekWindow} weeks)` : ""

        return (
          <article key={item.source} className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
              <div>
                <h4 className="text-base font-bold text-slate-900">{item.display_name}</h4>
                <p className="text-xs text-slate-500">
                  Live slide view{weekLabel}. Changes here are reflected in download output.
                </p>
              </div>
              <div className="flex items-center gap-3">
                {item.logo ? (
                  <Image
                    src={`/${item.logo}`}
                    alt={`${item.display_name} logo`}
                    width={64}
                    height={20}
                    className="h-5 w-16 object-contain"
                  />
                ) : null}
                <Image
                  src="/Zopper Logo Original 1.png"
                  alt="Zopper logo"
                  width={64}
                  height={20}
                  className="h-5 w-16 object-contain"
                />
              </div>
            </div>

            <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
              <section className="rounded-xl border border-slate-200 bg-[#efefef] p-3">
                <div className="mb-2 text-[10px] font-black uppercase tracking-[0.14em] text-slate-500">Slide 1: Snapshot</div>
                <div className="grid grid-cols-2 gap-2">
                  <div className="rounded-lg bg-white p-2">
                    <div className="text-[10px] font-bold uppercase tracking-[0.08em] text-slate-500">{primaryMetricLabel}</div>
                    <div className="mt-1 text-sm font-bold text-slate-900">{formatCurrency(item.summary.gross_premium)}</div>
                  </div>
                  <div className="rounded-lg bg-white p-2">
                    <div className="text-[10px] font-bold uppercase tracking-[0.08em] text-slate-500">Quantity</div>
                    <div className="mt-1 text-sm font-bold text-slate-900">{formatQty(item.summary.quantity)}</div>
                  </div>
                </div>
                <div className="mt-3 rounded-lg border border-slate-200 bg-white p-2">
                  <div className="text-[10px] font-bold uppercase tracking-[0.08em] text-slate-500">Insights</div>
                  <ul className="mt-1 space-y-1 text-[11px] text-slate-700">
                    {item.insights.slice(0, 5).map((insight, index) => (
                      <li key={index}>{insight}</li>
                    ))}
                  </ul>
                </div>
              </section>

              <section className="rounded-xl border border-slate-200 bg-[#efefef] p-3 xl:col-span-2">
                <div className="mb-2 text-[10px] font-black uppercase tracking-[0.14em] text-slate-500">
                  Slide 2: Trend ({item.trend_dimension === "week" ? "Week-wise" : "Month-wise"})
                </div>
                <div className="h-[230px] rounded-lg bg-white p-2">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={trendRows}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                      <XAxis dataKey="label" tick={{ fontSize: 11, fill: "#475569" }} />
                      <YAxis yAxisId="left" tick={{ fontSize: 11, fill: "#475569" }} />
                      <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11, fill: "#475569" }} />
                      <Tooltip
                        formatter={(value, name) => {
                          const numeric = Number(value || 0)
                          const metricName = String(name || "Value")
                          if (metricName === primaryMetricLabel) return [formatCurrency(numeric), metricName]
                          return [formatQty(numeric), metricName]
                        }}
                      />
                      <Legend />
                      <Bar yAxisId="left" dataKey="gross_premium" name={primaryMetricLabel} fill="#2d6be8" radius={[6, 6, 0, 0]} />
                      <Line yAxisId="right" type="monotone" dataKey="quantity" name="Quantity" stroke="#f97316" strokeWidth={2.2} />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              </section>

              <section className="rounded-xl border border-slate-200 bg-[#efefef] p-3 xl:col-span-3">
                <div className="mb-2 text-[10px] font-black uppercase tracking-[0.14em] text-slate-500">Slide 3: State Drilldown</div>
                <div className="h-[235px] rounded-lg bg-white p-2">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={stateRows}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                      <XAxis dataKey="label" tick={{ fontSize: 11, fill: "#475569" }} />
                      <YAxis yAxisId="left" tick={{ fontSize: 11, fill: "#475569" }} />
                      <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11, fill: "#475569" }} />
                      <Tooltip
                        formatter={(value, name) => {
                          const numeric = Number(value || 0)
                          const metricName = String(name || "Value")
                          if (metricName === primaryMetricLabel) return [formatCurrency(numeric), metricName]
                          return [formatQty(numeric), metricName]
                        }}
                      />
                      <Legend />
                      <Bar yAxisId="left" dataKey="gross_premium" name={primaryMetricLabel} fill="#2d6be8" radius={[6, 6, 0, 0]} />
                      <Line yAxisId="right" type="monotone" dataKey="quantity" name="Quantity" stroke="#f97316" strokeWidth={2.2} />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              </section>

              {productRows.length > 0 ? (
                <section className="rounded-xl border border-slate-200 bg-[#efefef] p-3 xl:col-span-3">
                  <div className="mb-2 text-[10px] font-black uppercase tracking-[0.14em] text-slate-500">
                    Slide 4: Product Model Drilldown (A17, Fold 6, etc.)
                  </div>
                  <div className="h-[235px] rounded-lg bg-white p-2">
                    <ResponsiveContainer width="100%" height="100%">
                      <ComposedChart data={productRows}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                        <XAxis dataKey="label" tick={{ fontSize: 11, fill: "#475569" }} />
                        <YAxis yAxisId="left" tick={{ fontSize: 11, fill: "#475569" }} />
                        <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11, fill: "#475569" }} />
                        <Tooltip
                          formatter={(value, name) => {
                            const numeric = Number(value || 0)
                            const metricName = String(name || "Value")
                            if (metricName === primaryMetricLabel) return [formatCurrency(numeric), metricName]
                            return [formatQty(numeric), metricName]
                          }}
                        />
                        <Legend />
                        <Bar yAxisId="left" dataKey="gross_premium" name={primaryMetricLabel} fill="#2d6be8" radius={[6, 6, 0, 0]} />
                        <Line yAxisId="right" type="monotone" dataKey="quantity" name="Quantity" stroke="#f97316" strokeWidth={2.2} />
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                </section>
              ) : null}
            </div>
          </article>
        )
      })}
    </div>
  )
}
