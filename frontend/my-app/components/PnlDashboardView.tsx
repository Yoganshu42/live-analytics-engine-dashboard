"use client"

import { useEffect, useMemo, useState } from "react"
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from "recharts"

import {
  fetchPnlBoard,
  fetchPnlStoreDetail,
  type PnlBoardResponse,
  type PnlStoreDetailResponse,
} from "@/app/lib/api"

type Props = {
  source: string
  jobId?: string | null
  fromDate?: string
  toDate?: string
}

const money = (value: number) => {
  const abs = Math.abs(value)
  if (abs >= 1e7) return `Rs ${(value / 1e7).toFixed(2)} Cr`
  if (abs >= 1e5) return `Rs ${(value / 1e5).toFixed(2)} L`
  if (abs >= 1e3) return `Rs ${(value / 1e3).toFixed(1)} K`
  return `Rs ${new Intl.NumberFormat("en-IN").format(Math.round(value))}`
}

const whole = (value: number) => new Intl.NumberFormat("en-IN").format(Math.round(value || 0))
const percent = (value: number) => `${(value || 0).toFixed(1)}%`

const MIX_COLORS = ["#2563eb", "#ef4444", "#cbd5e1"]

function EmptyState({ message }: { message: string }) {
  return (
    <div className="flex min-h-[220px] items-center justify-center rounded-3xl border border-dashed border-slate-200 bg-white p-6 text-sm text-slate-500">
      {message}
    </div>
  )
}

function MetricCard({
  label,
  value,
  helper,
  tone = "text-slate-900",
  loading = false,
}: {
  label: string
  value: string
  helper: string
  tone?: string
  loading?: boolean
}) {
  return (
    <div className="rounded-[28px] border border-slate-200 bg-white p-5 shadow-sm">
      <div className="text-[11px] font-black uppercase tracking-[0.22em] text-slate-400">{label}</div>
      {loading ? (
        <div className="mt-3 space-y-3">
          <div className="h-10 w-32 animate-pulse rounded-2xl bg-slate-100" />
          <div className="h-4 w-44 animate-pulse rounded-xl bg-slate-100" />
        </div>
      ) : (
        <>
          <div className={`mt-3 text-3xl font-black tracking-tight ${tone}`}>{value}</div>
          <div className="mt-2 text-sm text-slate-500">{helper}</div>
        </>
      )}
    </div>
  )
}

export default function PnlDashboardView({ source, jobId, fromDate, toDate }: Props) {
  const [selectedState, setSelectedState] = useState("")
  const [selectedCity, setSelectedCity] = useState("")
  const [selectedStoreKey, setSelectedStoreKey] = useState("")
  const [boardData, setBoardData] = useState<PnlBoardResponse | null>(null)
  const [detailData, setDetailData] = useState<PnlStoreDetailResponse | null>(null)
  const [boardError, setBoardError] = useState<string | null>(null)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [boardResolvedKey, setBoardResolvedKey] = useState("")
  const [detailResolvedKey, setDetailResolvedKey] = useState("")

  const boardRequestKey = useMemo(
    () => JSON.stringify([source, jobId || "", fromDate || "", toDate || "", selectedState || "", selectedCity || ""]),
    [source, jobId, fromDate, toDate, selectedState, selectedCity]
  )
  const detailRequestKey = useMemo(
    () => JSON.stringify([source, jobId || "", fromDate || "", toDate || "", selectedState || "", selectedCity || "", selectedStoreKey || ""]),
    [source, jobId, fromDate, toDate, selectedState, selectedCity, selectedStoreKey]
  )
  const boardLoading = boardResolvedKey !== boardRequestKey
  const detailLoading = Boolean(selectedStoreKey) && detailResolvedKey !== detailRequestKey
  const visibleBoardError = boardResolvedKey === boardRequestKey ? boardError : null
  const visibleDetailError = selectedStoreKey && detailResolvedKey === detailRequestKey ? detailError : null
  const initialBoardLoading = boardLoading && !boardData && !visibleBoardError

  useEffect(() => {
    let mounted = true
    const controller = new AbortController()

    fetchPnlBoard(
      {
        source,
        job_id: jobId || undefined,
        from_date: fromDate,
        to_date: toDate,
        state: selectedState || undefined,
        city: selectedCity || undefined,
        limit: 40,
      },
      { signal: controller.signal }
    )
      .then((payload) => {
        if (!mounted) return
        setBoardData(payload)
        setBoardError(null)
        setBoardResolvedKey(boardRequestKey)
        const rowKeys = new Set((payload.rows || []).map((row) => row.store_key))
        const fallbackStoreKey = payload.default_store_key || payload.rows?.[0]?.store_key || ""
        setSelectedStoreKey((current) => (current && rowKeys.has(current) ? current : fallbackStoreKey))
      })
      .catch((error: unknown) => {
        if (!mounted || controller.signal.aborted) return
        setBoardError(error instanceof Error ? error.message : "Failed to load P&L board.")
        setBoardResolvedKey(boardRequestKey)
        setBoardData(null)
      })

    return () => {
      mounted = false
      controller.abort()
    }
  }, [boardRequestKey, source, jobId, fromDate, toDate, selectedState, selectedCity])

  useEffect(() => {
    if (!selectedStoreKey) return
    let mounted = true
    const controller = new AbortController()

    fetchPnlStoreDetail(
      {
        source,
        store_key: selectedStoreKey,
        job_id: jobId || undefined,
        from_date: fromDate,
        to_date: toDate,
        state: selectedState || undefined,
        city: selectedCity || undefined,
      },
      { signal: controller.signal }
    )
      .then((payload) => {
        if (!mounted) return
        setDetailData(payload)
        setDetailError(null)
        setDetailResolvedKey(detailRequestKey)
      })
      .catch((error: unknown) => {
        if (!mounted || controller.signal.aborted) return
        setDetailError(error instanceof Error ? error.message : "Failed to load store detail.")
        setDetailResolvedKey(detailRequestKey)
        setDetailData(null)
      })

    return () => {
      mounted = false
      controller.abort()
    }
  }, [detailRequestKey, source, jobId, fromDate, toDate, selectedState, selectedCity, selectedStoreKey])

  const summaryMix = useMemo(() => {
    const summary = boardData?.summary
    return [
      { name: "Profitable", value: summary?.profitable_stores || 0 },
      { name: "Loss Making", value: summary?.loss_making_stores || 0 },
      { name: "Breakeven", value: summary?.breakeven_stores || 0 },
    ].filter((item) => item.value > 0)
  }, [boardData])

  const activeStore = detailData?.selected_store || boardData?.rows?.find((row) => row.store_key === selectedStoreKey) || null

  return (
    <div className="space-y-5">
      <div className="grid gap-5 xl:grid-cols-[260px_minmax(0,1fr)]">
        <aside className="space-y-5">
          <div className="rounded-[30px] border border-slate-200 bg-white p-5 shadow-sm">
            <div className="text-[11px] font-black uppercase tracking-[0.22em] text-slate-400">P&L Filters</div>
            <div className="mt-5 space-y-4">
              <div>
                <label className="mb-2 block text-xs font-bold uppercase tracking-[0.16em] text-slate-500">State</label>
                <select
                  value={selectedState}
                  onChange={(event) => {
                    setSelectedState(event.target.value)
                    setSelectedCity("")
                  }}
                  className="w-full rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-semibold text-slate-700 outline-none transition focus:border-sky-300"
                >
                  <option value="">All States</option>
                  {(boardData?.state_options || []).map((item) => (
                    <option key={item.label} value={item.label}>{`${item.label} (${item.count})`}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="mb-2 block text-xs font-bold uppercase tracking-[0.16em] text-slate-500">City</label>
                <select
                  value={selectedCity}
                  onChange={(event) => setSelectedCity(event.target.value)}
                  disabled={!selectedState || !(boardData?.city_options || []).length}
                  className="w-full rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-semibold text-slate-700 outline-none transition focus:border-sky-300 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-400"
                >
                  <option value="">{selectedState ? "All Cities" : "Select state first"}</option>
                  {(boardData?.city_options || []).map((item) => (
                    <option key={item.label} value={item.label}>{`${item.label} (${item.count})`}</option>
                  ))}
                </select>
              </div>
            </div>
          </div>

          <div className="rounded-[30px] border border-slate-200 bg-white p-5 shadow-sm">
            <div className="text-[11px] font-black uppercase tracking-[0.22em] text-slate-400">Portfolio Pulse</div>
            <div className="mt-4 space-y-4 text-sm text-slate-600">
              <div className="rounded-2xl bg-slate-50 px-4 py-3">
                <div className="text-xs font-bold uppercase tracking-[0.16em] text-slate-400">Best Store</div>
                <div className="mt-2 text-base font-bold text-slate-900">{boardData?.summary.best_store_name || "Waiting for data"}</div>
              </div>
              <div className="rounded-2xl bg-slate-50 px-4 py-3">
                <div className="text-xs font-bold uppercase tracking-[0.16em] text-slate-400">Highest Loss Pressure</div>
                <div className="mt-2 text-base font-bold text-slate-900">{boardData?.summary.worst_store_name || "Waiting for data"}</div>
              </div>
            </div>
          </div>
        </aside>

        <div className="space-y-5">
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <MetricCard
              label="Total Profit"
              value={money(boardData?.summary.total_profit || 0)}
              helper="Zopper earned premium minus claims cost"
              tone={(boardData?.summary.total_profit || 0) >= 0 ? "text-emerald-600" : "text-rose-600"}
              loading={initialBoardLoading}
            />
            <MetricCard
              label="Claims Cost"
              value={money(boardData?.summary.total_claims_cost || 0)}
              helper={`Overall loss ratio ${percent(boardData?.summary.overall_loss_ratio || 0)}`}
              tone="text-rose-600"
              loading={initialBoardLoading}
            />
            <MetricCard
              label="Stores Tracked"
              value={whole(boardData?.summary.total_stores || 0)}
              helper={`${whole(boardData?.summary.total_units_sold || 0)} units in selected scope`}
              loading={initialBoardLoading}
            />
            <MetricCard
              label="Claim Volume"
              value={whole(boardData?.summary.total_claim_count || 0)}
              helper={`${whole(boardData?.summary.profitable_stores || 0)} profitable vs ${whole(boardData?.summary.loss_making_stores || 0)} loss making`}
              loading={initialBoardLoading}
            />
          </div>

          <div className="grid gap-5 xl:grid-cols-[minmax(0,1.2fr)_360px]">
            <div className="rounded-[32px] border border-slate-200 bg-white p-5 shadow-sm">
              <div className="mb-4 flex items-center justify-between gap-3">
                <div>
                  <div className="text-[11px] font-black uppercase tracking-[0.22em] text-slate-400">Selected Store Performance</div>
                  <h3 className="mt-2 text-xl font-black tracking-tight text-slate-900">{activeStore?.store_name || "Choose a store from the table"}</h3>
                </div>
                <div className="text-right text-xs text-slate-500">
                  <div>{activeStore?.store_id ? `ID ${activeStore.store_id}` : "Store ID unavailable"}</div>
                  <div>{activeStore?.city || activeStore?.state || ""}</div>
                </div>
              </div>
              {detailLoading ? (
                <EmptyState message="Loading store performance..." />
              ) : visibleDetailError ? (
                <EmptyState message={visibleDetailError || "Failed to load store performance."} />
              ) : (detailData?.performance_rows || []).length ? (
                <div className="h-[320px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={detailData?.performance_rows}>
                      <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" />
                      <XAxis dataKey="month" tick={{ fontSize: 11, fill: "#64748b" }} />
                      <YAxis tick={{ fontSize: 11, fill: "#64748b" }} tickFormatter={(value) => money(Number(value)).replace("Rs ", "")} />
                      <RechartsTooltip formatter={(value) => money(Number(value || 0))} />
                      <Line type="monotone" dataKey="zopper_earned_premium" stroke="#2563eb" strokeWidth={3} dot={false} name="Zopper EP" />
                      <Line type="monotone" dataKey="claims_cost" stroke="#ef4444" strokeWidth={3} dot={false} name="Claims Cost" />
                      <Line type="monotone" dataKey="profit" stroke="#16a34a" strokeWidth={3} dot={false} name="Profit" />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              ) : (
                <EmptyState message="No store trend is available for the current selection." />
              )}
            </div>

            <div className="rounded-[32px] border border-slate-200 bg-white p-5 shadow-sm">
              <div className="text-[11px] font-black uppercase tracking-[0.22em] text-slate-400">Store Mix</div>
              <h3 className="mt-2 text-xl font-black tracking-tight text-slate-900">Profitability Split</h3>
              {(summaryMix || []).length ? (
                <div className="mt-4 h-[320px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie data={summaryMix} dataKey="value" nameKey="name" innerRadius={72} outerRadius={112} paddingAngle={3}>
                        {summaryMix.map((entry, index) => <Cell key={entry.name} fill={MIX_COLORS[index % MIX_COLORS.length]} />)}
                      </Pie>
                      <RechartsTooltip formatter={(value) => whole(Number(value || 0))} />
                    </PieChart>
                  </ResponsiveContainer>
                </div>
              ) : (
                <EmptyState message={boardLoading ? "Loading portfolio split..." : visibleBoardError || "No store mix available."} />
              )}
            </div>
          </div>

          <div className="grid gap-5 xl:grid-cols-[minmax(0,1.25fr)_minmax(0,0.95fr)]">
            <div className="rounded-[32px] border border-slate-200 bg-white p-5 shadow-sm">
              <div className="mb-4 flex items-center justify-between gap-3">
                <div>
                  <div className="text-[11px] font-black uppercase tracking-[0.22em] text-slate-400">Store Drilldown</div>
                  <h3 className="mt-2 text-xl font-black tracking-tight text-slate-900">Profit vs Loss Leaderboard</h3>
                </div>
                <div className="text-xs text-slate-500">Select a row to refresh the charts</div>
              </div>
              {boardLoading ? (
                <EmptyState message="Loading P&L leaderboard..." />
              ) : visibleBoardError ? (
                <EmptyState message={visibleBoardError || "Failed to load P&L leaderboard."} />
              ) : !(boardData?.rows || []).length ? (
                <EmptyState message={boardData?.message || "No store-level P&L rows are available for the current filters."} />
              ) : (
                <div className="overflow-auto">
                  <table className="min-w-full text-left">
                    <thead className="border-b border-slate-200 text-[11px] font-black uppercase tracking-[0.16em] text-slate-400">
                      <tr>
                        <th className="px-3 py-3">Store</th><th className="px-3 py-3">Product</th><th className="px-3 py-3">Plan / Channel</th><th className="px-3 py-3">Profit</th><th className="px-3 py-3">Loss Ratio</th><th className="px-3 py-3">Claim Cause</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(boardData?.rows || []).map((row) => (
                        <tr key={row.store_key} onClick={() => setSelectedStoreKey(row.store_key)} className={`cursor-pointer border-b border-slate-100 transition hover:bg-sky-50 ${selectedStoreKey === row.store_key ? "bg-sky-50" : ""}`}>
                          <td className="px-3 py-3 align-top"><div className="font-bold text-slate-900">{row.store_name}</div><div className="text-xs text-slate-500">{row.store_id || "No ID"} • {row.city || row.state || "Unknown location"}</div></td>
                          <td className="px-3 py-3 align-top text-sm text-slate-700">{row.product_name || "Not mapped"}</td>
                          <td className="px-3 py-3 align-top text-sm text-slate-700">{row.plan_label || "Plan N/A"}<div className="text-xs text-slate-500">{row.channel_label || "Channel N/A"}</div></td>
                          <td className={`px-3 py-3 align-top text-sm font-bold ${(row.profit || 0) >= 0 ? "text-emerald-600" : "text-rose-600"}`}>{money(row.profit)}</td>
                          <td className="px-3 py-3 align-top text-sm font-semibold text-slate-700">{percent(row.loss_ratio)}</td>
                          <td className="px-3 py-3 align-top text-sm text-slate-700">{row.top_claim_reason || "No dominant cause"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            <div className="space-y-5">
              <div className="rounded-[32px] border border-slate-200 bg-white p-5 shadow-sm">
                <div className="text-[11px] font-black uppercase tracking-[0.22em] text-slate-400">Store Spotlight</div>
                <div className="mt-3 space-y-2 text-sm text-slate-600">
                  <div className="text-lg font-black tracking-tight text-slate-900">{activeStore?.store_name || "No store selected"}</div>
                  <div>{activeStore?.store_id ? `Store ID: ${activeStore.store_id}` : "Store ID unavailable"}</div>
                  <div>{activeStore?.product_name || "Product mix unavailable"}</div>
                  <div>{activeStore?.plan_label || "Plan unavailable"}{activeStore?.channel_label ? ` • ${activeStore.channel_label}` : ""}</div>
                  <div>Top claim cause: {activeStore?.top_claim_reason || "No dominant claim cause"}</div>
                </div>
              </div>

              <div className="rounded-[32px] border border-slate-200 bg-white p-5 shadow-sm">
                <div className="mb-4 text-[11px] font-black uppercase tracking-[0.22em] text-slate-400">Planwise Profit & Loss</div>
                {(detailData?.plan_rows || []).length ? (
                  <div className="h-[250px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={detailData?.plan_rows}>
                        <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" />
                        <XAxis dataKey="label" tick={{ fontSize: 11, fill: "#64748b" }} interval={0} angle={-18} textAnchor="end" height={60} />
                        <YAxis tick={{ fontSize: 11, fill: "#64748b" }} tickFormatter={(value) => money(Number(value)).replace("Rs ", "")} />
                        <RechartsTooltip formatter={(value) => money(Number(value || 0))} />
                        <Bar dataKey="profit" fill="#2563eb" radius={[8, 8, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                ) : (
                  <EmptyState message={detailLoading ? "Loading planwise profit..." : "No planwise P&L rows are available."} />
                )}
              </div>

              <div className="rounded-[32px] border border-slate-200 bg-white p-5 shadow-sm">
                <div className="mb-4 text-[11px] font-black uppercase tracking-[0.22em] text-slate-400">Claim Cause Pressure</div>
                {(detailData?.cause_rows || []).length ? (
                  <div className="h-[250px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={detailData?.cause_rows} layout="vertical">
                        <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" />
                        <XAxis type="number" tick={{ fontSize: 11, fill: "#64748b" }} tickFormatter={(value) => money(Number(value)).replace("Rs ", "")} />
                        <YAxis type="category" dataKey="label" width={110} tick={{ fontSize: 11, fill: "#64748b" }} />
                        <RechartsTooltip formatter={(value) => money(Number(value || 0))} />
                        <Bar dataKey="claims_cost" fill="#ef4444" radius={[0, 8, 8, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                ) : (
                  <EmptyState message={detailLoading ? "Loading claim causes..." : "No dominant claim causes are available."} />
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
