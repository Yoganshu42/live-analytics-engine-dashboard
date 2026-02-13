"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { Maximize2 } from "lucide-react"
import { AnimatePresence, motion } from "framer-motion"

import GraphView from "@/components/GraphView"
import type { GraphDataSnapshot } from "@/components/GraphView"
import { prefetchGraphData } from "@/components/GraphView"
import { fetchGraphInsights } from "@/app/lib/api"
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
} | null

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
  time: "Time-based Analysis",
  region: "Regional Performance",
  category: "Category Performance",
  device_category: "Device Plan Category",
}

/* ---- order of sections ---- */
const GROUP_ORDER = ["time", "region", "category", "device_category"]
const FAST_LOAD_COUNT = 4
const DEFER_STEP_MS = 120

const GODREJ_PRESETS: Preset[] = [
  {
    group: "time",
    dimension: "month",
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
  time: "Time-based Analysis",
  channel: "Channel Performance",
  product: "Product Category Performance",
}

const getGraphTitle = (
  metric: string,
  dimension: string,
  source?: string
) => {
  const m = metric.toLowerCase()
  const d = dimension.toLowerCase()
  const isReliance = source === "reliance"

  if (d.includes("month")) {
    if (m.includes("zopper")) return "Zopper Earned Premium - Month on Month"
    if (m.includes("earned")) return "Earned Premium - Month on Month"
    if (m.includes("gross")) return "Gross Premium - Month on Month"
    if (m.includes("net_claims")) return "Net Claims - Month on Month"
    if (m === "claims") return "Total Claims Cost - Month on Month"
    if (m.includes("loss_ratio")) return "Loss Ratio - Month on Month"
    if (m.includes("quantity")) return "Quantity - Month on Month"
  }

  if (d.includes("state")) {
    if (m.includes("gross")) return "Gross Premium - State-wise"
    if (m.includes("quantity")) return "Quantity of Plans Sold - State-wise"
    if (m.includes("net_claims")) return "Net Claims - State-wise"
    if (m === "claims") return "Total Claims Cost - State-wise"
    if (m.includes("loss_ratio")) return "Loss Ratio - State-wise"
  }

  if (d.includes("channel")) {
    if (m.includes("gross")) return "Gross Premium - Channel-wise"
    if (m.includes("earned")) return "Earned Premium - Channel-wise"
    if (m.includes("zopper")) return "Zopper Earned Premium - Channel-wise"
    if (m.includes("net_claims")) return "Net Claims - Channel-wise"
    if (m === "claims") return "Total Claims Cost - Channel-wise"
    if (m.includes("loss_ratio")) return "Loss Ratio - Channel-wise"
    if (m.includes("quantity")) return "Quantity - Channel-wise"
  }

  if (d.includes("product_category")) {
    if (m.includes("gross")) return "Gross Premium - Product Category"
    if (m.includes("earned")) return "Earned Premium - Product Category"
    if (m.includes("zopper")) return "Zopper Earned Premium - Product Category"
    if (m.includes("net_claims")) return "Net Claims - Product Category"
    if (m === "claims") return "Total Claims Cost - Product Category"
    if (m.includes("loss_ratio")) return "Loss Ratio - Product Category"
    if (m.includes("quantity")) return "Quantity - Product Category"
  }

  if (d.includes("device_plan_category")) {
    const suffix = isReliance ? "Brand Category" : "Device Plan Category"
    if (m.includes("gross")) return `Gross Premium - ${suffix}`
    if (m.includes("earned")) return `Earned Premium - ${suffix}`
    if (m.includes("zopper")) return `Zopper Earned Premium - ${suffix}`
    if (m.includes("net_claims")) return `Net Claims - ${suffix}`
    if (m === "claims") return `Total Claims Cost - ${suffix}`
    if (m.includes("loss_ratio")) return `Loss Ratio - ${suffix}`
    if (m.includes("quantity")) return `Quantity - ${suffix}`
  }

  if (d.includes("plan_category")) {
    if (m.includes("gross")) return "Gross Premium - Plan Category"
    if (m.includes("earned")) return "Earned Premium - Plan Category"
    if (m.includes("zopper")) return "Zopper Earned Premium - Plan Category"
    if (m.includes("net_claims")) return "Net Claims - Plan Category"
    if (m === "claims") return "Total Claims Cost - Plan Category"
    if (m.includes("loss_ratio")) return "Loss Ratio - Plan Category"
    if (m.includes("quantity")) return "Quantity - Plan Category"
  }

  return metric.replace(/_/g, " ").toUpperCase()
}

export default function MultiGraphView({
  source,
  datasetType,
  jobId,
  fromDate,
  toDate,
}: Props) {
  const isGodrej = source === "godrej"
  const activeGroupOrder = useMemo(
    () => (isGodrej ? ["time", "channel", "product"] : GROUP_ORDER),
    [isGodrej]
  )
  const activePresets = useMemo(
    () => (isGodrej ? GODREJ_PRESETS : Object.values(GRAPH_PRESETS)),
    [isGodrej]
  )

  const [fullscreen, setFullscreen] = useState<FullscreenGraph>(null)
  const [zoom, setZoom] = useState(1)
  const [openedGraphData, setOpenedGraphData] = useState<GraphDataSnapshot | null>(null)
  const [insights, setInsights] = useState<string[]>([])
  const [insightsModel, setInsightsModel] = useState<string>("")
  const [insightsLoading, setInsightsLoading] = useState(false)
  const [insightsError, setInsightsError] = useState<string | null>(null)
  const lastInsightsKeyRef = useRef("")
  const sectionConfigs = useMemo(() => {
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
  }, [activeGroupOrder, activePresets, datasetType])

  const graphQueue = useMemo(() => {
    return sectionConfigs.flatMap(section =>
      section.entries.flatMap(entry =>
        entry.visibleMetrics.map(metric => ({
          dimension: entry.preset.dimension,
          metric,
          bucket: entry.preset.bucket,
        }))
      )
    )
  }, [sectionConfigs])

  const graphOrderIndex = useMemo(() => {
    const map = new Map<string, number>()
    graphQueue.forEach((item, idx) => {
      map.set(`${item.dimension}|${item.metric}|${item.bucket || ""}`, idx)
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

  const handleOpenFullscreen = (item: NonNullable<FullscreenGraph>) => {
    setZoom(1)
    setFullscreen(item)
    setOpenedGraphData(null)
    setInsights([])
    setInsightsModel("")
    setInsightsError(null)
    setInsightsLoading(false)
    lastInsightsKeyRef.current = ""
  }

  const handleCloseFullscreen = () => {
    setFullscreen(null)
    setOpenedGraphData(null)
    setInsights([])
    setInsightsModel("")
    setInsightsError(null)
    setInsightsLoading(false)
    lastInsightsKeyRef.current = ""
  }

  return (
    <>
      {sectionConfigs.map(({ group, entries }) => {
        return (
          <div
            key={group}
            className="mb-12 border border-slate-200 rounded-2xl p-6"
          >
            {/* SECTION HEADER */}
            <h2 className="text-base font-black uppercase text-slate-700 mb-4 tracking-wider">
              {isGodrej
                ? (GODREJ_GROUP_TITLES[group] || group)
                : (group === "device_category" && source === "reliance"
                    ? "Brand Category"
                    : GROUP_TITLES[group])}
            </h2>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-px bg-slate-200/70 rounded-2xl overflow-hidden">
              {entries.map(({ preset, visibleMetrics }) => {
                return visibleMetrics.map((metric: string) => (
                  <motion.div
                    key={`${preset.dimension}-${metric}`}
                    initial={{ opacity: 0, y: 16 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ duration: 0.35, ease: "easeOut" }}
                    whileHover={{ y: -4 }}
                    className="bg-gradient-to-b from-white to-slate-50/60 p-4 shadow-sm transition-shadow hover:shadow-md"
                  >
                    <div className="flex justify-between mb-2">
                      <div className="text-sm font-semibold">
                        {getGraphTitle(metric, preset.dimension, source)}
                      </div>

                      <button
                        onClick={() => {
                          handleOpenFullscreen({
                            metric,
                            dimension: preset.dimension,
                            bucket: preset.bucket,
                          })
                        }}
                      >
                        <Maximize2 size={16} />
                      </button>
                    </div>

                  {(() => {
                    const queueKey = `${preset.dimension}|${metric}|${preset.bucket || ""}`
                    const queueIndex = graphOrderIndex.get(queueKey) ?? 0
                    const fetchDelayMs =
                      queueIndex < FAST_LOAD_COUNT
                        ? 0
                        : Math.min((queueIndex - FAST_LOAD_COUNT + 1) * DEFER_STEP_MS, 1400)
                    return (
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
                  />
                    )
                  })()}
                  </motion.div>
                ))
              })}
            </div>
          </div>
        )
      })}

      {/* FULLSCREEN */}
      <AnimatePresence>
        {fullscreen && (
          <motion.div className="fixed inset-0 z-50 bg-white">
            <div className="h-full w-full overflow-auto">
              <div className="sticky top-0 z-20 bg-white/90 backdrop-blur border-b px-6 py-4 flex items-center justify-between">
                <div className="text-sm font-semibold">
                  {getGraphTitle(fullscreen.metric, fullscreen.dimension, source)}
                </div>
                <button
                  className="text-sm px-3 py-1 border rounded-lg"
                  onClick={handleCloseFullscreen}
                >
                  Close
                </button>
              </div>

              <div className="sticky top-14 z-10 bg-white/90 backdrop-blur border-b px-6 py-3">
                <div className="flex flex-wrap items-center gap-3">
                  <div className="text-[10px] font-black uppercase tracking-widest text-slate-400">
                    Time Period Filter
                  </div>
                  <div className="flex gap-2">
                    <input
                      type="date"
                      value={fromDate || ""}
                      readOnly
                      className="border-slate-200 rounded-xl px-3 py-2 text-xs font-semibold bg-slate-50 text-slate-500"
                    />
                    <input
                      type="date"
                      value={toDate || ""}
                      readOnly
                      className="border-slate-200 rounded-xl px-3 py-2 text-xs font-semibold bg-slate-50 text-slate-500"
                    />
                  </div>
                  <div className="ml-auto flex items-center gap-2">
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

              <div className="min-h-[70vh] px-6 py-6 flex items-center justify-center">
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
                    onDataReady={setOpenedGraphData}
                  />
                  <div className="mt-6 rounded-2xl border border-slate-200 bg-slate-50/60 p-5">
                    <div className="flex items-center justify-between gap-3 mb-3">
                      <h4 className="text-xs font-black uppercase tracking-widest text-slate-500">
                        AI Sahyogi Insights
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
                            - {line}
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
          </motion.div>
        )}
      </AnimatePresence>
    </>
  )
}

