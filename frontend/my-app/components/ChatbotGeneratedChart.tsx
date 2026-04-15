"use client"

import { useRef, useState, type ComponentProps } from "react"
import html2canvas from "html2canvas"
import { Download } from "lucide-react"
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"
import type { ChatbotChart } from "@/app/lib/api"

const CHART_COLORS = ["#2563eb", "#f97316", "#10b981", "#8b5cf6", "#ef4444", "#14b8a6"]

const hslToHex = (h: number, s: number, l: number) => {
  const sat = s / 100
  const light = l / 100
  const c = (1 - Math.abs(2 * light - 1)) * sat
  const hp = h / 60
  const x = c * (1 - Math.abs((hp % 2) - 1))

  let r = 0
  let g = 0
  let b = 0

  if (hp >= 0 && hp < 1) {
    r = c
    g = x
  } else if (hp >= 1 && hp < 2) {
    r = x
    g = c
  } else if (hp >= 2 && hp < 3) {
    g = c
    b = x
  } else if (hp >= 3 && hp < 4) {
    g = x
    b = c
  } else if (hp >= 4 && hp < 5) {
    r = x
    b = c
  } else {
    r = c
    b = x
  }

  const m = light - c / 2
  const toHex = (value: number) =>
    Math.round((value + m) * 255)
      .toString(16)
      .padStart(2, "0")

  return `#${toHex(r)}${toHex(g)}${toHex(b)}`
}

const buildDistinctChartColors = (count: number, palette: string[]) => {
  const colors: string[] = []
  for (let index = 0; index < count; index += 1) {
    if (index < palette.length) {
      colors.push(palette[index])
      continue
    }

    const hue = (index * 137.508) % 360
    const saturation = 68 + (index % 3) * 6
    const lightness = 46 + (index % 4) * 5
    colors.push(hslToHex(hue, saturation, lightness))
  }
  return colors
}

const formatMetricValue = (format: string | undefined, rawValue: number) => {
  const value = Number.isFinite(rawValue) ? rawValue : 0
  const metric = (format || "").toLowerCase()
  if (metric.includes("loss_ratio")) {
    return `${value.toFixed(2)}%`
  }
  if (metric.includes("quantity") || metric.includes("count")) {
    return new Intl.NumberFormat("en-IN", { maximumFractionDigits: 0 }).format(value)
  }
  if (Math.abs(value) >= 1e7) {
    return `Rs ${(value / 1e7).toFixed(2)} Cr`
  }
  if (Math.abs(value) >= 1e5) {
    return `Rs ${(value / 1e5).toFixed(2)} L`
  }
  if (Math.abs(value) >= 1e3) {
    return `Rs ${(value / 1e3).toFixed(1)} K`
  }
  return `Rs ${new Intl.NumberFormat("en-IN", { maximumFractionDigits: 2 }).format(value)}`
}

const escapeCsvValue = (value: unknown) => {
  const text = String(value ?? "")
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, "\"\"")}"`
  }
  return text
}

const downloadBlob = (blob: Blob, filename: string) => {
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement("a")
  anchor.href = url
  anchor.download = filename
  anchor.click()
  window.setTimeout(() => URL.revokeObjectURL(url), 0)
}

const toFileSafeName = (value: string) =>
  (value || "chatbot-chart")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "chatbot-chart"

type Props = {
  chart: ChatbotChart
}

type TooltipFormatter = NonNullable<ComponentProps<typeof Tooltip>["formatter"]>

export default function ChatbotGeneratedChart({ chart }: Props) {
  const chartRef = useRef<HTMLDivElement | null>(null)
  const [isDownloading, setIsDownloading] = useState(false)

  if (!chart.rows.length || !chart.series.length) {
    return null
  }

  const primarySeries = chart.series[0]
  const secondarySeries = chart.series[1]
  const pieColors = buildDistinctChartColors(chart.rows.length, CHART_COLORS)
  const hasSecondaryAxis =
    chart.chart_type === "composed" &&
    Boolean(primarySeries && secondarySeries && primarySeries.format !== secondarySeries.format)
  const baseName = toFileSafeName(chart.download_name || chart.title)

  const handleCsvDownload = () => {
    const headers = ["Label", ...chart.series.map((series) => series.label)]
    const rows = chart.rows.map((row) => [
      escapeCsvValue(row[chart.x_key] ?? row.label ?? ""),
      ...chart.series.map((series) => escapeCsvValue(row[series.key] ?? "")),
    ])
    const csv = [headers.map(escapeCsvValue).join(","), ...rows.map((row) => row.join(","))].join("\n")
    downloadBlob(new Blob([csv], { type: "text/csv;charset=utf-8" }), `${baseName}.csv`)
  }

  const handlePngDownload = async () => {
    if (!chartRef.current || isDownloading) return
    setIsDownloading(true)
    try {
      const canvas = await html2canvas(chartRef.current, {
        backgroundColor: "#ffffff",
        scale: Math.max(2, window.devicePixelRatio || 1),
        useCORS: true,
      })
      await new Promise<void>((resolve) => {
        canvas.toBlob((blob) => {
          if (blob) {
            downloadBlob(blob, `${baseName}.png`)
          }
          resolve()
        }, "image/png")
      })
    } finally {
      setIsDownloading(false)
    }
  }

  const tooltipFormatter: TooltipFormatter = (value, name) => {
    const series = chart.series.find((entry) => entry.label === name || entry.key === String(name))
    const numericValue = typeof value === "number" ? value : Number(value || 0)
    return [formatMetricValue(series?.format, numericValue), series?.label || String(name)] as [string, string]
  }

  return (
    <section className="mt-3 overflow-hidden rounded-2xl border border-slate-200 bg-slate-50/70">
      <div className="flex items-start justify-between gap-3 border-b border-slate-200 bg-white/90 px-3 py-3">
        <div className="min-w-0">
          <h4 className="truncate text-sm font-semibold text-slate-900">{chart.title}</h4>
          {chart.subtitle ? (
            <p className="mt-1 text-[11px] font-medium text-slate-500">{chart.subtitle}</p>
          ) : null}
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <button
            type="button"
            onClick={handleCsvDownload}
            className="inline-flex items-center gap-1 rounded-lg border border-slate-300 bg-white px-2.5 py-1.5 text-[11px] font-semibold text-slate-700 hover:bg-slate-50"
          >
            <Download size={13} />
            CSV
          </button>
          <button
            type="button"
            onClick={() => void handlePngDownload()}
            disabled={isDownloading}
            className="inline-flex items-center gap-1 rounded-lg border border-indigo-200 bg-indigo-50 px-2.5 py-1.5 text-[11px] font-semibold text-indigo-700 hover:bg-indigo-100 disabled:cursor-not-allowed disabled:opacity-60"
          >
            <Download size={13} />
            {isDownloading ? "Preparing..." : "PNG"}
          </button>
        </div>
      </div>

      <div ref={chartRef} className="bg-white px-2 pb-3 pt-2 sm:px-3 sm:pb-4 sm:pt-3">
        <div className="h-[260px] w-full sm:h-[320px]">
          <ResponsiveContainer width="100%" height="100%">
            {chart.chart_type === "pie" ? (
              <PieChart>
                <Tooltip formatter={tooltipFormatter} />
                <Legend />
                <Pie
                  data={chart.rows}
                  dataKey={primarySeries.key}
                  nameKey={chart.x_key}
                  outerRadius="78%"
                  isAnimationActive={false}
                >
                  {chart.rows.map((_, index) => (
                    <Cell key={`pie-${index}`} fill={pieColors[index]} />
                  ))}
                </Pie>
              </PieChart>
            ) : chart.chart_type === "line" ? (
              <LineChart data={chart.rows}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey={chart.x_key} tick={{ fontSize: 11 }} minTickGap={16} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={(value) => formatMetricValue(primarySeries.format, Number(value))} />
                <Tooltip formatter={tooltipFormatter} />
                <Legend />
                {chart.series.map((series, index) => (
                  <Line
                    key={series.key}
                    type="monotone"
                    dataKey={series.key}
                    name={series.label}
                    stroke={CHART_COLORS[index % CHART_COLORS.length]}
                    strokeWidth={2.5}
                    dot={{ r: 2.5 }}
                    activeDot={{ r: 4 }}
                    isAnimationActive={false}
                  />
                ))}
              </LineChart>
            ) : chart.chart_type === "composed" ? (
              <ComposedChart data={chart.rows}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey={chart.x_key} tick={{ fontSize: 11 }} minTickGap={16} />
                <YAxis
                  yAxisId="left"
                  tick={{ fontSize: 11 }}
                  tickFormatter={(value) => formatMetricValue(primarySeries.format, Number(value))}
                />
                {secondarySeries ? (
                  <YAxis
                    yAxisId={hasSecondaryAxis ? "right" : "left"}
                    orientation={hasSecondaryAxis ? "right" : "left"}
                    hide={!hasSecondaryAxis}
                    tick={{ fontSize: 11 }}
                    tickFormatter={(value) => formatMetricValue(secondarySeries.format, Number(value))}
                  />
                ) : null}
                <Tooltip formatter={tooltipFormatter} />
                <Legend />
                {chart.series.map((series, index) =>
                  (series.render_as || (index === 1 ? "line" : "bar")) === "line" ? (
                    <Line
                      key={series.key}
                      yAxisId={index === 1 && hasSecondaryAxis ? "right" : "left"}
                      type="monotone"
                      dataKey={series.key}
                      name={series.label}
                      stroke={CHART_COLORS[index % CHART_COLORS.length]}
                      strokeWidth={2.5}
                      dot={{ r: 2.5 }}
                      activeDot={{ r: 4 }}
                      isAnimationActive={false}
                    />
                  ) : (
                    <Bar
                      key={series.key}
                      yAxisId="left"
                      dataKey={series.key}
                      name={series.label}
                      fill={CHART_COLORS[index % CHART_COLORS.length]}
                      radius={[6, 6, 0, 0]}
                      isAnimationActive={false}
                    />
                  )
                )}
              </ComposedChart>
            ) : (
              <BarChart data={chart.rows}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey={chart.x_key} tick={{ fontSize: 11 }} minTickGap={16} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={(value) => formatMetricValue(primarySeries.format, Number(value))} />
                <Tooltip formatter={tooltipFormatter} />
                <Legend />
                {chart.series.map((series, index) => (
                  <Bar
                    key={series.key}
                    dataKey={series.key}
                    name={series.label}
                    fill={CHART_COLORS[index % CHART_COLORS.length]}
                    radius={[6, 6, 0, 0]}
                    isAnimationActive={false}
                  />
                ))}
              </BarChart>
            )}
          </ResponsiveContainer>
        </div>
      </div>
    </section>
  )
}
