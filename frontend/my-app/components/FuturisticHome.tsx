"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import Image from "next/image"
import { motion, useInView } from "framer-motion"
import {
  ArrowUpRight,
  BrainCircuit,
  ChartNoAxesColumn,
  Gauge,
  Radar,
  ShieldCheck,
  Sparkles,
  Zap,
} from "lucide-react"
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Line,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import AdminFileAccess from "@/components/AdminFileAccess"

type BrandConfig = {
  label: string
  value: string
  logo: string
  caption: string
}

type Props = {
  brandConfigs: BrandConfig[]
  onSelectBrand: (brand: string) => void
  onSelectMaster: () => void
  isAdmin: boolean
}

type NeuralNode = {
  x: number
  y: number
  vx: number
  vy: number
  r: number
}

const trendData = [
  { month: "Jan", pipeline: 42, conversion: 27 },
  { month: "Feb", pipeline: 55, conversion: 34 },
  { month: "Mar", pipeline: 61, conversion: 37 },
  { month: "Apr", pipeline: 59, conversion: 39 },
  { month: "May", pipeline: 74, conversion: 45 },
  { month: "Jun", pipeline: 82, conversion: 52 },
]

const segmentData = [
  { name: "Retail", value: 36, color: "#2563eb" },
  { name: "Online", value: 29, color: "#0ea5e9" },
  { name: "Enterprise", value: 21, color: "#14b8a6" },
  { name: "Partners", value: 14, color: "#8b5cf6" },
]

const systemData = [
  { bucket: "API", latency: 88, uptime: 99.95 },
  { bucket: "ETL", latency: 79, uptime: 99.8 },
  { bucket: "ML", latency: 74, uptime: 99.7 },
  { bucket: "Cache", latency: 94, uptime: 99.99 },
]

const features = [
  {
    title: "Predictive Signals",
    text: "Forecast premium and claim movement with adaptive trend scoring.",
    icon: BrainCircuit,
  },
  {
    title: "Live Data Pipelines",
    text: "Ingest and normalize multi-partner files into one coherent control plane.",
    icon: Zap,
  },
  {
    title: "Decision Radar",
    text: "Detect anomalies, concentration risk, and underperforming slices in real-time.",
    icon: Radar,
  },
]

const monitoringItems = [
  { label: "Ingestion Health", value: 97, color: "from-blue-500 to-cyan-400" },
  { label: "Model Confidence", value: 91, color: "from-violet-500 to-indigo-500" },
  { label: "Alert Precision", value: 94, color: "from-teal-500 to-emerald-500" },
]

function formatCompact(value: number) {
  if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`
  return `${Math.round(value)}`
}

function AnimatedCounter({
  value,
  suffix = "",
  decimals = 0,
}: {
  value: number
  suffix?: string
  decimals?: number
}) {
  const ref = useRef<HTMLSpanElement | null>(null)
  const inView = useInView(ref, { once: true, margin: "-20% 0px" })
  const [display, setDisplay] = useState(0)

  useEffect(() => {
    if (!inView) return
    let frame = 0
    const duration = 1200
    const startedAt = performance.now()

    const tick = (now: number) => {
      const p = Math.min((now - startedAt) / duration, 1)
      const eased = 1 - (1 - p) ** 3
      setDisplay(value * eased)
      if (p < 1) {
        frame = requestAnimationFrame(tick)
      }
    }
    frame = requestAnimationFrame(tick)
    return () => {
      if (frame) cancelAnimationFrame(frame)
    }
  }, [inView, value])

  return (
    <span ref={ref}>
      {display.toFixed(decimals)}
      {suffix}
    </span>
  )
}

function NeuralBackdrop() {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const pointerRef = useRef({ x: 0, y: 0, active: false })

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext("2d")
    if (!ctx) return

    let raf = 0
    const media = window.matchMedia("(prefers-reduced-motion: reduce)")

    const nodes: NeuralNode[] = []
    const resize = () => {
      const dpr = Math.min(window.devicePixelRatio || 1, 1.8)
      const width = window.innerWidth
      const height = window.innerHeight
      canvas.width = Math.floor(width * dpr)
      canvas.height = Math.floor(height * dpr)
      canvas.style.width = `${width}px`
      canvas.style.height = `${height}px`
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)

      const targetCount = Math.max(24, Math.min(56, Math.round((width * height) / 40000)))
      nodes.length = 0
      for (let i = 0; i < targetCount; i += 1) {
        nodes.push({
          x: Math.random() * width,
          y: Math.random() * height,
          vx: (Math.random() - 0.5) * 0.35,
          vy: (Math.random() - 0.5) * 0.35,
          r: Math.random() * 1.8 + 1.4,
        })
      }
    }

    const onPointerMove = (event: PointerEvent) => {
      pointerRef.current = { x: event.clientX, y: event.clientY, active: true }
    }
    const onPointerLeave = () => {
      pointerRef.current.active = false
    }

    const animate = () => {
      const width = canvas.clientWidth
      const height = canvas.clientHeight
      ctx.clearRect(0, 0, width, height)

      for (const n of nodes) {
        n.x += n.vx
        n.y += n.vy

        if (n.x < -30 || n.x > width + 30) n.vx *= -1
        if (n.y < -30 || n.y > height + 30) n.vy *= -1

        if (pointerRef.current.active && !media.matches) {
          const dx = pointerRef.current.x - n.x
          const dy = pointerRef.current.y - n.y
          const distance = Math.hypot(dx, dy)
          if (distance < 140 && distance > 0.1) {
            n.x -= (dx / distance) * 0.35
            n.y -= (dy / distance) * 0.35
          }
        }
      }

      for (let i = 0; i < nodes.length; i += 1) {
        const a = nodes[i]
        for (let j = i + 1; j < nodes.length; j += 1) {
          const b = nodes[j]
          const dx = a.x - b.x
          const dy = a.y - b.y
          const d = Math.hypot(dx, dy)
          if (d < 130) {
            const alpha = (1 - d / 130) * 0.18
            ctx.strokeStyle = `rgba(59, 130, 246, ${alpha.toFixed(3)})`
            ctx.lineWidth = 1
            ctx.beginPath()
            ctx.moveTo(a.x, a.y)
            ctx.lineTo(b.x, b.y)
            ctx.stroke()
          }
        }
      }

      for (const n of nodes) {
        ctx.fillStyle = "rgba(14, 165, 233, 0.55)"
        ctx.beginPath()
        ctx.arc(n.x, n.y, n.r, 0, Math.PI * 2)
        ctx.fill()
      }

      raf = requestAnimationFrame(animate)
    }

    resize()
    raf = requestAnimationFrame(animate)
    window.addEventListener("resize", resize)
    window.addEventListener("pointermove", onPointerMove)
    window.addEventListener("pointerleave", onPointerLeave)

    return () => {
      if (raf) cancelAnimationFrame(raf)
      window.removeEventListener("resize", resize)
      window.removeEventListener("pointermove", onPointerMove)
      window.removeEventListener("pointerleave", onPointerLeave)
    }
  }, [])

  return <canvas ref={canvasRef} className="absolute inset-0 h-full w-full" />
}

export default function FuturisticHome({
  brandConfigs,
  onSelectBrand,
  onSelectMaster,
  isAdmin,
}: Props) {
  const pageRef = useRef<HTMLDivElement | null>(null)
  const chartSectionRef = useRef<HTMLDivElement | null>(null)
  const chartInView = useInView(chartSectionRef, { once: true, margin: "-15% 0px" })
  const anchorItems = useMemo(
    () => [
      { id: "overview", label: "Overview" },
      { id: "metrics", label: "Real-time Metrics" },
      { id: "features", label: "Capabilities" },
      { id: "charts", label: "Interactive Charts" },
      { id: "monitoring", label: "Monitoring" },
      { id: "footer", label: "Footer" },
    ],
    []
  )

  useEffect(() => {
    let cancelled = false
    let gsapCtx: { revert: () => void } | null = null

    const setupScrollAnimations = async () => {
      try {
        const gsapModule = await import("gsap")
        const triggerModule = await import("gsap/ScrollTrigger")
        if (cancelled || !pageRef.current) return
        const gsap = gsapModule.gsap
        const ScrollTrigger = triggerModule.ScrollTrigger
        gsap.registerPlugin(ScrollTrigger)

        gsapCtx = gsap.context(() => {
          const reveals = gsap.utils.toArray<HTMLElement>("[data-reveal]")
          reveals.forEach((el, index) => {
            gsap.fromTo(
              el,
              { autoAlpha: 0, y: 24, scale: 0.985 },
              {
                autoAlpha: 1,
                y: 0,
                scale: 1,
                duration: 0.72,
                delay: Math.min(index * 0.03, 0.18),
                ease: "power2.out",
                scrollTrigger: {
                  trigger: el,
                  start: "top 86%",
                  once: true,
                },
              }
            )
          })
        }, pageRef)
      } catch (error) {
        console.error("Failed to initialize ScrollTrigger:", error)
      }
    }

    void setupScrollAnimations()
    return () => {
      cancelled = true
      gsapCtx?.revert()
    }
  }, [])

  return (
    <div ref={pageRef} className="relative min-h-full w-full overflow-x-hidden scroll-smooth">
      <div className="pointer-events-none absolute inset-0 -z-30 bg-[radial-gradient(circle_at_20%_18%,rgba(59,130,246,0.14),transparent_40%),radial-gradient(circle_at_82%_20%,rgba(139,92,246,0.14),transparent_40%),radial-gradient(circle_at_60%_80%,rgba(20,184,166,0.12),transparent_45%)]" />
      <div className="pointer-events-none absolute inset-0 -z-20 bg-gradient-to-br from-[#f6fbff] via-[#f8f9ff] to-[#f5fcff]" />
      <div className="pointer-events-none fixed inset-0 -z-10">
        <NeuralBackdrop />
      </div>

      {isAdmin && (
        <div className="fixed right-4 top-24 z-30 hidden w-[220px] xl:block">
          <div className="rounded-2xl border border-white/70 bg-white/75 p-3 shadow-[0_12px_30px_rgba(15,23,42,0.08)] backdrop-blur-md">
            <div className="mb-2 text-[10px] font-black uppercase tracking-[0.16em] text-slate-500">
              Admin Controls
            </div>
            <AdminFileAccess isAdmin={isAdmin} />
          </div>
        </div>
      )}

      <div className="mx-auto grid w-full max-w-[1500px] grid-cols-1 gap-5 px-3 pb-16 pt-5 sm:px-6 lg:grid-cols-[220px_minmax(0,1fr)] lg:gap-7 lg:px-8">
        <aside className="hidden lg:block">
          <div className="sticky top-24 space-y-4">
            <div className="rounded-2xl border border-white/70 bg-white/80 p-4 shadow-[0_14px_28px_rgba(15,23,42,0.07)] backdrop-blur-md">
              <div className="mb-3 flex items-center gap-2 text-sm font-bold text-slate-700">
                <ChartNoAxesColumn className="h-4 w-4 text-blue-600" />
                Section Navigator
              </div>
              <div className="space-y-1.5">
                {anchorItems.map((item) => (
                  <a
                    key={item.id}
                    href={`#${item.id}`}
                    className="block rounded-lg border border-transparent px-2.5 py-2 text-xs font-semibold text-slate-600 transition hover:border-blue-100 hover:bg-blue-50 hover:text-blue-700"
                  >
                    {item.label}
                  </a>
                ))}
              </div>
            </div>

            <motion.button
              whileHover={{ y: -2, scale: 1.01 }}
              onClick={onSelectMaster}
              className="w-full rounded-2xl bg-gradient-to-r from-blue-600 to-cyan-500 px-4 py-3 text-left text-white shadow-[0_16px_28px_rgba(37,99,235,0.35)]"
            >
              <div className="text-[11px] font-black uppercase tracking-[0.14em] text-blue-100">Workspace</div>
              <div className="mt-1 text-sm font-bold">Open Master Dashboard</div>
            </motion.button>
          </div>
        </aside>

        <main className="space-y-6 sm:space-y-8">
          <section
            id="overview"
            data-reveal
            className="relative overflow-hidden rounded-3xl border border-white/80 bg-white/85 p-5 shadow-[0_24px_40px_rgba(15,23,42,0.08)] backdrop-blur-xl sm:p-7"
          >
            <div className="absolute right-0 top-0 h-44 w-44 rounded-full bg-gradient-to-br from-blue-400/20 to-cyan-300/10 blur-3xl" />
            <div className="absolute bottom-0 left-0 h-40 w-40 rounded-full bg-gradient-to-br from-violet-400/15 to-indigo-300/10 blur-3xl" />

            <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
              <div className="inline-flex items-center gap-2 rounded-full border border-blue-200 bg-blue-50/80 px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.2em] text-blue-700">
                <Sparkles className="h-3.5 w-3.5" />
                AI Control Layer
              </div>
              <div className="flex flex-wrap gap-2">
                {anchorItems.slice(0, 4).map((item) => (
                  <a
                    key={item.id}
                    href={`#${item.id}`}
                    className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-[11px] font-semibold text-slate-600 transition hover:border-blue-200 hover:text-blue-700"
                  >
                    {item.label}
                  </a>
                ))}
              </div>
            </div>

            <div className="grid grid-cols-1 gap-6 xl:grid-cols-[1.1fr_0.9fr]">
              <div>
                <h2 className="text-3xl font-black leading-tight text-slate-900 sm:text-5xl">
                  Business Control Centre
                  <span className="mt-2 block bg-gradient-to-r from-blue-600 via-cyan-500 to-violet-600 bg-clip-text text-transparent">
                    Intelligent. Live. Actionable.
                  </span>
                </h2>
                <p className="mt-4 max-w-2xl text-sm leading-relaxed text-slate-600 sm:text-base">
                  Premium-grade analytics workspace with interactive insight cards, animated KPI rails, and partner
                  drilldowns engineered for enterprise decision loops.
                </p>

                <div className="mt-6 flex flex-wrap gap-3">
                  <motion.button
                    whileHover={{ y: -2 }}
                    onClick={onSelectMaster}
                    className="inline-flex items-center gap-2 rounded-xl bg-gradient-to-r from-blue-600 to-cyan-500 px-4 py-2.5 text-sm font-bold text-white shadow-[0_14px_26px_rgba(37,99,235,0.3)]"
                  >
                    Launch Master View
                    <ArrowUpRight className="h-4 w-4" />
                  </motion.button>
                  <a
                    href="#charts"
                    className="inline-flex items-center gap-2 rounded-xl border border-slate-200 bg-white px-4 py-2.5 text-sm font-semibold text-slate-700 transition hover:border-blue-200 hover:text-blue-700"
                  >
                    Explore Live Charts
                  </a>
                </div>
              </div>

              <div className="rounded-2xl border border-slate-200/80 bg-white/90 p-4 shadow-[0_12px_22px_rgba(15,23,42,0.07)]">
                <div className="mb-2 text-[11px] font-black uppercase tracking-[0.16em] text-slate-500">
                  Live Signal Preview
                </div>
                <div className="h-60">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={trendData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                      <XAxis dataKey="month" tick={{ fontSize: 11, fill: "#64748b" }} />
                      <YAxis yAxisId="left" tick={{ fontSize: 11, fill: "#64748b" }} />
                      <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11, fill: "#64748b" }} />
                      <Tooltip />
                      <Bar yAxisId="left" dataKey="pipeline" fill="#2563eb" radius={[6, 6, 0, 0]} />
                      <Line yAxisId="right" dataKey="conversion" stroke="#0ea5e9" strokeWidth={2.5} dot={{ r: 3 }} />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>

            {isAdmin && (
              <div className="mt-5 xl:hidden">
                <div className="rounded-2xl border border-slate-200 bg-white/90 p-3">
                  <div className="mb-2 text-[10px] font-black uppercase tracking-[0.16em] text-slate-500">
                    Admin Manual Access
                  </div>
                  <AdminFileAccess isAdmin={isAdmin} />
                </div>
              </div>
            )}
          </section>

          <section id="metrics" data-reveal className="rounded-3xl border border-white/80 bg-white/85 p-5 shadow-[0_20px_34px_rgba(15,23,42,0.07)] sm:p-7">
            <div className="mb-5 flex items-center justify-between gap-4">
              <div>
                <div className="text-[11px] font-black uppercase tracking-[0.16em] text-slate-500">Real-time Metrics</div>
                <h3 className="mt-1 text-2xl font-black text-slate-900">Operational Signal Board</h3>
              </div>
              <div className="rounded-full bg-slate-100 px-3 py-1 text-[11px] font-semibold text-slate-600">Updated every 5 min</div>
            </div>

            <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
              <div className="rounded-2xl border border-slate-200 bg-gradient-to-b from-white to-blue-50/60 p-4">
                <div className="text-[11px] font-semibold text-slate-500">Gross Premium Flow</div>
                <div className="mt-1 text-3xl font-black text-slate-900">
                  Rs <AnimatedCounter value={12.7} decimals={1} /> Cr
                </div>
                <div className="mt-2 text-xs font-semibold text-emerald-600">+8.4% vs previous cycle</div>
              </div>
              <div className="rounded-2xl border border-slate-200 bg-gradient-to-b from-white to-cyan-50/60 p-4">
                <div className="text-[11px] font-semibold text-slate-500">Claims Cost</div>
                <div className="mt-1 text-3xl font-black text-slate-900">
                  Rs <AnimatedCounter value={18.4} decimals={1} /> L
                </div>
                <div className="mt-2 text-xs font-semibold text-amber-600">Watchlist: 2 states rising</div>
              </div>
              <div className="rounded-2xl border border-slate-200 bg-gradient-to-b from-white to-violet-50/60 p-4">
                <div className="text-[11px] font-semibold text-slate-500">Active Partners</div>
                <div className="mt-1 text-3xl font-black text-slate-900">
                  <AnimatedCounter value={4} />
                </div>
                <div className="mt-2 text-xs font-semibold text-blue-600">All ingestion jobs healthy</div>
              </div>
              <div className="rounded-2xl border border-slate-200 bg-gradient-to-b from-white to-teal-50/60 p-4">
                <div className="text-[11px] font-semibold text-slate-500">Insight Throughput</div>
                <div className="mt-1 text-3xl font-black text-slate-900">
                  <AnimatedCounter value={324} /> /hr
                </div>
                <div className="mt-2 text-xs font-semibold text-slate-600">AI recommendation engine online</div>
              </div>
            </div>
          </section>

          <section id="features" data-reveal className="rounded-3xl border border-white/80 bg-white/85 p-5 shadow-[0_20px_34px_rgba(15,23,42,0.07)] sm:p-7">
            <div className="mb-5">
              <div className="text-[11px] font-black uppercase tracking-[0.16em] text-slate-500">Capability Stack</div>
              <h3 className="mt-1 text-2xl font-black text-slate-900">AI-driven Business Intelligence Features</h3>
            </div>
            <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
              {features.map((item) => (
                <motion.article
                  key={item.title}
                  whileHover={{ y: -4, scale: 1.01 }}
                  className="rounded-2xl border border-slate-200 bg-white p-4 shadow-[0_10px_18px_rgba(15,23,42,0.05)]"
                >
                  <div className="mb-3 inline-flex h-9 w-9 items-center justify-center rounded-lg bg-gradient-to-br from-blue-100 to-cyan-100 text-blue-700">
                    <item.icon className="h-4 w-4" />
                  </div>
                  <h4 className="text-base font-black text-slate-900">{item.title}</h4>
                  <p className="mt-2 text-sm leading-relaxed text-slate-600">{item.text}</p>
                </motion.article>
              ))}
            </div>
          </section>

          <section id="charts" ref={chartSectionRef} data-reveal className="rounded-3xl border border-white/80 bg-white/85 p-5 shadow-[0_20px_34px_rgba(15,23,42,0.07)] sm:p-7">
            <div className="mb-5 flex items-center justify-between gap-4">
              <div>
                <div className="text-[11px] font-black uppercase tracking-[0.16em] text-slate-500">Interactive Analytics</div>
                <h3 className="mt-1 text-2xl font-black text-slate-900">Animated Insight Charts</h3>
              </div>
              <div className="hidden items-center gap-2 rounded-full border border-slate-200 bg-white px-3 py-1 text-[11px] font-semibold text-slate-600 sm:flex">
                <Gauge className="h-3.5 w-3.5 text-cyan-600" />
                60 FPS Optimized
              </div>
            </div>

            {chartInView ? (
              <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                <div className="rounded-2xl border border-slate-200 bg-white p-4">
                  <div className="mb-2 text-sm font-bold text-slate-800">Revenue Velocity (Quarter)</div>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={trendData}>
                        <defs>
                          <linearGradient id="trendFill" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#2563eb" stopOpacity={0.35} />
                            <stop offset="95%" stopColor="#2563eb" stopOpacity={0.04} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="month" tick={{ fontSize: 11, fill: "#64748b" }} />
                        <YAxis tick={{ fontSize: 11, fill: "#64748b" }} />
                        <Tooltip formatter={(value: number | string | undefined) => formatCompact(Number(value || 0))} />
                        <Area type="monotone" dataKey="pipeline" stroke="#2563eb" fill="url(#trendFill)" strokeWidth={2.2} />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                <div className="rounded-2xl border border-slate-200 bg-white p-4">
                  <div className="mb-2 text-sm font-bold text-slate-800">Channel Contribution</div>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Tooltip />
                        <Pie data={segmentData} dataKey="value" innerRadius={52} outerRadius={88} paddingAngle={3} label>
                          {segmentData.map((entry) => (
                            <Cell key={entry.name} fill={entry.color} />
                          ))}
                        </Pie>
                      </PieChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
            ) : (
              <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                <div className="h-72 animate-pulse rounded-2xl border border-slate-200 bg-slate-100" />
                <div className="h-72 animate-pulse rounded-2xl border border-slate-200 bg-slate-100" />
              </div>
            )}
          </section>

          <section id="monitoring" data-reveal className="rounded-3xl border border-white/80 bg-white/85 p-5 shadow-[0_20px_34px_rgba(15,23,42,0.07)] sm:p-7">
            <div className="mb-5">
              <div className="text-[11px] font-black uppercase tracking-[0.16em] text-slate-500">System Monitoring</div>
              <h3 className="mt-1 text-2xl font-black text-slate-900">Platform Health and Reliability</h3>
            </div>
            <div className="grid grid-cols-1 gap-4 xl:grid-cols-[1.1fr_0.9fr]">
              <div className="rounded-2xl border border-slate-200 bg-white p-4">
                <div className="mb-2 text-sm font-bold text-slate-800">Service Performance Grid</div>
                <div className="h-64">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={systemData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                      <XAxis dataKey="bucket" tick={{ fontSize: 11, fill: "#64748b" }} />
                      <YAxis tick={{ fontSize: 11, fill: "#64748b" }} />
                      <Tooltip />
                      <Bar dataKey="latency" fill="#0ea5e9" radius={[6, 6, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="rounded-2xl border border-slate-200 bg-white p-4">
                <div className="mb-3 flex items-center gap-2 text-sm font-bold text-slate-800">
                  <ShieldCheck className="h-4 w-4 text-emerald-600" />
                  Stability Indicators
                </div>
                <div className="space-y-4">
                  {monitoringItems.map((item) => (
                    <div key={item.label}>
                      <div className="mb-1 flex items-center justify-between text-xs text-slate-600">
                        <span>{item.label}</span>
                        <span className="font-bold text-slate-800">{item.value}%</span>
                      </div>
                      <div className="h-2 rounded-full bg-slate-100">
                        <div className={`h-2 rounded-full bg-gradient-to-r ${item.color}`} style={{ width: `${item.value}%` }} />
                      </div>
                    </div>
                  ))}
                </div>
                <div className="mt-5 rounded-xl border border-blue-100 bg-blue-50/70 p-3 text-xs text-blue-900">
                  Neural anomaly scans are operating within expected confidence ranges.
                </div>
              </div>
            </div>
          </section>

          <section id="footer" data-reveal className="rounded-3xl border border-white/80 bg-white/90 p-5 shadow-[0_16px_28px_rgba(15,23,42,0.06)] sm:p-7">
            <div className="grid grid-cols-1 gap-5 lg:grid-cols-[1.1fr_0.9fr]">
              <div>
                <div className="text-[11px] font-black uppercase tracking-[0.16em] text-slate-500">Partner Launchpad</div>
                <h3 className="mt-1 text-2xl font-black text-slate-900">Select a Data Source</h3>
                <p className="mt-2 text-sm text-slate-600">
                  Jump straight into partner-specific analytics dashboards with normalized data lenses and drilldowns.
                </p>
                <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
                  {brandConfigs.map((cfg) => (
                    <motion.button
                      key={cfg.value}
                      whileHover={{ y: -2, scale: 1.01 }}
                      onClick={() => onSelectBrand(cfg.value)}
                      className="flex items-center gap-3 rounded-xl border border-slate-200 bg-white px-3 py-2.5 text-left shadow-sm transition hover:border-blue-200"
                    >
                      <Image src={cfg.logo} alt={cfg.label} width={52} height={22} className="h-5 w-12 object-contain" />
                      <div>
                        <div className="text-sm font-bold text-slate-800">{cfg.label}</div>
                        <div className="text-[11px] text-slate-500">{cfg.caption}</div>
                      </div>
                    </motion.button>
                  ))}
                </div>
              </div>

              <div className="rounded-2xl border border-slate-200 bg-gradient-to-br from-slate-50 to-white p-4">
                <div className="text-sm font-bold text-slate-800">Product Links</div>
                <ul className="mt-3 space-y-2 text-sm text-slate-600">
                  <li className="flex items-center justify-between rounded-lg border border-slate-200 bg-white px-3 py-2">
                    <span>Deck Studio</span>
                    <ArrowUpRight className="h-4 w-4 text-slate-400" />
                  </li>
                  <li className="flex items-center justify-between rounded-lg border border-slate-200 bg-white px-3 py-2">
                    <span>Master Dashboard</span>
                    <ArrowUpRight className="h-4 w-4 text-slate-400" />
                  </li>
                  <li className="flex items-center justify-between rounded-lg border border-slate-200 bg-white px-3 py-2">
                    <span>AI Sahyogi</span>
                    <ArrowUpRight className="h-4 w-4 text-slate-400" />
                  </li>
                </ul>
              </div>
            </div>
            <div className="mt-6 border-t border-slate-200 pt-4 text-xs text-slate-500">
              Zopper Analytics Platform. Built for intelligent, enterprise-grade business control loops.
            </div>
          </section>
        </main>
      </div>
    </div>
  )
}
