"use client"

import { useState, useMemo, useEffect, useRef, useCallback, useLayoutEffect } from "react"
import Image from "next/image"
import { useRouter } from "next/navigation"
import { motion, AnimatePresence, Variants, useReducedMotion } from "framer-motion"
import {
  Maximize2,
  BarChart3,
  ShieldCheck,
  Activity,
  LogOut,
  ChevronRight
} from "lucide-react"

import Sidebar from "@/components/Sidebar"
import Tabs from "@/components/Tabs"
import GraphSection from "@/components/GraphSection"
import { clearGraphDataCache } from "@/components/GraphView"
import DateRangePicker from "@/components/DateRangePicker"
import KpiCardsRow from "@/components/KpiCardsRow"
import RightSideChatbot from "@/components/RightSideChatbot"
import { fetchDateBounds, fetchAuthMe } from "./lib/api"

// --- ENHANCED ANIMATION VARIANTS ---
const fadeIn: Variants = {
  initial: { opacity: 0, y: 15 },
  animate: { opacity: 1, y: 0, transition: { duration: 0.6, ease: [0.22, 1, 0.36, 1] } }
}

const staggerContainer: Variants = {
  animate: { transition: { staggerChildren: 0.08 } }
}

const cardHover: Variants = {
  initial: { scale: 1, y: 0, boxShadow: "0px 10px 30px rgba(0,0,0,0.05)" },
  hover: { 
    scale: 1.02, 
    y: -10, 
    boxShadow: "0px 20px 40px rgba(0,0,0,0.1)",
    transition: { duration: 0.4, ease: "easeOut" } 
  }
}

const headerSlide: Variants = {
  initial: { y: -20, opacity: 0 },
  animate: { y: 0, opacity: 1, transition: { duration: 0.5, delay: 0.2 } }
}

type InitialDashboardState = {
  view: "home" | "dashboard"
  brand: string
  mode: "sales" | "claims"
  jobId: string | null
  from: string
  to: string
}

export default function DashboardPage() {
  const clampToCurrentMonth = useCallback((value: string) => {
    return value
  }, [])

  const normalizeToken = (value: string | null) => {
    if (!value) return null
    let token = value.trim()
    token = token.replace(/^['"]+|['"]+$/g, "")
    token = token.replace(/^Bearer\s+/i, "").trim()
    if (!token || token === "null" || token === "undefined") return null
    return token
  }

  const normalizeBrand = (value: string) => {
    const key = (value || "").trim().toLowerCase()
    if (key === "goodrej" || key === "goddrej") return "godrej"
    if (key === "reliance resq" || key === "reliance_resq" || key === "reliance-resq" || key === "resq") {
      return "reliance"
    }
    return key
  }

  const normalizeDateRange = useCallback((
    from: string,
    to: string,
    fallbackFrom = "",
    fallbackTo = ""
  ) => {
    const nextFrom = clampToCurrentMonth(from || fallbackFrom)
    const nextTo = clampToCurrentMonth(to || fallbackTo)
    if (!nextFrom || !nextTo) return { from: nextFrom, to: nextTo }
    if (nextFrom <= nextTo) return { from: nextFrom, to: nextTo }
    return { from: nextTo, to: nextFrom }
  }, [clampToCurrentMonth])

  const router = useRouter()
  const [initialDashboardState] = useState<InitialDashboardState>(() => {
    if (typeof window === "undefined") {
      return {
        view: "home" as "home" | "dashboard",
        brand: "samsung",
        mode: "sales" as "sales" | "claims",
        jobId: null as string | null,
        from: "",
        to: "",
      }
    }

    const storedView = localStorage.getItem("dashboard_view")
    const storedBrand = localStorage.getItem("dashboard_brand")
    const storedMode = localStorage.getItem("dashboard_mode")
    const storedFrom = localStorage.getItem("dashboard_from_date") || ""
    const storedTo = localStorage.getItem("dashboard_to_date") || ""

    const useJobFilter = localStorage.getItem("use_job_filter") === "1"
    const rawJobId = (localStorage.getItem("job_id") || "").trim()
    const jobId =
      useJobFilter && rawJobId && rawJobId !== "null" && rawJobId !== "undefined" && rawJobId !== "all"
        ? rawJobId
        : null

    const normalizedBrand = storedBrand ? normalizeBrand(storedBrand) : "samsung"
    const normalizedMode: "sales" | "claims" = storedMode === "claims" ? "claims" : "sales"

    return {
      view: storedView === "dashboard" ? "dashboard" : "home",
      brand: normalizedBrand,
      mode: normalizedMode,
      jobId,
      from: storedFrom,
      to: storedTo,
    }
  })

  const [view, setView] = useState<"home" | "dashboard">(initialDashboardState.view)
  const [brand, setBrand] = useState<string>(initialDashboardState.brand)
  const [mode, setMode] = useState<"sales" | "claims">(initialDashboardState.mode)
  const [isFullscreen, setIsFullscreen] = useState<boolean>(() => {
    if (typeof window === "undefined") return false
    return localStorage.getItem("dashboard_fullscreen") === "1"
  })
  const [isLoggingOut, setIsLoggingOut] = useState(false)
  const [jobId] = useState<string | null>(initialDashboardState.jobId)
  const effectiveJobId = brand === "godrej" ? null : jobId
  const [authRole, setAuthRole] = useState<"admin" | "employee" | null>(null)
  const [authName, setAuthName] = useState<string>("")
  const [authReady, setAuthReady] = useState(false)
  const prefersReducedMotion = useReducedMotion()
  const homeViewportRef = useRef<HTMLDivElement | null>(null)
  const homeSceneRef = useRef<HTMLDivElement | null>(null)
  const [homeSceneScale, setHomeSceneScale] = useState(1)

  const [fromDate, setFromDate] = useState<string>(initialDashboardState.from)
  const [toDate, setToDate] = useState<string>(initialDashboardState.to)
  const [draftFromDate, setDraftFromDate] = useState<string>(initialDashboardState.from)
  const [draftToDate, setDraftToDate] = useState<string>(initialDashboardState.to)
  const [defaultFromDate, setDefaultFromDate] = useState<string>("")
  const [defaultToDate, setDefaultToDate] = useState<string>("")
  const [defaultKey, setDefaultKey] = useState<string>("")
  const [filterRefreshTick, setFilterRefreshTick] = useState(0)

  const forcePageRefresh = useCallback((delayMs = 60) => {
    if (typeof window === "undefined") return
    window.setTimeout(() => {
      window.location.reload()
    }, delayMs)
  }, [])

  const dateStateRef = useRef({
    fromDate: initialDashboardState.from,
    toDate: initialDashboardState.to,
    draftFromDate: initialDashboardState.from,
    draftToDate: initialDashboardState.to,
    defaultFromDate: "",
    defaultToDate: "",
    defaultKey: "",
  })

  useEffect(() => {
    dateStateRef.current = {
      fromDate,
      toDate,
      draftFromDate,
      draftToDate,
      defaultFromDate,
      defaultToDate,
      defaultKey,
    }
  }, [fromDate, toDate, draftFromDate, draftToDate, defaultFromDate, defaultToDate, defaultKey])

  const forceFilterRefresh = useCallback(() => {
    clearGraphDataCache()
    setFilterRefreshTick((prev) => prev + 1)
  }, [])

  const todayIso = useCallback(() => new Date().toISOString().slice(0, 10), [])

  const applyBrandChange = (nextBrandRaw: string) => {
    const nextBrand = normalizeBrand(nextBrandRaw)
    const nextMode: "sales" | "claims" = mode
    setBrand(nextBrand)
    setMode(nextMode)
    setIsFullscreen(false)
    setDefaultKey("")
    setFromDate("")
    setToDate("")
    setDraftFromDate("")
    setDraftToDate("")
    setView("dashboard")
    if (typeof window !== "undefined") {
      localStorage.setItem("dashboard_brand", nextBrand)
      localStorage.setItem("dashboard_mode", nextMode)
      localStorage.setItem("dashboard_view", "dashboard")
      localStorage.setItem("dashboard_from_date", "")
      localStorage.setItem("dashboard_to_date", "")
      localStorage.setItem("dashboard_fullscreen", "0")
    }
    forceFilterRefresh()
  }

  useEffect(() => {
    const token = normalizeToken(localStorage.getItem("auth_token"))
    if (!token) {
      router.replace("/login")
      return
    }

    let active = true
    fetchAuthMe()
      .then((profile) => {
        if (!active) return
        setAuthRole(profile.role)
        setAuthName(profile.email)
        setAuthReady(true)
      })
      .catch((err: unknown) => {
        if (!active) return
        const msg = err instanceof Error ? err.message.toLowerCase() : String(err).toLowerCase()
        const isAuthError =
          msg.includes("not authenticated") ||
          msg.includes("invalid token") ||
          msg.includes("user inactive") ||
          msg.includes("http 401") ||
          msg.includes("http 403")

        if (isAuthError) {
          localStorage.removeItem("auth_token")
          router.replace("/login")
          return
        }

        const storedRole = localStorage.getItem("auth_role")
        const storedName = localStorage.getItem("auth_name") || ""
        if (storedRole === "admin" || storedRole === "employee") {
          setAuthRole(storedRole)
          setAuthName(storedName)
        }
        setAuthReady(true)
      })

    return () => { active = false }
  }, [router])

  useEffect(() => { localStorage.setItem("dashboard_view", view) }, [view])
  useEffect(() => { localStorage.setItem("dashboard_brand", brand) }, [brand])
  useEffect(() => { localStorage.setItem("dashboard_mode", mode) }, [mode])
  useEffect(() => {
    localStorage.setItem("dashboard_from_date", fromDate)
    localStorage.setItem("dashboard_to_date", toDate)
  }, [fromDate, toDate])

  useEffect(() => {
    if (!brand || !mode) return
    const nextKey = `${brand}|${mode}|${effectiveJobId || ""}`
    const snapshot = dateStateRef.current
    if (snapshot.defaultKey === nextKey && snapshot.defaultFromDate && snapshot.defaultToDate) return
    let mounted = true

    fetchDateBounds({ job_id: effectiveJobId || undefined, source: brand, dataset_type: mode })
      .then((res) => {
        if (!mounted) return
        const min = clampToCurrentMonth(res?.min_date ?? "")
        const max = clampToCurrentMonth(res?.max_date ?? "")
        const today = clampToCurrentMonth(todayIso())
        const hasAnyBounds = Boolean(min || max)
        if (!hasAnyBounds) {
          setDefaultFromDate("")
          setDefaultToDate("")
          setDefaultKey(nextKey)
          setFromDate("")
          setToDate("")
          setDraftFromDate("")
          setDraftToDate("")
          return
        }
        const fallbackFrom = min || max || today
        const upperBound = max
          ? (max <= today ? max : today)
          : today
        const fallbackTo = upperBound
        const clampToFilterWindow = (value: string) => {
          if (!value) return value
          if (min && value < min) return min
          if (upperBound && value > upperBound) return upperBound
          return value
        }
        const defaultRange = normalizeDateRange(
          clampToFilterWindow(fallbackFrom),
          clampToFilterWindow(fallbackTo),
          clampToFilterWindow(fallbackFrom),
          clampToFilterWindow(fallbackTo)
        )
        setDefaultFromDate(defaultRange.from)
        setDefaultToDate(defaultRange.to)
        setDefaultKey(nextKey)

        const isNewKeyLoad = snapshot.defaultKey !== nextKey

        if (isNewKeyLoad) {
          setFromDate(defaultRange.from)
          setToDate(defaultRange.to)
          setDraftFromDate(defaultRange.from)
          setDraftToDate(defaultRange.to)
          return
        }

        const normalizedSnapshotRange = normalizeDateRange(
          clampToFilterWindow(clampToCurrentMonth(snapshot.fromDate || defaultRange.from)),
          clampToFilterWindow(clampToCurrentMonth(snapshot.toDate || defaultRange.to)),
          defaultRange.from,
          defaultRange.to
        )
        const orderedFrom = normalizedSnapshotRange.from
        const orderedTo = normalizedSnapshotRange.to
        if (orderedFrom !== snapshot.fromDate || orderedTo !== snapshot.toDate) {
          setFromDate(orderedFrom); setToDate(orderedTo)
        }
        if (orderedFrom !== snapshot.draftFromDate || orderedTo !== snapshot.draftToDate) {
          setDraftFromDate(orderedFrom); setDraftToDate(orderedTo)
        }
      })
      .catch((err: unknown) => {
        if (!mounted) return
        console.error("Date bounds fetch failed; continuing without bounds.", err)
        const snapshot = dateStateRef.current
        const fallbackRange = normalizeDateRange(
          clampToCurrentMonth(snapshot.fromDate || ""),
          clampToCurrentMonth(snapshot.toDate || ""),
          "",
          ""
        )
        setDefaultFromDate("")
        setDefaultToDate("")
        setDefaultKey(nextKey)
        setFromDate(fallbackRange.from)
        setToDate(fallbackRange.to)
        setDraftFromDate(fallbackRange.from)
        setDraftToDate(fallbackRange.to)
      })
    return () => { mounted = false }
  }, [brand, mode, effectiveJobId, clampToCurrentMonth, todayIso, normalizeDateRange])

  const handleModeChange = (nextMode: "sales" | "claims") => {
    setIsFullscreen(false)
    if (nextMode === mode) {
      if (typeof window !== "undefined") {
        localStorage.setItem("dashboard_mode", nextMode)
        localStorage.setItem("dashboard_view", "dashboard")
        localStorage.setItem("dashboard_fullscreen", "0")
      }
      forcePageRefresh()
      return
    }

    setMode(nextMode)
    setDefaultKey("")
    setFromDate("")
    setToDate("")
    setDraftFromDate("")
    setDraftToDate("")
    if (typeof window !== "undefined") {
      localStorage.setItem("dashboard_mode", nextMode)
      localStorage.setItem("dashboard_view", "dashboard")
      localStorage.setItem("dashboard_fullscreen", "0")
    }
    forceFilterRefresh()
    forcePageRefresh()
  }

  const handleViewChange = (nextView: "home" | "dashboard") => {
    setView(nextView)
    if (typeof window !== "undefined") {
      localStorage.setItem("dashboard_view", nextView)
      if (nextView !== "dashboard") {
        localStorage.setItem("dashboard_fullscreen", "0")
      }
    }
    setIsFullscreen(false)
    forcePageRefresh()
  }

  const handleFullscreenToggle = (next: boolean) => {
    setIsFullscreen(next)
    if (typeof window !== "undefined") {
      localStorage.setItem("dashboard_fullscreen", next ? "1" : "0")
    }
    forcePageRefresh()
  }

  const theme = useMemo(() => ({
    primary: mode === "sales" ? "#6366f1" : "#f43f5e",
    secondary: mode === "sales" ? "#a5b4fc" : "#fda4af",
    accent: mode === "sales" ? "text-indigo-600" : "text-rose-600",
    bgLight: mode === "sales" ? "bg-indigo-50/50" : "bg-rose-50/50",
  }), [mode])

  useLayoutEffect(() => {
    if (view !== "home") {
      return
    }

    const viewportNode = homeViewportRef.current
    const sceneNode = homeSceneRef.current
    if (!viewportNode || !sceneNode) return

    let frame = 0
    const recalc = () => {
      const viewportWidth = Math.max(0, viewportNode.clientWidth - 20)
      const viewportHeight = Math.max(0, viewportNode.clientHeight - 20)
      const sceneWidth = sceneNode.scrollWidth
      const sceneHeight = sceneNode.scrollHeight
      if (!viewportWidth || !viewportHeight || !sceneWidth || !sceneHeight) return

      const widthScale = viewportWidth / sceneWidth
      const heightScale = viewportHeight / sceneHeight
      const nextScale = Math.min(1, widthScale, heightScale)
      const rounded = Number(nextScale.toFixed(3))
      setHomeSceneScale((prev) => (Math.abs(prev - rounded) > 0.005 ? rounded : prev))
    }

    const schedule = () => {
      if (frame) cancelAnimationFrame(frame)
      frame = requestAnimationFrame(recalc)
    }

    const observer = typeof ResizeObserver !== "undefined" ? new ResizeObserver(schedule) : null
    observer?.observe(viewportNode)
    observer?.observe(sceneNode)

    window.addEventListener("resize", schedule)
    window.addEventListener("orientationchange", schedule)

    schedule()
    const lateTimer = window.setTimeout(schedule, 180)

    return () => {
      window.clearTimeout(lateTimer)
      if (frame) cancelAnimationFrame(frame)
      window.removeEventListener("resize", schedule)
      window.removeEventListener("orientationchange", schedule)
      observer?.disconnect()
    }
  }, [view])

  const activeDateKey = `${brand}|${mode}|${effectiveJobId || ""}`
  const hasResolvedDateBounds = defaultKey === activeDateKey
  const hasDateBounds = Boolean(defaultFromDate && defaultToDate)
  const isDashboardDataReady =
    hasResolvedDateBounds && (!hasDateBounds || Boolean(fromDate && toDate))

  const handleGraphDateRangeApply = (nextFromRaw: string, nextToRaw: string) => {
    const next = normalizeDateRange(
      nextFromRaw,
      nextToRaw,
      fromDate || defaultFromDate,
      toDate || defaultToDate
    )
    setDraftFromDate(next.from)
    setDraftToDate(next.to)
    setFromDate(next.from)
    setToDate(next.to)
    forceFilterRefresh()
  }

  const resetDateRange = useCallback(() => {
    if (!defaultFromDate && !defaultToDate) {
      setFromDate("")
      setToDate("")
      setDraftFromDate("")
      setDraftToDate("")
      forceFilterRefresh()
      return
    }
    const defaultUpper = defaultToDate || todayIso()
    const resetRange = normalizeDateRange(
      defaultFromDate,
      defaultUpper,
      defaultFromDate || fromDate || defaultUpper,
      defaultUpper
    )
    setFromDate(resetRange.from)
    setToDate(resetRange.to)
    setDraftFromDate(resetRange.from)
    setDraftToDate(resetRange.to)
    forceFilterRefresh()
  }, [defaultFromDate, defaultToDate, fromDate, todayIso, normalizeDateRange, forceFilterRefresh])

  const brandLabel = (value: string) => {
    const labels: Record<string, string> = {
      samsung: "Samsung Overview",
      samsung_vs: "Samsung Vijay Sales",
      samsung_croma: "Samsung Croma",
      reliance: "Reliance ResQ",
      godrej: "Godrej"
    }
    return labels[value] || value.replace("_", " ")
  }

  const brandConfigs = [
    { label: "Samsung", value: "samsung", logo: "/WhatsApp Image 2026-02-04 at 11.14.29.jpeg", caption: "B2C Protect Max Analysis" },
    { label: "Reliance ResQ", value: "reliance", logo: "/resq.png", caption: "Reliance ResQ Analysis" },
    { label: "Godrej", value: "godrej", logo: "/Group 1244833444.png", caption: "Godrej Care Plus Analysis" },
  ]

  if (!authReady) {
    return (
      <div className="h-screen flex items-center justify-center bg-white">
        <motion.div 
          animate={{ scale: [0.95, 1, 0.95], opacity: [0.4, 1, 0.4] }} 
          transition={{ duration: 2, repeat: Infinity, ease: "easeInOut" }}
          className="flex flex-col items-center gap-4"
        >
          <Image src="/Zopper Logo Original 1.png" width={160} height={40} className="h-10 w-auto grayscale" alt="Loading" />
          <span className="text-[10px] font-bold tracking-[0.4em] text-slate-400 uppercase">Initializing System</span>
        </motion.div>
      </div>
    )
  }

  return (
    <div className="smooth-surface h-[100dvh] min-h-screen flex flex-col bg-[#fbfcfd] text-slate-900 font-sans selection:bg-indigo-100 overflow-hidden">
      {/* HEADER */}
      <motion.header 
        variants={headerSlide}
        initial="initial"
        animate="animate"
        className="sticky top-0 z-40 flex h-16 items-center justify-between border-b bg-white/80 px-3 backdrop-blur-2xl sm:h-20 sm:px-6 lg:px-10"
      >
        <div className="flex items-center gap-3 sm:gap-6">
          <motion.button
            whileHover={{ scale: 1.05 }}
            onClick={() => handleViewChange("home")}
            className="cursor-pointer"
          >
            <Image
              src="/Zopper Logo Original 1.png"
              alt="Zopper Logo"
              width={140}
              height={36}
              className="h-7 w-auto object-contain sm:h-9"
            />
          </motion.button>
          <div className="hidden sm:block h-8 w-[1px] bg-slate-200" />
          <h1 className="hidden items-center gap-3 sm:flex">
            <span className={`${theme.accent} font-black uppercase text-[11px] tracking-[0.4em]`}>
              Analytics 
            </span>
          </h1>
        </div>

        <div className="flex items-center gap-2 sm:gap-6">
          <AnimatePresence>
            {view === "dashboard" && (
              <motion.div 
                initial={{ opacity: 0, x: -20 }} 
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -20 }}
                className="hidden lg:flex items-center gap-3 px-4 py-2 rounded-full bg-slate-50 border border-slate-200/60"
              >
                <div className={`h-2 w-2 rounded-full animate-pulse ${mode === "sales" ? "bg-indigo-500 shadow-[0_0_10px_#6366f1]" : "bg-rose-500 shadow-[0_0_10px_#f43f5e]"}`} />
                <span className="text-[10px] font-bold uppercase tracking-widest text-slate-500">Live Insights Engine</span>
              </motion.div>
            )}
          </AnimatePresence>

          <div className="flex items-center gap-3">
            {authRole && (
              <div className="mr-2 hidden text-right sm:block">
                <p className="text-[10px] font-black uppercase tracking-tighter text-slate-400 leading-none mb-1">{authRole}</p>
                <p className="text-xs font-bold text-slate-700">{authName}</p>
              </div>
            )}
            <motion.button
              whileHover={{ backgroundColor: "#f1f5f9" }}
              whileTap={{ scale: 0.95 }}
              onClick={() => {
                setIsLoggingOut(true)
                setTimeout(() => {
                  localStorage.clear()
                  router.replace("/login")
                }, 800)
              }}
              className="rounded-full border border-slate-200 bg-white p-2 text-slate-500 transition-colors hover:text-slate-900 sm:p-2.5"
            >
              <LogOut size={18} />
            </motion.button>
          </div>
        </div>
      </motion.header>

      <AnimatePresence mode="wait">
        {view === "home" ? (
          <motion.div 
            key="home"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0, scale: 0.98 }}
            transition={{ duration: 0.5 }}
            ref={homeViewportRef}
            className="relative flex-1 min-h-0 overflow-y-auto overflow-x-hidden p-3 sm:p-6"
          >
            {/* Background Elements */}
            <div className="absolute inset-0 -z-10">
               <video autoPlay={!prefersReducedMotion} muted loop={!prefersReducedMotion} playsInline className="h-full w-full object-cover opacity-20 scale-105 blur-[2px]">
                <source src="/Business_Analytics_Video_Generation_Prompt.mp4" type="video/mp4" />
              </video>
              <div className="absolute inset-0 bg-gradient-to-b from-white/80 via-white/40 to-white" />
            </div>

            <div className="pointer-events-none absolute inset-0 z-0 overflow-hidden opacity-70">
              <svg
                viewBox="0 0 1400 900"
                preserveAspectRatio="none"
                className="h-full w-full"
                aria-hidden="true"
              >
                <defs>
                  <linearGradient id="growthLineA" x1="0%" y1="0%" x2="100%" y2="0%">
                    <stop offset="0%" stopColor="#38bdf8" stopOpacity="0.25" />
                    <stop offset="50%" stopColor="#f97316" stopOpacity="0.6" />
                    <stop offset="100%" stopColor="#1e40af" stopOpacity="0.25" />
                  </linearGradient>
                  <linearGradient id="growthLineB" x1="0%" y1="0%" x2="100%" y2="0%">
                    <stop offset="0%" stopColor="#818cf8" stopOpacity="0.15" />
                    <stop offset="100%" stopColor="#22d3ee" stopOpacity="0.35" />
                  </linearGradient>
                  <pattern id="bizGrid" width="48" height="48" patternUnits="userSpaceOnUse">
                    <path d="M 48 0 L 0 0 0 48" fill="none" stroke="#94a3b8" strokeOpacity="0.14" strokeWidth="1" />
                  </pattern>
                </defs>
                <rect width="1400" height="900" fill="url(#bizGrid)" />
                <motion.g
                  animate={{ x: [0, -80, 0] }}
                  transition={{ duration: 12, repeat: Infinity, ease: "linear" }}
                >
                  <motion.path
                    d="M-40,700 C120,640 220,690 340,560 C430,470 520,530 640,410 C740,310 860,380 960,260 C1050,150 1200,220 1460,110"
                    fill="none"
                    stroke="url(#growthLineA)"
                    strokeWidth="8"
                    strokeLinecap="round"
                    initial={{ pathLength: 0.25 }}
                    animate={{ pathLength: [0.25, 1, 0.25] }}
                    transition={{ duration: 7, repeat: Infinity, ease: "easeInOut" }}
                  />
                  <motion.path
                    d="M-60,760 C80,730 180,770 300,650 C410,540 520,600 630,490 C740,390 850,440 960,340 C1070,250 1210,300 1460,210"
                    fill="none"
                    stroke="url(#growthLineB)"
                    strokeWidth="4"
                    strokeLinecap="round"
                    strokeDasharray="14 12"
                    animate={{ pathOffset: [0, -26] }}
                    transition={{ duration: 1.6, repeat: Infinity, ease: "linear" }}
                  />
                </motion.g>
              </svg>
            </div>
            
            <motion.div 
              animate={{ 
                scale: [1, 1.15, 1], 
                x: [0, 20, 0], 
                y: [0, -20, 0] 
              }} 
              transition={{ duration: 15, repeat: Infinity, ease: "linear" }} 
              className="absolute top-0 right-0 w-[600px] h-[600px] rounded-full bg-indigo-100/30 blur-[100px]" 
            />

            {/* Motion cartoons */}
            <motion.div
              initial={{ opacity: 0, y: 30 }}
              animate={{ opacity: 1, y: [0, -10, 0], rotate: [0, -2, 0] }}
              transition={{ duration: 6, repeat: Infinity, ease: "easeInOut" }}
              className="pointer-events-none absolute left-12 top-24 hidden xl:flex items-center gap-3 rounded-3xl border border-indigo-200 bg-white/80 px-3 py-3 shadow-xl backdrop-blur-sm"
            >
              <div className="rounded-2xl bg-indigo-100 p-2 text-indigo-600">
                <BarChart3 size={18} />
              </div>
              <div className="flex gap-1.5">
                <motion.span className="h-8 w-1.5 rounded-full bg-indigo-300" animate={{ scaleY: [0.45, 1, 0.45] }} transition={{ duration: 1, repeat: Infinity }} />
                <motion.span className="h-8 w-1.5 rounded-full bg-indigo-400" animate={{ scaleY: [1, 0.55, 1] }} transition={{ duration: 1, repeat: Infinity, delay: 0.15 }} />
                <motion.span className="h-8 w-1.5 rounded-full bg-indigo-500" animate={{ scaleY: [0.55, 1, 0.55] }} transition={{ duration: 1, repeat: Infinity, delay: 0.3 }} />
              </div>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: -20 }}
              animate={{ opacity: 1, y: [0, 12, 0], rotate: [0, 2, 0] }}
              transition={{ duration: 7, repeat: Infinity, ease: "easeInOut", delay: 0.4 }}
              className="pointer-events-none absolute right-16 top-32 hidden xl:flex items-center gap-3 rounded-3xl border border-rose-200 bg-white/80 px-3 py-3 shadow-xl backdrop-blur-sm"
            >
              <div className="rounded-2xl bg-rose-100 p-2 text-rose-600">
                <ShieldCheck size={18} />
              </div>
              <div className="flex items-end gap-1">
                <motion.span className="h-2 w-2 rounded-full bg-rose-400" animate={{ y: [0, -5, 0] }} transition={{ duration: 0.9, repeat: Infinity }} />
                <motion.span className="h-3 w-2 rounded-full bg-rose-500" animate={{ y: [0, -8, 0] }} transition={{ duration: 0.9, repeat: Infinity, delay: 0.15 }} />
                <motion.span className="h-5 w-2 rounded-full bg-rose-600" animate={{ y: [0, -10, 0] }} transition={{ duration: 0.9, repeat: Infinity, delay: 0.3 }} />
              </div>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, x: 20 }}
              animate={{ opacity: 1, x: [0, -10, 0], y: [0, -8, 0] }}
              transition={{ duration: 5.5, repeat: Infinity, ease: "easeInOut", delay: 0.8 }}
              className="pointer-events-none absolute bottom-20 right-28 hidden xl:flex items-center gap-3 rounded-3xl border border-cyan-200 bg-white/80 px-3 py-3 shadow-xl backdrop-blur-sm"
            >
              <div className="rounded-2xl bg-cyan-100 p-2 text-cyan-700">
                <Activity size={18} />
              </div>
              <div className="h-8 w-12 overflow-hidden rounded-xl bg-cyan-50 p-1.5">
                <motion.div
                  className="h-0.5 w-full origin-left rounded bg-cyan-500"
                  animate={{ scaleX: [0.2, 1, 0.2], y: [0, 6, 12, 6, 0] }}
                  transition={{ duration: 1.4, repeat: Infinity, ease: "easeInOut" }}
                />
              </div>
            </motion.div>

            <div
              ref={homeSceneRef}
              className="w-full max-w-6xl mx-auto relative z-20 min-h-full flex flex-col justify-center py-6 items-center will-change-transform"
              style={{
                transform: `scale(${homeSceneScale})`,
                transformOrigin: "top center",
              }}
            >
              <div className="text-center mb-20">
                <motion.div variants={staggerContainer} initial="initial" animate="animate">
                  <motion.div variants={fadeIn} className="inline-flex items-center gap-2 px-4 py-1.5 rounded-full bg-white border border-slate-200 shadow-sm mb-8">
                    <span className="relative flex h-2 w-2">
                      <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-indigo-400 opacity-75"></span>
                      <span className="relative inline-flex rounded-full h-2 w-2 bg-indigo-500"></span>
                    </span>
                    <span className="text-[#1E6FFF] text-[10px] font-black uppercase tracking-[0.25em]">Unified Data Experience</span>
                  </motion.div>

                  <motion.h2 variants={fadeIn} className="font-black tracking-tight mb-8 text-center">
                    <span className="block text-slate-900 leading-tight text-2xl md:text-3xl">Welcome to</span>
                    <span className="block text-4xl text-transparent bg-clip-text bg-gradient-to-r from-indigo-600 via-purple-600 to-rose-500 italic font-serif sm:text-5xl md:text-7xl">
                     Business Control Centre
                    </span>
                  </motion.h2>
                  
                  <motion.p variants={fadeIn} className="mx-auto max-w-2xl text-center text-base font-medium leading-relaxed text-slate-500 sm:text-lg md:text-xl">
                    Navigate through partner ecosystems with precision. <br/>Real-time performance metrics at your fingertips.
                  </motion.p>
                </motion.div>
              </div>

              <motion.div 
                variants={staggerContainer} 
                initial="initial" 
                animate="animate"
                className="grid grid-cols-1 gap-4 justify-items-center sm:gap-8 md:grid-cols-3"
              >
                {brandConfigs.map((cfg) => (
                  <motion.div
                    key={cfg.value}
                    variants={cardHover}
                    whileHover="hover"
                    onClick={() => {
                      applyBrandChange(cfg.value)
                    }}
                    className="group relative w-full max-w-[360px] cursor-pointer rounded-[30px] border border-white bg-white/70 p-6 text-center shadow-2xl transition-all duration-500 backdrop-blur-md sm:max-w-[380px] sm:rounded-[48px] sm:p-10"
                  >
                    <div className="mb-6 flex h-20 items-center justify-center overflow-hidden sm:mb-10 sm:h-24">
                      <motion.div
                        whileHover={{ scale: 1.1, rotate: 2 }}
                      >
                        <Image
                          src={cfg.logo}
                          alt={cfg.label}
                          width={180}
                          height={80}
                          className={`max-h-full max-w-full object-contain filter drop-shadow-md ${cfg.value === "reliance" ? "scale-125" : ""}`}
                        />
                      </motion.div>
                    </div>
                    <div className="space-y-4 text-center">
                      <div className="flex items-center justify-center gap-3">
                        <h3 className="text-xl font-bold tracking-tight text-slate-800 sm:text-2xl">{cfg.label}</h3>
                        <div className="flex h-9 w-9 scale-0 items-center justify-center rounded-full bg-slate-900 text-white transition-transform duration-300 group-hover:scale-100 sm:h-10 sm:w-10">
                           <ChevronRight size={20} />
                        </div>
                      </div>
                      <p className="text-[10px] text-slate-400 font-black uppercase tracking-[0.2em]">{cfg.caption}</p>
                    </div>
                  </motion.div>
                ))}
              </motion.div>

            </div>
          </motion.div>
        ) : (
          <motion.div
            key="dashboard"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="flex flex-1 flex-col overflow-hidden bg-[#edf1f6] md:flex-row"
          >
            <Sidebar
              brand={brand}
              onChange={(b) => applyBrandChange(b)}
              currentView={view}
              onViewChange={handleViewChange}
              authRole={authRole}
            />

            <main className="custom-scrollbar min-w-0 flex-1 overflow-y-auto bg-[#edf1f6] px-3 py-3 sm:px-6 sm:py-4 lg:px-8">
              <div className="mx-auto w-full max-w-[1380px] pb-10">
                <motion.div
                  initial={{ opacity: 0, x: 16 }}
                  animate={{ opacity: 1, x: 0 }}
                  className="mb-4 rounded-2xl border border-slate-200 bg-white px-3 py-3 shadow-sm sm:px-5 sm:py-4"
                >
                  <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
                    <div className="space-y-1">
                      <h2 className="text-2xl font-bold tracking-tight text-slate-800 sm:text-3xl">
                        Dashboard <span className="text-slate-500">Control panel</span>
                      </h2>
                      <p className="text-sm text-slate-500">
                        {brandLabel(brand)} | {mode === "sales" ? "Sales Analytics" : "Claims Analytics"}
                      </p>
                    </div>

                    <div className="flex flex-wrap items-center gap-2 sm:gap-3 lg:justify-end">
                      {brand.startsWith("samsung") && (
                        <div className="flex items-center gap-2 rounded-xl border border-slate-200 bg-slate-50 px-2.5 py-2 sm:gap-3 sm:px-3">
                          <Image src="/WhatsApp Image 2026-02-04 at 11.14.29.jpeg" alt="Samsung" width={104} height={32} className="h-7 w-auto" />
                          {brand !== "samsung" && <div className="h-4 w-[1px] bg-slate-300" />}
                          {brand === "samsung_vs" && <Image src="/vs_logo.jpg" width={78} height={26} className="h-6 w-auto" alt="VS" />}
                          {brand === "samsung_croma" && <Image src="/croma_logo.jpg" width={78} height={26} className="h-6 w-auto" alt="Croma" />}
                        </div>
                      )}
                      <div className="hidden rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-[11px] font-semibold text-slate-500 sm:block">
                        Home <span className="mx-1 text-slate-300">-</span> Dashboard
                      </div>
                    </div>
                  </div>
                </motion.div>

                <div className="sticky top-0 z-30 mb-4 bg-[#edf1f6]/95 py-1.5 backdrop-blur sm:mb-5 sm:py-2">
                  <KpiCardsRow
                    source={brand}
                    datasetType={mode}
                    jobId={effectiveJobId || undefined}
                    fromDate={fromDate || undefined}
                    toDate={toDate || undefined}
                    refreshTick={filterRefreshTick}
                  />
                </div>

                <div className="grid grid-cols-1 gap-5 xl:grid-cols-12">
                  <motion.section
                    initial={{ opacity: 0, y: 24 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 0.15 }}
                    className="rounded-2xl border border-slate-200 bg-white p-3.5 shadow-sm sm:p-5 xl:col-span-8"
                  >
                    <div className="mb-5 flex flex-wrap items-center justify-between gap-4">
                      <div className="space-y-1">
                        <h3 className="text-xl font-bold text-slate-800">Traffic Sources</h3>
                        <p className="text-xs text-slate-500">Interactive trend workspace for selected filters</p>
                      </div>
                      <motion.button
                        whileHover={{ scale: 1.06, rotate: 4 }}
                        onClick={() => handleFullscreenToggle(true)}
                        className="rounded-full border border-slate-200 bg-slate-50 p-2.5 text-slate-600 transition-colors hover:bg-slate-100"
                      >
                        <Maximize2 size={18} />
                      </motion.button>
                    </div>

                    <div className="min-h-[360px] w-full sm:min-h-[460px]">
                      {isDashboardDataReady ? (
                        <GraphSection
                          key={`main-graph-${brand}-${mode}-${effectiveJobId || ""}-${fromDate}-${toDate}-${filterRefreshTick}`}
                          source={brand}
                          datasetType={mode}
                          jobId={effectiveJobId}
                          primaryColor={theme.primary}
                          secondaryColor={theme.secondary}
                          fromDate={fromDate || undefined}
                          toDate={toDate || undefined}
                          resetFromDate={defaultFromDate || undefined}
                          resetToDate={defaultToDate || undefined}
                          onDateRangeApply={handleGraphDateRangeApply}
                        />
                      ) : (
                        <div className="flex h-[360px] items-center justify-center text-sm text-slate-400 sm:h-[460px]">
                          Loading charts...
                        </div>
                      )}
                    </div>
                  </motion.section>

                  <motion.aside
                    variants={staggerContainer}
                    initial="initial"
                    animate="animate"
                    className="space-y-4 xl:col-span-4 xl:sticky xl:top-4 xl:self-start"
                  >
                    <motion.div variants={fadeIn} className="sticky top-2 z-20 rounded-2xl border border-slate-200 bg-white p-3.5 shadow-sm sm:p-5">
                      <div className="mb-3 flex items-center justify-between gap-3">
                        <h4 className="text-sm font-bold text-slate-800">Control Filters</h4>
                        <span className="rounded-full bg-slate-100 px-2 py-1 text-[10px] font-bold uppercase tracking-[0.12em] text-slate-500">
                          {mode}
                        </span>
                      </div>
                      <Tabs value={mode} onChange={handleModeChange} disableClaims={false} />
                      <DateRangePicker
                        draftFromDate={draftFromDate}
                        draftToDate={draftToDate}
                        minDate={defaultFromDate || undefined}
                        maxDate={defaultToDate && defaultToDate > todayIso() ? defaultToDate : todayIso()}
                        compact
                        onDraftChange={(from, to) => {
                          setDraftFromDate(clampToCurrentMonth(from))
                          setDraftToDate(clampToCurrentMonth(to))
                        }}
                        onApply={handleGraphDateRangeApply}
                        onReset={resetDateRange}
                      />
                    </motion.div>

                    <motion.div variants={fadeIn}>
                      <RightSideChatbot variant="card" />
                    </motion.div>
                  </motion.aside>
                </div>
              </div>
            </main>
          </motion.div>
        )}
      </AnimatePresence>

      {/* MODALS & OVERLAYS */}
      <AnimatePresence>
        {isFullscreen && (
          <motion.div initial={{ opacity: 0, scale: 1.1 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 1.1 }} className="fixed inset-0 z-[60] flex flex-col overflow-hidden bg-white">
            <div className="border-b bg-white/80 p-3 backdrop-blur-md sm:p-6 lg:p-8">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div className="flex items-center gap-3 sm:gap-4">
                <div className={`h-10 w-10 rounded-xl flex items-center justify-center ${theme.bgLight} ${theme.accent}`}>
                   <Activity size={20} />
                </div>
                <div>
                  <h4 className="text-lg font-black tracking-tight">{brandLabel(brand)}</h4>
                  <p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{mode === "sales" ? "Sales Velocity" : "Claims Integrity"}</p>
                </div>
              </div>
              <button className="w-full whitespace-nowrap rounded-2xl bg-slate-900 px-4 py-3 text-[11px] font-bold text-white transition-all hover:bg-black sm:w-auto sm:px-6 sm:text-xs lg:px-8" onClick={() => handleFullscreenToggle(false)}>Close Focus View</button>
              </div>
            </div>
            <div className="flex-1 overflow-auto p-3 sm:p-6 lg:p-12">
              {isDashboardDataReady ? (
                <GraphSection
                  key={`fullscreen-graph-${brand}-${mode}-${effectiveJobId || ""}-${fromDate}-${toDate}-${filterRefreshTick}`}
                  source={brand}
                  datasetType={mode}
                  jobId={effectiveJobId}
                  primaryColor={theme.primary}
                  fromDate={fromDate || undefined}
                  toDate={toDate || undefined}
                  resetFromDate={defaultFromDate || undefined}
                  resetToDate={defaultToDate || undefined}
                  onDateRangeApply={handleGraphDateRangeApply}
                />
              ) : (
                <div className="flex h-full min-h-[360px] items-center justify-center text-sm text-slate-400 sm:min-h-[500px]">
                  Loading charts...
                </div>
              )}
            </div>
            <RightSideChatbot variant="floating" />
          </motion.div>
        )}
      </AnimatePresence>

      {isLoggingOut && (
        <motion.div initial={{ y: 100 }} animate={{ y: 0 }} className="fixed bottom-10 left-1/2 -translate-x-1/2 z-[100]">
          <div className="bg-slate-900 text-white px-8 py-4 rounded-[24px] shadow-2xl flex items-center gap-4 border border-slate-800">
            <div className="h-2 w-2 bg-indigo-500 rounded-full animate-ping" />
            <span className="text-sm font-bold tracking-tight">Securing session and logging out...</span>
          </div>
        </motion.div>
      )}
    </div>
  )
}
