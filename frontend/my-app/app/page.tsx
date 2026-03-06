"use client"

import { useState, useMemo, useEffect, useRef, useCallback } from "react"
import Image from "next/image"
import dynamic from "next/dynamic"
import { useRouter } from "next/navigation"
import { motion, AnimatePresence, Variants, useReducedMotion, useScroll, useTransform, useMotionTemplate, useMotionValue, useSpring } from "framer-motion"
import {
  Maximize2,
  Activity,
  LogOut,
  ChevronRight
} from "lucide-react"

import Sidebar from "@/components/Sidebar"
import Tabs from "@/components/Tabs"
import { clearGraphDataCache } from "@/components/GraphView"
import DateRangePicker from "@/components/DateRangePicker"
import { fetchDateBounds, fetchAuthMe } from "./lib/api"

const GraphSection = dynamic(() => import("@/components/GraphSection"), {
  ssr: false,
  loading: () => (
    <div className="flex h-[360px] items-center justify-center text-sm text-slate-400 sm:h-[460px]">
      Loading charts...
    </div>
  ),
})

const KpiCardsRow = dynamic(() => import("@/components/KpiCardsRow"), {
  ssr: false,
  loading: () => (
    <div className="grid grid-cols-1 gap-3">
      {Array.from({ length: 3 }).map((_, index) => (
        <div
          key={`kpi-loader-${index}`}
          className="h-[92px] animate-pulse rounded-2xl bg-slate-100"
        />
      ))}
    </div>
  ),
})

const MasterDashboardView = dynamic(() => import("@/components/MasterDashboardView"), {
  ssr: false,
  loading: () => (
    <div className="min-h-[420px] animate-pulse rounded-3xl border border-slate-200 bg-white/70" />
  ),
})

const RightSideChatbot = dynamic(() => import("@/components/RightSideChatbot"), {
  ssr: false,
})

// --- ENHANCED ANIMATION VARIANTS ---
const fadeIn: Variants = {
  initial: { opacity: 0, y: 15 },
  animate: { opacity: 1, y: 0, transition: { duration: 0.6, ease: [0.22, 1, 0.36, 1] } }
}

const staggerContainer: Variants = {
  animate: { transition: { staggerChildren: 0.08 } }
}

const cardHover: Variants = {
  initial: { scale: 1, y: 0, boxShadow: "0px 18px 44px rgba(148,163,184,0.2)" },
  hover: { 
    scale: 1.018, 
    y: -10, 
    boxShadow: "0px 28px 80px rgba(14,165,233,0.16)",
    transition: { duration: 0.4, ease: "easeOut" } 
  }
}

const headerSlide: Variants = {
  initial: { y: -20, opacity: 0 },
  animate: { y: 0, opacity: 1, transition: { duration: 0.5, delay: 0.2 } }
}

type NeuralGlowField = {
  left: string
  top: string
  size: number
  color: string
  dx: number
  dy: number
  duration: number
  delay: number
  blur: number
}

type NeuralNode = {
  left: string
  top: string
  size: number
  color: string
  duration: number
  delay: number
}

type NeuralConnection = {
  left: string
  top: string
  width: number
  rotate: number
  color: string
  duration: number
  delay: number
}

type AnalyticalPanel = {
  left: string
  top: string
  width: number
  height: number
  rotate: number
  delay: number
  label: string
  bars: number[]
  accent: string
  secondary: string
  trendPath: string
}

const NEURAL_GLOW_FIELDS: NeuralGlowField[] = [
  { left: "4%", top: "8%", size: 210, color: "rgba(56,189,248,0.14)", dx: 18, dy: -14, duration: 14.8, delay: 0.3, blur: 20 },
  { left: "22%", top: "54%", size: 190, color: "rgba(99,102,241,0.1)", dx: -14, dy: -20, duration: 17.1, delay: 1.0, blur: 18 },
  { left: "38%", top: "18%", size: 230, color: "rgba(59,130,246,0.12)", dx: 16, dy: -16, duration: 18.3, delay: 0.5, blur: 22 },
  { left: "58%", top: "34%", size: 200, color: "rgba(20,184,166,0.12)", dx: 20, dy: -12, duration: 16.5, delay: 1.4, blur: 19 },
  { left: "78%", top: "12%", size: 190, color: "rgba(125,211,252,0.11)", dx: -18, dy: -20, duration: 15.9, delay: 0.8, blur: 18 },
  { left: "72%", top: "72%", size: 220, color: "rgba(14,165,233,0.12)", dx: 16, dy: -18, duration: 17.7, delay: 0.9, blur: 21 },
  { left: "12%", top: "78%", size: 180, color: "rgba(15,23,42,0.08)", dx: 22, dy: -14, duration: 15.6, delay: 1.3, blur: 17 },
]

const NEURAL_CONNECTIONS: NeuralConnection[] = [
  { left: "10%", top: "28%", width: 190, rotate: 14, color: "rgba(56,189,248,0.2)", duration: 6.8, delay: 0.2 },
  { left: "18%", top: "44%", width: 165, rotate: -11, color: "rgba(59,130,246,0.18)", duration: 6.2, delay: 0.9 },
  { left: "34%", top: "24%", width: 200, rotate: 16, color: "rgba(99,102,241,0.18)", duration: 5.9, delay: 0.4 },
  { left: "48%", top: "52%", width: 210, rotate: -9, color: "rgba(20,184,166,0.2)", duration: 6.5, delay: 1.1 },
  { left: "62%", top: "20%", width: 185, rotate: 12, color: "rgba(14,165,233,0.16)", duration: 5.6, delay: 0.7 },
  { left: "70%", top: "64%", width: 170, rotate: -17, color: "rgba(6,182,212,0.18)", duration: 6.1, delay: 0.6 },
  { left: "56%", top: "74%", width: 150, rotate: 10, color: "rgba(15,23,42,0.14)", duration: 5.7, delay: 1.3 },
]

const NEURAL_NODES: NeuralNode[] = [
  { left: "12%", top: "26%", size: 12, color: "rgba(56,189,248,0.9)", duration: 4.3, delay: 0.2 },
  { left: "20%", top: "41%", size: 9, color: "rgba(59,130,246,0.84)", duration: 3.9, delay: 0.7 },
  { left: "31%", top: "22%", size: 11, color: "rgba(99,102,241,0.88)", duration: 4.5, delay: 0.3 },
  { left: "44%", top: "34%", size: 8, color: "rgba(20,184,166,0.86)", duration: 3.8, delay: 1.0 },
  { left: "51%", top: "50%", size: 10, color: "rgba(14,165,233,0.82)", duration: 4.1, delay: 1.2 },
  { left: "63%", top: "24%", size: 10, color: "rgba(125,211,252,0.82)", duration: 4.6, delay: 0.9 },
  { left: "71%", top: "40%", size: 9, color: "rgba(59,130,246,0.84)", duration: 3.7, delay: 0.5 },
  { left: "78%", top: "62%", size: 11, color: "rgba(6,182,212,0.88)", duration: 4.0, delay: 0.6 },
  { left: "60%", top: "76%", size: 8, color: "rgba(30,41,59,0.6)", duration: 3.6, delay: 1.4 },
  { left: "87%", top: "28%", size: 12, color: "rgba(20,184,166,0.8)", duration: 4.4, delay: 1.1 },
]

const ANALYTICAL_PANELS: AnalyticalPanel[] = [
  {
    left: "5%",
    top: "18%",
    width: 220,
    height: 138,
    rotate: -8,
    delay: 0.2,
    label: "Flow Signal",
    bars: [18, 30, 26, 42, 58, 46],
    accent: "rgba(37,99,235,0.82)",
    secondary: "rgba(125,211,252,0.46)",
    trendPath: "M6 82 C 24 70, 38 72, 54 58 S 92 42, 122 46 S 162 54, 194 20",
  },
  {
    left: "78%",
    top: "26%",
    width: 212,
    height: 132,
    rotate: 7,
    delay: 0.7,
    label: "Node Load",
    bars: [24, 18, 36, 54, 44, 62],
    accent: "rgba(8,145,178,0.82)",
    secondary: "rgba(165,243,252,0.42)",
    trendPath: "M8 76 C 30 56, 52 64, 74 48 S 114 28, 142 34 S 176 58, 194 26",
  },
  {
    left: "12%",
    top: "72%",
    width: 230,
    height: 144,
    rotate: 6,
    delay: 1.1,
    label: "Forecast Grid",
    bars: [28, 36, 30, 48, 54, 68],
    accent: "rgba(13,148,136,0.8)",
    secondary: "rgba(153,246,228,0.4)",
    trendPath: "M6 86 C 28 72, 50 62, 70 58 S 112 40, 140 46 S 176 66, 206 24",
  },
]

type InitialDashboardState = {
  view: "home" | "master" | "dashboard"
  brand: string
  mode: "sales" | "claims"
  jobId: string | null
  from: string
  to: string
}

export default function DashboardPage() {
  const clampToCurrentMonth = useCallback((value: string) => {
    const normalized = (value || "").trim()
    if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) return ""
    const today = new Date().toISOString().slice(0, 10)
    return normalized > today ? today : normalized
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
        view: "home" as "home" | "master" | "dashboard",
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

    const normalizedView: "home" | "master" | "dashboard" =
      storedView === "dashboard" ? "dashboard" : storedView === "master" ? "master" : "home"

    return {
      view: normalizedView,
      brand: normalizedBrand,
      mode: normalizedMode,
      jobId,
      from: storedFrom,
      to: storedTo,
    }
  })

  const [view, setView] = useState<"home" | "master" | "dashboard">(initialDashboardState.view)
  const [brand, setBrand] = useState<string>(initialDashboardState.brand)
  const [mode, setMode] = useState<"sales" | "claims">(initialDashboardState.mode)
  const [isFullscreen, setIsFullscreen] = useState<boolean>(() => {
    if (typeof window === "undefined") return false
    return localStorage.getItem("dashboard_fullscreen") === "1"
  })
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState<boolean>(() => {
    if (typeof window === "undefined") return false
    return localStorage.getItem("dashboard_sidebar_collapsed") === "1"
  })
  const [isLoggingOut, setIsLoggingOut] = useState(false)
  const [jobId] = useState<string | null>(initialDashboardState.jobId)
  const effectiveJobId = jobId
  const [authRole, setAuthRole] = useState<"admin" | "employee" | null>(null)
  const [authName, setAuthName] = useState<string>("")
  const [authReady, setAuthReady] = useState(false)
  const prefersReducedMotion = useReducedMotion()
  const homeViewportRef = useRef<HTMLDivElement | null>(null)
  const { scrollYProgress: homeScrollProgress } = useScroll({ container: homeViewportRef })
  const homePointerX = useMotionValue(0.5)
  const homePointerY = useMotionValue(0.5)
  const homePointerXSpring = useSpring(homePointerX, { stiffness: 180, damping: 24, mass: 0.45 })
  const homePointerYSpring = useSpring(homePointerY, { stiffness: 180, damping: 24, mass: 0.45 })
  const homeHeroY = useTransform(homeScrollProgress, [0, 1], [0, -132])
  const homeHeroOpacity = useTransform(homeScrollProgress, [0, 0.85, 1], [1, 0.92, 0.62])
  const homeCardsY = useTransform(homeScrollProgress, [0, 1], [0, -56])
  const homeCardsRotateX = useTransform(homeScrollProgress, [0, 1], [0, 10])
  const homeBackgroundY = useTransform(homeScrollProgress, [0, 1], [0, -120])
  const homeNetworkX = useTransform(homePointerXSpring, [0, 1], [-32, 32])
  const homeNetworkY = useTransform(homePointerYSpring, [0, 1], [-24, 24])
  const homeGlowX = useTransform(homePointerXSpring, [0, 1], [-18, 18])
  const homeGlowY = useTransform(homePointerYSpring, [0, 1], [-14, 14])
  const homeCursorPrimaryX = useTransform(homePointerXSpring, [0, 1], ["18%", "82%"])
  const homeCursorPrimaryY = useTransform(homePointerYSpring, [0, 1], ["16%", "78%"])
  const homeCursorSecondaryX = useTransform(homePointerXSpring, [0, 1], ["80%", "26%"])
  const homeCursorSecondaryY = useTransform(homePointerYSpring, [0, 1], ["74%", "22%"])
  const homeCursorGlow = useMotionTemplate`radial-gradient(circle at ${homeCursorPrimaryX} ${homeCursorPrimaryY}, rgba(56,189,248,0.16), transparent 24%), radial-gradient(circle at ${homeCursorSecondaryX} ${homeCursorSecondaryY}, rgba(20,184,166,0.14), transparent 28%)`
  const [isMobileViewport, setIsMobileViewport] = useState<boolean>(() => {
    if (typeof window === "undefined") return false
    return window.innerWidth < 768
  })
  const [isMobileFiltersCollapsed, setIsMobileFiltersCollapsed] = useState(false)

  const [fromDate, setFromDate] = useState<string>(initialDashboardState.from)
  const [toDate, setToDate] = useState<string>(initialDashboardState.to)
  const [draftFromDate, setDraftFromDate] = useState<string>(initialDashboardState.from)
  const [draftToDate, setDraftToDate] = useState<string>(initialDashboardState.to)
  const [defaultFromDate, setDefaultFromDate] = useState<string>("")
  const [defaultToDate, setDefaultToDate] = useState<string>("")
  const [defaultKey, setDefaultKey] = useState<string>("")
  const [filterRefreshTick, setFilterRefreshTick] = useState(0)

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

  useEffect(() => {
    if (typeof window === "undefined") return
    const handleResize = () => {
      const mobile = window.innerWidth < 768
      setIsMobileViewport(mobile)
      if (!mobile) {
        setIsMobileFiltersCollapsed(false)
      }
    }
    handleResize()
    window.addEventListener("resize", handleResize)
    return () => {
      window.removeEventListener("resize", handleResize)
    }
  }, [])

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
  useEffect(() => {
    localStorage.setItem("dashboard_brand", brand)
    window.dispatchEvent(new CustomEvent("dashboard-context-changed", { detail: { brand, mode } }))
  }, [brand, mode])
  useEffect(() => {
    localStorage.setItem("dashboard_mode", mode)
    window.dispatchEvent(new CustomEvent("dashboard-context-changed", { detail: { brand, mode } }))
  }, [mode, brand])
  useEffect(() => {
    localStorage.setItem("dashboard_sidebar_collapsed", isSidebarCollapsed ? "1" : "0")
  }, [isSidebarCollapsed])
  useEffect(() => {
    localStorage.setItem("dashboard_from_date", fromDate)
    localStorage.setItem("dashboard_to_date", toDate)
  }, [fromDate, toDate])

  useEffect(() => {
    if (view !== "dashboard" || !brand || !mode) return
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
        const effectiveMin = min && min <= today ? min : ""
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
        const fallbackFrom = effectiveMin || today
        const upperBound = today
        const fallbackTo = upperBound
        const clampToFilterWindow = (value: string) => {
          if (!value) return value
          if (effectiveMin && value < effectiveMin) return effectiveMin
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
  }, [view, brand, mode, effectiveJobId, clampToCurrentMonth, todayIso, normalizeDateRange])

  const handleModeChange = (nextMode: "sales" | "claims") => {
    setIsFullscreen(false)
    if (nextMode === mode) {
      if (typeof window !== "undefined") {
        localStorage.setItem("dashboard_mode", nextMode)
        localStorage.setItem("dashboard_view", "dashboard")
        localStorage.setItem("dashboard_fullscreen", "0")
      }
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
  }

  const handleViewChange = (nextView: "home" | "master" | "dashboard") => {
    if (nextView === view) return
    setView(nextView)
    if (typeof window !== "undefined") {
      localStorage.setItem("dashboard_view", nextView)
      if (nextView !== "dashboard") {
        localStorage.setItem("dashboard_fullscreen", "0")
      }
    }
    setIsFullscreen(false)
  }

  const handleFullscreenToggle = (next: boolean) => {
    if (next === isFullscreen) return
    setIsFullscreen(next)
    if (typeof window !== "undefined") {
      localStorage.setItem("dashboard_fullscreen", next ? "1" : "0")
    }
  }

  const theme = useMemo(() => ({
    primary: mode === "sales" ? "#6366f1" : "#f43f5e",
    secondary: mode === "sales" ? "#a5b4fc" : "#fda4af",
    accent: mode === "sales" ? "text-indigo-600" : "text-rose-600",
    bgLight: mode === "sales" ? "bg-indigo-50/50" : "bg-rose-50/50",
  }), [mode])

  const activeDateKey = `${brand}|${mode}|${effectiveJobId || ""}`
  const hasResolvedDateBounds = defaultKey === activeDateKey
  const hasDateBounds = Boolean(defaultFromDate && defaultToDate)
  const isDashboardDataReady =
    hasResolvedDateBounds && (!hasDateBounds || Boolean(fromDate && toDate))

  const persistCurrentDashboardContext = useCallback(() => {
    if (typeof window === "undefined") return
    localStorage.setItem("dashboard_view", "dashboard")
    localStorage.setItem("dashboard_brand", brand)
    localStorage.setItem("dashboard_mode", mode)
    localStorage.setItem("dashboard_fullscreen", isFullscreen ? "1" : "0")
  }, [brand, mode, isFullscreen])

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
    persistCurrentDashboardContext()
    forceFilterRefresh()
  }

  const resetDateRange = useCallback(() => {
    if (!defaultFromDate && !defaultToDate) {
      setFromDate("")
      setToDate("")
      setDraftFromDate("")
      setDraftToDate("")
      persistCurrentDashboardContext()
      forceFilterRefresh()
      return
    }
    const defaultUpper = todayIso()
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
    persistCurrentDashboardContext()
    forceFilterRefresh()
  }, [defaultFromDate, defaultToDate, fromDate, todayIso, normalizeDateRange, forceFilterRefresh, persistCurrentDashboardContext])

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

  type PartnerHeaderLogo = {
    src: string
    alt: string
    width: number
    height: number
    className?: string
  }

  const samsungProtectMaxLogo: PartnerHeaderLogo = {
    src: "/WhatsApp Image 2026-02-04 at 11.14.29.jpeg",
    alt: "Samsung Protect Max logo",
    width: 88,
    height: 28,
    className: "h-5 w-auto object-contain",
  }

  const partnerLogoConfig: Record<string, PartnerHeaderLogo[]> = {
    samsung: [samsungProtectMaxLogo],
    samsung_vs: [
      samsungProtectMaxLogo,
      {
        src: "/vs_logo.jpg",
        alt: "Vijay Sales logo",
        width: 88,
        height: 28,
        className: "h-5 w-auto object-contain",
      },
    ],
    samsung_croma: [
      samsungProtectMaxLogo,
      {
        src: "/croma_logo.jpg",
        alt: "Croma logo",
        width: 88,
        height: 28,
        className: "h-5 w-auto object-contain",
      },
    ],
    reliance: [
      {
        src: "/resq.png",
        alt: "Reliance ResQ logo",
        width: 96,
        height: 28,
        className: "h-5 w-auto object-contain",
      },
    ],
    godrej: [
      {
        src: "/Group 1244833444.png",
        alt: "Godrej logo",
        width: 96,
        height: 28,
        className: "h-5 w-auto object-contain",
      },
    ],
  }

  const masterCardLogos: PartnerHeaderLogo[] = [
    samsungProtectMaxLogo,
    {
      src: "/croma_logo.jpg",
      alt: "Croma logo",
      width: 88,
      height: 28,
      className: "h-5 w-auto object-contain",
    },
    {
      src: "/vs_logo.jpg",
      alt: "Vijay Sales logo",
      width: 88,
      height: 28,
      className: "h-5 w-auto object-contain",
    },
    {
      src: "/resq.png",
      alt: "Reliance ResQ logo",
      width: 96,
      height: 28,
      className: "h-5 w-auto object-contain",
    },
    {
      src: "/Group 1244833444.png",
      alt: "Godrej logo",
      width: 96,
      height: 28,
      className: "h-5 w-auto object-contain",
    },
  ]

  const activePartnerLogos = partnerLogoConfig[brand] || []
  const headerPartnerLogos = view === "master" ? masterCardLogos : view === "dashboard" ? activePartnerLogos : []
  const headerPartnerLabel = view === "master" ? "Master Dashboard" : brandLabel(brand)
  const isHomeView = view === "home"

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
    <div
      className={`smooth-surface h-[100dvh] min-h-screen flex flex-col font-sans overflow-hidden ${
        isHomeView
          ? "bg-[#f6f9fc] text-slate-900 selection:bg-sky-100"
          : "bg-[#fbfcfd] text-slate-900 selection:bg-indigo-100"
      }`}
    >
      {/* HEADER */}
      <motion.header 
        variants={headerSlide}
        initial="initial"
        animate="animate"
        className={`sticky top-0 z-40 flex h-16 items-center justify-between border-b px-3 backdrop-blur-2xl sm:h-20 sm:px-6 lg:px-10 ${
          isHomeView
            ? "border-slate-200/80 bg-white/72"
            : "border-slate-200 bg-white/80"
        }`}
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
          <div className={`hidden h-8 w-[1px] sm:block ${isHomeView ? "bg-slate-200" : "bg-slate-200"}`} />
          <h1 className="hidden items-center gap-3 sm:flex">
            <span className={`${isHomeView ? "text-sky-700" : theme.accent} font-black uppercase text-[11px] tracking-[0.4em]`}>
              Analytics 
            </span>
          </h1>
          {headerPartnerLogos.length > 0 && (
            <motion.div
              initial={{ opacity: 0, y: -4 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.25 }}
              className="hidden items-center gap-2 rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 sm:flex"
            >
              <div className="flex items-center gap-1.5">
                {headerPartnerLogos.map((logo, index) => (
                  <Image
                    key={`${logo.src}-${index}`}
                    src={logo.src}
                    alt={logo.alt}
                    width={logo.width}
                    height={logo.height}
                    className={logo.className}
                  />
                ))}
              </div>
              <span className="text-[10px] font-bold uppercase tracking-[0.12em] text-slate-600">
                {headerPartnerLabel}
              </span>
            </motion.div>
          )}
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
                <p className={`mb-1 text-[10px] font-black uppercase tracking-tighter leading-none ${isHomeView ? "text-slate-400" : "text-slate-400"}`}>{authRole}</p>
                <p className={`text-xs font-bold ${isHomeView ? "text-slate-700" : "text-slate-700"}`}>{authName}</p>
              </div>
            )}
            <motion.button
              whileHover={{ backgroundColor: isHomeView ? "rgba(255,255,255,0.96)" : "#f1f5f9" }}
              whileTap={{ scale: 0.95 }}
              onClick={() => {
                setIsLoggingOut(true)
                setTimeout(() => {
                  localStorage.clear()
                  router.replace("/login")
                }, 800)
              }}
              className={`rounded-full border p-2 transition-colors sm:p-2.5 ${
                isHomeView
                  ? "border-slate-200 bg-white/80 text-slate-500 hover:text-slate-900"
                  : "border-slate-200 bg-white text-slate-500 hover:text-slate-900"
              }`}
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
            onPointerMove={(event) => {
              if (prefersReducedMotion) return
              const rect = event.currentTarget.getBoundingClientRect()
              const nextX = Math.min(1, Math.max(0, (event.clientX - rect.left) / Math.max(rect.width, 1)))
              const nextY = Math.min(1, Math.max(0, (event.clientY - rect.top) / Math.max(rect.height, 1)))
              homePointerX.set(nextX)
              homePointerY.set(nextY)
            }}
            onPointerLeave={() => {
              if (prefersReducedMotion) return
              homePointerX.set(0.5)
              homePointerY.set(0.5)
            }}
            className="relative flex-1 min-h-0 overflow-y-auto overflow-x-hidden"
          >
            <div className="absolute inset-0 -z-20 bg-[linear-gradient(180deg,#f8fbfe_0%,#f4f8fc_48%,#f7fafc_100%)]" />

            <div className="pointer-events-none absolute inset-0 z-0 overflow-hidden">
              <motion.div
                className="absolute inset-0"
                style={prefersReducedMotion ? undefined : { y: homeBackgroundY }}
              >
                <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,rgba(255,255,255,0.98),transparent_34%),radial-gradient(circle_at_16%_18%,rgba(56,189,248,0.12),transparent_28%),radial-gradient(circle_at_84%_20%,rgba(59,130,246,0.12),transparent_28%),radial-gradient(circle_at_68%_74%,rgba(20,184,166,0.1),transparent_34%)]" />
                <motion.div
                  className="absolute inset-0 opacity-90"
                  style={prefersReducedMotion ? undefined : { background: homeCursorGlow }}
                />
                <div className="absolute inset-0 bg-[radial-gradient(rgba(148,163,184,0.16)_1px,transparent_1px)] bg-[size:24px_24px] opacity-26 [mask-image:linear-gradient(180deg,black,transparent_92%)]" />
              </motion.div>

              <motion.div
                className="absolute inset-[-6%]"
                style={prefersReducedMotion ? undefined : { x: homeGlowX, y: homeGlowY }}
              >
                {NEURAL_GLOW_FIELDS.map((field, index) => (
                  <motion.span
                    key={`neural-glow-${index}`}
                    className="absolute rounded-full"
                    style={{
                      left: field.left,
                      top: field.top,
                      width: field.size,
                      height: field.size,
                      background: field.color,
                      filter: `blur(${field.blur}px)`,
                    }}
                    initial={{ opacity: 0.16, scale: 0.92 }}
                    animate={
                      prefersReducedMotion
                        ? { opacity: 0.2, scale: 1 }
                        : {
                            opacity: [0.12, 0.28, 0.16],
                            scale: [0.92, 1.08, 0.96],
                            x: [0, field.dx, field.dx * 0.4],
                            y: [0, field.dy, field.dy * 0.45],
                          }
                    }
                    transition={
                      prefersReducedMotion
                        ? { duration: 0 }
                        : {
                            duration: field.duration,
                            repeat: Infinity,
                            repeatType: "loop",
                            ease: "easeInOut",
                            delay: field.delay,
                          }
                    }
                  />
                ))}
              </motion.div>

              <motion.div
                className="absolute inset-[-8%]"
                style={prefersReducedMotion ? undefined : { x: homeNetworkX, y: homeNetworkY }}
              >
                {ANALYTICAL_PANELS.map((panel, index) => (
                  <motion.div
                    key={`analytical-panel-${index}`}
                    className="absolute hidden overflow-hidden rounded-[28px] border border-white/80 bg-white/68 p-4 shadow-[0_18px_55px_rgba(148,163,184,0.18)] backdrop-blur-md lg:block"
                    style={{
                      left: panel.left,
                      top: panel.top,
                      width: panel.width,
                      height: panel.height,
                      rotate: panel.rotate,
                    }}
                    initial={{ opacity: 0.22, y: 0 }}
                    animate={
                      prefersReducedMotion
                        ? { opacity: 0.28 }
                        : {
                            opacity: [0.2, 0.4, 0.24],
                            y: [0, -8, 0],
                          }
                    }
                    transition={
                      prefersReducedMotion
                        ? { duration: 0 }
                        : {
                            duration: 8.4,
                            repeat: Infinity,
                            repeatType: "loop",
                            ease: "easeInOut",
                            delay: panel.delay,
                          }
                    }
                  >
                    <div className="mb-3 flex items-center justify-between">
                      <span className="text-[10px] font-black uppercase tracking-[0.18em] text-slate-500">
                        {panel.label}
                      </span>
                      <span className="rounded-full bg-slate-900/5 px-2 py-1 text-[9px] font-bold uppercase tracking-[0.14em] text-slate-500">
                        Live
                      </span>
                    </div>
                    <div className="absolute inset-x-4 top-11 h-px bg-slate-200/80" />
                    <div className="mt-5 flex h-[52px] items-end gap-2">
                      {panel.bars.map((bar, barIndex) => (
                        <motion.span
                          key={`${panel.label}-bar-${barIndex}`}
                          className="block w-3 rounded-full"
                          style={{
                            height: bar,
                            background: barIndex === panel.bars.length - 1 ? panel.accent : panel.secondary,
                          }}
                          animate={
                            prefersReducedMotion
                              ? undefined
                              : {
                                  opacity: [0.75, 1, 0.78],
                                  y: [0, -2, 0],
                                }
                          }
                          transition={
                            prefersReducedMotion
                              ? undefined
                              : {
                                  duration: 2.8,
                                  repeat: Infinity,
                                  repeatType: "mirror",
                                  ease: "easeInOut",
                                  delay: panel.delay + barIndex * 0.08,
                                }
                          }
                        />
                      ))}
                    </div>
                    <svg
                      className="absolute inset-x-4 bottom-4 h-[42px] w-[calc(100%-2rem)]"
                      viewBox="0 0 210 92"
                      fill="none"
                      aria-hidden="true"
                    >
                      <path d={panel.trendPath} stroke={panel.accent} strokeWidth="4" strokeLinecap="round" />
                      <path d={panel.trendPath} stroke="rgba(255,255,255,0.55)" strokeWidth="1.6" strokeLinecap="round" />
                    </svg>
                  </motion.div>
                ))}

                {NEURAL_CONNECTIONS.map((connection, index) => (
                  <motion.span
                    key={`neural-link-${index}`}
                    className="absolute h-[1.5px] origin-left rounded-full"
                    style={{
                      left: connection.left,
                      top: connection.top,
                      width: connection.width,
                      background: `linear-gradient(90deg, transparent, ${connection.color}, transparent)`,
                      rotate: connection.rotate,
                      boxShadow: `0 0 18px ${connection.color}`,
                    }}
                    initial={{ opacity: 0.2, scaleX: 0.38 }}
                    animate={
                      prefersReducedMotion
                        ? { opacity: 0.26, scaleX: 1 }
                        : {
                            opacity: [0.16, 0.54, 0.22],
                            scaleX: [0.38, 1, 0.52],
                          }
                    }
                    transition={
                      prefersReducedMotion
                        ? { duration: 0 }
                        : {
                            duration: connection.duration,
                            repeat: Infinity,
                            repeatType: "loop",
                            ease: "easeInOut",
                            delay: connection.delay,
                          }
                    }
                  />
                ))}

                {NEURAL_NODES.map((node, index) => (
                  <motion.div
                    key={`neural-node-${index}`}
                    className="absolute rounded-full border border-white/80"
                    style={{
                      left: node.left,
                      top: node.top,
                      width: node.size,
                      height: node.size,
                      background: node.color,
                      boxShadow: `0 0 0 7px rgba(255,255,255,0.42), 0 0 20px ${node.color}`,
                    }}
                    initial={{ opacity: 0.4, scale: 0.82 }}
                    animate={
                      prefersReducedMotion
                        ? { opacity: 0.6, scale: 1 }
                        : {
                            opacity: [0.28, 0.95, 0.36],
                            scale: [0.82, 1.24, 0.92],
                          }
                    }
                    transition={
                      prefersReducedMotion
                        ? { duration: 0 }
                        : {
                            duration: node.duration,
                            repeat: Infinity,
                            repeatType: "loop",
                            ease: "easeInOut",
                            delay: node.delay,
                          }
                    }
                  />
                ))}
              </motion.div>
            </div>

            <div className={`relative z-20 mx-auto w-full max-w-7xl px-3 pb-10 pt-8 sm:px-6 sm:pb-14 ${isMobileViewport ? "" : "min-h-[150vh]"}`}>
              <div className={`${isMobileViewport ? "relative py-8" : "sticky top-0 flex min-h-[calc(100dvh-4rem)] items-center py-8 sm:min-h-[calc(100dvh-5rem)] sm:py-10"}`}>
                <div className="mx-auto w-full max-w-6xl">
                  <div className="grid gap-10 lg:gap-14">
                    <motion.div
                      className="text-center"
                      style={prefersReducedMotion ? undefined : { y: homeHeroY, opacity: homeHeroOpacity }}
                    >
                      <motion.div variants={staggerContainer} initial="initial" animate="animate">
                        <motion.div variants={fadeIn} className="mb-8 inline-flex items-center gap-2 rounded-full border border-emerald-200 bg-gradient-to-r from-white/96 via-emerald-50/92 to-amber-50/92 px-4 py-1.5 shadow-[0_20px_60px_rgba(16,185,129,0.14)] backdrop-blur-xl">
                          <span className="relative flex h-2.5 w-2.5">
                            <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-75"></span>
                            <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-emerald-500"></span>
                          </span>
                          <span className="text-[10px] font-black uppercase tracking-[0.25em] text-slate-800">Live Neural Feed</span>
                        </motion.div>

                        <motion.h2 variants={fadeIn} className="mb-8 text-center font-black tracking-tight">
                          <span className="block text-2xl leading-tight text-slate-950 md:text-3xl">Welcome to</span>
                          <span className="relative mx-auto mt-2 block w-fit">
                            <motion.span
                              aria-hidden="true"
                              className="pointer-events-none absolute inset-0 bg-[linear-gradient(96deg,#1e3a8a_0%,#2563eb_18%,#d946ef_50%,#ef4444_82%,#1e3a8a_100%)] bg-[length:240%_100%] bg-clip-text font-serif text-[2.8rem] font-black italic leading-[0.9] tracking-[0.03em] text-transparent opacity-40 blur-[16px] sm:text-[4.15rem] md:text-7xl md:[transform:scaleX(1.02)]"
                              style={{ backgroundPosition: "0% 50%" }}
                              animate={prefersReducedMotion ? undefined : { backgroundPosition: ["0% 50%", "100% 50%", "0% 50%"], opacity: [0.3, 0.5, 0.34] }}
                              transition={prefersReducedMotion ? undefined : { duration: 10, repeat: Infinity, ease: "linear" }}
                            >
                              Business Control Centre
                            </motion.span>
                            <motion.span
                              className="relative block bg-[linear-gradient(96deg,#1e3a8a_0%,#2563eb_18%,#d946ef_50%,#ef4444_82%,#1e3a8a_100%)] bg-[length:240%_100%] bg-clip-text font-serif text-[2.8rem] font-black italic leading-[0.9] tracking-[0.03em] text-transparent drop-shadow-[0_14px_34px_rgba(217,70,239,0.18)] sm:text-[4.15rem] md:text-7xl md:[transform:scaleX(1.02)]"
                              style={{ backgroundPosition: "0% 50%" }}
                              animate={prefersReducedMotion ? undefined : { backgroundPosition: ["0% 50%", "100% 50%", "0% 50%"] }}
                              transition={prefersReducedMotion ? undefined : { duration: 10, repeat: Infinity, ease: "linear" }}
                            >
                              Business Control Centre
                            </motion.span>
                          </span>
                        </motion.h2>
                        
                        <motion.p variants={fadeIn} className="mx-auto max-w-2xl text-center text-base font-medium leading-relaxed text-slate-600 sm:text-lg md:text-xl">
                          Navigate through partner ecosystems with precision. <br />Real-time performance metrics at your fingertips.
                        </motion.p>
                      </motion.div>
                    </motion.div>

                    <motion.div
                      variants={staggerContainer}
                      initial="initial"
                      animate="animate"
                      className="w-full space-y-4 [perspective:1800px] sm:space-y-8"
                      style={prefersReducedMotion ? undefined : { y: homeCardsY, rotateX: homeCardsRotateX }}
                    >
                      <motion.div
                        variants={cardHover}
                        whileHover="hover"
                        onClick={() => handleViewChange("master")}
                        className="group relative mx-auto w-full max-w-[1160px] cursor-pointer overflow-hidden rounded-[30px] border border-slate-200/90 bg-[linear-gradient(135deg,rgba(255,255,255,0.99),rgba(248,250,252,0.98),rgba(241,248,255,0.96))] p-6 text-center shadow-[0_20px_56px_rgba(15,23,42,0.1),0_0_0_1px_rgba(255,255,255,0.92)_inset] backdrop-blur-sm sm:rounded-[40px] sm:p-8"
                      >
                        <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,rgba(56,189,248,0.06),transparent_44%),radial-gradient(circle_at_bottom_right,rgba(20,184,166,0.05),transparent_36%)]" />
                        <div className="relative space-y-3 text-center">
                          <div className="flex items-center justify-center gap-3">
                            <h3 className="text-2xl font-black tracking-[-0.04em] text-slate-900 sm:text-3xl">Master Dashboard</h3>
                            <div className="flex h-9 w-9 scale-0 items-center justify-center rounded-full bg-gradient-to-br from-sky-600 to-teal-600 text-white transition-transform duration-300 group-hover:scale-100 sm:h-10 sm:w-10">
                              <ChevronRight size={20} />
                            </div>
                          </div>
                          <p className="text-[10px] font-black uppercase tracking-[0.22em] text-sky-800/90">
                            Unified view across Samsung, Croma, Vijay Sales, Reliance ResQ and Godrej
                          </p>
                        </div>
                      </motion.div>

                      <div className="grid grid-cols-1 justify-items-center gap-4 sm:gap-8 md:grid-cols-3">
                        {brandConfigs.map((cfg) => (
                          <motion.div
                            key={cfg.value}
                            variants={cardHover}
                            whileHover="hover"
                            onClick={() => {
                              applyBrandChange(cfg.value)
                            }}
                            className="group relative w-full max-w-[360px] cursor-pointer overflow-hidden rounded-[30px] border border-white/85 bg-[linear-gradient(155deg,rgba(255,255,255,0.97),rgba(247,250,252,0.95),rgba(239,246,255,0.92))] p-6 text-center shadow-[0_24px_70px_rgba(148,163,184,0.2)] transition-all duration-500 backdrop-blur-xl sm:max-w-[380px] sm:rounded-[48px] sm:p-10"
                          >
                            <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,rgba(56,189,248,0.07),transparent_42%),radial-gradient(circle_at_bottom_right,rgba(20,184,166,0.07),transparent_36%)] opacity-90" />
                            <div className="relative mb-6 flex h-20 items-center justify-center overflow-hidden rounded-[24px] border border-slate-100 bg-white/95 shadow-[0_16px_40px_rgba(148,163,184,0.2)] sm:mb-10 sm:h-24">
                              <motion.div whileHover={{ scale: 1.08, rotate: 1.5 }}>
                                <Image
                                  src={cfg.logo}
                                  alt={cfg.label}
                                  width={180}
                                  height={80}
                                  className={`max-h-full max-w-full object-contain filter drop-shadow-md ${cfg.value === "reliance" ? "scale-125" : ""}`}
                                />
                              </motion.div>
                            </div>
                            <div className="relative space-y-4 text-center">
                              <div className="flex items-center justify-center gap-3">
                                <h3 className="text-xl font-bold tracking-tight text-slate-800 sm:text-2xl">{cfg.label}</h3>
                                <div className="flex h-9 w-9 scale-0 items-center justify-center rounded-full bg-gradient-to-br from-sky-600 to-cyan-600 text-white transition-transform duration-300 group-hover:scale-100 sm:h-10 sm:w-10">
                                  <ChevronRight size={20} />
                                </div>
                              </div>
                              <p className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-500">{cfg.caption}</p>
                            </div>
                          </motion.div>
                        ))}
                      </div>
                    </motion.div>
                  </div>
                </div>
              </div>
            </div>
          </motion.div>
        ) : view === "master" ? (
          <motion.div
            key="master"
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
              collapsed={isSidebarCollapsed}
              onToggleCollapse={() => setIsSidebarCollapsed((prev) => !prev)}
            />

            <main className="custom-scrollbar min-w-0 flex-1 overflow-y-auto bg-[#edf1f6] px-3 py-3 sm:px-6 sm:py-4 lg:px-8">
              <div className="mx-auto w-full max-w-[1380px] pb-10">
                <MasterDashboardView jobId={jobId || undefined} />
              </div>
            </main>
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
              collapsed={isSidebarCollapsed}
              onToggleCollapse={() => setIsSidebarCollapsed((prev) => !prev)}
            />

            <main className="custom-scrollbar min-w-0 flex-1 overflow-y-auto bg-[#edf1f6] px-3 py-3 sm:px-6 sm:py-4 lg:px-8">
              <div className="mx-auto w-full max-w-[1380px] pb-10">
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
                    className="space-y-4 xl:col-span-4"
                  >
                    <div className="space-y-4 xl:sticky xl:top-4">
                      <motion.div variants={fadeIn} className="rounded-2xl border border-slate-200 bg-white p-3.5 shadow-sm sm:p-5">
                        <div className="mb-3 flex items-center justify-between gap-3">
                          <h4 className="text-sm font-bold text-slate-800">Control Filters</h4>
                          <div className="flex items-center gap-2">
                            <span className="rounded-full bg-slate-100 px-2 py-1 text-[10px] font-bold uppercase tracking-[0.12em] text-slate-500">
                              {mode}
                            </span>
                            <button
                              type="button"
                              onClick={() => setIsMobileFiltersCollapsed((prev) => !prev)}
                              className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-white px-2 py-1 text-[10px] font-bold uppercase tracking-[0.08em] text-slate-500 md:hidden"
                              aria-expanded={!isMobileFiltersCollapsed}
                              aria-label={isMobileFiltersCollapsed ? "Expand filters" : "Collapse filters"}
                            >
                              <ChevronRight
                                size={12}
                                className={`transition-transform ${isMobileFiltersCollapsed ? "" : "rotate-90"}`}
                              />
                              {isMobileFiltersCollapsed ? "Expand" : "Collapse"}
                            </button>
                          </div>
                        </div>
                        <AnimatePresence initial={false}>
                          {(!isMobileViewport || !isMobileFiltersCollapsed) && (
                            <motion.div
                              key="filters-body"
                              initial={{ opacity: 0, y: -6 }}
                              animate={{ opacity: 1, y: 0 }}
                              exit={{ opacity: 0, y: -6 }}
                              transition={{ duration: 0.16, ease: "easeOut" }}
                              className="overflow-visible"
                            >
                              <div className="space-y-3">
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
                              </div>
                            </motion.div>
                          )}
                        </AnimatePresence>
                      </motion.div>

                      <motion.div variants={fadeIn} className="rounded-2xl border border-slate-200 bg-white p-3.5 shadow-sm sm:p-4">
                        <div className="mb-3 text-[10px] font-black uppercase tracking-[0.12em] text-slate-400">
                          KPI Snapshot
                        </div>
                        <KpiCardsRow
                          source={brand}
                          datasetType={mode}
                          jobId={effectiveJobId || undefined}
                          fromDate={fromDate || undefined}
                          toDate={toDate || undefined}
                          refreshTick={filterRefreshTick}
                          layout="vertical"
                        />
                      </motion.div>
                    </div>
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
            <div className="relative z-[120] border-b bg-white/80 p-3 backdrop-blur-md sm:p-6 lg:p-8">
              <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
                <div className="flex items-center gap-3 sm:gap-4">
                  <div className={`h-10 w-10 rounded-xl flex items-center justify-center ${theme.bgLight} ${theme.accent}`}>
                     <Activity size={20} />
                  </div>
                  <div>
                    <h4 className="text-lg font-black tracking-tight">{brandLabel(brand)}</h4>
                    <p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{mode === "sales" ? "Sales Velocity" : "Claims Integrity"}</p>
                  </div>
                </div>
                <div className="flex w-full flex-col items-stretch gap-2 sm:flex-row sm:items-center sm:justify-end xl:w-auto">
                  <div className="w-full max-w-[360px]">
                    <DateRangePicker
                      draftFromDate={draftFromDate}
                      draftToDate={draftToDate}
                      minDate={defaultFromDate || undefined}
                      maxDate={defaultToDate && defaultToDate > todayIso() ? defaultToDate : todayIso()}
                      compact
                      align="right"
                      onDraftChange={(from, to) => {
                        setDraftFromDate(clampToCurrentMonth(from))
                        setDraftToDate(clampToCurrentMonth(to))
                      }}
                      onApply={handleGraphDateRangeApply}
                      onReset={resetDateRange}
                    />
                  </div>
                  <button className="w-full whitespace-nowrap rounded-2xl bg-slate-900 px-4 py-3 text-[11px] font-bold text-white transition-all hover:bg-black sm:w-auto sm:px-6 sm:text-xs lg:px-8" onClick={() => handleFullscreenToggle(false)}>Close Focus View</button>
                </div>
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
          </motion.div>
        )}
      </AnimatePresence>

      <RightSideChatbot variant="floating" />

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
