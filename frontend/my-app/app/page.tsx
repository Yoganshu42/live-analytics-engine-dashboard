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
import HomeParticleField from "@/components/HomeParticleField"
import { fetchDateBounds, fetchAuthMe } from "./lib/api"
import { normalizeSamsungSource } from "@/lib/samsungPartners"

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
  initial: { scale: 1, y: 0, boxShadow: "0px 26px 72px rgba(94,118,160,0.12)" },
  hover: {
    scale: 1.01,
    y: -6,
    boxShadow: "0px 34px 88px rgba(109,137,179,0.18)",
    transition: { duration: 0.32, ease: [0.22, 1, 0.36, 1] }
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

type HomeWave = {
  top: string
  path: string
  stroke: string
  opacity: number
  strokeWidth: number
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
  { left: "-7%", top: "1%", size: 460, color: "rgba(255,255,255,0.94)", dx: 14, dy: -8, duration: 26, delay: 0.2, blur: 52 },
  { left: "10%", top: "14%", size: 360, color: "rgba(132,211,255,0.18)", dx: 12, dy: -10, duration: 24, delay: 0.7, blur: 42 },
  { left: "33%", top: "10%", size: 460, color: "rgba(255,255,255,0.88)", dx: 12, dy: -6, duration: 23, delay: 0.45, blur: 48 },
  { left: "56%", top: "8%", size: 360, color: "rgba(189,181,255,0.18)", dx: -10, dy: -8, duration: 24, delay: 1, blur: 38 },
  { left: "74%", top: "11%", size: 430, color: "rgba(240,245,255,0.92)", dx: -14, dy: -10, duration: 26, delay: 0.35, blur: 44 },
  { left: "73%", top: "45%", size: 460, color: "rgba(126,187,255,0.15)", dx: -10, dy: -10, duration: 22, delay: 1.25, blur: 44 },
  { left: "4%", top: "58%", size: 410, color: "rgba(255,255,255,0.86)", dx: 8, dy: -6, duration: 28, delay: 0.65, blur: 46 },
  { left: "31%", top: "72%", size: 360, color: "rgba(255,255,255,0.9)", dx: 10, dy: -6, duration: 24, delay: 1.05, blur: 36 },
]

const HOME_WAVES: HomeWave[] = [
  {
    top: "10%",
    path: "M-140 184 C 44 76, 214 268, 472 170 S 916 92, 1248 170 S 1528 284, 1760 160",
    stroke: "rgba(255,255,255,0.7)",
    opacity: 0.64,
    strokeWidth: 1.35,
    duration: 30,
    delay: 0.1,
  },
  {
    top: "24%",
    path: "M-170 228 C 54 114, 258 294, 536 224 S 960 124, 1226 216 S 1542 310, 1772 220",
    stroke: "rgba(178,224,255,0.54)",
    opacity: 0.58,
    strokeWidth: 1.18,
    duration: 34,
    delay: 0.8,
  },
  {
    top: "42%",
    path: "M-128 252 C 124 164, 330 336, 580 256 S 948 160, 1220 236 S 1532 326, 1750 230",
    stroke: "rgba(255,255,255,0.62)",
    opacity: 0.52,
    strokeWidth: 1.08,
    duration: 32,
    delay: 1.2,
  },
  {
    top: "58%",
    path: "M-126 268 C 86 188, 278 342, 520 282 S 938 198, 1202 260 S 1504 366, 1726 276",
    stroke: "rgba(195,227,255,0.48)",
    opacity: 0.46,
    strokeWidth: 1,
    duration: 38,
    delay: 0.5,
  },
]

const ANALYTICAL_PANELS: AnalyticalPanel[] = [
  {
    left: "0.5%",
    top: "15%",
    width: 238,
    height: 152,
    rotate: -8,
    delay: 0.2,
    label: "Flow Signal",
    bars: [22, 40, 30, 58, 82, 50],
    accent: "rgba(101,198,255,0.94)",
    secondary: "rgba(214,235,255,0.9)",
    trendPath: "M8 82 C 28 74, 42 78, 60 62 S 98 42, 126 46 S 166 60, 198 30",
  },
  {
    left: "79.7%",
    top: "24%",
    width: 230,
    height: 148,
    rotate: 8,
    delay: 0.7,
    label: "Node Load",
    bars: [18, 24, 22, 44, 36, 76],
    accent: "rgba(126,194,255,0.95)",
    secondary: "rgba(219,240,255,0.88)",
    trendPath: "M10 78 C 30 64, 50 74, 74 56 S 116 30, 146 42 S 180 56, 200 48",
  },
]

const HOME_DESKTOP_CONTENT_WIDTH = 1280
const HOME_DESKTOP_CONTENT_FALLBACK_HEIGHT = 860

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
    const samsungSource = normalizeSamsungSource(key)
    if (samsungSource && samsungSource !== key) return samsungSource
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
  const [authRole, setAuthRole] = useState<"admin" | "employee" | null>(() => {
    if (typeof window === "undefined") return null
    const storedRole = localStorage.getItem("auth_role")
    return storedRole === "admin" || storedRole === "employee" ? storedRole : null
  })
  const [authName, setAuthName] = useState<string>(() => {
    if (typeof window === "undefined") return ""
    return localStorage.getItem("auth_name") || ""
  })
  const [authReady, setAuthReady] = useState<boolean>(() => {
    if (typeof window === "undefined") return false
    return Boolean(normalizeToken(localStorage.getItem("auth_token")))
  })
  const prefersReducedMotion = useReducedMotion()
  const homeViewportRef = useRef<HTMLDivElement | null>(null)
  const homeFitContentRef = useRef<HTMLDivElement | null>(null)
  const { scrollYProgress: homeScrollProgress } = useScroll()
  const homePointerX = useMotionValue(0.5)
  const homePointerY = useMotionValue(0.5)
  const homePointerXSpring = useSpring(homePointerX, { stiffness: 180, damping: 24, mass: 0.45 })
  const homePointerYSpring = useSpring(homePointerY, { stiffness: 180, damping: 24, mass: 0.45 })
  const homeHeroY = useTransform(homeScrollProgress, [0, 1], [0, -34])
  const homeHeroOpacity = useTransform(homeScrollProgress, [0, 0.7, 1], [1, 0.985, 0.92])
  const homeCardsY = useTransform(homeScrollProgress, [0, 1], [0, -16])
  const homeCardsRotateX = useTransform(homeScrollProgress, [0, 1], [0, 2.4])
  const homeNetworkX = useTransform(homePointerXSpring, [0, 1], [-10, 10])
  const homeNetworkY = useTransform(homePointerYSpring, [0, 1], [-8, 8])
  const homeGlowX = useTransform(homePointerXSpring, [0, 1], [-14, 14])
  const homeGlowY = useTransform(homePointerYSpring, [0, 1], [-12, 12])
  const homeCursorPrimaryX = useTransform(homePointerXSpring, [0, 1], ["18%", "82%"])
  const homeCursorPrimaryY = useTransform(homePointerYSpring, [0, 1], ["18%", "76%"])
  const homeCursorSecondaryX = useTransform(homePointerXSpring, [0, 1], ["76%", "22%"])
  const homeCursorSecondaryY = useTransform(homePointerYSpring, [0, 1], ["64%", "22%"])
  const homeCursorGlow = useMotionTemplate`radial-gradient(circle at ${homeCursorPrimaryX} ${homeCursorPrimaryY}, rgba(123, 208, 255, 0.26), transparent 18%), radial-gradient(circle at ${homeCursorSecondaryX} ${homeCursorSecondaryY}, rgba(178, 167, 255, 0.22), transparent 21%), radial-gradient(circle at 50% 52%, rgba(255,255,255,0.52), transparent 40%)`
  const [isMobileViewport, setIsMobileViewport] = useState<boolean>(() => {
    if (typeof window === "undefined") return false
    return window.innerWidth < 768
  })
  const [homeViewportSize, setHomeViewportSize] = useState(() => ({
    width: typeof window === "undefined" ? HOME_DESKTOP_CONTENT_WIDTH : window.innerWidth,
    height: typeof window === "undefined" ? HOME_DESKTOP_CONTENT_FALLBACK_HEIGHT : window.innerHeight,
  }))
  const [homeContentHeight, setHomeContentHeight] = useState(HOME_DESKTOP_CONTENT_FALLBACK_HEIGHT)
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
      setHomeViewportSize({
        width: window.innerWidth,
        height: window.innerHeight,
      })
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

  useEffect(() => {
    if (typeof window === "undefined" || isMobileViewport || view !== "home") return

    const node = homeFitContentRef.current
    if (!node) return

    const updateMeasuredHeight = () => {
      const nextHeight = Math.ceil(node.offsetHeight)
      if (!nextHeight) return
      setHomeContentHeight((prev) => (prev === nextHeight ? prev : nextHeight))
    }

    const frame = window.requestAnimationFrame(updateMeasuredHeight)
    const observer =
      typeof ResizeObserver !== "undefined"
        ? new ResizeObserver(() => {
            updateMeasuredHeight()
          })
        : null

    observer?.observe(node)

    return () => {
      window.cancelAnimationFrame(frame)
      observer?.disconnect()
    }
  }, [isMobileViewport, view])

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
        localStorage.setItem("auth_role", profile.role)
        localStorage.setItem("auth_name", profile.email)
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
      samsung_reliance_digital: "Samsung Reliance Digital",
      reliance: "Reliance ResQ",
      godrej: "Godrej",
      hitachi: "Hitachi",
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
  alt: "Samsung Croma logo",
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
    samsung_reliance_digital: [
      samsungProtectMaxLogo,
      {
        src: "/reliance_digital_logo.png",
        alt: "Reliance Digital logo",
        width: 108,
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
    hitachi: [
      {
        src: "/hitachi_logo.png",
        alt: "Hitachi logo",
        width: 104,
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
      src: "/reliance_digital_logo.png",
      alt: "Reliance Digital logo",
      width: 108,
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
    {
      src: "/hitachi_logo.png",
      alt: "Hitachi logo",
      width: 104,
      height: 28,
      className: "h-5 w-auto object-contain",
    },
  ]

  const activePartnerLogos = partnerLogoConfig[brand] || []
  const headerPartnerLogos = view === "master" ? masterCardLogos : view === "dashboard" ? activePartnerLogos : []
  const headerPartnerLabel = view === "master" ? "Master Dashboard" : brandLabel(brand)
  const isHomeView = view === "home"

  const brandConfigs = [
    {
      label: "Samsung Care Services",
      value: "samsung",
      logo: "/WhatsApp Image 2026-02-04 at 11.14.29.jpeg",
      surfaceClass: "bg-[linear-gradient(180deg,rgba(255,255,255,0.62),rgba(240,245,252,0.54),rgba(231,239,249,0.48))]",
      logoSurfaceClass: "bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(255,255,255,0.88))]",
      logoClass: "max-h-[78px] max-w-[88%] object-contain",
    },
    {
      label: "Reliance resQ",
      value: "reliance",
      logo: "/resq.png",
      surfaceClass: "bg-[linear-gradient(180deg,rgba(255,255,255,0.64),rgba(236,246,253,0.55),rgba(230,238,248,0.48))]",
      logoSurfaceClass: "bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(255,255,255,0.88))]",
      logoClass: "max-h-[94px] max-w-[90%] object-contain scale-[1.08]",
    },
    {
      label: "Godrej",
      value: "godrej",
      logo: "/Group 1244833444.png",
      surfaceClass: "bg-[linear-gradient(180deg,rgba(255,255,255,0.63),rgba(243,241,250,0.56),rgba(231,238,247,0.48))]",
      logoSurfaceClass: "bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(255,255,255,0.88))]",
      logoClass: "max-h-[92px] max-w-[88%] object-contain",
    },
    {
      label: "Hitachi",
      value: "hitachi",
      logo: "/hitachi_logo.png",
      surfaceClass: "bg-[linear-gradient(180deg,rgba(255,255,255,0.64),rgba(242,247,253,0.58),rgba(232,239,247,0.48))]",
      logoSurfaceClass: "bg-[linear-gradient(180deg,rgba(255,255,255,0.97),rgba(255,255,255,0.9))]",
      logoClass: "max-h-[82px] max-w-[84%] object-contain",
    },
  ]

  const homeLayoutScale = useMemo(() => {
    if (isMobileViewport || view !== "home") return 1

    const horizontalPadding = homeViewportSize.width >= 1024 ? 64 : 36
    const headerHeight = homeViewportSize.width >= 640 ? 86 : 68
    const verticalPadding = homeViewportSize.width >= 1024 ? 28 : 20
    const availableWidth = Math.max(homeViewportSize.width - horizontalPadding, 320)
    const availableHeight = Math.max(homeViewportSize.height - headerHeight - verticalPadding, 320)
    const widthScale = availableWidth / HOME_DESKTOP_CONTENT_WIDTH
    const heightScale = availableHeight / Math.max(homeContentHeight, 1)
    const fittedScale = Math.min(1, widthScale, heightScale)
    const breathingRoomScale =
      homeViewportSize.height < 900 || homeViewportSize.width < 1440 ? 0.92 : 0.96

    return Math.max(0.72, fittedScale * breathingRoomScale)
  }, [homeContentHeight, homeViewportSize.height, homeViewportSize.width, isMobileViewport, view])

  const homeScaledWrapperStyle = isMobileViewport || view !== "home"
    ? undefined
    : {
        width: `${Math.round(HOME_DESKTOP_CONTENT_WIDTH * homeLayoutScale)}px`,
        minHeight: `${Math.round(homeContentHeight * homeLayoutScale)}px`,
        position: "relative" as const,
      }

  const homeScaledContentStyle = isMobileViewport || view !== "home"
    ? undefined
    : {
        width: `${HOME_DESKTOP_CONTENT_WIDTH}px`,
        position: "absolute" as const,
        left: "50%",
        top: 0,
        transform: `translateX(-50%) scale(${homeLayoutScale})`,
        transformOrigin: "top center",
      }

  const homeVisualEffectsEnabled = view === "home" && !prefersReducedMotion && !isMobileViewport
  const showHomeAmbientEffects = homeVisualEffectsEnabled && homeViewportSize.width >= 1200
  const showHomePanels = homeVisualEffectsEnabled && homeViewportSize.width >= 1440
  const homeParticleQuality: "low" | "high" =
    homeVisualEffectsEnabled && homeViewportSize.width >= 1680 ? "high" : "low"

  const homeDisplayName = authName || "analytics@zopper.com"
  const homeProfileInitials = homeDisplayName
    .split(/[@._\-\s]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase() || "")
    .join("") || "ZA"

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
          ? "bg-white text-slate-900 selection:bg-sky-100"
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
            ? "relative border-slate-200/75 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(246,248,252,0.9))] shadow-[0_12px_28px_rgba(125,145,177,0.08)] before:absolute before:inset-x-0 before:top-0 before:h-[5px] before:bg-[#526074] before:content-['']"
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
            <span className={`${isHomeView ? "text-[#3279bf]" : theme.accent} font-black uppercase text-[11px] tracking-[0.4em]`}>
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
            {isHomeView ? (
              <div className="hidden items-center gap-3 sm:flex">
                <p className="text-sm font-medium text-[#2a3957]">{homeDisplayName}</p>
                <div className="flex h-10 w-10 items-center justify-center rounded-full border border-slate-200/80 bg-[linear-gradient(180deg,#ffffff,#eef3fb)] text-sm font-semibold text-[#2b3d5d] shadow-[0_8px_22px_rgba(120,140,172,0.12)]">
                  {homeProfileInitials}
                </div>
              </div>
            ) : authRole ? (
              <div className="mr-2 hidden text-right sm:block">
                <p className={`mb-1 text-[10px] font-black uppercase tracking-tighter leading-none ${isHomeView ? "text-slate-400" : "text-slate-400"}`}>{authRole}</p>
                <p className={`text-xs font-bold ${isHomeView ? "text-slate-700" : "text-slate-700"}`}>{authName}</p>
              </div>
            ) : null}
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
                  ? "border-slate-200/90 bg-white/92 text-slate-500 shadow-[0_8px_22px_rgba(120,140,172,0.1)] hover:text-slate-900"
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
              if (!homeVisualEffectsEnabled) return
              const rect = event.currentTarget.getBoundingClientRect()
              const nextX = Math.min(1, Math.max(0, (event.clientX - rect.left) / Math.max(rect.width, 1)))
              const nextY = Math.min(1, Math.max(0, (event.clientY - rect.top) / Math.max(rect.height, 1)))
              homePointerX.set(nextX)
              homePointerY.set(nextY)
            }}
            onPointerLeave={() => {
              if (!homeVisualEffectsEnabled) return
              homePointerX.set(0.5)
              homePointerY.set(0.5)
            }}
            className={`relative flex-1 min-h-0 overflow-x-hidden ${isMobileViewport ? "overflow-y-auto" : "overflow-hidden"}`}
          >
            <div className="pointer-events-none fixed inset-0 z-0 overflow-hidden">
              <div className="absolute inset-0 bg-[linear-gradient(180deg,#f4f7fb_0%,#edf3fb_20%,#e7f1fb_44%,#e8ecfa_72%,#f3f4fa_100%)]" />
              <div className="absolute inset-0 bg-[radial-gradient(circle_at_16%_16%,rgba(255,255,255,0.96),transparent_18%),radial-gradient(circle_at_78%_18%,rgba(229,235,255,0.72),transparent_24%),radial-gradient(circle_at_50%_44%,rgba(255,255,255,0.92),transparent_26%),radial-gradient(circle_at_18%_74%,rgba(227,242,255,0.7),transparent_24%),radial-gradient(circle_at_82%_70%,rgba(226,233,255,0.76),transparent_26%)]" />
              <motion.div
                className="absolute inset-0 opacity-95"
                style={homeVisualEffectsEnabled ? { background: homeCursorGlow } : undefined}
              />
              <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(255,255,255,0.3)_0%,rgba(255,255,255,0)_26%,rgba(255,255,255,0.18)_58%,rgba(255,255,255,0.46)_100%)]" />

              {showHomeAmbientEffects && (
                <>
                  <motion.div
                    className="absolute inset-[-8%]"
                    style={{ x: homeGlowX, y: homeGlowY }}
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
                        initial={{ opacity: 0.18, scale: 0.96 }}
                        animate={{
                          opacity: [0.16, 0.34, 0.2],
                          scale: [0.96, 1.05, 0.98],
                          x: [0, field.dx, field.dx * 0.45],
                          y: [0, field.dy, field.dy * 0.4],
                        }}
                        transition={{
                          duration: field.duration,
                          repeat: Infinity,
                          repeatType: "mirror",
                          ease: "easeInOut",
                          delay: field.delay,
                        }}
                      />
                    ))}
                  </motion.div>

                  <div className="absolute inset-0 overflow-hidden opacity-[0.72]">
                    {HOME_WAVES.map((wave, index) => (
                      <motion.svg
                        key={`home-wave-${index}`}
                        viewBox="0 0 1720 420"
                        fill="none"
                        aria-hidden="true"
                        className="absolute left-1/2 h-[330px] w-[170%] -translate-x-1/2"
                        style={{ top: wave.top }}
                        animate={{
                          x: [-14, 18, -10],
                          opacity: [wave.opacity * 0.94, wave.opacity, wave.opacity * 0.9],
                        }}
                        transition={{
                          duration: wave.duration,
                          repeat: Infinity,
                          repeatType: "mirror",
                          ease: "easeInOut",
                          delay: wave.delay,
                        }}
                      >
                        <path
                          d={wave.path}
                          stroke={wave.stroke}
                          strokeWidth={wave.strokeWidth}
                          strokeLinecap="round"
                        />
                      </motion.svg>
                    ))}
                  </div>
                </>
              )}

              <div className="absolute inset-0 opacity-75">
                <HomeParticleField reducedMotion={Boolean(prefersReducedMotion)} quality={homeParticleQuality} />
              </div>

              <div className="absolute inset-0 bg-[radial-gradient(rgba(255,255,255,0.78)_0.95px,transparent_0.95px)] bg-[size:30px_30px] opacity-[0.18] [mask-image:radial-gradient(circle_at_center,black_28%,transparent_84%)]" />
              <div className="absolute inset-x-0 bottom-0 h-[30%] bg-[linear-gradient(180deg,rgba(243,245,250,0),rgba(243,245,250,0.62)_76%,rgba(243,245,250,0.96)_100%)]" />

              {showHomePanels && (
                <motion.div
                  className="absolute inset-[-4%]"
                  style={{ x: homeNetworkX, y: homeNetworkY }}
                >
                  {ANALYTICAL_PANELS.map((panel, index) => (
                    <motion.div
                      key={`analytical-panel-${index}`}
                      className="absolute hidden overflow-hidden rounded-[36px] border border-white/82 bg-white/34 p-4 shadow-[0_28px_80px_rgba(141,165,205,0.18)] backdrop-blur-[18px] xl:block"
                      style={{
                        left: panel.left,
                        top: panel.top,
                        width: panel.width,
                        height: panel.height,
                        rotate: panel.rotate,
                      }}
                      initial={{ opacity: 0.22, y: 0 }}
                      animate={{ opacity: [0.22, 0.42, 0.28], y: [0, -8, 0] }}
                      transition={{
                        duration: 8.8,
                        repeat: Infinity,
                        repeatType: "mirror",
                        ease: "easeInOut",
                        delay: panel.delay,
                      }}
                    >
                      <div className="mb-3 flex items-center justify-between">
                        <span className="text-[10px] font-black uppercase tracking-[0.28em] text-[#9aa7bc]">
                          {panel.label}
                        </span>
                        <span className="rounded-full border border-white/75 bg-white/52 px-2 py-1 text-[9px] font-bold uppercase tracking-[0.18em] text-[#a4afc1]">
                          Live
                        </span>
                      </div>
                      <div className="absolute inset-x-4 top-11 h-px bg-white/55" />
                      <div className="mt-6 flex h-[56px] items-end gap-2.5">
                        {panel.bars.map((bar, barIndex) => (
                          <motion.span
                            key={`${panel.label}-bar-${barIndex}`}
                            className="block w-3 rounded-full"
                            style={{
                              height: bar,
                              background: barIndex === panel.bars.length - 1 ? panel.accent : panel.secondary,
                            }}
                            animate={{ opacity: [0.78, 1, 0.82], y: [0, -2, 0] }}
                            transition={{
                              duration: 2.8,
                              repeat: Infinity,
                              repeatType: "mirror",
                              ease: "easeInOut",
                              delay: panel.delay + barIndex * 0.08,
                            }}
                          />
                        ))}
                      </div>
                      <svg
                        className="absolute inset-x-4 bottom-4 h-[40px] w-[calc(100%-2rem)]"
                        viewBox="0 0 210 92"
                        fill="none"
                        aria-hidden="true"
                      >
                        <path d={panel.trendPath} stroke={panel.accent} strokeWidth="4" strokeLinecap="round" />
                        <path d={panel.trendPath} stroke="rgba(255,255,255,0.72)" strokeWidth="1.5" strokeLinecap="round" />
                      </svg>
                    </motion.div>
                  ))}
                </motion.div>
              )}
            </div>

            <div className="relative z-20 mx-auto flex w-full max-w-[1460px] justify-center px-3 pb-6 pt-3 sm:px-5 sm:pb-8 sm:pt-4 lg:px-6">
              <div className={isMobileViewport ? "mx-auto w-full max-w-[1240px]" : "relative mx-auto"} style={homeScaledWrapperStyle}>
                <div
                  ref={homeFitContentRef}
                  className={`flex flex-col gap-4 ${isMobileViewport ? "mx-auto w-full max-w-[1240px]" : ""}`}
                  style={homeScaledContentStyle}
                >
                  <motion.div
                    className="relative px-2 pt-2 text-center sm:px-4"
                    style={prefersReducedMotion ? undefined : { y: homeHeroY, opacity: homeHeroOpacity }}
                  >
                    <motion.div
                      className="pointer-events-none absolute left-1/2 top-1 h-[340px] w-[78%] -translate-x-1/2 rounded-full bg-[radial-gradient(circle,rgba(255,255,255,0.98),rgba(231,242,255,0.78),rgba(228,232,255,0.46),transparent_72%)] blur-[72px]"
                      animate={prefersReducedMotion ? undefined : { opacity: [0.74, 1, 0.8], scale: [0.985, 1.025, 1] }}
                      transition={prefersReducedMotion ? undefined : { duration: 10, repeat: Infinity, ease: "easeInOut" }}
                    />

                    <motion.div variants={staggerContainer} initial="initial" animate="animate" className="relative">
                      <motion.h2 variants={fadeIn} className="space-y-4 text-center">
                        <span className="block text-[clamp(1.95rem,3.6vw,3.15rem)] font-black tracking-[-0.06em] text-[#4a5975] drop-shadow-[0_1px_0_rgba(255,255,255,0.7)]">
                          Welcome to
                        </span>
                        <motion.span
                          className="block bg-[linear-gradient(92deg,#1894f3_10%,#2a7eea_34%,#5a7de5_64%,#9b63cf_92%)] bg-[length:180%_100%] bg-clip-text font-serif text-[clamp(3.1rem,5.2vw,5.45rem)] font-black italic leading-[0.92] tracking-[-0.05em] text-transparent md:whitespace-nowrap"
                          style={{
                            backgroundPosition: "0% 50%",
                            WebkitTextStroke: "0.35px rgba(255,255,255,0.28)",
                            filter: "drop-shadow(0 10px 22px rgba(97,136,235,0.12))",
                          }}
                          animate={prefersReducedMotion ? undefined : { backgroundPosition: ["0% 50%", "100% 50%", "0% 50%"] }}
                          transition={prefersReducedMotion ? undefined : { duration: 14, repeat: Infinity, ease: "linear" }}
                        >
                          Business Control Centre
                        </motion.span>
                      </motion.h2>

                      <motion.p variants={fadeIn} className="mx-auto mt-4 max-w-[700px] text-center text-[0.95rem] font-medium leading-[1.6] text-[#53627d] sm:text-[1.02rem]">
                        Navigate through partner ecosystems with precision.
                        <br />
                        Real-time performance metrics at your fingertips.
                      </motion.p>
                    </motion.div>
                  </motion.div>

                  <motion.div
                    variants={staggerContainer}
                    initial="initial"
                    animate="animate"
                    className="w-full space-y-4 [perspective:1800px]"
                    style={prefersReducedMotion ? undefined : { y: homeCardsY, rotateX: homeCardsRotateX }}
                  >
                    <motion.div
                      variants={cardHover}
                      whileHover="hover"
                      onClick={() => handleViewChange("master")}
                      className="group relative mx-auto w-full max-w-[940px] cursor-pointer overflow-hidden rounded-[38px] border border-white/78 bg-[linear-gradient(180deg,rgba(255,255,255,0.72),rgba(240,246,255,0.66),rgba(231,238,249,0.58))] px-7 py-6 text-center shadow-[0_28px_84px_rgba(118,139,176,0.14)] backdrop-blur-[18px] sm:px-9"
                    >
                      <div className="absolute inset-0 bg-[radial-gradient(circle_at_50%_4%,rgba(255,255,255,0.94),transparent_48%),radial-gradient(circle_at_50%_100%,rgba(144,195,255,0.16),transparent_56%)]" />
                      <div className="absolute inset-[1px] rounded-[38px] border border-white/52" />
                      <div className="relative space-y-3 text-center">
                        <h3 className="text-[clamp(1.85rem,3.3vw,3.05rem)] font-black tracking-[-0.06em] text-[#1c2944]">
                          Master Dashboard
                        </h3>
                        <p className="mx-auto max-w-5xl text-[10px] font-black uppercase tracking-[0.26em] text-[#30486a] sm:text-[11px]">
                          Unified view across Samsung, Croma, Vijay Sales, Reliance ResQ, Godrej and Hitachi
                        </p>
                      </div>
                    </motion.div>

                    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                      {brandConfigs.map((cfg) => (
                        <motion.div
                          key={cfg.value}
                          variants={cardHover}
                          whileHover="hover"
                          onClick={() => {
                            applyBrandChange(cfg.value)
                          }}
                          className={`group relative min-h-[248px] cursor-pointer overflow-hidden rounded-[32px] border border-white/80 px-5 pb-5 pt-5 text-center shadow-[0_24px_74px_rgba(118,139,176,0.12)] backdrop-blur-[18px] ${cfg.surfaceClass}`}
                        >
                          <div className="absolute inset-[1px] rounded-[32px] border border-white/46" />
                          <div className="absolute inset-0 bg-[radial-gradient(circle_at_50%_0%,rgba(255,255,255,0.9),transparent_42%),radial-gradient(circle_at_50%_100%,rgba(163,200,243,0.12),transparent_64%)]" />
                          <div className={`relative mb-5 flex h-[106px] items-center justify-center overflow-hidden rounded-[24px] border border-white/86 shadow-[0_18px_48px_rgba(147,168,203,0.14)] ${cfg.logoSurfaceClass}`}>
                            <motion.div whileHover={{ scale: 1.02, y: -1 }} className="flex h-full w-full items-center justify-center">
                              <Image
                                src={cfg.logo}
                                alt={cfg.label}
                                width={240}
                                height={110}
                                className={cfg.logoClass}
                              />
                            </motion.div>
                          </div>
                          <p className="relative text-[0.98rem] font-medium tracking-[-0.02em] text-[#243755] sm:text-[1.08rem]">
                            {cfg.label}
                          </p>
                        </motion.div>
                      ))}
                    </div>
                  </motion.div>
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
