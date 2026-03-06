"use client"

import { useState, useMemo, useEffect, useRef, useCallback, useLayoutEffect } from "react"
import Image from "next/image"
import dynamic from "next/dynamic"
import { useRouter } from "next/navigation"
import { motion, AnimatePresence, Variants, useReducedMotion } from "framer-motion"
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

type HoliBurst = {
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

type HoliPowderParticle = {
  left: string
  top: string
  size: number
  color: string
  driftX: number
  driftY: number
  duration: number
  delay: number
}

const HOLI_POPUP_BURSTS: HoliBurst[] = [
  { left: "6%", top: "14%", size: 180, color: "rgba(236,72,153,0.34)", dx: 46, dy: -28, duration: 6.2, delay: 0.1, blur: 2.5 },
  { left: "20%", top: "42%", size: 140, color: "rgba(245,158,11,0.32)", dx: -40, dy: -20, duration: 5.7, delay: 0.6, blur: 2.2 },
  { left: "34%", top: "22%", size: 210, color: "rgba(59,130,246,0.27)", dx: 38, dy: -32, duration: 7.0, delay: 0.4, blur: 2.8 },
  { left: "48%", top: "12%", size: 170, color: "rgba(132,204,22,0.31)", dx: -30, dy: -26, duration: 6.4, delay: 1.1, blur: 2.2 },
  { left: "62%", top: "34%", size: 190, color: "rgba(249,115,22,0.33)", dx: 34, dy: -30, duration: 6.8, delay: 0.2, blur: 2.6 },
  { left: "76%", top: "18%", size: 150, color: "rgba(168,85,247,0.34)", dx: -36, dy: -24, duration: 6.0, delay: 0.8, blur: 2.4 },
  { left: "88%", top: "46%", size: 210, color: "rgba(16,185,129,0.26)", dx: 28, dy: -36, duration: 7.2, delay: 0.5, blur: 2.7 },
  { left: "10%", top: "72%", size: 160, color: "rgba(244,63,94,0.28)", dx: 42, dy: -20, duration: 6.3, delay: 1.4, blur: 2.3 },
  { left: "28%", top: "78%", size: 190, color: "rgba(14,165,233,0.27)", dx: -34, dy: -24, duration: 6.6, delay: 0.9, blur: 2.5 },
  { left: "46%", top: "66%", size: 165, color: "rgba(234,179,8,0.34)", dx: 26, dy: -28, duration: 5.9, delay: 1.2, blur: 2.1 },
  { left: "64%", top: "78%", size: 210, color: "rgba(236,72,153,0.26)", dx: -22, dy: -34, duration: 7.1, delay: 0.3, blur: 2.9 },
  { left: "82%", top: "72%", size: 180, color: "rgba(34,197,94,0.28)", dx: 30, dy: -22, duration: 6.5, delay: 1.0, blur: 2.4 },
]

const HOLI_FLYING_POWDER: HoliPowderParticle[] = [
  { left: "5%", top: "84%", size: 14, color: "rgba(236,72,153,0.72)", driftX: 240, driftY: -190, duration: 8.2, delay: 0.2 },
  { left: "14%", top: "78%", size: 10, color: "rgba(245,158,11,0.72)", driftX: 220, driftY: -160, duration: 7.8, delay: 1.0 },
  { left: "23%", top: "88%", size: 12, color: "rgba(14,165,233,0.7)", driftX: 210, driftY: -170, duration: 8.6, delay: 0.5 },
  { left: "31%", top: "82%", size: 16, color: "rgba(132,204,22,0.7)", driftX: 180, driftY: -155, duration: 8.0, delay: 1.3 },
  { left: "41%", top: "90%", size: 11, color: "rgba(249,115,22,0.68)", driftX: 170, driftY: -180, duration: 7.4, delay: 0.7 },
  { left: "52%", top: "84%", size: 13, color: "rgba(168,85,247,0.7)", driftX: 160, driftY: -150, duration: 8.4, delay: 1.1 },
  { left: "63%", top: "88%", size: 15, color: "rgba(16,185,129,0.7)", driftX: 150, driftY: -160, duration: 7.7, delay: 0.4 },
  { left: "74%", top: "82%", size: 10, color: "rgba(244,63,94,0.72)", driftX: 130, driftY: -165, duration: 8.1, delay: 1.5 },
  { left: "84%", top: "90%", size: 12, color: "rgba(234,179,8,0.72)", driftX: 120, driftY: -150, duration: 7.9, delay: 0.9 },
  { left: "92%", top: "86%", size: 14, color: "rgba(59,130,246,0.72)", driftX: 100, driftY: -175, duration: 8.5, delay: 1.7 },
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
  const effectiveJobId = brand === "godrej" ? null : jobId
  const [authRole, setAuthRole] = useState<"admin" | "employee" | null>(null)
  const [authName, setAuthName] = useState<string>("")
  const [authReady, setAuthReady] = useState(false)
  const prefersReducedMotion = useReducedMotion()
  const homeViewportRef = useRef<HTMLDivElement | null>(null)
  const homeSceneRef = useRef<HTMLDivElement | null>(null)
  const [homeSceneScale, setHomeSceneScale] = useState(1)
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
            <div className="absolute inset-0 -z-10 bg-gradient-to-b from-[#fff1f2]/90 via-[#fff7ed]/70 to-[#ecfdf5]/90" />

            <div className="pointer-events-none fixed inset-0 z-0 overflow-hidden">
              <div className="absolute inset-0 bg-[radial-gradient(circle_at_12%_18%,rgba(236,72,153,0.2),transparent_42%),radial-gradient(circle_at_82%_20%,rgba(245,158,11,0.2),transparent_40%),radial-gradient(circle_at_24%_82%,rgba(59,130,246,0.16),transparent_42%),radial-gradient(circle_at_76%_78%,rgba(34,197,94,0.16),transparent_42%)]" />

              {HOLI_POPUP_BURSTS.map((burst, index) => (
                <motion.span
                  key={`holi-popup-${index}`}
                  className="absolute rounded-full"
                  style={{
                    left: burst.left,
                    top: burst.top,
                    width: burst.size,
                    height: burst.size,
                    background: burst.color,
                    filter: `blur(${burst.blur}px)`,
                  }}
                  initial={{ opacity: 0, scale: 0.2 }}
                  animate={
                    prefersReducedMotion
                      ? { opacity: 0.2, scale: 1 }
                      : {
                          opacity: [0, 0.48, 0],
                          scale: [0.15, 1.25, 0.72],
                          x: [0, burst.dx, burst.dx * 0.38],
                          y: [0, burst.dy, burst.dy * 0.42],
                        }
                  }
                  transition={
                    prefersReducedMotion
                      ? { duration: 0 }
                      : {
                          duration: burst.duration,
                          repeat: Infinity,
                          repeatType: "loop",
                          ease: "easeOut",
                          delay: burst.delay,
                        }
                  }
                />
              ))}

              {HOLI_FLYING_POWDER.map((particle, index) => (
                <motion.span
                  key={`holi-powder-${index}`}
                  className="absolute rounded-full"
                  style={{
                    left: particle.left,
                    top: particle.top,
                    width: particle.size,
                    height: particle.size,
                    background: particle.color,
                    boxShadow: `0 0 14px ${particle.color}`,
                  }}
                  initial={{ opacity: 0, scale: 0.4 }}
                  animate={
                    prefersReducedMotion
                      ? { opacity: 0.22, scale: 1 }
                      : {
                          opacity: [0, 0.85, 0],
                          scale: [0.4, 1, 0.6],
                          x: [0, particle.driftX],
                          y: [0, particle.driftY],
                        }
                  }
                  transition={
                    prefersReducedMotion
                      ? { duration: 0 }
                      : {
                          duration: particle.duration,
                          repeat: Infinity,
                          repeatType: "loop",
                          ease: "easeOut",
                          delay: particle.delay,
                        }
                  }
                />
              ))}
            </div>

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
                  <motion.div variants={fadeIn} className="mb-8 inline-flex items-center gap-2 rounded-full border border-fuchsia-200 bg-gradient-to-r from-fuchsia-50 via-amber-50 to-emerald-50 px-4 py-1.5 shadow-sm">
                    <span className="relative flex h-2 w-2">
                      <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-fuchsia-400 opacity-75"></span>
                      <span className="relative inline-flex h-2 w-2 rounded-full bg-emerald-500"></span>
                    </span>
                    <span className="text-[10px] font-black uppercase tracking-[0.25em] text-fuchsia-700">Holi Insights Mode</span>
                  </motion.div>

                  <motion.h2 variants={fadeIn} className="font-black tracking-tight mb-8 text-center">
                    <span className="block text-2xl leading-tight text-slate-900 md:text-3xl">Welcome to</span>
                    <span className="block bg-gradient-to-r from-fuchsia-600 via-amber-500 to-emerald-600 bg-clip-text text-4xl font-serif italic text-transparent sm:text-5xl md:text-7xl">
                     Business Control Centre
                    </span>
                  </motion.h2>
                  
                  <motion.p variants={fadeIn} className="mx-auto max-w-2xl text-center text-base font-medium leading-relaxed text-slate-600 sm:text-lg md:text-xl">
                    Navigate through partner ecosystems with precision. <br/>Real-time performance metrics at your fingertips.
                  </motion.p>
                </motion.div>
              </div>

              <motion.div variants={staggerContainer} initial="initial" animate="animate" className="w-full space-y-4 sm:space-y-8">
                <motion.div
                  variants={cardHover}
                  whileHover="hover"
                  onClick={() => handleViewChange("master")}
                  className="group relative mx-auto w-full max-w-[1160px] cursor-pointer rounded-[30px] border border-fuchsia-100 bg-gradient-to-br from-white/90 via-amber-50/75 to-emerald-50/80 p-6 text-center shadow-2xl backdrop-blur-md sm:rounded-[40px] sm:p-8"
                >
                  <div className="space-y-3 text-center">
                    <div className="flex items-center justify-center gap-3">
                      <h3 className="text-2xl font-black tracking-tight text-fuchsia-800 sm:text-3xl">Master Dashboard</h3>
                      <div className="flex h-9 w-9 scale-0 items-center justify-center rounded-full bg-gradient-to-br from-fuchsia-600 to-emerald-600 text-white transition-transform duration-300 group-hover:scale-100 sm:h-10 sm:w-10">
                        <ChevronRight size={20} />
                      </div>
                    </div>
                    <p className="text-[10px] font-black uppercase tracking-[0.2em] text-fuchsia-500">
                      Unified view across Samsung, Croma, Vijay Sales, Reliance ResQ and Godrej
                    </p>
                  </div>
                </motion.div>

                <div className="grid grid-cols-1 gap-4 justify-items-center sm:gap-8 md:grid-cols-3">
                  {brandConfigs.map((cfg) => (
                    <motion.div
                      key={cfg.value}
                      variants={cardHover}
                      whileHover="hover"
                      onClick={() => {
                        applyBrandChange(cfg.value)
                      }}
                      className="group relative w-full max-w-[360px] cursor-pointer rounded-[30px] border border-rose-100 bg-gradient-to-br from-white/90 via-rose-50/70 to-cyan-50/80 p-6 text-center shadow-2xl transition-all duration-500 backdrop-blur-md sm:max-w-[380px] sm:rounded-[48px] sm:p-10"
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
                          <div className="flex h-9 w-9 scale-0 items-center justify-center rounded-full bg-gradient-to-br from-fuchsia-600 to-orange-500 text-white transition-transform duration-300 group-hover:scale-100 sm:h-10 sm:w-10">
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
