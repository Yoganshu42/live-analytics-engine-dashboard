"use client"

import { useState, useMemo, useEffect, useRef, useCallback } from "react"
import Image from "next/image"
import { useRouter } from "next/navigation"
import { motion, AnimatePresence, Variants } from "framer-motion"
import {
  Maximize2,
  BarChart3,
  ShieldCheck,
  Calendar,
  Activity,
  LogOut,
  ChevronRight
} from "lucide-react"

import Sidebar from "@/components/Sidebar"
import Tabs from "@/components/Tabs"
import GraphSection from "@/components/GraphSection"
import ResultCard from "@/components/ResultCard"
import LastUpdatedCard from "@/components/LastUpdatedCard"
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
  const currentMonthDateMax = useCallback(() => {
    const now = new Date()
    const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0)
    return monthEnd.toISOString().slice(0, 10)
  }, [])

  const clampToCurrentMonth = useCallback((value: string) => {
    if (!value) return value
    const maxDate = currentMonthDateMax()
    return value > maxDate ? maxDate : value
  }, [currentMonthDateMax])

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

  const normalizeDateRange = (
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
  }

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

    return {
      view: storedView === "dashboard" ? "dashboard" : "home",
      brand: storedBrand ? normalizeBrand(storedBrand) : "samsung",
      mode: storedMode === "claims" ? "claims" : "sales",
      jobId,
      from: storedFrom,
      to: storedTo,
    }
  })

  const [view, setView] = useState<"home" | "dashboard">(initialDashboardState.view)
  const [brand, setBrand] = useState<string>(initialDashboardState.brand)
  const [mode, setMode] = useState<"sales" | "claims">(initialDashboardState.mode)
  const [isFullscreen, setIsFullscreen] = useState(false)
  const [isLoggingOut, setIsLoggingOut] = useState(false)
  const [jobId] = useState<string | null>(initialDashboardState.jobId)
  const [authRole, setAuthRole] = useState<"admin" | "employee" | null>(null)
  const [authName, setAuthName] = useState<string>("")
  const [authReady, setAuthReady] = useState(false)

  const [fromDate, setFromDate] = useState<string>(initialDashboardState.from)
  const [toDate, setToDate] = useState<string>(initialDashboardState.to)
  const [draftFromDate, setDraftFromDate] = useState<string>(initialDashboardState.from)
  const [draftToDate, setDraftToDate] = useState<string>(initialDashboardState.to)
  const [defaultFromDate, setDefaultFromDate] = useState<string>("")
  const [defaultToDate, setDefaultToDate] = useState<string>("")
  const [defaultKey, setDefaultKey] = useState<string>("")

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

  const applyBrandChange = (nextBrandRaw: string) => {
    const nextBrand = normalizeBrand(nextBrandRaw)
    setBrand(nextBrand)
    setIsFullscreen(false)
    setDefaultKey("")
    setFromDate("")
    setToDate("")
    setDraftFromDate("")
    setDraftToDate("")
    setView("dashboard")
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
    const nextKey = `${brand}|${mode}|${jobId || ""}`
    const snapshot = dateStateRef.current
    if (snapshot.defaultKey === nextKey && (snapshot.defaultFromDate || snapshot.defaultToDate)) return
    let mounted = true

    fetchDateBounds({ job_id: jobId || undefined, source: brand, dataset_type: mode })
      .then((res) => {
        if (!mounted) return
        const min = clampToCurrentMonth(res?.min_date ?? "")
        const max = clampToCurrentMonth(res?.max_date ?? "")
        setDefaultFromDate(min)
        setDefaultToDate(max)
        setDefaultKey(nextKey)

        if (!min || !max) {
          setFromDate("")
          setToDate("")
          setDraftFromDate("")
          setDraftToDate("")
          return
        }

        if (!snapshot.fromDate && !snapshot.toDate) {
          setFromDate(min); setToDate(max); setDraftFromDate(min); setDraftToDate(max);
        } else if (min && max) {
          const clamp = (value: string) => {
            if (!value) return value
            if (value < min) return min
            if (value > max) return max
            return value
          }
          const nextFrom = clamp(clampToCurrentMonth(snapshot.fromDate || min))
          const nextTo = clamp(clampToCurrentMonth(snapshot.toDate || max))
          const orderedFrom = nextFrom <= nextTo ? nextFrom : min
          const orderedTo = nextFrom <= nextTo ? nextTo : max
          if (orderedFrom !== snapshot.fromDate || orderedTo !== snapshot.toDate) {
            setFromDate(orderedFrom); setToDate(orderedTo)
          }
          if (orderedFrom !== snapshot.draftFromDate || orderedTo !== snapshot.draftToDate) {
            setDraftFromDate(orderedFrom); setDraftToDate(orderedTo)
          }
        }
      })
    return () => { mounted = false }
  }, [brand, mode, jobId, clampToCurrentMonth])

  const handleModeChange = (nextMode: "sales" | "claims") => {
    setIsFullscreen(false)
    setMode(nextMode)
  }

  const theme = useMemo(() => ({
    primary: mode === "sales" ? "#6366f1" : "#f43f5e",
    secondary: mode === "sales" ? "#a5b4fc" : "#fda4af",
    accent: mode === "sales" ? "text-indigo-600" : "text-rose-600",
    bgLight: mode === "sales" ? "bg-indigo-50/50" : "bg-rose-50/50",
  }), [mode])
  const activeDateKey = `${brand}|${mode}|${jobId || ""}`
  const hasResolvedDateBounds = defaultKey === activeDateKey
  const hasDateBounds = Boolean(defaultFromDate && defaultToDate)
  const isDashboardDataReady =
    hasResolvedDateBounds && (!hasDateBounds || Boolean(fromDate && toDate))

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
    <div className="h-screen flex flex-col bg-[#fbfcfd] text-slate-900 font-sans selection:bg-indigo-100 overflow-hidden">
      {/* HEADER */}
      <motion.header 
        variants={headerSlide}
        initial="initial"
        animate="animate"
        className="h-20 flex items-center justify-between px-10 border-b bg-white/80 backdrop-blur-2xl sticky top-0 z-40"
      >
        <div className="flex items-center gap-6">
          <motion.button
            whileHover={{ scale: 1.05 }}
            onClick={() => setView("home")}
            className="cursor-pointer"
          >
            <Image
              src="/Zopper Logo Original 1.png"
              alt="Zopper Logo"
              width={140}
              height={36}
              className="h-9 w-auto object-contain"
            />
          </motion.button>
          <div className="h-8 w-[1px] bg-slate-200" />
          <h1 className="flex items-center gap-3">
            <span className={`${theme.accent} font-black uppercase text-[11px] tracking-[0.4em]`}>
              Analytics 
            </span>
          </h1>
        </div>

        <div className="flex items-center gap-6">
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
              <div className="text-right mr-2">
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
              className="p-2.5 rounded-full border border-slate-200 bg-white text-slate-500 hover:text-slate-900 transition-colors"
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
            className="relative flex-1 flex items-center justify-center p-6 overflow-hidden"
          >
            {/* Background Elements */}
            <div className="absolute inset-0 -z-10">
               <video autoPlay muted loop playsInline className="h-full w-full object-cover opacity-20 scale-105 blur-[2px]">
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

            <div className="w-full max-w-6xl relative z-20">
              <div className="text-center mb-20">
                <motion.div variants={staggerContainer} initial="initial" animate="animate">
                  <motion.div variants={fadeIn} className="inline-flex items-center gap-2 px-4 py-1.5 rounded-full bg-white border border-slate-200 shadow-sm mb-8">
                    <span className="relative flex h-2 w-2">
                      <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-indigo-400 opacity-75"></span>
                      <span className="relative inline-flex rounded-full h-2 w-2 bg-indigo-500"></span>
                    </span>
                    <span className="text-[#1E6FFF] text-[10px] font-black uppercase tracking-[0.25em]">Unified Data Experience</span>
                  </motion.div>

                  <motion.h2 variants={fadeIn} className="font-black tracking-tight mb-8">
                    <span className="block text-slate-900 leading-tight text-2xl md:text-3xl">Welcome to</span>
                    <span className="block text-5xl md:text-7xl text-transparent bg-clip-text bg-gradient-to-r from-indigo-600 via-purple-600 to-rose-500 italic font-serif">
                     Business Control Centre
                    </span>
                  </motion.h2>
                  
                  <motion.p variants={fadeIn} className="text-slate-500 text-xl max-w-2xl mx-auto leading-relaxed font-medium">
                    Navigate through partner ecosystems with precision. <br/>Real-time performance metrics at your fingertips.
                  </motion.p>
                </motion.div>
              </div>

              <motion.div 
                variants={staggerContainer} 
                initial="initial" 
                animate="animate"
                className="grid grid-cols-1 md:grid-cols-3 gap-8"
              >
                {brandConfigs.map((cfg) => (
                  <motion.div
                    key={cfg.value}
                    variants={cardHover}
                    whileHover="hover"
                    onClick={() => {
                      applyBrandChange(cfg.value)
                    }}
                    className="group cursor-pointer relative bg-white/70 backdrop-blur-md border border-white p-10 rounded-[48px] shadow-2xl transition-all duration-500"
                  >
                    <div className="h-24 flex items-center justify-center mb-10 overflow-hidden">
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
                    <div className="space-y-4">
                      <div className="flex items-center justify-between">
                        <h3 className="font-bold text-2xl text-slate-800 tracking-tight">{cfg.label}</h3>
                        <div className="w-10 h-10 rounded-full flex items-center justify-center bg-slate-900 text-white scale-0 group-hover:scale-100 transition-transform duration-300">
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
            className="flex flex-1 overflow-hidden"
          >
            <Sidebar
              brand={brand}
              onChange={(b) => applyBrandChange(b)}
              currentView={view}
              onViewChange={setView}
              authRole={authRole}
            />

            <main className="flex-1 p-10 overflow-y-auto bg-[#fbfcfd] custom-scrollbar">
              <div className="max-w-7xl mx-auto">
                <motion.div initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} className="flex justify-between items-end mb-12">
                  <div className="space-y-2">
                    <div className="flex items-center gap-3">
                       <div className={`p-2 rounded-lg ${theme.bgLight}`}>
                         <Activity size={18} className={theme.accent} />
                       </div>
                       <span className="text-[11px] font-black uppercase tracking-[0.3em] text-slate-400">Strategic Performance</span>
                    </div>
                    <h2 className="text-5xl font-black capitalize text-slate-900 tracking-tighter">
                      {brandLabel(brand)}
                    </h2>
                  </div>

                  <div className="flex flex-col items-end gap-5">
                    {brand.startsWith("samsung") && (
                      <motion.div 
                        initial={{ opacity: 0, scale: 0.9 }} 
                        animate={{ opacity: 1, scale: 1 }} 
                        className="flex items-center gap-4 bg-white px-5 py-3 rounded-[24px] shadow-sm border border-slate-100"
                      >
                        <Image src="/WhatsApp Image 2026-02-04 at 11.14.29.jpeg" alt="Samsung" width={120} height={40} className="h-10 w-auto" />
                        {brand !== "samsung" && <div className="h-4 w-[1px] bg-slate-200" />}
                        {brand === "samsung_vs" && <Image src="/vs_logo.jpg" width={96} height={32} className="h-8 w-auto" alt="VS" />}
                        {brand === "samsung_croma" && <Image src="/croma_logo.jpg" width={96} height={32} className="h-8 w-auto" alt="Croma" />}
                      </motion.div>
                    )}
                    <Tabs value={mode} onChange={handleModeChange} />
                  </div>
                </motion.div>

                <motion.div variants={staggerContainer} initial="initial" animate="animate" className="grid grid-cols-1 lg:grid-cols-12 gap-8 mb-10">
                  <motion.div variants={fadeIn} className="lg:col-span-4">
                    {isDashboardDataReady ? (
                      <ResultCard source={brand} datasetType={mode} color={theme.primary} jobId={jobId || undefined} fromDate={fromDate || undefined} toDate={toDate || undefined} />
                    ) : (
                      <div className="h-full min-h-[220px] rounded-[32px] border border-slate-200 bg-white p-8 flex items-center justify-center text-sm text-slate-400">
                        Loading summary...
                      </div>
                    )}
                  </motion.div>
                  
                  <motion.div variants={fadeIn} className="lg:col-span-4">
                    {isDashboardDataReady ? (
                      <LastUpdatedCard source={brand} datasetType={mode} jobId={jobId || undefined} fromDate={fromDate || undefined} toDate={toDate || undefined} />
                    ) : (
                      <div className="h-full min-h-[220px] rounded-[32px] border border-slate-200 bg-white p-8 flex items-center justify-center text-sm text-slate-400">
                        Loading freshness...
                      </div>
                    )}
                  </motion.div>

                  <motion.div variants={fadeIn} className="lg:col-span-4 bg-white p-8 rounded-[32px] border border-slate-200 shadow-sm flex flex-col justify-between">
                    <div>
                      <div className="flex gap-3 items-center mb-6">
                        <div className="p-2.5 rounded-xl bg-slate-50 border border-slate-100 text-slate-600"><Calendar size={20} /></div>
                        <span className="text-[11px] font-black uppercase tracking-widest text-slate-500">Date Range</span>
                      </div>
                      <div className="grid grid-cols-2 gap-3 mb-6">
                        <div className="space-y-1">
                          <label className="text-[9px] font-bold uppercase text-slate-400 ml-1">From</label>
                          <input type="date" value={draftFromDate} max={currentMonthDateMax()} onChange={e => setDraftFromDate(clampToCurrentMonth(e.target.value))} className="w-full border-slate-200 rounded-2xl px-4 py-3 text-xs font-bold focus:ring-4 focus:ring-indigo-500/10 transition-all outline-none bg-slate-50/50" />
                        </div>
                        <div className="space-y-1">
                          <label className="text-[9px] font-bold uppercase text-slate-400 ml-1">To</label>
                          <input type="date" value={draftToDate} max={currentMonthDateMax()} onChange={e => setDraftToDate(clampToCurrentMonth(e.target.value))} className="w-full border-slate-200 rounded-2xl px-4 py-3 text-xs font-bold focus:ring-4 focus:ring-indigo-500/10 transition-all outline-none bg-slate-50/50" />
                        </div>
                      </div>
                    </div>
                    <div className="flex gap-3">
                      <motion.button 
                        whileTap={{ scale: 0.95 }}
                        className="flex-1 text-xs font-black uppercase tracking-widest py-4 rounded-2xl bg-slate-900 text-white hover:bg-black transition-all shadow-lg shadow-slate-200" 
                        onClick={() => {
                          const next = normalizeDateRange(
                            draftFromDate,
                            draftToDate,
                            defaultFromDate,
                            defaultToDate
                          )
                          setDraftFromDate(next.from)
                          setDraftToDate(next.to)
                          setFromDate(next.from)
                          setToDate(next.to)
                        }}
                      >
                        Apply Filters
                      </motion.button>
                      <button className="px-5 py-4 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 transition-all text-xs font-bold text-slate-400" onClick={() => {
                        setFromDate(defaultFromDate); setToDate(defaultToDate); setDraftFromDate(defaultFromDate); setDraftToDate(defaultToDate);
                      }}>Reset</button>
                    </div>
                  </motion.div>
                </motion.div>

                <motion.section initial={{ opacity: 0, y: 40 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.4 }} className="bg-white p-10 rounded-[48px] border border-slate-200 shadow-sm relative group">
                  <div className="flex justify-between items-center mb-10">
                    <div className="space-y-1">
                      <h3 className="font-bold text-2xl flex items-center gap-4">
                        <div className={`p-3 rounded-2xl ${theme.bgLight} ${theme.accent}`}>
                          {mode === "sales" ? <BarChart3 size={24} /> : <ShieldCheck size={24} />}
                        </div>
                        Analysis Workspace
                      </h3>
                      <p className="text-slate-400 text-xs font-medium ml-16">Deep dive into granular data points and trends</p>
                    </div>
                    <motion.button 
                      whileHover={{ scale: 1.1, rotate: 90 }}
                      onClick={() => setIsFullscreen(true)} 
                      className="p-3 bg-slate-50 hover:bg-slate-100 rounded-full transition-colors"
                    >
                      <Maximize2 size={20} className="text-slate-600" />
                    </motion.button>
                  </div>
                  <div className="min-h-[500px] w-full">
                    {isDashboardDataReady ? (
                      <GraphSection source={brand} datasetType={mode} jobId={jobId} primaryColor={theme.primary} secondaryColor={theme.secondary} fromDate={fromDate || undefined} toDate={toDate || undefined} />
                    ) : (
                      <div className="h-[500px] flex items-center justify-center text-sm text-slate-400">
                        Loading charts...
                      </div>
                    )}
                  </div>
                </motion.section>
              </div>
            </main>
          </motion.div>
        )}
      </AnimatePresence>

      {/* MODALS & OVERLAYS */}
      <AnimatePresence>
        {isFullscreen && (
          <motion.div initial={{ opacity: 0, scale: 1.1 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 1.1 }} className="fixed inset-0 z-50 bg-white overflow-hidden flex flex-col">
            <div className="p-8 border-b flex items-center justify-between bg-white/80 backdrop-blur-md">
              <div className="flex items-center gap-4">
                <div className={`h-10 w-10 rounded-xl flex items-center justify-center ${theme.bgLight} ${theme.accent}`}>
                   <Activity size={20} />
                </div>
                <div>
                  <h4 className="text-lg font-black tracking-tight">{brandLabel(brand)}</h4>
                  <p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{mode === "sales" ? "Sales Velocity" : "Claims Integrity"}</p>
                </div>
              </div>
              <button className="px-8 py-3 bg-slate-900 text-white rounded-2xl text-xs font-bold hover:bg-black transition-all" onClick={() => setIsFullscreen(false)}>Close Focus View</button>
            </div>
            <div className="flex-1 p-12 overflow-auto">
              {isDashboardDataReady ? (
                <GraphSection source={brand} datasetType={mode} jobId={jobId} primaryColor={theme.primary} fromDate={fromDate || undefined} toDate={toDate || undefined} />
              ) : (
                <div className="h-full min-h-[500px] flex items-center justify-center text-sm text-slate-400">
                  Loading charts...
                </div>
              )}
            </div>
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
