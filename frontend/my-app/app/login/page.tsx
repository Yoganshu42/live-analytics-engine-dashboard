"use client"

import { useEffect, useState } from "react"
import Image from "next/image"
import { useRouter } from "next/navigation"
import { motion, AnimatePresence, Variants } from "framer-motion"
import { login } from "../lib/api"

type Role = "admin" | "employee"

// --- ANIMATION VARIANTS (Fixed for TypeScript) ---
const containerVariants: Variants = {
  hidden: { opacity: 0, scale: 0.98 },
  visible: { 
    opacity: 1, 
    scale: 1,
    transition: { 
      duration: 0.8, 
      ease: "easeOut", 
      staggerChildren: 0.1 
    }
  }
}

const itemVariants: Variants = {
  hidden: { opacity: 0, y: 15 },
  visible: { 
    opacity: 1, 
    y: 0, 
    transition: { duration: 0.5, ease: "easeOut" } 
  }
}

const floatingModule: Variants = {
  animate: {
    y: [0, -15, 0],
    transition: { 
      duration: 4, 
      repeat: Infinity, 
      ease: "easeInOut" 
    }
  }
}

export default function LoginPage() {
  const normalizeToken = (value: string | null) => {
    if (!value) return null
    let token = value.trim()
    token = token.replace(/^['"]+|['"]+$/g, "")
    token = token.replace(/^Bearer\s+/i, "").trim()
    if (!token || token === "null" || token === "undefined") return null
    return token
  }

  const router = useRouter()
  const [role, setRole] = useState<Role>("admin")
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [error, setError] = useState("")
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    const storedRole = localStorage.getItem("auth_role")
    const token = normalizeToken(localStorage.getItem("auth_token"))
    if ((storedRole === "admin" || storedRole === "employee") && token) {
      router.replace("/")
    }
  }, [router])

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setError("")
    setLoading(true)

    const trimmedUser = email.trim().toLowerCase()
    const trimmedPass = password.trim()
    const zopperEmail = /^[a-z]+\.[a-z]+@zopper\.com$/

    if (!trimmedUser || !trimmedPass) {
      setError("Enter your email and password.")
      setLoading(false)
      return
    }

    if (!zopperEmail.test(trimmedUser)) {
      setError("Email must be in the format name.surname@zopper.com.")
      setLoading(false)
      return
    }

    try {
      const result = await login({
        email: trimmedUser,
        password: trimmedPass,
        role,
      })

      localStorage.setItem("auth_token", result.access_token)
      localStorage.setItem("auth_role", result.role)
      localStorage.setItem("auth_name", result.email)
      localStorage.setItem("auth_login_at", new Date().toISOString())
      localStorage.setItem("dashboard_view", "home")

      router.replace("/")
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Login failed"
      if (/invalid credentials|http 401/i.test(msg)) {
        setError("Wrong password.")
      } else if (/email.*format|pattern/i.test(msg)) {
        setError("Email must be in the format name.surname@zopper.com.")
      } else if (/failed to fetch|network|connect/i.test(msg)) {
        setError("Cannot reach the server. Is the backend running?")
      } else {
        setError("Login failed. Please try again.")
      }
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="relative min-h-screen bg-[#0f172a] flex items-center justify-center px-4 py-10 overflow-hidden">
      
      {/* BACKGROUND ANIMATION: DIGITAL GRID */}
      <div className="absolute inset-0 z-0 opacity-20">
        <div className="absolute inset-0 bg-[linear-gradient(to_right,#1e293b_1px,transparent_1px),linear-gradient(to_bottom,#1e293b_1px,transparent_1px)] bg-[size:40px_40px] [mask-image:radial-gradient(ellipse_60%_50%_at_50%_0%,#000_70%,transparent_100%)]" />
      </div>

      <motion.div
        variants={containerVariants}
        initial="hidden"
        animate="visible"
        className="relative z-10 w-full max-w-5xl grid grid-cols-1 md:grid-cols-2 bg-white rounded-[40px] shadow-[0_0_80px_rgba(79,70,229,0.15)] overflow-hidden"
      >
        {/* LEFT PANEL: BUSINESS BRANDING */}
        <div className="relative p-12 bg-indigo-600 text-white flex flex-col justify-between overflow-hidden">
          
          <motion.div 
            animate={{ rotate: 360 }}
            transition={{ duration: 50, repeat: Infinity, ease: "linear" }}
            className="absolute -top-20 -left-20 w-80 h-80 border border-white/10 rounded-full"
          />
          
          <div className="relative z-10">
            <motion.div variants={itemVariants} className="flex items-center gap-3 mb-12">
              <div className="bg-white p-2.5 rounded-2xl shadow-lg">
                <Image src="/Zopper Logo Original 1.png" alt="Zopper" width={96} height={24} className="h-6 w-auto" />
              </div>
              <span className="text-[10px] font-black tracking-[0.3em] uppercase opacity-70">Secure Access</span>
            </motion.div>

            <motion.h1 variants={itemVariants} className="text-5xl font-extrabold leading-tight tracking-tighter">
              Business Control <br />
              <span className="text-indigo-200">Centre.</span>
            </motion.h1>
            
            <motion.p variants={itemVariants} className="mt-6 text-indigo-100/80 text-sm max-w-xs leading-relaxed">
              Access real-time claims data and sales intelligence through our secure portal(Alpha Testing).
            </motion.p>
          </div>

          {/* Animated Business Graphic */}
          <motion.div 
            variants={floatingModule}
            animate="animate"
            className="relative z-10 bg-white/10 backdrop-blur-md border border-white/20 p-6 rounded-3xl mt-8 shadow-2xl"
          >
            <div className="flex items-center justify-between mb-4">
               <div className="h-2 w-12 bg-white/30 rounded-full" />
               <div className="h-6 w-6 rounded-full bg-emerald-400 animate-pulse shadow-[0_0_15px_rgba(52,211,153,0.5)]" />
            </div>
            <div className="space-y-3">
              <div className="h-1.5 w-full bg-white/20 rounded-full overflow-hidden">
                <motion.div 
                    initial={{ x: "-100%" }}
                    animate={{ x: "0%" }}
                    transition={{ duration: 2, repeat: Infinity, repeatDelay: 1 }}
                    className="h-full w-1/2 bg-white/60" 
                />
              </div>
              <div className="h-1.5 w-3/4 bg-white/20 rounded-full" />
              <div className="h-1.5 w-5/6 bg-white/20 rounded-full" />
            </div>
          </motion.div>

          <div className="mt-12 text-[10px] uppercase tracking-widest text-indigo-200 font-bold opacity-50">
             2026 Zopper Analytics Inc.
          </div>
        </div>

        {/* RIGHT PANEL: LOGIN FORM */}
        <div className="p-12 flex flex-col justify-center bg-white">
          <div className="max-w-sm mx-auto w-full">
            <motion.div variants={itemVariants} className="mb-10 text-center md:text-left">
              <h2 className="text-3xl font-black text-slate-900">Sign In</h2>
              <div className="h-1 w-10 bg-indigo-600 mt-3 rounded-full mx-auto md:mx-0" />
            </motion.div>

            <form onSubmit={handleSubmit} className="space-y-6">
              
              <motion.div variants={itemVariants}>
                <label className="text-[11px] font-bold text-slate-400 uppercase tracking-wider mb-3 block">Account Type</label>
                <div className="grid grid-cols-2 gap-3 p-1.5 bg-slate-100 rounded-2xl relative">
                  {(["admin", "employee"] as Role[]).map((item) => (
                    <button
                      key={item}
                      type="button"
                      onClick={() => setRole(item)}
                      className={`relative z-10 px-4 py-2.5 rounded-xl text-xs font-bold transition-all ${
                        role === item ? "text-indigo-600" : "text-slate-500 hover:text-slate-700"
                      }`}
                    >
                      {item === "admin" ? "Admin" : "Employee"}
                      {role === item && (
                        <motion.div 
                          layoutId="activeRole"
                          className="absolute inset-0 bg-white rounded-xl shadow-sm -z-10"
                          transition={{ type: "spring", stiffness: 300, damping: 30 }}
                        />
                      )}
                    </button>
                  ))}
                </div>
              </motion.div>

              <motion.div variants={itemVariants} className="space-y-4">
                <div className="group">
                  <label
                    htmlFor="email"
                    className="mb-2 block text-[11px] font-bold uppercase tracking-wider text-slate-500"
                  >
                    Email
                  </label>
                  <input
                    id="email"
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder="Email Address"
                    className="w-full rounded-2xl border-2 border-slate-300 bg-white px-5 py-4 text-sm font-semibold text-slate-900 placeholder:text-slate-400 transition-all focus:border-indigo-500 focus:bg-white focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  />
                </div>

                <div className="group">
                  <label
                    htmlFor="password"
                    className="mb-2 block text-[11px] font-bold uppercase tracking-wider text-slate-500"
                  >
                    Password
                  </label>
                  <input
                    id="password"
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder="Password"
                    className="w-full rounded-2xl border-2 border-slate-300 bg-white px-5 py-4 text-sm font-semibold text-slate-900 placeholder:text-slate-400 transition-all focus:border-indigo-500 focus:bg-white focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  />
                </div>
              </motion.div>

              <AnimatePresence mode="wait">
                {error && (
                  <motion.div 
                    initial={{ opacity: 0, x: -10 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0 }}
                    className="bg-rose-50 text-rose-600 text-xs font-bold p-4 rounded-2xl border border-rose-100"
                  >
                     {error}
                  </motion.div>
                )}
              </AnimatePresence>

              <motion.button
                variants={itemVariants}
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
                type="submit"
                disabled={loading}
                className="w-full bg-indigo-600 text-white rounded-2xl py-4 text-sm font-black shadow-lg shadow-indigo-200 transition-all flex items-center justify-center gap-2 disabled:opacity-50"
              >
                {loading ? (
                    <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                ) : (
                    "Login"
                )}
              </motion.button>

              <motion.div variants={itemVariants} className="text-center">
                <button type="button" className="text-xs font-bold text-slate-400 hover:text-indigo-600 transition-colors">
                  {/* Forgot Security Credentials? */}
                </button>
              </motion.div>

            </form>
          </div>
        </div>
      </motion.div>
    </div>
  )
}
