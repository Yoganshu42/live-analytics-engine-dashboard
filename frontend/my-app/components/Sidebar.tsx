"use client"

import { useState } from "react"
import Image from "next/image"
import {
  ChartNoAxesColumn,
  ChevronDown,
  ChevronRight,
  House,
  PanelLeftClose,
  PanelLeftOpen,
  Shield,
  Store,
} from "lucide-react"

import AdminFileAccess from "@/components/AdminFileAccess"
import DeckStudioAccess from "@/components/DeckStudioAccess"

type Props = {
  brand: string
  onChange: (brand: string) => void
  currentView: "home" | "master" | "dashboard"
  onViewChange: (view: "home" | "master" | "dashboard") => void
  authRole: "admin" | "employee" | null
  collapsed?: boolean
  onToggleCollapse?: () => void
}

const railLinkBase =
  "flex w-full items-center gap-2 rounded-lg px-2.5 py-2 text-left text-[13px] font-medium transition-colors sm:text-sm"

export default function Sidebar({
  brand,
  onChange,
  currentView,
  onViewChange,
  authRole,
  collapsed = false,
  onToggleCollapse,
}: Props) {
  const [openSamsung, setOpenSamsung] = useState(true)
  const [openSourcesMobile, setOpenSourcesMobile] = useState(false)

  const isSamsungActive =
    brand === "samsung" || brand === "samsung_croma" || brand === "samsung_vs"

  const sourceItemClass = (active: boolean) =>
    `${railLinkBase} ${active ? "bg-[#e8efff] text-[#1f5cc8]" : "text-slate-600 hover:bg-slate-100"}`

  const handleSourceChange = (nextBrand: string) => {
    onChange(nextBrand)
    setOpenSourcesMobile(false)
  }

  return (
    <aside
      className={`custom-scrollbar w-full shrink-0 overflow-y-auto border-b border-slate-200 bg-[#f6f8fc] p-2.5 pb-4 sm:p-3 md:h-full md:border-b-0 md:border-r md:p-4 md:pt-5 ${
        collapsed ? "md:w-[82px]" : "md:w-[230px]"
      }`}
    >
      <div className="mb-2 hidden items-center justify-end md:flex">
        <button
          type="button"
          onClick={onToggleCollapse}
          className="rounded-lg border border-slate-200 bg-white p-1.5 text-slate-600 transition hover:bg-slate-50"
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        >
          {collapsed ? <PanelLeftOpen size={16} /> : <PanelLeftClose size={16} />}
        </button>
      </div>

      <div className="rounded-2xl border border-slate-200 bg-white p-2.5 shadow-sm sm:p-3">
        <div className="grid grid-cols-2 gap-2 md:grid-cols-1">
        <button
          onClick={() => onViewChange("home")}
          title="Home"
          className={`${railLinkBase} ${collapsed ? "md:justify-center md:px-2" : ""} ${currentView === "home" ? "bg-[#1f6fe5] text-white" : "text-slate-700 hover:bg-slate-100"}`}
        >
          <House size={16} />
          <span className={collapsed ? "md:hidden" : ""}>Home</span>
        </button>

        <button
          onClick={() => onViewChange("master")}
          title="Master Dashboard"
          className={`${railLinkBase} ${collapsed ? "md:justify-center md:px-2" : ""} ${currentView === "master" ? "bg-[#1f6fe5] text-white" : "text-slate-700 hover:bg-slate-100"}`}
        >
          <ChartNoAxesColumn size={16} />
          <span className={collapsed ? "md:hidden" : ""}>Master Dashboard</span>
        </button>
        </div>
      </div>

      <div className="mt-3 rounded-2xl border border-slate-200 bg-white p-2.5 shadow-sm sm:mt-4 sm:p-3">
        <button
          type="button"
          onClick={() => setOpenSourcesMobile((prev) => !prev)}
          className="flex w-full items-center justify-between rounded-lg px-2 py-2 text-left md:hidden"
        >
          <span className="text-[10px] font-black uppercase tracking-[0.18em] text-slate-400">
            Data Sources
          </span>
          {openSourcesMobile ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        </button>

        <div className={`hidden px-2 pb-2 text-[10px] font-black uppercase tracking-[0.18em] text-slate-400 md:block ${collapsed ? "md:hidden" : ""}`}>
          Data Sources
        </div>

        <div className={`${openSourcesMobile ? "block" : "hidden"} md:block`}>
          <button
            title="Samsung"
            onClick={() => {
              handleSourceChange("samsung")
              setOpenSamsung(true)
            }}
            className={`${sourceItemClass(isSamsungActive)} ${collapsed ? "md:justify-center md:px-2" : ""}`}
          >
            <Store size={16} />
            <span className={`flex-1 ${collapsed ? "md:hidden" : ""}`}>Samsung</span>
            <span
              className={`rounded p-0.5 hover:bg-slate-200/70 ${collapsed ? "md:hidden" : ""}`}
              onClick={(event) => {
                event.stopPropagation()
                setOpenSamsung((prev) => !prev)
              }}
            >
              {openSamsung ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
            </span>
          </button>

          {openSamsung && !collapsed && (
            <ul className="mt-1 space-y-1 pl-2">
              {[
                { label: "Croma", value: "samsung_croma", logo: "/croma_logo.jpg" },
                { label: "Vijay Sales", value: "samsung_vs", logo: "/vs_logo.jpg" },
              ].map((item) => {
                const active = brand === item.value
                return (
                  <li key={item.value}>
                    <button
                      title={item.label}
                      onClick={() => handleSourceChange(item.value)}
                      className={`${railLinkBase} ${active ? "bg-[#e8efff] text-[#1f5cc8]" : "text-slate-600 hover:bg-slate-100"}`}
                    >
                      <Image
                        src={item.logo}
                        alt={`${item.label} logo`}
                        width={40}
                        height={16}
                        className="h-4 w-10 rounded object-contain"
                      />
                      <span>{item.label}</span>
                    </button>
                  </li>
                )
              })}
            </ul>
          )}

          <button
            title="Reliance ResQ"
            onClick={() => handleSourceChange("reliance")}
            className={`${sourceItemClass(brand === "reliance")} mt-1 ${collapsed ? "md:justify-center md:px-2" : ""}`}
          >
            <ChartNoAxesColumn size={16} />
            <span className={collapsed ? "md:hidden" : ""}>Reliance ResQ</span>
          </button>

          <button
            title="Godrej"
            onClick={() => handleSourceChange("godrej")}
            className={`${sourceItemClass(brand === "godrej")} mt-1 ${collapsed ? "md:justify-center md:px-2" : ""}`}
          >
            <Shield size={16} />
            <span className={collapsed ? "md:hidden" : ""}>Godrej</span>
          </button>
        </div>
      </div>

      <div className="mt-3 sm:mt-4">
        <DeckStudioAccess collapsed={collapsed} />
      </div>

      <div className="mt-4 border-t border-slate-200 pt-4">
        <AdminFileAccess isAdmin={authRole === "admin"} compact={collapsed} />
      </div>
    </aside>
  )
}
