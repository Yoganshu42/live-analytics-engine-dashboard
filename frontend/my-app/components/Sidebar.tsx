"use client"

import { useEffect, useState } from "react"
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
import { VISIBLE_SAMSUNG_PARTNERS, isSamsungSource } from "@/lib/samsungPartners"

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
  const [isSourcesCollapsed, setIsSourcesCollapsed] = useState<boolean>(() => {
    if (typeof window === "undefined") return false
    return localStorage.getItem("dashboard_sources_card_collapsed") === "1"
  })
  const [isDeckStudioCollapsed, setIsDeckStudioCollapsed] = useState<boolean>(() => {
    if (typeof window === "undefined") return false
    return localStorage.getItem("dashboard_deck_card_collapsed") === "1"
  })

  const isSamsungActive = isSamsungSource(brand)

  const sourceItemClass = (active: boolean) =>
    `${railLinkBase} ${active ? "bg-[#e8efff] text-[#1f5cc8]" : "text-slate-600 hover:bg-slate-100"}`

  const handleSourceChange = (nextBrand: string) => {
    onChange(nextBrand)
  }

  useEffect(() => {
    if (typeof window === "undefined") return
    localStorage.setItem("dashboard_sources_card_collapsed", isSourcesCollapsed ? "1" : "0")
  }, [isSourcesCollapsed])

  useEffect(() => {
    if (typeof window === "undefined") return
    localStorage.setItem("dashboard_deck_card_collapsed", isDeckStudioCollapsed ? "1" : "0")
  }, [isDeckStudioCollapsed])

  return (
    <aside
      className={`custom-scrollbar w-full shrink-0 overflow-y-auto border-b border-slate-200 bg-[#f6f8fc] p-2.5 pb-4 sm:p-3 md:h-full md:border-b-0 md:border-r md:pt-5 ${
        collapsed
          ? "md:w-[88px] md:p-3"
          : "md:w-[230px] md:p-4"
      }`}
    >
      <div className={`rounded-2xl border border-slate-200 bg-white shadow-sm ${collapsed ? "p-2" : "p-2.5 sm:p-3"}`}>
        <div className={`mb-2 flex items-center ${collapsed ? "justify-center" : "justify-end"}`}>
          {onToggleCollapse ? (
            <button
              type="button"
              onClick={onToggleCollapse}
              className={`inline-flex items-center rounded-full border border-slate-200 bg-slate-50 text-[10px] font-bold uppercase tracking-[0.08em] text-slate-500 transition hover:bg-slate-100 ${
                collapsed ? "justify-center p-2" : "gap-1 px-2 py-1"
              }`}
              aria-expanded={!collapsed}
              aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
              title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
            >
              {collapsed ? <PanelLeftOpen size={14} /> : <PanelLeftClose size={14} />}
              {!collapsed && "Collapse"}
            </button>
          ) : null}
        </div>

        <div className={`grid gap-2 ${collapsed ? "grid-cols-1" : "grid-cols-2 md:grid-cols-1"}`}>
        <button
          onClick={() => onViewChange("home")}
          title="Home"
          className={`${railLinkBase} ${collapsed ? "justify-center px-2" : ""} ${currentView === "home" ? "bg-[#1f6fe5] text-white" : "text-slate-700 hover:bg-slate-100"}`}
        >
          <House size={16} />
          {!collapsed && <span>Home</span>}
        </button>

        <button
          onClick={() => onViewChange("master")}
          title="Master Dashboard"
          className={`${railLinkBase} ${collapsed ? "justify-center px-2" : ""} ${currentView === "master" ? "bg-[#1f6fe5] text-white" : "text-slate-700 hover:bg-slate-100"}`}
        >
          <ChartNoAxesColumn size={16} />
          {!collapsed && <span>Master Dashboard</span>}
        </button>
        </div>
      </div>

      {!collapsed && (
        <>
      <div className="mt-3 rounded-2xl border border-slate-200 bg-white p-2.5 shadow-sm sm:mt-4 sm:p-3">
        <div className="flex items-center justify-between gap-2 px-2 pb-2">
          <div className="text-[10px] font-black uppercase tracking-[0.18em] text-slate-400">
            Data Sources
          </div>

          <button
            type="button"
            onClick={() => setIsSourcesCollapsed((prev) => !prev)}
            className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2 py-1 text-[10px] font-bold uppercase tracking-[0.08em] text-slate-500 transition hover:bg-slate-100"
            aria-expanded={!isSourcesCollapsed}
            aria-label={isSourcesCollapsed ? "Expand data sources" : "Collapse data sources"}
          >
            <ChevronDown
              size={12}
              className={`transition-transform ${isSourcesCollapsed ? "-rotate-90" : ""}`}
            />
            {isSourcesCollapsed ? "Expand" : "Collapse"}
          </button>
        </div>

        {!isSourcesCollapsed && (
          <div>
          <button
            title="Samsung"
            onClick={() => {
              handleSourceChange("samsung")
              setOpenSamsung(true)
            }}
            className={sourceItemClass(isSamsungActive)}
          >
            <Store size={16} />
            <span className="flex-1">Samsung</span>
            <span
              className="rounded p-0.5 hover:bg-slate-200/70"
              onClick={(event) => {
                event.stopPropagation()
                setOpenSamsung((prev) => !prev)
              }}
            >
              {openSamsung ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
            </span>
          </button>

          {openSamsung && (
            <ul className="mt-1 space-y-1 pl-2">
              {VISIBLE_SAMSUNG_PARTNERS.map((partner) => {
                const active = brand === partner.key
                return (
                  <li key={partner.key}>
                    <button
                      title={partner.label}
                      onClick={() => handleSourceChange(partner.key)}
                      className={`${railLinkBase} ${active ? "bg-[#e8efff] text-[#1f5cc8]" : "text-slate-600 hover:bg-slate-100"}`}
                    >
                      <Image
                        src={partner.logo}
                        alt={`${partner.label} logo`}
                        width={40}
                        height={16}
                        className="h-4 w-10 rounded object-contain"
                      />
                      <span>{partner.shortLabel}</span>
                    </button>
                  </li>
                )
              })}
            </ul>
          )}

          <button
            title="Reliance ResQ"
            onClick={() => handleSourceChange("reliance")}
            className={`${sourceItemClass(brand === "reliance")} mt-1`}
          >
            <ChartNoAxesColumn size={16} />
            <span>Reliance ResQ</span>
          </button>

          <button
            title="Godrej"
            onClick={() => handleSourceChange("godrej")}
            className={`${sourceItemClass(brand === "godrej")} mt-1`}
          >
            <Shield size={16} />
            <span>Godrej</span>
          </button>

          <button
            title="Hitachi"
            onClick={() => handleSourceChange("hitachi")}
            className={`${sourceItemClass(brand === "hitachi")} mt-1`}
          >
            <Store size={16} />
            <span>Hitachi</span>
          </button>
          </div>
        )}
      </div>

      <div className="mt-3 sm:mt-4">
        <DeckStudioAccess
          collapsed={isDeckStudioCollapsed}
          onToggleCollapse={() => setIsDeckStudioCollapsed((prev) => !prev)}
        />
      </div>

      <div className="mt-4 border-t border-slate-200 pt-4">
        <AdminFileAccess isAdmin={authRole === "admin"} compact={false} />
      </div>
        </>
      )}
    </aside>
  )
}
