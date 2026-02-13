"use client"

import { useState } from "react"
import Image from "next/image"
import AdminFileAccess from "@/components/AdminFileAccess"

type Props = {
  brand: string
  onChange: (brand: string) => void
  currentView: "home" | "dashboard"
  onViewChange: (view: "home" | "dashboard") => void
  authRole: "admin" | "employee" | null
}

export default function Sidebar({
  brand,
  onChange,
  currentView,
  onViewChange,
  authRole,
}: Props) {
  const [openSamsung, setOpenSamsung] = useState(true)

  const isSamsungActive =
    brand === "samsung" || brand === "samsung_croma" || brand === "samsung_vs"

  return (
    <div className="w-64 border-r bg-white p-4 h-full flex flex-col overflow-y-auto">
      <div>
        <h2 className="text-xs font-bold text-gray-400 mb-4 uppercase tracking-wider">
          Navigation
        </h2>

        <div className="mb-4">
          <div
            onClick={() => onViewChange("home")}
            className={`
              cursor-pointer rounded-lg px-3 py-2 text-sm font-semibold
              ${currentView === "home"
                ? "bg-indigo-600 text-white"
                : "text-gray-700 hover:bg-gray-100"}
            `}
          >
            Home
          </div>
        </div>

        <div className="mb-4">
          <div
            onClick={() => {
              onChange("samsung")
              setOpenSamsung(true)
            }}
            className={`
              flex items-center justify-between cursor-pointer
              rounded-lg px-3 py-2 text-sm font-semibold
              ${
                isSamsungActive
                  ? "bg-indigo-50 text-indigo-700"
                  : "text-gray-700 hover:bg-gray-100"
              }
            `}
          >
            <span>Samsung</span>
            <span
              className="text-xs"
              onClick={(e) => {
                e.stopPropagation()
                setOpenSamsung((v) => !v)
              }}
            >
              {openSamsung ? "v" : ">"}
            </span>
          </div>

          {openSamsung && (
            <ul className="mt-2 ml-3 space-y-1">
              {[
                { label: "Croma", value: "samsung_croma", logo: "/croma_logo.jpg" },
                { label: "Vijay Sales", value: "samsung_vs", logo: "/vs_logo.jpg" },
              ].map((item) => {
                const active = brand === item.value

                return (
                  <li
                    key={item.value}
                    onClick={() => onChange(item.value)}
                    className={`
                      cursor-pointer rounded-md px-3 py-2 text-sm
                      transition-colors
                      ${
                        active
                          ? "bg-indigo-600 text-white"
                          : "text-gray-600 hover:bg-gray-100"
                      }
                    `}
                  >
                    <div className="flex items-center gap-2">
                      <Image
                        src={item.logo}
                        alt={`${item.label} logo`}
                        width={40}
                        height={16}
                        className="h-4 w-10 object-contain"
                      />
                      <span>{item.label}</span>
                    </div>
                  </li>
                )
              })}
            </ul>
          )}
        </div>

        <ul className="space-y-1">
          {[
            { label: "Reliance", value: "reliance" },
            { label: "Godrej", value: "godrej" },
          ].map((item) => {
            const active = brand === item.value

            return (
              <li
                key={item.value}
                onClick={() => onChange(item.value)}
                className={`
                  cursor-pointer rounded-lg px-3 py-2 text-sm font-medium
                  transition-colors
                  ${
                    active
                      ? "bg-indigo-600 text-white"
                      : "text-gray-700 hover:bg-gray-100"
                  }
                `}
              >
                {item.label}
              </li>
            )
          })}
        </ul>
      </div>

      <div className="mt-auto pt-4">
        <AdminFileAccess isAdmin={authRole === "admin"} />
      </div>
    </div>
  )
}
