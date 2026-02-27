"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { CalendarDays, ChevronLeft, ChevronRight } from "lucide-react"

type Props = {
  draftFromDate: string
  draftToDate: string
  minDate?: string
  maxDate?: string
  compact?: boolean
  align?: "auto" | "left" | "right"
  onDraftChange: (fromDate: string, toDate: string) => void
  onApply: (fromDate: string, toDate: string) => void
  onReset: () => void
}

type PresetKey =
  | "this_month"
  | "last_month"
  | "last_6_months"
  | "last_1_year"
  | "since_inception"

const PRESETS: Array<{ key: PresetKey; label: string }> = [
  { key: "this_month", label: "This Month" },
  { key: "last_month", label: "Last Month" },
  { key: "last_6_months", label: "Last 6 Months" },
  { key: "last_1_year", label: "Last Year" },
  { key: "since_inception", label: "Since Inception" },
]

const MONTH_NAMES = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
]

const WEEKDAY_SHORT = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

const pad2 = (value: number) => String(value).padStart(2, "0")

const toIso = (date: Date) =>
  `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`

const parseIso = (value: string): Date | null => {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value || "")) return null
  const [yRaw, mRaw, dRaw] = value.split("-")
  const y = Number(yRaw)
  const m = Number(mRaw)
  const d = Number(dRaw)
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return null
  const date = new Date(y, m - 1, d)
  if (
    date.getFullYear() !== y ||
    date.getMonth() !== m - 1 ||
    date.getDate() !== d
  ) {
    return null
  }
  return date
}

const startOfDay = (date: Date) => new Date(date.getFullYear(), date.getMonth(), date.getDate())
const startOfMonth = (date: Date) => new Date(date.getFullYear(), date.getMonth(), 1)
const endOfMonth = (date: Date) => new Date(date.getFullYear(), date.getMonth() + 1, 0)
const addDays = (date: Date, days: number) => new Date(date.getFullYear(), date.getMonth(), date.getDate() + days)
const addMonths = (date: Date, months: number) => new Date(date.getFullYear(), date.getMonth() + months, 1)

const sameDate = (a: Date | null, b: Date | null) =>
  Boolean(a && b && a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate())

const clampDate = (date: Date, minDate: Date | null, maxDate: Date | null) => {
  if (minDate && date < minDate) return minDate
  if (maxDate && date > maxDate) return maxDate
  return date
}

const normalizeRange = (
  fromDate: Date,
  toDate: Date,
  minDate: Date | null,
  maxDate: Date | null
) => {
  const clampedFrom = clampDate(startOfDay(fromDate), minDate, maxDate)
  const clampedTo = clampDate(startOfDay(toDate), minDate, maxDate)
  return clampedFrom <= clampedTo
    ? { from: clampedFrom, to: clampedTo }
    : { from: clampedTo, to: clampedFrom }
}

export default function DateRangePicker({
  draftFromDate,
  draftToDate,
  minDate,
  maxDate,
  compact = false,
  align = "auto",
  onDraftChange,
  onApply,
  onReset,
}: Props) {
  const [open, setOpen] = useState(false)
  const pickerRef = useRef<HTMLDivElement | null>(null)
  const triggerRef = useRef<HTMLButtonElement | null>(null)
  const [panelSide, setPanelSide] = useState<"left" | "right">("left")
  const [panelVertical, setPanelVertical] = useState<"down" | "up">("down")
  const [runtimePanelWidth, setRuntimePanelWidth] = useState<number>(compact ? 540 : 580)

  const today = useMemo(() => startOfDay(new Date()), [])
  const minBound = useMemo(() => parseIso(minDate || ""), [minDate])
  const maxBound = useMemo(() => parseIso(maxDate || "") || today, [maxDate, today])
  const panelWidth = compact ? 540 : 580
  const estimatedPanelHeight = compact ? 480 : 530
  const effectivePanelWidth = Math.min(panelWidth, runtimePanelWidth || panelWidth)
  const isNarrowPanel = effectivePanelWidth < 500

  const parsedFrom = useMemo(() => parseIso(draftFromDate || ""), [draftFromDate])
  const parsedTo = useMemo(() => parseIso(draftToDate || ""), [draftToDate])
  const [visibleMonth, setVisibleMonth] = useState<Date>(() =>
    startOfMonth(parsedFrom || parsedTo || maxBound || today)
  )

  useEffect(() => {
    setRuntimePanelWidth(panelWidth)
  }, [panelWidth])

  useEffect(() => {
    if (!open) return
    const onPointerDown = (event: MouseEvent) => {
      const target = event.target as Node | null
      if (!target || !pickerRef.current) return
      if (!pickerRef.current.contains(target)) {
        setOpen(false)
      }
    }
    document.addEventListener("mousedown", onPointerDown)
    return () => document.removeEventListener("mousedown", onPointerDown)
  }, [open])

  const monthGridDays = useMemo(() => {
    const monthStart = startOfMonth(visibleMonth)
    const gridStart = addDays(monthStart, -monthStart.getDay())
    return Array.from({ length: 42 }, (_, idx) => addDays(gridStart, idx))
  }, [visibleMonth])

  const monthYears = useMemo(() => {
    const fromYear = minBound?.getFullYear() ?? visibleMonth.getFullYear() - 6
    const toYear = maxBound?.getFullYear() ?? visibleMonth.getFullYear() + 6
    const years: number[] = []
    for (let year = fromYear; year <= toYear; year += 1) years.push(year)
    return years
  }, [minBound, maxBound, visibleMonth])

  const handlePreset = (preset: PresetKey) => {
    // If data starts in the future (relative to today), anchor presets to the
    // first available date so preset ranges do not collapse to an empty day.
    const base = clampDate(today, minBound, maxBound)
    let from = base
    let to = base

    if (preset === "last_6_months") {
      const sixMonthsBack = addMonths(base, -5)
      from = startOfMonth(sixMonthsBack)
      to = base
    } else if (preset === "last_1_year") {
      const oneYearBack = addMonths(base, -11)
      from = startOfMonth(oneYearBack)
      to = base
    } else if (preset === "since_inception") {
      from = minBound || base
      to = base
    } else if (preset === "this_month") {
      from = startOfMonth(base)
      to = base
    } else if (preset === "last_month") {
      const lastMonth = addMonths(base, -1)
      from = startOfMonth(lastMonth)
      to = endOfMonth(lastMonth)
    }

    const normalized = normalizeRange(from, to, minBound, maxBound)
    const fromIso = toIso(normalized.from)
    const toIsoValue = toIso(normalized.to)
    onDraftChange(fromIso, toIsoValue)
    onApply(fromIso, toIsoValue)
    setVisibleMonth(startOfMonth(from))
    setOpen(false)
  }

  const handleDaySelect = (day: Date) => {
    const disabled =
      (minBound && day < minBound) ||
      (maxBound && day > maxBound)
    if (disabled) return

    if (!parsedFrom || (parsedFrom && parsedTo)) {
      onDraftChange(toIso(day), "")
      return
    }

    if (!parsedTo) {
      const normalized = normalizeRange(parsedFrom, day, minBound, maxBound)
      onDraftChange(toIso(normalized.from), toIso(normalized.to))
      return
    }
  }

  const applySelection = () => {
    if (!parsedFrom && !parsedTo) return
    const fallback = parsedFrom || parsedTo || maxBound || today
    const from = parsedFrom || fallback
    const to = parsedTo || fallback
    const normalized = normalizeRange(from, to, minBound, maxBound)
    const fromIso = toIso(normalized.from)
    const toIsoValue = toIso(normalized.to)
    onDraftChange(fromIso, toIsoValue)
    onApply(fromIso, toIsoValue)
    setOpen(false)
  }

  const monthLabel = `${MONTH_NAMES[visibleMonth.getMonth()]} ${visibleMonth.getFullYear()}`

  const selectionSummary =
    parsedFrom && parsedTo
      ? `${draftFromDate} - ${draftToDate}`
      : parsedFrom
        ? `${draftFromDate} -`
        : "Select Date Range"

  const openPicker = () => {
    const viewportWidth = window.innerWidth
    const maxAllowedWidth = Math.max(300, viewportWidth - 16)
    const nextPanelWidth = Math.min(panelWidth, maxAllowedWidth)
    const panelHeightEstimate =
      nextPanelWidth < 500 ? estimatedPanelHeight + 120 : estimatedPanelHeight

    const rect = triggerRef.current?.getBoundingClientRect()
    if (rect) {
      let nextSide: "left" | "right" = "left"
      if (align === "right") {
        nextSide = "right"
      } else if (align === "left") {
        nextSide = "left"
      } else {
        nextSide = rect.left + nextPanelWidth > window.innerWidth - 12 ? "right" : "left"
      }
      const spaceBelow = window.innerHeight - rect.bottom
      const spaceAbove = rect.top
      const nextVertical: "down" | "up" =
        spaceBelow < panelHeightEstimate && spaceAbove > spaceBelow ? "up" : "down"
      setPanelSide(nextSide)
      setPanelVertical(nextVertical)
    }
    setRuntimePanelWidth(nextPanelWidth)
    setVisibleMonth(startOfMonth(parsedFrom || parsedTo || maxBound || today))
    setOpen(true)
  }

  return (
    <div ref={pickerRef} className="relative z-[120]">
      <button
        ref={triggerRef}
        type="button"
        onClick={() => {
          if (open) {
            setOpen(false)
            return
          }
          openPicker()
        }}
        className={`flex w-full items-center justify-between rounded-2xl border border-slate-200 bg-white text-left shadow-sm transition-colors hover:bg-slate-50 ${
          compact ? "px-3 py-2.5" : "px-4 py-3"
        }`}
      >
        <div className="flex min-w-0 flex-col">
          <span className="text-[10px] font-black uppercase tracking-[0.14em] text-slate-400">
            Date Range
          </span>
          <span className={`truncate font-semibold text-slate-700 ${compact ? "text-[13px]" : "text-sm"}`}>
            {selectionSummary}
          </span>
        </div>
        <CalendarDays className={`${compact ? "h-4 w-4" : "h-5 w-5"} text-slate-500`} />
      </button>

      {open && (
        <div
          className={`absolute z-[140] max-w-[calc(100vw-2rem)] overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-[0_30px_65px_-38px_rgba(15,23,42,0.6)] ${
            panelSide === "left" ? "left-0" : "right-0"
          } ${
            panelVertical === "down" ? "top-[calc(100%+10px)]" : "bottom-[calc(100%+10px)]"
          }`}
          style={{ width: `${effectivePanelWidth}px` }}
        >
          <div
            className={isNarrowPanel ? "grid grid-cols-1" : "grid"}
            style={isNarrowPanel ? undefined : { gridTemplateColumns: `${compact ? 114 : 126}px minmax(0, 1fr)` }}
          >
            <div
              className={
                isNarrowPanel
                  ? "grid grid-cols-2 gap-1 border-b border-slate-200 bg-slate-50/60 p-2"
                  : "border-r border-slate-200 bg-slate-50/60"
              }
            >
              {PRESETS.map((preset) => (
                <button
                  key={preset.key}
                  type="button"
                  onClick={() => handlePreset(preset.key)}
                  className={`block w-full border-b border-slate-200 text-left font-medium text-slate-700 transition-colors hover:bg-white ${
                    isNarrowPanel
                      ? "rounded-md border border-slate-200 bg-white px-2.5 py-2 text-[12px]"
                      : compact
                        ? "px-3 py-2.5 text-[13px]"
                        : "px-4 py-3 text-[15px]"
                  }`}
                >
                  {preset.label}
                </button>
              ))}
            </div>

            <div className={compact ? "p-3 sm:p-3.5" : "p-4"}>
              <div className={`${compact ? "mb-3" : "mb-4"} flex items-center justify-between`}>
                <button
                  type="button"
                  onClick={() => setVisibleMonth((value) => addMonths(value, -1))}
                  className={`rounded-md bg-slate-100 text-slate-600 transition-colors hover:bg-slate-200 ${
                    compact ? "p-1.5" : "p-1.5"
                  }`}
                >
                  <ChevronLeft className="h-4 w-4" />
                </button>

                <div className="flex items-center gap-2">
                  <select
                    value={visibleMonth.getMonth()}
                    onChange={(event) =>
                      setVisibleMonth(new Date(visibleMonth.getFullYear(), Number(event.target.value), 1))
                    }
                    className={`rounded-md border border-slate-200 font-medium text-slate-700 ${
                      compact ? "px-2 py-1 text-[12px]" : "px-2 py-1 text-sm"
                    }`}
                  >
                    {MONTH_NAMES.map((name, idx) => (
                      <option key={name} value={idx}>
                        {name}
                      </option>
                    ))}
                  </select>
                  <select
                    value={visibleMonth.getFullYear()}
                    onChange={(event) =>
                      setVisibleMonth(new Date(Number(event.target.value), visibleMonth.getMonth(), 1))
                    }
                    className={`rounded-md border border-slate-200 font-medium text-slate-700 ${
                      compact ? "px-2 py-1 text-[12px]" : "px-2 py-1 text-sm"
                    }`}
                  >
                    {monthYears.map((year) => (
                      <option key={year} value={year}>
                        {year}
                      </option>
                    ))}
                  </select>
                </div>

                <button
                  type="button"
                  onClick={() => setVisibleMonth((value) => addMonths(value, 1))}
                  className="rounded-md bg-slate-100 p-1.5 text-slate-600 transition-colors hover:bg-slate-200"
                >
                  <ChevronRight className="h-4 w-4" />
                </button>
              </div>

              <div className={`font-semibold text-slate-400 ${compact ? "mb-2 text-xl" : "mb-2 text-2xl"}`}>
                {monthLabel}
              </div>

              <div className="grid grid-cols-7 gap-y-1 text-center">
                {WEEKDAY_SHORT.map((weekday) => (
                  <div key={weekday} className={`${compact ? "py-1 text-[12px]" : "py-1 text-sm"} font-medium text-slate-400`}>
                    {weekday}
                  </div>
                ))}

                {monthGridDays.map((day) => {
                  const inCurrentMonth = day.getMonth() === visibleMonth.getMonth()
                  const disabled =
                    (minBound && day < minBound) ||
                    (maxBound && day > maxBound)

                  const startDate = parsedFrom
                  const endDate = parsedTo || parsedFrom
                  const isSelectedStart = sameDate(day, startDate)
                  const isSelectedEnd = sameDate(day, endDate)
                  const inRange =
                    startDate &&
                    endDate &&
                    day >= startOfDay(startDate) &&
                    day <= startOfDay(endDate)

                  const dayBaseClass = [
                    compact ? "h-8 w-full text-[13px] transition-colors" : "h-9 w-full text-sm transition-colors",
                    disabled ? "cursor-not-allowed" : "cursor-pointer",
                  ]

                  if (inRange && !disabled) {
                    dayBaseClass.push("bg-[#67ade8] text-white")
                    if (isSelectedStart && isSelectedEnd) {
                      dayBaseClass.push("rounded-full")
                    } else if (isSelectedStart) {
                      dayBaseClass.push("rounded-l-full")
                    } else if (isSelectedEnd) {
                      dayBaseClass.push("rounded-r-full")
                    }
                  } else {
                    dayBaseClass.push("rounded-full")
                    if (!inCurrentMonth) {
                      dayBaseClass.push("text-slate-300")
                    } else if (disabled) {
                      dayBaseClass.push("text-slate-300")
                    } else {
                      dayBaseClass.push("text-slate-600 hover:bg-slate-100")
                    }
                  }

                  return (
                    <button
                      key={toIso(day)}
                      type="button"
                      onClick={() => handleDaySelect(day)}
                      disabled={disabled}
                      className={dayBaseClass.join(" ")}
                    >
                      {day.getDate()}
                    </button>
                  )
                })}
              </div>
            </div>
          </div>

          <div className={`flex flex-wrap items-center gap-2 border-t border-slate-200 bg-slate-50 ${compact ? "px-3 py-2.5" : "px-4 py-3"}`}>
            <input
              type="text"
              readOnly
              value={draftFromDate || "-"}
              className={`rounded-lg border border-slate-300 bg-white text-center text-slate-700 ${
                compact ? "w-[104px] px-2 py-1.5 text-[12px] sm:w-[112px]" : "w-[126px] px-3 py-2 text-sm"
              }`}
            />
            <span className="text-slate-500">-</span>
            <input
              type="text"
              readOnly
              value={draftToDate || "-"}
              className={`rounded-lg border border-slate-300 bg-white text-center text-slate-700 ${
                compact ? "w-[104px] px-2 py-1.5 text-[12px] sm:w-[112px]" : "w-[126px] px-3 py-2 text-sm"
              }`}
            />

            <button
              type="button"
              onClick={onReset}
              className={`ml-auto font-semibold text-sky-600 transition-colors hover:text-sky-700 ${
                compact ? "text-sm" : "text-base"
              }`}
            >
              Reset
            </button>
            <button
              type="button"
              onClick={() => setOpen(false)}
              className={`font-semibold text-sky-600 transition-colors hover:text-sky-700 ${
                compact ? "text-sm" : "text-base"
              }`}
            >
              Close
            </button>
            <button
              type="button"
              onClick={applySelection}
              disabled={!draftFromDate && !draftToDate}
              className={`rounded-lg bg-slate-800 font-semibold text-white transition-colors enabled:hover:bg-slate-900 disabled:cursor-not-allowed disabled:bg-slate-300 ${
                compact ? "px-3 py-1.5 text-sm" : "px-4 py-2 text-base"
              }`}
            >
              Apply
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
