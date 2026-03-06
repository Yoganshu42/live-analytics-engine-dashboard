"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import Image from "next/image"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { Download, Loader2, ArrowLeft, ExternalLink } from "lucide-react"

import { downloadDeckPptx, fetchDateBounds } from "@/app/lib/api"
import { buildGoogleSlidesEditUrl, getGoogleDriveAccessToken, uploadPptxAsGoogleSlides } from "@/app/lib/googleSlides"
import DateRangePicker from "@/components/DateRangePicker"
import DeckSlidesPreview from "@/components/DeckSlidesPreview"

type DatasetType = "sales" | "claims"
type WeekWindow = 2 | 3 | 4 | 6

type PartnerOption = {
  key: string
  label: string
  logo: string
}

const PARTNERS: PartnerOption[] = [
  { key: "samsung_vs", label: "Samsung Vijay Sales", logo: "/vs_logo.jpg" },
  { key: "samsung_croma", label: "Samsung Croma", logo: "/croma_logo.jpg" },
  { key: "reliance", label: "Reliance ResQ", logo: "/resq.png" },
  { key: "godrej", label: "Godrej", logo: "/Group 1244833444.png" },
]

export default function DeckStudioPage() {
  const router = useRouter()
  const todayIso = useMemo(() => new Date().toISOString().slice(0, 10), [])
  const [isDownloading, setIsDownloading] = useState(false)
  const [isOpeningSlides, setIsOpeningSlides] = useState(false)
  const [error, setError] = useState("")

  const [datasetType, setDatasetType] = useState<DatasetType>("sales")
  const [jobId, setJobId] = useState("")
  const [fromDate, setFromDate] = useState("")
  const [toDate, setToDate] = useState("")
  const [draftFromDate, setDraftFromDate] = useState("")
  const [draftToDate, setDraftToDate] = useState("")
  const [defaultFromDate, setDefaultFromDate] = useState("")
  const [defaultToDate, setDefaultToDate] = useState("")
  const [minDate, setMinDate] = useState("")
  const [maxDate, setMaxDate] = useState("")
  const [includeTables, setIncludeTables] = useState(true)
  const [weekWindow, setWeekWindow] = useState<WeekWindow>(4)
  const [selectedPartners, setSelectedPartners] = useState<string[]>(PARTNERS.map((partner) => partner.key))
  const [boundsRefreshTick, setBoundsRefreshTick] = useState(0)
  const lastAutoBoundsKeyRef = useRef("")

  const toIsoDate = useCallback((value: string) => {
    const raw = (value || "").trim()
    if (!raw) return ""
    const directIso = raw.match(/^(\d{4}-\d{2}-\d{2})/)
    if (directIso?.[1]) return directIso[1]
    const parsed = new Date(raw)
    if (Number.isNaN(parsed.getTime())) return ""
    return parsed.toISOString().slice(0, 10)
  }, [])

  const clampToToday = useCallback((value: string) => {
    const iso = toIsoDate(value)
    if (!iso) return ""
    return iso > todayIso ? todayIso : iso
  }, [toIsoDate, todayIso])

  const applyDateRange = useCallback((nextFrom: string, nextTo: string) => {
    const normalizedFrom = clampToToday(nextFrom || "")
    const normalizedTo = clampToToday(nextTo || "")
    if (normalizedFrom && normalizedTo && normalizedFrom > normalizedTo) {
      setFromDate(normalizedTo)
      setToDate(normalizedFrom)
      setDraftFromDate(normalizedTo)
      setDraftToDate(normalizedFrom)
      if (typeof window !== "undefined") {
        localStorage.setItem("dashboard_from_date", normalizedTo)
        localStorage.setItem("dashboard_to_date", normalizedFrom)
        window.dispatchEvent(
          new CustomEvent("dashboard-filters-changed", {
            detail: {
              fromDate: normalizedTo,
              toDate: normalizedFrom,
            },
          })
        )
      }
      return
    }
    setFromDate(normalizedFrom)
    setToDate(normalizedTo)
    setDraftFromDate(normalizedFrom)
    setDraftToDate(normalizedTo)
    if (typeof window !== "undefined") {
      localStorage.setItem("dashboard_from_date", normalizedFrom)
      localStorage.setItem("dashboard_to_date", normalizedTo)
      window.dispatchEvent(
        new CustomEvent("dashboard-filters-changed", {
          detail: {
            fromDate: normalizedFrom,
            toDate: normalizedTo,
          },
        })
      )
    }
  }, [clampToToday])

  useEffect(() => {
    const token = (typeof window !== "undefined" ? localStorage.getItem("auth_token") : "") || ""
    if (!token.trim()) {
      router.replace("/login")
    }
  }, [router])

  useEffect(() => {
    if (typeof window === "undefined") return
    const params = new URLSearchParams(window.location.search)
    const partnerQuery = (params.get("partners") || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean)
    if (partnerQuery.length) {
      const allowed = new Set(PARTNERS.map((partner) => partner.key))
      const filtered = partnerQuery.filter((value) => allowed.has(value))
      if (filtered.length) {
        setSelectedPartners(filtered)
      }
    }
    const dataset = params.get("dataset_type")
    const storedMode = (localStorage.getItem("dashboard_mode") || "").trim().toLowerCase()
    if (dataset === "claims" || dataset === "sales") {
      setDatasetType(dataset)
    } else if (storedMode === "claims" || storedMode === "sales") {
      setDatasetType(storedMode)
    }
    const weekValue = Number(params.get("week_window") || 0)
    if (weekValue === 2 || weekValue === 3 || weekValue === 4 || weekValue === 6) {
      setWeekWindow(weekValue)
    }
    const useJobFilter = localStorage.getItem("use_job_filter") === "1"
    const storedJob = (localStorage.getItem("job_id") || "").trim()
    const nextJob =
      params.get("job_id") ||
      (useJobFilter && storedJob && storedJob !== "all" && storedJob !== "null" && storedJob !== "undefined"
        ? storedJob
        : "")
    const queryFrom = params.get("from_date") || ""
    const queryTo = params.get("to_date") || ""
    const storedFrom = clampToToday(localStorage.getItem("dashboard_from_date") || "")
    const storedTo = clampToToday(localStorage.getItem("dashboard_to_date") || "")
    const nextFrom = clampToToday(queryFrom || storedFrom)
    const nextTo = clampToToday(queryTo || storedTo)
    setJobId(nextJob)
    setFromDate(nextFrom)
    setToDate(nextTo)
    setDraftFromDate(nextFrom)
    setDraftToDate(nextTo)
    const includeTablesRaw = params.get("include_tables")
    if (includeTablesRaw !== null) {
      setIncludeTables(includeTablesRaw === "1")
    }
  }, [todayIso, clampToToday])

  useEffect(() => {
    if (typeof window === "undefined") return

    const syncFromStorage = (refreshBounds: boolean) => {
      const storedMode = (localStorage.getItem("dashboard_mode") || "").trim().toLowerCase()
      if (storedMode === "claims" || storedMode === "sales") {
        setDatasetType(storedMode)
      }

      const useJobFilter = localStorage.getItem("use_job_filter") === "1"
      const rawJob = (localStorage.getItem("job_id") || "").trim()
      const normalizedJob =
        useJobFilter && rawJob && rawJob !== "all" && rawJob !== "null" && rawJob !== "undefined"
          ? rawJob
          : ""
      setJobId(normalizedJob)

      const storedFrom = clampToToday(localStorage.getItem("dashboard_from_date") || "")
      const storedTo = clampToToday(localStorage.getItem("dashboard_to_date") || "")
      const orderedFrom = storedFrom && storedTo && storedFrom > storedTo ? storedTo : storedFrom
      const orderedTo = storedFrom && storedTo && storedFrom > storedTo ? storedFrom : storedTo
      setFromDate(orderedFrom)
      setToDate(orderedTo)
      setDraftFromDate(orderedFrom)
      setDraftToDate(orderedTo)

      if (refreshBounds) {
        lastAutoBoundsKeyRef.current = ""
        setBoundsRefreshTick((prev) => prev + 1)
      }
    }

    const handleDataRefresh = () => syncFromStorage(true)
    const handleFiltersChanged = () => syncFromStorage(false)
    const handleStorage = (event: StorageEvent) => {
      if (
        event.key === "dashboard_from_date" ||
        event.key === "dashboard_to_date" ||
        event.key === "dashboard_mode" ||
        event.key === "job_id" ||
        event.key === "use_job_filter" ||
        event.key === "dashboard_data_refresh_at"
      ) {
        syncFromStorage(event.key === "dashboard_data_refresh_at")
      }
    }

    window.addEventListener("dashboard-data-refreshed", handleDataRefresh as EventListener)
    window.addEventListener("dashboard-filters-changed", handleFiltersChanged as EventListener)
    window.addEventListener("dashboard-context-changed", handleFiltersChanged as EventListener)
    window.addEventListener("storage", handleStorage)
    return () => {
      window.removeEventListener("dashboard-data-refreshed", handleDataRefresh as EventListener)
      window.removeEventListener("dashboard-filters-changed", handleFiltersChanged as EventListener)
      window.removeEventListener("dashboard-context-changed", handleFiltersChanged as EventListener)
      window.removeEventListener("storage", handleStorage)
    }
  }, [todayIso, clampToToday])

  useEffect(() => {
    const activePartners = selectedPartners.map((value) => value.trim()).filter(Boolean)
    const boundsSelectionKey = `${datasetType}|${jobId.trim()}|${[...activePartners].sort().join(",")}`
    if (!activePartners.length) {
      setMinDate("")
      setMaxDate("")
      setDefaultFromDate("")
      setDefaultToDate("")
      lastAutoBoundsKeyRef.current = ""
      return
    }

    let cancelled = false
    Promise.all(
      activePartners.map(async (source) => {
        try {
          const res = await fetchDateBounds({
            source,
            dataset_type: datasetType,
            job_id: jobId.trim() || undefined,
          })
          const min = clampToToday(String(res?.min_date || "").trim())
          const max = clampToToday(String(res?.max_date || "").trim())
          return min || max ? { min, max } : null
        } catch {
          return null
        }
      })
    ).then((boundsList) => {
      if (cancelled) return
      const valid = boundsList.filter((item): item is { min: string; max: string } => Boolean(item))
      if (!valid.length) {
        setMinDate("")
        setMaxDate("")
        setDefaultFromDate("")
        setDefaultToDate("")
        return
      }

      const mins = valid.map((item) => item.min).filter(Boolean)
      const maxs = valid.map((item) => item.max).filter(Boolean)
      const unionMin = mins.length ? mins.reduce((acc, cur) => (cur < acc ? cur : acc)) : ""
      const overlapMin = mins.length ? mins.reduce((acc, cur) => (cur > acc ? cur : acc)) : ""
      const overlapMax = maxs.length ? maxs.reduce((acc, cur) => (cur < acc ? cur : acc)) : ""
      const hasOverlap = Boolean(overlapMin && overlapMax && overlapMin <= overlapMax)
      const nextTo = todayIso
      const candidateFrom = hasOverlap ? overlapMin : unionMin
      const nextFrom = candidateFrom && candidateFrom <= nextTo ? candidateFrom : nextTo

      setMinDate(nextFrom)
      setMaxDate(nextTo)
      setDefaultFromDate(nextFrom)
      setDefaultToDate(nextTo)

      const isSelectionChanged = lastAutoBoundsKeyRef.current !== boundsSelectionKey
      if (isSelectionChanged) {
        applyDateRange(nextFrom, nextTo)
        lastAutoBoundsKeyRef.current = boundsSelectionKey
        return
      }

      const currentFrom = fromDate && toDate && fromDate > toDate ? toDate : fromDate
      const currentTo = fromDate && toDate && fromDate > toDate ? fromDate : toDate
      const hasCurrentRange = Boolean(currentFrom && currentTo)
      const outOfBounds = !hasCurrentRange || currentFrom < nextFrom || currentTo > nextTo
      if (outOfBounds) {
        applyDateRange(nextFrom, nextTo)
      }
    })

    return () => {
      cancelled = true
    }
  }, [datasetType, jobId, selectedPartners, fromDate, toDate, todayIso, boundsRefreshTick, clampToToday, applyDateRange])

  const canDownload = useMemo(
    () => selectedPartners.length > 0 && !isDownloading && !isOpeningSlides,
    [selectedPartners, isDownloading, isOpeningSlides]
  )

  const togglePartner = (partnerKey: string) => {
    setSelectedPartners((prev) => {
      if (prev.includes(partnerKey)) {
        return prev.filter((key) => key !== partnerKey)
      }
      return [...prev, partnerKey]
    })
  }

  const handleDownload = async () => {
    if (!canDownload) return
    setIsDownloading(true)
    setError("")
    try {
      const { blob, filename } = await downloadDeckPptx({
        partners: selectedPartners,
        dataset_type: datasetType,
        job_id: jobId.trim() || undefined,
        from_date: fromDate || undefined,
        to_date: toDate || undefined,
        include_tables: includeTables,
        week_window: weekWindow,
      })

      const url = URL.createObjectURL(blob)
      const anchor = document.createElement("a")
      anchor.href = url
      anchor.download = filename
      document.body.appendChild(anchor)
      anchor.click()
      anchor.remove()
      URL.revokeObjectURL(url)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to download deck.")
    } finally {
      setIsDownloading(false)
    }
  }

  const handleOpenGoogleSlides = async () => {
    if (!canDownload) return
    const googleClientId = String(process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID || "").trim()
    if (!googleClientId) {
      setError("Google Slides integration is not configured. Set NEXT_PUBLIC_GOOGLE_CLIENT_ID and redeploy.")
      return
    }

    setIsOpeningSlides(true)
    setError("")
    try {
      const { blob, filename } = await downloadDeckPptx({
        partners: selectedPartners,
        dataset_type: datasetType,
        job_id: jobId.trim() || undefined,
        from_date: fromDate || undefined,
        to_date: toDate || undefined,
        include_tables: includeTables,
        week_window: weekWindow,
      })

      const token = await getGoogleDriveAccessToken(googleClientId)
      const upload = await uploadPptxAsGoogleSlides({
        accessToken: token,
        blob,
        filename,
      })
      const editUrl = buildGoogleSlidesEditUrl(upload.id)
      if (!editUrl) {
        throw new Error("Google Slides link could not be generated.")
      }
      const opened = window.open(editUrl, "_blank", "noopener,noreferrer")
      if (!opened) {
        window.location.href = editUrl
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to open deck in Google Slides.")
    } finally {
      setIsOpeningSlides(false)
    }
  }

  return (
    <div className="h-[100dvh] overflow-hidden bg-[#eef2f8] text-slate-900">
      <div className="mx-auto flex h-full w-full max-w-[1520px] flex-col overflow-hidden px-3 py-3 sm:px-4 sm:py-4">
        <div className="mb-3 flex items-center justify-between rounded-2xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
          <div>
            <h1 className="text-lg font-bold text-slate-900 sm:text-xl">Deck Studio Full Preview</h1>
            <p className="text-xs text-slate-500">Edit in real time and download the exact PPT output.</p>
          </div>
          <Link
            href="/"
            className="inline-flex items-center gap-2 rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50"
          >
            <ArrowLeft size={14} />
            Back
          </Link>
        </div>

        <div className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-2xl border border-slate-200 bg-[#eef2f8] shadow-sm lg:flex-row">
          <section className="custom-scrollbar min-h-0 flex-1 overflow-y-auto p-4 sm:p-6">
            <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
              <h4 className="mb-3 text-sm font-bold text-slate-800">Deck Of All Partners Separately</h4>
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                {PARTNERS.map((partner) => {
                  const active = selectedPartners.includes(partner.key)
                  return (
                    <label
                      key={partner.key}
                      className={`flex cursor-pointer items-center gap-3 rounded-xl border p-3 transition ${
                        active ? "border-[#1f6fe5] bg-[#eaf2ff]" : "border-slate-200 bg-white hover:bg-slate-50"
                      }`}
                    >
                      <input
                        type="checkbox"
                        checked={active}
                        onChange={() => togglePartner(partner.key)}
                        className="h-4 w-4 rounded border-slate-300 text-[#1f6fe5] focus:ring-[#1f6fe5]"
                      />
                      <Image src={partner.logo} alt={partner.label} width={58} height={22} className="h-5 w-14 object-contain" />
                      <span className="text-sm font-medium text-slate-700">{partner.label}</span>
                    </label>
                  )
                })}
              </div>
            </div>

            <div className="mt-4 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
              <h4 className="mb-3 text-sm font-bold text-slate-800">Live Slide Preview</h4>
              <DeckSlidesPreview
                selectedPartners={selectedPartners}
                datasetType={datasetType}
                jobId={jobId.trim() || undefined}
                fromDate={fromDate || undefined}
                toDate={toDate || undefined}
                weekWindow={weekWindow}
              />
            </div>

            <div className="mt-4 flex flex-wrap items-center gap-3">
              <button
                type="button"
                disabled={!canDownload}
                onClick={handleDownload}
                className="inline-flex items-center gap-2 rounded-xl bg-[#1f6fe5] px-4 py-2 text-sm font-semibold text-white transition hover:bg-[#165dcc] disabled:cursor-not-allowed disabled:bg-slate-300"
              >
                {isDownloading ? <Loader2 size={15} className="animate-spin" /> : <Download size={15} />}
                {isDownloading ? "Generating .pptx..." : "Download Partner Deck (.pptx)"}
              </button>
              <button
                type="button"
                disabled={!canDownload}
                onClick={handleOpenGoogleSlides}
                className="inline-flex items-center gap-2 rounded-xl border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-slate-700 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {isOpeningSlides ? <Loader2 size={15} className="animate-spin" /> : <ExternalLink size={15} />}
                {isOpeningSlides ? "Uploading to Google Slides..." : "Open in Google Slides"}
              </button>
              {error ? <span className="text-xs font-medium text-rose-600">{error}</span> : null}
            </div>
          </section>

          <aside className="w-full border-t border-slate-200 bg-white lg:w-[320px] lg:border-l lg:border-t-0">
            <div className="sticky top-0 space-y-3 p-4">
              <h4 className="text-sm font-bold text-slate-800">Deck Filters</h4>

              <label className="block text-xs font-semibold uppercase tracking-[0.1em] text-slate-500">
                Dataset
                <select
                  value={datasetType}
                  onChange={(event) => setDatasetType(event.target.value === "claims" ? "claims" : "sales")}
                  className="mt-1.5 w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 outline-none focus:border-[#1f6fe5]"
                >
                  <option value="sales">Sales</option>
                  <option value="claims">Claims</option>
                </select>
              </label>

              <label className="block text-xs font-semibold uppercase tracking-[0.1em] text-slate-500">
                Week Window
                <select
                  value={weekWindow}
                  onChange={(event) => {
                    const next = Number(event.target.value)
                    if (next === 2 || next === 3 || next === 4 || next === 6) {
                      setWeekWindow(next)
                    }
                  }}
                  className="mt-1.5 w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 outline-none focus:border-[#1f6fe5]"
                >
                  <option value={2}>2 weeks</option>
                  <option value={3}>3 weeks</option>
                  <option value={4}>4 weeks</option>
                  <option value={6}>6 weeks</option>
                </select>
              </label>

              <label className="block text-xs font-semibold uppercase tracking-[0.1em] text-slate-500">
                Job Tag (Optional)
                <input
                  type="text"
                  value={jobId}
                  onChange={(event) => setJobId(event.target.value)}
                  placeholder="e.g. Jan-26"
                  className="mt-1.5 w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 outline-none focus:border-[#1f6fe5]"
                />
              </label>

              <div className="space-y-1">
                <p className="text-xs font-semibold uppercase tracking-[0.1em] text-slate-500">Date Range</p>
                <DateRangePicker
                  compact
                  align="right"
                  draftFromDate={draftFromDate}
                  draftToDate={draftToDate}
                  minDate={minDate || undefined}
                  maxDate={maxDate || undefined}
                  onDraftChange={(nextFrom, nextTo) => {
                    setDraftFromDate(nextFrom)
                    setDraftToDate(nextTo)
                  }}
                  onApply={(nextFrom, nextTo) => applyDateRange(nextFrom, nextTo)}
                  onReset={() => applyDateRange(defaultFromDate, defaultToDate)}
                />
              </div>

              <label className="flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
                <input
                  type="checkbox"
                  checked={includeTables}
                  onChange={(event) => setIncludeTables(event.target.checked)}
                  className="h-4 w-4 rounded border-slate-300 text-[#1f6fe5] focus:ring-[#1f6fe5]"
                />
                Include related tables in slides
              </label>
            </div>
          </aside>
        </div>
      </div>
    </div>
  )
}
