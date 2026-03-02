"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import Image from "next/image"
import { useRouter } from "next/navigation"
import { Download, ExternalLink, FileChartColumnIncreasing, Loader2, Maximize2, X } from "lucide-react"

import { downloadDeckPptx, fetchDateBounds } from "@/app/lib/api"
import { buildGoogleSlidesEditUrl, getGoogleDriveAccessToken, uploadPptxAsGoogleSlides } from "@/app/lib/googleSlides"
import DateRangePicker from "@/components/DateRangePicker"
import DeckSlidesPreview from "@/components/DeckSlidesPreview"

type DatasetType = "sales" | "claims"

type Props = {
  collapsed?: boolean
}

type PartnerOption = {
  key: string
  label: string
  logo: string
  cardLabel: string
}

type WeekWindow = 2 | 3 | 4 | 6

const PARTNERS: PartnerOption[] = [
  { key: "samsung_vs", label: "Samsung Vijay Sales", logo: "/vs_logo.jpg", cardLabel: "Samsung VS" },
  { key: "samsung_croma", label: "Samsung Croma", logo: "/croma_logo.jpg", cardLabel: "Samsung Croma" },
  { key: "reliance", label: "Reliance ResQ", logo: "/resq.png", cardLabel: "Reliance ResQ" },
  { key: "godrej", label: "Godrej", logo: "/Group 1244833444.png", cardLabel: "Godrej" },
]

const cardButtonClass =
  "flex w-full items-center gap-2 rounded-lg px-2.5 py-2 text-left text-[13px] font-medium transition-colors sm:text-sm text-slate-700 hover:bg-slate-100"

export default function DeckStudioAccess({ collapsed = false }: Props) {
  const router = useRouter()
  const [open, setOpen] = useState(false)
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
  const lastAutoBoundsKeyRef = useRef("")
  const focusedPartner = selectedPartners.length === 1 ? selectedPartners[0] : null

  const canDownload = useMemo(
    () => selectedPartners.length > 0 && !isDownloading && !isOpeningSlides,
    [selectedPartners, isDownloading, isOpeningSlides]
  )

  const applyDateRange = (nextFrom: string, nextTo: string) => {
    const normalizedFrom = nextFrom || ""
    const normalizedTo = nextTo || ""
    if (normalizedFrom && normalizedTo && normalizedFrom > normalizedTo) {
      setFromDate(normalizedTo)
      setToDate(normalizedFrom)
      setDraftFromDate(normalizedTo)
      setDraftToDate(normalizedFrom)
      return
    }
    setFromDate(normalizedFrom)
    setToDate(normalizedTo)
    setDraftFromDate(normalizedFrom)
    setDraftToDate(normalizedTo)
  }

  useEffect(() => {
    if (typeof window === "undefined") return
    const storedFrom = localStorage.getItem("dashboard_from_date") || ""
    const storedTo = localStorage.getItem("dashboard_to_date") || ""
    if (storedFrom && storedTo && storedFrom > storedTo) {
      setFromDate(storedTo)
      setToDate(storedFrom)
      setDraftFromDate(storedTo)
      setDraftToDate(storedFrom)
      return
    }
    setFromDate(storedFrom)
    setToDate(storedTo)
    setDraftFromDate(storedFrom)
    setDraftToDate(storedTo)
  }, [])

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
          const min = String(res?.min_date || "").trim()
          const max = String(res?.max_date || "").trim()
          return min && max ? { min, max } : null
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

      const mins = valid.map((item) => item.min)
      const maxs = valid.map((item) => item.max)
      const unionMin = mins.reduce((acc, cur) => (cur < acc ? cur : acc))
      const unionMax = maxs.reduce((acc, cur) => (cur > acc ? cur : acc))
      const overlapMin = mins.reduce((acc, cur) => (cur > acc ? cur : acc))
      const overlapMax = maxs.reduce((acc, cur) => (cur < acc ? cur : acc))
      const hasOverlap = overlapMin <= overlapMax
      const nextFrom = hasOverlap ? overlapMin : unionMin
      const nextTo = hasOverlap ? overlapMax : unionMax

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
  }, [datasetType, jobId, selectedPartners])

  const togglePartner = (partnerKey: string) => {
    setSelectedPartners((prev) => {
      if (prev.includes(partnerKey)) {
        return prev.filter((key) => key !== partnerKey)
      }
      return [...prev, partnerKey]
    })
  }

  const openStudio = (partners?: string[]) => {
    if (partners && partners.length > 0) {
      lastAutoBoundsKeyRef.current = ""
      setSelectedPartners(partners)
    }
    setOpen(true)
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

  const openFullPreviewPage = () => {
    const query = new URLSearchParams()
    if (selectedPartners.length) query.set("partners", selectedPartners.join(","))
    query.set("dataset_type", datasetType)
    query.set("week_window", String(weekWindow))
    if (jobId.trim()) query.set("job_id", jobId.trim())
    if (fromDate) query.set("from_date", fromDate)
    if (toDate) query.set("to_date", toDate)
    if (includeTables) query.set("include_tables", "1")
    router.push(`/deck-studio?${query.toString()}`)
  }

  const renderFilterPanel = (showCloseStudio: boolean) => (
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

      <button
        type="button"
        onClick={openFullPreviewPage}
        className="inline-flex w-full items-center justify-center gap-2 rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm font-semibold text-slate-700 transition hover:bg-slate-50"
      >
        <Maximize2 size={14} />
        Open Full Preview Page
      </button>

      {showCloseStudio ? (
        <button
          type="button"
          onClick={() => setOpen(false)}
          className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm font-semibold text-slate-700 transition hover:bg-slate-50"
        >
          Close Studio
        </button>
      ) : null}
    </div>
  )

  return (
    <>
      <div className={`rounded-2xl border border-slate-200 bg-white p-2.5 shadow-sm sm:p-3 ${collapsed ? "md:px-1.5" : ""}`}>
        <div className={`px-2 pb-2 text-[10px] font-black uppercase tracking-[0.18em] text-slate-400 ${collapsed ? "md:hidden" : ""}`}>
          Deck Studio
        </div>

        <button
          type="button"
          title="Partner Decks (.pptx)"
          onClick={() => openStudio()}
          className={`${cardButtonClass} ${collapsed ? "md:justify-center md:px-2" : ""}`}
        >
          <FileChartColumnIncreasing size={16} />
          <span className={collapsed ? "md:hidden" : ""}>Partner Decks (.pptx)</span>
        </button>

        {!collapsed ? (
          <div className="mt-2 grid grid-cols-2 gap-2">
            {PARTNERS.map((partner) => {
              const active = focusedPartner === partner.key
              return (
                <button
                  key={partner.key}
                  type="button"
                  onClick={() => openStudio([partner.key])}
                  className={`flex items-center gap-1.5 rounded-lg border px-2 py-2 text-left text-[11px] font-semibold transition ${
                    active
                      ? "border-[#1f6fe5] bg-[#eaf2ff] text-[#1f5cc8]"
                      : "border-slate-200 bg-white text-slate-600 hover:bg-slate-50"
                  }`}
                  title={`Open ${partner.label} deck`}
                >
                  <Image src={partner.logo} alt={partner.label} width={34} height={14} className="h-4 w-8 object-contain" />
                  <span className="min-w-0 truncate">{partner.cardLabel}</span>
                </button>
              )
            })}
          </div>
        ) : null}
      </div>

      {open && (
        <div
          className="fixed inset-0 z-[260] bg-slate-900/40 backdrop-blur-[1px]"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) {
              setOpen(false)
            }
          }}
        >
          <div
            className="relative mx-auto mt-4 h-[calc(100vh-2rem)] w-[95vw] max-w-[1180px] rounded-2xl border border-slate-200 bg-[#eef2f8] shadow-2xl"
            onMouseDown={(event) => event.stopPropagation()}
          >
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="absolute right-4 top-4 z-10 rounded-lg border border-slate-300 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 shadow-sm transition hover:bg-slate-50"
            >
              Close Studio
            </button>
            <div className="flex h-full flex-col overflow-hidden lg:flex-row">
              <section className="custom-scrollbar min-h-0 flex-1 overflow-y-auto p-4 sm:p-6">
                <div className="mb-4 flex items-start justify-between gap-3">
                  <div>
                    <h3 className="text-xl font-bold text-slate-900">Partner Deck Studio</h3>
                    <p className="text-xs text-slate-500">
                      Live slide preview + download deck. Metrics are fixed to Gross Premium and Quantity.
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={() => setOpen(false)}
                    className="rounded-full border border-slate-200 bg-white p-2 text-slate-500 hover:bg-slate-50"
                    aria-label="Close deck studio"
                  >
                    <X size={16} />
                  </button>
                </div>

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
                  <h4 className="mb-3 text-sm font-bold text-slate-800">What The Deck Generates</h4>
                  <ul className="space-y-1 text-sm text-slate-600">
                    <li>{datasetType === "claims" ? "Claims and Quantity only" : "Gross Premium and Quantity only"} (no Earned Premium or Zopper Earned in deck).</li>
                    <li>Monthly and Samsung week-wise trend pages with selectable 2/3/4/6 recent-week window.</li>
                    <li>Drilldowns across state, city, category/channel/brand and Samsung product model (A17, Fold 6, etc.).</li>
                    <li>AI trend card: recent vs previous period, overall trend, top and bottom contributors.</li>
                    <li>Optional data table below chart blocks for quick value reference.</li>
                  </ul>
                </div>

                <div className="mt-4 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                  <div className="mb-3 flex items-center justify-between gap-2">
                    <h4 className="text-sm font-bold text-slate-800">Live Slide Preview</h4>
                    <button
                      type="button"
                      onClick={openFullPreviewPage}
                      className="inline-flex items-center gap-1 rounded-lg border border-slate-300 bg-white px-2 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-50"
                    >
                      <Maximize2 size={13} />
                      Open Full Page
                    </button>
                  </div>
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
                {renderFilterPanel(true)}
              </aside>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
