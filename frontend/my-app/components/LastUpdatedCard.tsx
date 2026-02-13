"use client"

import { useEffect, useState } from "react"
import { Calendar } from "lucide-react"
import { fetchLastUpdated } from "../app/lib/api"

type Props = {
  source: string
  datasetType: "sales" | "claims"
  jobId?: string
  fromDate?: string
  toDate?: string
}

type LastUpdated = {
  data_upto: string | null
}

const formatDate = (value: string | null) => {
  if (!value) return "Unknown"
  const d = new Date(value)
  if (isNaN(d.getTime())) return value
  return new Intl.DateTimeFormat("en-US", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  }).format(d)
}

export default function LastUpdatedCard({
  source,
  datasetType,
  jobId,
  fromDate,
  toDate,
}: Props) {
  const [data, setData] = useState<LastUpdated | null>(null)
  const [error, setError] = useState(false)

  useEffect(() => {
    let mounted = true

    fetchLastUpdated({
      job_id: jobId,
      source,
      dataset_type: datasetType,
      from_date: fromDate,
      to_date: toDate,
    })
      .then((res: LastUpdated) => {
        if (!mounted) return
        setError(false)
        setData(res)
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message.toLowerCase() : String(err).toLowerCase()
        const isAuthError = msg.includes("not authenticated") || msg.includes("invalid token")
        if (!isAuthError) {
          console.error("Last updated fetch failed:", err)
        }
        if (mounted) {
          setError(true)
          setData(null)
        }
      })

    return () => {
      mounted = false
    }
  }, [jobId, source, datasetType, fromDate, toDate])

  return (
    <div className="bg-white p-7 rounded-[24px] border border-slate-200 shadow-sm relative overflow-hidden group">
      <div className="absolute top-0 right-0 p-4 opacity-10 group-hover:scale-110 transition-transform">
        <Calendar size={80} />
      </div>
      <div className="flex gap-3 mb-6 text-slate-400 items-center">
        <div className="p-2 rounded-lg bg-slate-50">
          <Calendar size={18} className="text-slate-600" />
        </div>
        <span className="text-[10px] font-black uppercase tracking-widest">
          Data Update
        </span>
      </div>

      {error || !data ? (
        <div className="text-sm text-slate-400">Update unavailable</div>
      ) : (
        <>
          <div className="text-xs font-bold uppercase text-slate-400">
          Last Start Date
          </div>
          <div className="text-3xl font-black text-slate-900">
            {formatDate(data.data_upto)}
          </div>
          <div className="mt-3 text-[10px] uppercase tracking-widest text-emerald-500 font-black">
            Live
          </div>
        </>
      )}
    </div>
  )
}
