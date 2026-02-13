"use client"

import { useEffect, useState } from "react"
import { fetchSummary } from "../app/lib/api"

type Props = {
  source: string
  datasetType: "sales" | "claims"
  color: string
  jobId?: string
  fromDate?: string
  toDate?: string
}

type Summary = {
  gross_premium?: number
  earned_premium?: number
  zopper_earned_premium?: number
  units_sold?: number
}

const money = (v: number) => {
  const abs = Math.abs(v)
  if (abs >= 1e7) return `Rs ${(v / 1e7).toFixed(2)} Cr`
  if (abs >= 1e5) return `Rs ${(v / 1e5).toFixed(2)} L`
  if (abs >= 1e3) return `Rs ${(v / 1e3).toFixed(1)} K`
  return `Rs ${new Intl.NumberFormat("en-IN").format(Math.round(v))}`
}

export default function ResultCard({
  source,
  datasetType,
  color,
  jobId,
  fromDate,
  toDate,
}: Props) {
  const [data, setData] = useState<Summary | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(false)

  useEffect(() => {
    let mounted = true
    setLoading(true)
    setError(false)

    const isZeroSummary = (summary: Summary) =>
      Number(summary.gross_premium || 0) === 0 &&
      Number(summary.earned_premium || 0) === 0 &&
      Number(summary.zopper_earned_premium || 0) === 0

    const load = async () => {
      try {
        let res = await fetchSummary({
          job_id: jobId,
          source,
          dataset_type: datasetType,
          from_date: fromDate,
          to_date: toDate,
        })

        // Match graph behavior: if filtered summary is empty, retry once without date bounds.
        if ((fromDate || toDate) && isZeroSummary(res)) {
          res = await fetchSummary({
            job_id: jobId,
            source,
            dataset_type: datasetType,
          })
        }

        if (!mounted) return
        setData(res)
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message.toLowerCase() : String(err).toLowerCase()
        const isAuthError = msg.includes("not authenticated") || msg.includes("invalid token")
        if (!isAuthError) {
          console.error("Summary fetch failed:", err)
        }
        if (mounted) {
          setError(true)
          setData(null)
        }
      } finally {
        if (mounted) setLoading(false)
      }
    }

    load()

    return () => {
      mounted = false
    }
  }, [jobId, source, datasetType, fromDate, toDate])

  if (loading) {
    return (
      <div className="bg-white p-6 rounded-2xl border text-sm text-gray-400">
        Loading summary...
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="bg-white p-6 rounded-2xl border text-sm text-gray-400">
        Summary unavailable
      </div>
    )
  }

  const gross = data.gross_premium ?? 0
  const earned = data.earned_premium ?? 0
  const zopper = data.zopper_earned_premium ?? 0
  const units = data.units_sold ?? 0

  if (datasetType === "claims") {
    return (
      <div className="bg-white p-6 rounded-2xl border space-y-4">
        <div>
          <div className="text-xs font-bold uppercase text-gray-400">
            Total Claims Cost
          </div>
          <div className="text-xl font-black">
            {money(gross)}
          </div>
        </div>

        <div>
          <div className="text-xs font-bold uppercase text-gray-400">
            Net Claims Cost Paid
          </div>
          <div
            className="text-2xl font-black"
            style={{ color }}
          >
            {money(earned)}
          </div>
        </div>

        <div className="pt-2 border-t text-xs text-gray-500">
          No. of Claims:{" "}
          <span className="font-semibold">
            {units.toLocaleString()}
          </span>
        </div>
      </div>
    )
  }

  return (
    <div className="bg-white p-6 rounded-2xl border space-y-4">
      <div>
        <div className="text-xs font-bold uppercase text-gray-400">
          Gross Premium
        </div>
        <div className="text-xl font-black">
          {money(gross)}
        </div>
      </div>

      <div>
        <div className="text-xs font-bold uppercase text-gray-400">
          Earned Premium
        </div>
        <div className="text-2xl font-black">
          {money(earned)}
        </div>
      </div>

      <div>
        <div className="text-xs font-bold uppercase text-gray-400">
          Zopper Earned
        </div>
        <div
          className="text-2xl font-black"
          style={{ color }}
        >
          {money(zopper)}
        </div>
      </div>

      <div className="pt-2 border-t text-xs text-gray-500">
        Units Sold :{" "}
        <span className="font-semibold">
          {units.toLocaleString()}
        </span>
      </div>
    </div>
  )
}
