"use client"

import MultiGraphView from "@/components/MultiGraphView"

type Props = {
  source: string
  datasetType: "sales" | "claims"
  jobId?: string | null
  primaryColor?: string
  secondaryColor?: string
  fromDate?: string
  toDate?: string
  resetFromDate?: string
  resetToDate?: string
  onDateRangeApply?: (fromDate: string, toDate: string) => void
}

export default function GraphSection({
  source,
  datasetType,
  jobId,
  fromDate,
  toDate,
  resetFromDate,
  resetToDate,
  onDateRangeApply,
}: Props) {
  return (
    <MultiGraphView
      source={source}
      datasetType={datasetType}
      jobId={jobId}
      fromDate={fromDate}
      toDate={toDate}
      resetFromDate={resetFromDate}
      resetToDate={resetToDate}
      onDateRangeApply={onDateRangeApply}
    />
  )
}
