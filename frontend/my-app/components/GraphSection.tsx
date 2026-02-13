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
}

export default function GraphSection({
  source,
  datasetType,
  jobId,
  fromDate,
  toDate,
}: Props) {
  return (
    <MultiGraphView
      source={source}
      datasetType={datasetType}
      jobId={jobId}
      fromDate={fromDate}
      toDate={toDate}
    />
  )
}
