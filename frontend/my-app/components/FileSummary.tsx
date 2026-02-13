"use client"

type Props = {
  rows?: number
  columns?: number
  nulls?: number
}

export default function FileSummary({
  rows,
  columns,
  nulls,
}: Props) {
  return (
    <div className="mt-4 bg-white rounded-xl border p-4 shadow-sm">
      <h3 className="text-sm font-semibold text-gray-800 mb-3">
        File Summary
      </h3>

      <div className="space-y-2 text-sm">
        <div className="flex justify-between">
          <span className="text-gray-500">Rows</span>
          <span className="font-medium">
            {rows ?? "—"}
          </span>
        </div>

        <div className="flex justify-between">
          <span className="text-gray-500">Columns</span>
          <span className="font-medium">
            {columns ?? "—"}
          </span>
        </div>

        <div className="flex justify-between">
          <span className="text-gray-500">Null Values</span>
          <span className="font-medium">
            {nulls ?? "—"}
          </span>
        </div>
      </div>
    </div>
  )
}
