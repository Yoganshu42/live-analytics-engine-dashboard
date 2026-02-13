"use client"

type Props = {
  columns: string[]
  selectedColumns: string[]
  onChange: (cols: string[]) => void
}

/* ---------- helpers ---------- */
const DIMENSION_KEYWORDS = [
  "type",
  "brand",
  "state",
  "category",
  "plan",
]

const isDimension = (col: string) =>
  DIMENSION_KEYWORDS.some(k =>
    col.toLowerCase().includes(k)
  )

export default function ColumnSelector({
  columns,
  selectedColumns,
  onChange,
}: Props) {
  const toggle = (c: string) => {
    if (selectedColumns.includes(c)) {
      onChange(selectedColumns.filter(x => x !== c))
    } else {
      onChange([...selectedColumns, c])
    }
  }

  if (columns.length === 0) {
    return (
      <div className="text-sm text-neutral-500">
        No columns detected yet.
      </div>
    )
  }

  return (
    <div className="bg-white rounded-lg border p-4 space-y-3">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold text-gray-800">
          Select Columns
        </h3>
        <span className="text-xs text-gray-500">
          {selectedColumns.length} selected
        </span>
      </div>

      <div className="grid grid-cols-2 gap-2 max-h-56 overflow-y-auto pr-1">
        {columns.map((c) => {
          const dimension = isDimension(c)
          const checked = selectedColumns.includes(c)

          return (
            <label
              key={c}
              className={`
                flex items-center gap-2 text-sm cursor-pointer px-2 py-1 rounded
                hover:bg-gray-50
                ${checked ? "bg-blue-50" : ""}
              `}
            >
              <input
                type="checkbox"
                className="accent-blue-600"
                checked={checked}
                onChange={() => toggle(c)}
              />

              <span className="truncate flex-1">{c}</span>

              {dimension && (
                <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-100 text-emerald-700 font-medium">
                  Dimension
                </span>
              )}
            </label>
          )
        })}
      </div>
    </div>
  )
}
