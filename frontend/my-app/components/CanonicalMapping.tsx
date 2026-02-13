"use client"

type Mapping = {
  rawColumn: string
  canonicalField: string
}

type Props = {
  rawColumn: string
  suggestedField: string | null
  onChange: (mapping: Mapping) => void
}

const CANONICAL_FIELDS = [
  "gross_premium",
  "earned_premium",
  "claim_amount",
  "total_loss",
  "selling_price",
  "our_share_price",
  "gst_amount",
  "costing",
  "brand",
  "plan_category",
]

export default function CanonicalMapping({
  rawColumn,
  suggestedField,
  onChange,
}: Props) {
  return (
    <div className="bg-neutral-900 text-white rounded-lg p-4 border border-neutral-700 space-y-2">
      <h4 className="text-sm font-semibold">
        Confirm Business Mapping
      </h4>

      <div className="text-sm flex items-center justify-between">
        <span className="text-neutral-400">Raw Column</span>
        <span className="font-mono">{rawColumn}</span>
      </div>

      <div className="flex flex-col gap-1">
        <label className="text-xs text-neutral-400">
          Map to Canonical Field
        </label>

        <select
          defaultValue={suggestedField ?? ""}
          onChange={(e) =>
            onChange({
              rawColumn,
              canonicalField: e.target.value,
            })
          }
          className="bg-neutral-800 border border-neutral-700 rounded px-2 py-1 text-sm"
        >
          <option value="" disabled>
            Select field
          </option>

          {CANONICAL_FIELDS.map(f => (
            <option key={f} value={f}>
              {f.replace("_", " ").toUpperCase()}
            </option>
          ))}
        </select>
      </div>
    </div>
  )
}
