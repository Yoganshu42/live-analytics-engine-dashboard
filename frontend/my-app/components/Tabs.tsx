"use client"

type Props = {
  value: "sales" | "claims"
  onChange: (v: "sales" | "claims") => void
  disableClaims?: boolean
}

export default function Tabs({ value, onChange, disableClaims }: Props) {
  return (
    <div className="flex gap-2 border-b mb-6">
      {["sales", "claims"].map(tab => (
        // Claims tab can be disabled for sources that don't have claims data.
        <button
          key={tab}
          onClick={() => onChange(tab as "sales" | "claims")}
          disabled={tab === "claims" && Boolean(disableClaims)}
          className={`px-4 py-2 text-sm font-semibold border-b-2
            ${value === tab
              ? "border-indigo-600 text-indigo-600"
              : "border-transparent text-gray-500 hover:text-gray-800"}
            ${tab === "claims" && disableClaims ? "opacity-40 cursor-not-allowed hover:text-gray-500" : ""}
          `}
        >
          {tab.toUpperCase()} ANALYSIS
        </button>
      ))}
    </div>
  )
}
