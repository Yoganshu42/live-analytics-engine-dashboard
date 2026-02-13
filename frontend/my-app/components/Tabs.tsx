"use client"

type Props = {
  value: "sales" | "claims"
  onChange: (v: "sales" | "claims") => void
}

export default function Tabs({ value, onChange }: Props) {
  return (
    <div className="flex gap-2 border-b mb-6">
      {["sales", "claims"].map(tab => (
        <button
          key={tab}
          onClick={() => onChange(tab as "sales" | "claims")}
          className={`px-4 py-2 text-sm font-semibold border-b-2
            ${value === tab
              ? "border-indigo-600 text-indigo-600"
              : "border-transparent text-gray-500 hover:text-gray-800"}
          `}
        >
          {tab.toUpperCase()} ANALYSIS
        </button>
      ))}
    </div>
  )
}
