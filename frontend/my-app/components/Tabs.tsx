"use client"

type Props = {
  value: "sales" | "claims"
  onChange: (v: "sales" | "claims") => void
  disableClaims?: boolean
}

export default function Tabs({ value, onChange, disableClaims }: Props) {
  const tabOptions: Array<{
    key: "sales" | "claims"
    mobileLabel: string
    desktopLabel: string
  }> = [
    { key: "sales", mobileLabel: "Sales", desktopLabel: "SALES ANALYSIS" },
    { key: "claims", mobileLabel: "Claims", desktopLabel: "CLAIMS ANALYSIS" },
  ]

  return (
    <div className="mb-5 flex flex-wrap gap-2 border-b border-slate-200 pb-1">
      {tabOptions.map((tab) => (
        // Claims tab can be disabled for sources that don't have claims data.
        <button
          key={tab.key}
          onClick={() => onChange(tab.key)}
          disabled={tab.key === "claims" && Boolean(disableClaims)}
          className={`rounded-t-md border-b-2 px-3 py-2 text-xs font-semibold sm:px-4 sm:text-sm
            ${value === tab.key
              ? "border-indigo-600 text-indigo-600"
              : "border-transparent text-gray-500 hover:text-gray-800"}
            ${tab.key === "claims" && disableClaims ? "cursor-not-allowed opacity-40 hover:text-gray-500" : ""}
          `}
        >
          <span className="sm:hidden">{tab.mobileLabel}</span>
          <span className="hidden sm:inline">{tab.desktopLabel}</span>
        </button>
      ))}
    </div>
  )
}
