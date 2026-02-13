import { ColumnProfile } from "@/utils/columnProfiler"

export type GraphDecision = {
  graphType: "bar" | "line" | "distribution"
  xKey: string
  yKey: string
  title: string
  description: string
}

export function decideGraph(
  profiles: ColumnProfile[]
): GraphDecision | null {
  const numeric = profiles.filter((p) => p.type === "numeric")
  const categorical = profiles.filter((p) => p.type === "categorical")

  // Amount distribution
  const amount = numeric.find((p) => p.isAmount)
  if (amount) {
    return {
      graphType: "distribution",
      xKey: "index",
      yKey: amount.name,
      title: `${amount.name} Distribution`,
      description:
        "Shows how values are spread across records to identify spikes or outliers.",
    }
  }

  // Percentage trend
  const percent = numeric.find((p) => p.isPercentage)
  if (percent) {
    return {
      graphType: "line",
      xKey: "index",
      yKey: percent.name,
      title: `${percent.name} Trend`,
      description:
        "Shows how the percentage varies across records.",
    }
  }

  // Category comparison
  if (categorical.length > 0 && numeric.length > 0) {
    return {
      graphType: "bar",
      xKey: categorical[0].name,
      yKey: numeric[0].name,
      title: `${numeric[0].name} by ${categorical[0].name}`,
      description:
        "Compares numeric values across different categories.",
    }
  }

  return null
}
