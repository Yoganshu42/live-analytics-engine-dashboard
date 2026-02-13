export type ColumnProfile = {
  name: string
  type: "numeric" | "categorical"
  isAmount: boolean
  isPercentage: boolean
  uniqueCount: number
}

type DataRow = Record<string, unknown>

export function profileColumns(data: DataRow[]): ColumnProfile[] {
  if (!data || data.length === 0) return []

  const sample = data.slice(0, 50)
  const columns = Object.keys(sample[0])

  return columns.map((col) => {
    const values = sample
      .map((row) => row[col])
      .filter((v) => v !== null && v !== undefined)

    const numericCount = values.filter(
      (v) => typeof v === "number" && !isNaN(v)
    ).length

    const uniqueCount = new Set(values).size

    return {
      name: col,
      type: numericCount > values.length * 0.7 ? "numeric" : "categorical",
      isAmount: /amount|premium|value|net/i.test(col),
      isPercentage: /share|percent|%/i.test(col),
      uniqueCount,
    }
  })
}
