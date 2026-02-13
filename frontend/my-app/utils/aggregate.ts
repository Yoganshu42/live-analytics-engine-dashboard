type Row = Record<string, unknown>

export function aggregateBy(
  data: Row[],
  dimension: string,
  measure: string
) {
  const map: Record<string, number> = {}

  data.forEach((row) => {
    const key = row[dimension]
    const value = Number(row[measure])

    if (key == null || isNaN(value)) return

    const keyString = String(key)
    map[keyString] = (map[keyString] || 0) + value
  })

  return Object.entries(map).map(([key, value]) => ({
    [dimension]: key,
    [measure]: value,
  }))
}
