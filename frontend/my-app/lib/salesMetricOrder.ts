export type SalesMetricKey =
  | "gross_premium"
  | "earned_premium"
  | "zopper_earned_premium"
  | "quantity"

export const SALES_METRIC_ORDER: SalesMetricKey[] = [
  "gross_premium",
  "earned_premium",
  "zopper_earned_premium",
  "quantity",
]
