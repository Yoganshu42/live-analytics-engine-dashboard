export const GRAPH_PRESETS = {
  earned_premium: {
    title: "Earned Premium Analysis",
    group: "time",
    dimension: "month", // ✅ FIXED
    metrics: [
      "gross_premium",
      "earned_premium",
      "zopper_earned_premium",
      "net_claims",
      "loss_ratio",
      "quantity",
    ],
    bucket: "month",
  },

  state_performance: {
    title: "State-wise Performance",
    group: "region",
    dimension: "state",
    metrics: [
      "gross_premium",
      "earned_premium",
      "zopper_earned_premium",
      "net_claims",
      "loss_ratio",
      "quantity",
    ],
  },

  plan_category: {
    title: "Plan Category Performance",
    group: "category",
    dimension: "plan_category",
    metrics: [
      "gross_premium",
      "earned_premium",
      "zopper_earned_premium",
      "net_claims",
      "loss_ratio",
      "quantity",
    ],
  },

  device_plan_category: {
    title: "Device Plan Category Performance",
    group: "device_category",
    dimension: "device_plan_category",
    metrics: [
      "gross_premium",
      "earned_premium",
      "zopper_earned_premium",
      "net_claims",
      "loss_ratio",
      "quantity",
    ],
  },
} as const
