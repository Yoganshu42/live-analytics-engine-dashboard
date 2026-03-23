export type SamsungPartnerKey =
  | "samsung_vs"
  | "samsung_croma"
  | "samsung_croma_dsdsg"
  | "samsung_reliance_digital"

export type SamsungPartnerConfig = {
  key: SamsungPartnerKey
  label: string
  shortLabel: string
  cardLabel: string
  logo: string
  color: string
}

export const SAMSUNG_PARTNERS: readonly SamsungPartnerConfig[] = [
  {
    key: "samsung_vs",
    label: "Samsung Vijay Sales",
    shortLabel: "Vijay Sales",
    cardLabel: "Samsung VS",
    logo: "/vs_logo.jpg",
    color: "#2563eb",
  },
  {
    key: "samsung_croma",
    label: "Samsung Croma",
    shortLabel: "Croma",
    cardLabel: "Croma",
    logo: "/croma_logo.jpg",
    color: "#0ea5a4",
  },
  {
    key: "samsung_croma_dsdsg",
    label: "Samsung Croma DSDSG",
    shortLabel: "Croma DSDSG",
    cardLabel: "Croma DSDSG",
    logo: "/croma_logo.jpg",
    color: "#14b8a6",
  },
  {
    key: "samsung_reliance_digital",
    label: "Samsung Reliance Digital",
    shortLabel: "Reliance Digital",
    cardLabel: "Reliance Digital",
    logo: "/reliance_digital_logo.png",
    color: "#ef4444",
  },
] as const

export const VISIBLE_SAMSUNG_PARTNERS = SAMSUNG_PARTNERS.filter(
  (partner) => partner.key !== "samsung_croma_dsdsg"
)

export const SAMSUNG_PARTNER_KEYS: readonly SamsungPartnerKey[] = [
  "samsung_vs",
  "samsung_croma",
  "samsung_croma_dsdsg",
  "samsung_reliance_digital",
]

export const normalizeSamsungSource = (value: string) => {
  const key = (value || "").trim().toLowerCase()
  if (key === "samsung" || key === "samsung_vijay_sales") return key === "samsung_vijay_sales" ? "samsung_vs" : "samsung"
  if (key === "samsung_vs" || key === "samsung vs" || key === "samsung vijay sales" || key === "vijay sales") {
    return "samsung_vs"
  }
  if (
    key === "samsung_croma"
    || key === "samsung croma"
    || key === "croma"
    || key === "samsung protect max"
    || key === "samsung protect max croma"
    || key === "protect max"
    || key === "protect max croma"
    || key === "croma protect max"
    || key === "samsung_croma_dsdsg"
    || key === "samsung croma dsdsg"
    || key === "samsung croma ds dsg"
    || key === "samsung_croma_ds_dsg"
    || key === "croma ds dsg"
    || key === "croma ds/dsg"
    || key === "dsdsg"
    || key === "ds dsg"
    || key === "ds/dsg"
    || key === "ds-dsg"
  ) {
    return "samsung_croma"
  }
  if (
    key === "samsung_reliance_digital"
    || key === "samsung reliance digital"
    || key === "samsungreliancedigital"
    || key === "reliance digital"
    || key === "reliance_digital"
    || key === "reliance-digital"
    || key === "reliancedigital"
  ) {
    return "samsung_reliance_digital"
  }
  return key
}

export const isSamsungPartnerSource = (value: string) =>
  SAMSUNG_PARTNER_KEYS.includes(normalizeSamsungSource(value) as SamsungPartnerKey)

export const isSamsungSource = (value: string) => {
  const normalized = normalizeSamsungSource(value)
  return normalized === "samsung" || SAMSUNG_PARTNER_KEYS.includes(normalized as SamsungPartnerKey)
}

export const getSamsungPartnerValue = (row: Record<string, unknown>, key: SamsungPartnerKey) => {
  const value = Number(row[key] ?? 0)
  return Number.isFinite(value) ? value : 0
}

export const sumSamsungPartnerValues = (row: Record<string, unknown>) =>
  SAMSUNG_PARTNERS.reduce((sum, partner) => sum + getSamsungPartnerValue(row, partner.key), 0)
