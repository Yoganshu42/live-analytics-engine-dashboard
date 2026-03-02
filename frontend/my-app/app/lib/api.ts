type FetchSummaryParams = {
  job_id?: string
  source: string
  dataset_type: "sales" | "claims"
  from_date?: string
  to_date?: string
}

type FetchLastUpdatedParams = {
  job_id?: string
  source: string
  dataset_type: "sales" | "claims"
  from_date?: string
  to_date?: string
}

type FetchByDimensionParams = {
  job_id?: string
  source: string
  dataset_type: "sales" | "claims"
  dimension: string
  metric: string
  bucket?: "day" | "week" | "month"
  from_date?: string
  to_date?: string
  filter_1_dimension?: string
  filter_1_values?: string
  filter_2_dimension?: string
  filter_2_values?: string
}

type FetchDateBoundsParams = {
  job_id?: string
  source: string
  dataset_type: "sales" | "claims"
}

export type FetchMasterDashboardParams = {
  job_id?: string
  from_date?: string
  to_date?: string
}

export type MasterDashboardSummary = {
  gross_premium?: number
  earned_premium?: number
  zopper_earned_premium?: number
  units_sold?: number
}

export type MasterDashboardResponse = {
  summaries: Record<string, MasterDashboardSummary>
  rows: Record<string, Array<Record<string, unknown>>>
  date_bounds?: {
    min_date?: string | null
    max_date?: string | null
  }
}

type LoginPayload = {
  email: string
  password: string
  role: "admin" | "employee"
}

type LoginResponse = {
  access_token: string
  token_type: string
  role: "admin" | "employee"
  email: string
}

type AuthMeResponse = {
  email: string
  role: "admin" | "employee"
  is_active: boolean
}

export type AdminUser = {
  email: string
  role: "admin" | "employee"
  is_active: boolean
}

export type AdminFileItem = {
  source: string
  dataset_type: string
  job_id: string | null
  tag: string
  rows: number
  latest_row_id: number | null
}

export type AdminReverseMapCandidate = {
  column: string
  score: number
  confidence: number
  reasons: string[]
  sample_values: string[]
}

export type AdminReverseMapField = {
  field: string
  required: boolean
  found: boolean
  suggested_column: string | null
  confidence: number
  reasoning: string[]
  sample_values: string[]
  candidates: AdminReverseMapCandidate[]
}

export type AdminReverseMapResponse = {
  source: string
  dataset_type: "sales" | "claims"
  total_rows: number
  total_columns: number
  required_fields_found: number
  required_fields_total: number
  coverage: number
  can_reverse_map: boolean
  message: string
  file_name?: string
  mappings: AdminReverseMapField[]
}

type AdminFileListResponse = {
  items: AdminFileItem[]
}

export type GraphInsightsPayload = {
  source: string
  dataset_type: "sales" | "claims"
  dimension: string
  metric: string
  bucket?: "day" | "week" | "month"
  job_id?: string
  from_date?: string
  to_date?: string
  compare_mode?: boolean
  rows: Array<Record<string, unknown>>
}

type GraphInsightsResponse = {
  insights: string[]
  model?: string
  message?: string
}

export type ChatbotTurn = {
  role: "user" | "assistant"
  content: string
}

export type ChatbotPayload = {
  message: string
  history?: ChatbotTurn[]
  system_prompt?: string
  temperature?: number
  max_tokens?: number
  source?: string
  dataset_type?: "sales" | "claims"
  job_id?: string
  from_date?: string
  to_date?: string
  global_scope?: boolean
  ui_context?: Record<string, unknown>
}

export type ChatbotResponse = {
  response: string
  model?: string
  message?: string
}

export type ChatbotFileTransformResult = {
  blob: Blob
  filename: string
  summary: string
  operations: number
  rows_affected: number
  columns_touched: number
  skipped: number
}

export type CityBreakdownParams = {
  state: string
  metric: string
  source: string
  dataset_type: "sales" | "claims"
  job_id?: string
  from_date?: string
  to_date?: string
  limit?: number
}

export type CityBreakdownRow = {
  city: string
  value: number
}

type CityBreakdownResponse = {
  state: string
  metric: string
  rows: CityBreakdownRow[]
  total?: number
  message?: string
}

export type CategoryPercentageParams = {
  dimension:
    | "plan_category"
    | "device_plan_category"
    | "article_brand"
    | "brand"
    | "channel"
    | "product_category"
  source: string
  dataset_type: "sales" | "claims"
  metric?: string
  state?: string
  job_id?: string
  from_date?: string
  to_date?: string
  limit?: number
}

export type CategoryPercentageRow = {
  label: string
  value: number
  percentage: number
}

export type DeckDownloadParams = {
  partners?: string[]
  dataset_type: "sales" | "claims"
  job_id?: string
  from_date?: string
  to_date?: string
  include_tables?: boolean
  week_window?: 2 | 3 | 4 | 6
}

export type DeckPreviewPartnerItem = {
  source: string
  display_name: string
  logo?: string | null
  summary: {
    gross_premium: number
    quantity: number
  }
  trend_dimension: "month" | "week" | string
  trend_points: Array<{
    label: string
    gross_premium: number
    quantity: number
  }>
  state_points: Array<{
    label: string
    gross_premium: number
    quantity: number
  }>
  product_points?: Array<{
    label: string
    gross_premium: number
    quantity: number
  }>
  insights: string[]
}

type DeckPreviewResponse = {
  items: DeckPreviewPartnerItem[]
  dataset_type: "sales" | "claims" | string
  week_window: number
}

type CategoryPercentageResponse = {
  dimension: string
  metric: string
  state?: string
  total?: number
  rows: CategoryPercentageRow[]
  message?: string
}

const DEFAULT_API_BASE =
  typeof window !== "undefined"
    ? (window.location.origin || "")
    : "http://127.0.0.1:8000"

const normalizeApiBase = (value: string) => {
  const cleaned = value.replace(/\s+/g, "")
  const withoutMarker = cleaned.replace(/^-?NoNewline/i, "")
  const match = withoutMarker.match(/https?:\/\/.*/)
  const normalized = match ? match[0] : withoutMarker
  return normalized.replace(/\/+$/, "")
}
const ENV_API_BASE = normalizeApiBase(process.env.NEXT_PUBLIC_API_BASE || "")
const API_BASE = ENV_API_BASE || DEFAULT_API_BASE

const runtimeOverride =
  typeof window !== "undefined"
    ? normalizeApiBase(new URLSearchParams(window.location.search).get("api") || "")
    : ""

const browserOriginApiBases =
  typeof window !== "undefined"
    ? [
        `${window.location.origin}/api`,
        window.location.origin,
      ]
    : []

const browserHostApiBases =
  typeof window !== "undefined"
    ? [
        `${window.location.protocol}//${window.location.hostname}:8000`,
        `http://${window.location.hostname}:8000`,
      ]
    : []

const API_FALLBACKS = Array.from(
  new Set([
    runtimeOverride,
    ...browserOriginApiBases,
    ENV_API_BASE,
    ...browserHostApiBases,
    API_BASE,
    "http://127.0.0.1:8000",
    "http://localhost:8000",
    "http://0.0.0.0:8000",
  ].map(v => normalizeApiBase(v)).filter(Boolean))
)
const API_REQUEST_TIMEOUT_MS = Number(process.env.NEXT_PUBLIC_API_TIMEOUT_MS || 20000)
const ADMIN_UPLOAD_TIMEOUT_MS = Number(process.env.NEXT_PUBLIC_ADMIN_UPLOAD_TIMEOUT_MS || 180000)
const CHATBOT_REQUEST_TIMEOUT_MS = Number(process.env.NEXT_PUBLIC_CHATBOT_TIMEOUT_MS || 120000)
const DECK_DOWNLOAD_TIMEOUT_MS = Number(process.env.NEXT_PUBLIC_DECK_DOWNLOAD_TIMEOUT_MS || 240000)
const DECK_PREVIEW_TIMEOUT_MS = Number(process.env.NEXT_PUBLIC_DECK_PREVIEW_TIMEOUT_MS || 120000)
let preferredApiBase = API_FALLBACKS[0] || ""

const orderedApiBases = () => {
  const ordered = [preferredApiBase, ...API_FALLBACKS].filter(Boolean)
  return Array.from(new Set(ordered))
}

const normalizeToken = (value: string | null) => {
  if (!value) return null
  let token = value.trim()
  token = token.replace(/^['"]+|['"]+$/g, "")
  token = token.replace(/^Bearer\s+/i, "").trim()
  if (!token || token === "null" || token === "undefined") return null
  return token
}

const getAuthToken = () =>
  typeof window !== "undefined"
    ? normalizeToken(localStorage.getItem("auth_token"))
    : null

const handleUnauthorized = () => {
  if (typeof window === "undefined") return
  localStorage.removeItem("auth_token")
  if (window.location.pathname !== "/login") {
    window.location.replace("/login")
  }
}

class NoFallbackError extends Error {
  noFallback = true
}

type ApiRequestInit = RequestInit & {
  timeoutMs?: number
}

const fetchWithTimeout = async (
  url: string,
  init: RequestInit = {},
  timeoutMs: number = API_REQUEST_TIMEOUT_MS
) => {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    return await fetch(url, { ...init, signal: controller.signal })
  } finally {
    clearTimeout(timer)
  }
}

async function fetchJsonWithFallback(path: string, query: string, init: ApiRequestInit = {}) {
  const response = await fetchResponseWithFallback(path, query, init)
  return response.json()
}

async function fetchResponseWithFallback(path: string, query: string, init: ApiRequestInit = {}) {
  const errors: string[] = []
  let sawUnauthorized = false
  let sawNonAuthFailure = false
  const { timeoutMs, ...requestInit } = init

  for (const base of orderedApiBases()) {
    const url = query ? `${base}${path}?${query}` : `${base}${path}`
    try {
      const headers = new Headers(requestInit.headers || {})
      headers.set("Accept", "application/json")

      const isFormDataBody =
        typeof FormData !== "undefined" && requestInit.body instanceof FormData

      if (requestInit.body && !isFormDataBody && !headers.has("Content-Type")) {
        headers.set("Content-Type", "application/json")
      }

      const token = getAuthToken()
      if (token) {
        headers.set("Authorization", `Bearer ${token}`)
      }

      const res = await fetchWithTimeout(url, {
        ...requestInit,
        mode: "cors",
        headers,
      }, timeoutMs)

      if (!res.ok) {
        let detail = ""
        try {
          const data = await res.json()
          if (data?.detail) detail = data.detail
        } catch {
          // ignore
        }
        const message = detail || `HTTP ${res.status}`
        const shouldRedirectOnUnauthorized = path !== "/auth/login"

        if (res.status === 401 || res.status === 403) {
          if (shouldRedirectOnUnauthorized) sawUnauthorized = true
          errors.push(`${url} -> ${message}`)
          continue
        }
        // Allow fallback for route/method mismatches (common with stale/wrong API base).
        // Keep no-fallback for auth/validation-style client errors.
        if (res.status >= 400 && res.status < 500 && ![404, 405].includes(res.status)) {
          throw new NoFallbackError(message)
        }
        throw new Error(message)
      }

      preferredApiBase = base
      return res
    } catch (err) {
      if (err instanceof NoFallbackError) {
        throw err
      }
      sawNonAuthFailure = true
      const msg =
        err instanceof Error && err.name === "AbortError"
          ? `Request timed out after ${timeoutMs ?? API_REQUEST_TIMEOUT_MS}ms`
          : err instanceof Error
            ? err.message
            : String(err)
      errors.push(`${url} -> ${msg}`)
      continue
    }
  }

  if (sawUnauthorized && !sawNonAuthFailure) {
    if (path === "/auth/me" || path === "/insights/graph") {
      handleUnauthorized()
    }
    throw new NoFallbackError("Not authenticated")
  }

  throw new Error(`Failed to fetch. Tried: ${errors.join(" | ")}`)
}

function withSafeDateRange<T extends { from_date?: string; to_date?: string }>(params: T): T {
  const from = params.from_date
  const to = params.to_date
  if (!from || !to || from <= to) return params
  return {
    ...params,
    from_date: to,
    to_date: from,
  }
}

export async function fetchSummary(params: FetchSummaryParams) {
  const safeParams = withSafeDateRange(params)
  const query = new URLSearchParams(
    Object.entries(safeParams).reduce((acc, [k, v]) => {
      if (v !== undefined && v !== null) acc[k] = String(v)
      return acc
    }, {} as Record<string, string>)
  ).toString()

  return fetchJsonWithFallback("/analytics/summary", query)
}

export async function fetchLastUpdated(params: FetchLastUpdatedParams) {
  const safeParams = withSafeDateRange(params)
  const query = new URLSearchParams(
    Object.entries(safeParams).reduce((acc, [k, v]) => {
      if (v !== undefined && v !== null) acc[k] = String(v)
      return acc
    }, {} as Record<string, string>)
  ).toString()

  return fetchJsonWithFallback("/analytics/last-updated", query)
}

export async function fetchByDimensionRows(params: FetchByDimensionParams): Promise<Array<Record<string, unknown>>> {
  const safeParams = withSafeDateRange(params)
  const query = new URLSearchParams(
    Object.entries(safeParams).reduce((acc, [k, v]) => {
      if (v !== undefined && v !== null && v !== "") acc[k] = String(v)
      return acc
    }, {} as Record<string, string>)
  ).toString()

  const response = await fetchJsonWithFallback("/analytics/by-dimension", query)
  return Array.isArray(response) ? (response as Array<Record<string, unknown>>) : []
}

export async function fetchDateBounds(params: FetchDateBoundsParams) {
  const query = new URLSearchParams(
    Object.entries(params).reduce((acc, [k, v]) => {
      if (v !== undefined && v !== null) acc[k] = String(v)
      return acc
    }, {} as Record<string, string>)
  ).toString()

  return fetchJsonWithFallback("/analytics/date-bounds", query)
}

export async function fetchMasterDashboard(
  params: FetchMasterDashboardParams
): Promise<MasterDashboardResponse> {
  const safeParams = withSafeDateRange(params)
  const query = new URLSearchParams(
    Object.entries(safeParams).reduce((acc, [k, v]) => {
      if (v !== undefined && v !== null && String(v).trim() !== "") acc[k] = String(v)
      return acc
    }, {} as Record<string, string>)
  ).toString()

  return fetchJsonWithFallback("/analytics/master-dashboard", query)
}

export async function login(payload: LoginPayload): Promise<LoginResponse> {
  return fetchJsonWithFallback("/auth/login", "", {
    method: "POST",
    body: JSON.stringify(payload),
  })
}

export async function fetchAuthMe(): Promise<AuthMeResponse> {
  return fetchJsonWithFallback("/auth/me", "", { method: "GET" })
}

export async function fetchAdminUsers(params: { search?: string; limit?: number } = {}): Promise<AdminUser[]> {
  const query = new URLSearchParams(
    Object.entries(params).reduce((acc, [k, v]) => {
      if (v !== undefined && v !== null) acc[k] = String(v)
      return acc
    }, {} as Record<string, string>)
  ).toString()
  return fetchJsonWithFallback("/auth/users", query, { method: "GET" })
}

export async function createAdminUser(payload: {
  email: string
  password: string
  role: "admin" | "employee"
}): Promise<AdminUser> {
  return fetchJsonWithFallback("/auth/users", "", {
    method: "POST",
    body: JSON.stringify(payload),
  })
}

export async function deleteAdminUser(email: string): Promise<{ deleted: boolean; email: string }> {
  return fetchJsonWithFallback(`/auth/users/${encodeURIComponent(email)}`, "", {
    method: "DELETE",
  })
}

export async function updateAdminUserPassword(
  email: string,
  password: string
): Promise<{ updated: boolean; email: string }> {
  return fetchJsonWithFallback(`/auth/users/${encodeURIComponent(email)}/password`, "", {
    method: "PATCH",
    body: JSON.stringify({ password }),
  })
}

export async function fetchAdminFiles(params: {
  source?: string
  dataset_type?: string
  job_id?: string
} = {}): Promise<AdminFileListResponse> {
  const query = new URLSearchParams(
    Object.entries(params).reduce((acc, [k, v]) => {
      if (v !== undefined && v !== null) acc[k] = String(v)
      return acc
    }, {} as Record<string, string>)
  ).toString()

  return fetchJsonWithFallback("/admin/files", query)
}

export async function deleteAdminFile(params: {
  source: string
  dataset_type: string
  job_id?: string
}) {
  const query = new URLSearchParams(
    Object.entries(params).reduce((acc, [k, v]) => {
      if (v !== undefined && v !== null) acc[k] = String(v)
      return acc
    }, {} as Record<string, string>)
  ).toString()

  return fetchJsonWithFallback("/admin/files", query, { method: "DELETE" })
}

export async function replaceAdminFile(payload: {
  file: File
  source: string
  dataset_type: string
  job_id?: string
}) {
  const form = new FormData()
  form.append("file", payload.file)
  form.append("source", payload.source)
  form.append("dataset_type", payload.dataset_type)
  if (payload.job_id !== undefined) {
    form.append("job_id", payload.job_id)
  }

  return fetchJsonWithFallback("/admin/files/replace", "", {
    method: "POST",
    body: form,
    timeoutMs: ADMIN_UPLOAD_TIMEOUT_MS,
  })
}

export async function updateAdminFile(payload: {
  file: File
  source: string
  dataset_type: string
  job_id?: string
}) {
  const form = new FormData()
  form.append("file", payload.file)
  form.append("source", payload.source)
  form.append("dataset_type", payload.dataset_type)
  if (payload.job_id !== undefined) {
    form.append("job_id", payload.job_id)
  }

  return fetchJsonWithFallback("/admin/files/update", "", {
    method: "POST",
    body: form,
    timeoutMs: ADMIN_UPLOAD_TIMEOUT_MS,
  })
}

export async function reverseMapAdminFile(payload: {
  file: File
  source: string
  dataset_type: "sales" | "claims"
}): Promise<AdminReverseMapResponse> {
  const form = new FormData()
  form.append("file", payload.file)
  form.append("source", payload.source)
  form.append("dataset_type", payload.dataset_type)

  return fetchJsonWithFallback("/admin/files/reverse-map", "", {
    method: "POST",
    body: form,
    timeoutMs: ADMIN_UPLOAD_TIMEOUT_MS,
  })
}

export async function downloadAdminFile(params: {
  source: string
  dataset_type: string
  job_id?: string
  format?: "csv" | "json"
}): Promise<{ blob: Blob; filename: string }> {
  const query = new URLSearchParams(
    Object.entries(params).reduce((acc, [k, v]) => {
      if (v !== undefined && v !== null) acc[k] = String(v)
      return acc
    }, {} as Record<string, string>)
  ).toString()

  const res = await fetchResponseWithFallback("/admin/files/download", query, { method: "GET" })
  const blob = await res.blob()

  const contentDisposition = res.headers.get("content-disposition") || ""
  const match = contentDisposition.match(/filename=\"?([^\";]+)\"?/)
  const fallbackName = `${params.source}_${params.dataset_type}_${params.job_id || "untagged"}.${params.format || "csv"}`

  return {
    blob,
    filename: match?.[1] || fallbackName,
  }
}

export async function downloadDeckPptx(
  params: DeckDownloadParams
): Promise<{ blob: Blob; filename: string }> {
  const safeParams = withSafeDateRange(params)
  const queryParams = new URLSearchParams()
  const partners = Array.isArray(safeParams.partners)
    ? safeParams.partners.map((value) => String(value).trim()).filter(Boolean)
    : []

  if (partners.length) {
    queryParams.set("partners", partners.join(","))
  }
  queryParams.set("dataset_type", safeParams.dataset_type)
  if (safeParams.job_id) queryParams.set("job_id", safeParams.job_id)
  if (safeParams.from_date) queryParams.set("from_date", safeParams.from_date)
  if (safeParams.to_date) queryParams.set("to_date", safeParams.to_date)
  queryParams.set("include_tables", String(safeParams.include_tables !== false))
  if (safeParams.week_window) queryParams.set("week_window", String(safeParams.week_window))

  const res = await fetchResponseWithFallback("/deck/download-pptx", queryParams.toString(), {
    method: "GET",
    timeoutMs: DECK_DOWNLOAD_TIMEOUT_MS,
  })
  const blob = await res.blob()
  const contentDisposition = res.headers.get("content-disposition") || ""
  const match = contentDisposition.match(/filename=\"?([^\";]+)\"?/)
  const fallbackName = `partner_deck_${safeParams.dataset_type}.pptx`

  return {
    blob,
    filename: match?.[1] || fallbackName,
  }
}

export async function fetchDeckPreview(params: DeckDownloadParams): Promise<DeckPreviewResponse> {
  const safeParams = withSafeDateRange(params)
  const queryParams = new URLSearchParams()
  const partners = Array.isArray(safeParams.partners)
    ? safeParams.partners.map((value) => String(value).trim()).filter(Boolean)
    : []

  if (partners.length) {
    queryParams.set("partners", partners.join(","))
  }
  queryParams.set("dataset_type", safeParams.dataset_type)
  if (safeParams.job_id) queryParams.set("job_id", safeParams.job_id)
  if (safeParams.from_date) queryParams.set("from_date", safeParams.from_date)
  if (safeParams.to_date) queryParams.set("to_date", safeParams.to_date)
  if (safeParams.week_window) queryParams.set("week_window", String(safeParams.week_window))

  return fetchJsonWithFallback("/deck/preview", queryParams.toString(), {
    method: "GET",
    timeoutMs: DECK_PREVIEW_TIMEOUT_MS,
  })
}

export async function fetchGraphInsights(payload: GraphInsightsPayload): Promise<GraphInsightsResponse> {
  return fetchJsonWithFallback("/insights/graph", "", {
    method: "POST",
    body: JSON.stringify(payload),
  })
}

export async function sendChatbotMessage(payload: ChatbotPayload): Promise<ChatbotResponse> {
  return fetchJsonWithFallback("/chatbot/message", "", {
    method: "POST",
    body: JSON.stringify(payload),
    timeoutMs: CHATBOT_REQUEST_TIMEOUT_MS,
  })
}

export async function transformChatbotFile(payload: {
  file: File
  instruction: string
  source?: string
  dataset_type?: "sales" | "claims"
  output_format?: "csv" | "xlsx"
}): Promise<ChatbotFileTransformResult> {
  const form = new FormData()
  form.append("file", payload.file)
  form.append("instruction", payload.instruction)
  if (payload.source) {
    form.append("source", payload.source)
  }
  if (payload.dataset_type) {
    form.append("dataset_type", payload.dataset_type)
  }
  if (payload.output_format) {
    form.append("output_format", payload.output_format)
  }

  const res = await fetchResponseWithFallback("/chatbot/file-transform", "", {
    method: "POST",
    body: form,
    timeoutMs: CHATBOT_REQUEST_TIMEOUT_MS,
  })
  const blob = await res.blob()

  const contentDisposition = res.headers.get("content-disposition") || ""
  const fileMatch = contentDisposition.match(/filename=\"?([^\";]+)\"?/)

  const parseHeaderNumber = (headerName: string) => {
    const raw = Number(res.headers.get(headerName) || "0")
    return Number.isFinite(raw) ? raw : 0
  }

  const fallbackName = `${payload.file.name.replace(/\.[^.]+$/, "") || "chatbot_file"}_updated.${payload.output_format || "csv"}`

  return {
    blob,
    filename: fileMatch?.[1] || fallbackName,
    summary: res.headers.get("x-transform-summary") || "File updated successfully.",
    operations: parseHeaderNumber("x-transform-operations"),
    rows_affected: parseHeaderNumber("x-transform-rows-affected"),
    columns_touched: parseHeaderNumber("x-transform-columns-touched"),
    skipped: parseHeaderNumber("x-transform-skipped"),
  }
}

export async function fetchCityBreakdownByState(params: CityBreakdownParams): Promise<CityBreakdownResponse> {
  const safeParams = withSafeDateRange(params)
  const query = new URLSearchParams(
    Object.entries(safeParams).reduce((acc, [k, v]) => {
      if (v !== undefined && v !== null && String(v).trim() !== "") acc[k] = String(v)
      return acc
    }, {} as Record<string, string>)
  ).toString()

  return fetchJsonWithFallback("/analytics/city-breakdown", query)
}

export async function fetchCategoryPercentage(
  params: CategoryPercentageParams
): Promise<CategoryPercentageResponse> {
  const safeParams = withSafeDateRange(params)
  const query = new URLSearchParams(
    Object.entries(safeParams).reduce((acc, [k, v]) => {
      if (v !== undefined && v !== null && String(v).trim() !== "") acc[k] = String(v)
      return acc
    }, {} as Record<string, string>)
  ).toString()

  return fetchJsonWithFallback("/analytics/category-percentage", query)
}
