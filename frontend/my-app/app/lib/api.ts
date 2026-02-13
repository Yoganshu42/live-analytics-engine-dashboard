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

type FetchDateBoundsParams = {
  job_id?: string
  source: string
  dataset_type: "sales" | "claims"
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

const DEFAULT_API_BASE =
  typeof window !== "undefined"
    ? `http://${window.location.hostname || "127.0.0.1"}:8000`
    : "http://127.0.0.1:8000"

const normalizeApiBase = (value: string) => {
  const cleaned = value.replace(/\s+/g, "")
  const withoutMarker = cleaned.replace(/^-?NoNewline/i, "")
  const match = withoutMarker.match(/https?:\/\/.*/)
  return match ? match[0] : withoutMarker
}
const API_BASE = normalizeApiBase(process.env.NEXT_PUBLIC_API_BASE || DEFAULT_API_BASE)

const runtimeOverride =
  typeof window !== "undefined"
    ? normalizeApiBase(new URLSearchParams(window.location.search).get("api") || "")
    : ""

const API_FALLBACKS = Array.from(
  new Set([
    runtimeOverride,
    API_BASE,
    "http://127.0.0.1:8000",
    "http://localhost:8000",
    "http://0.0.0.0:8000",
  ].map(v => normalizeApiBase(v)).filter(Boolean))
)

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

async function fetchJsonWithFallback(path: string, query: string, init: RequestInit = {}) {
  const response = await fetchResponseWithFallback(path, query, init)
  return response.json()
}

async function fetchResponseWithFallback(path: string, query: string, init: RequestInit = {}) {
  const errors: string[] = []
  let sawUnauthorized = false

  for (const base of API_FALLBACKS) {
    const url = query ? `${base}${path}?${query}` : `${base}${path}`
    try {
      const headers = new Headers(init.headers || {})
      headers.set("Accept", "application/json")

      const isFormDataBody =
        typeof FormData !== "undefined" && init.body instanceof FormData

      if (init.body && !isFormDataBody && !headers.has("Content-Type")) {
        headers.set("Content-Type", "application/json")
      }

      const token = getAuthToken()
      if (token) {
        headers.set("Authorization", `Bearer ${token}`)
      }

      const res = await fetch(url, {
        ...init,
        mode: "cors",
        headers,
      })

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

      return res
    } catch (err) {
      if (err instanceof NoFallbackError) {
        throw err
      }
      const msg = err instanceof Error ? err.message : String(err)
      errors.push(`${url} -> ${msg}`)
      continue
    }
  }

  if (sawUnauthorized) {
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

export async function fetchDateBounds(params: FetchDateBoundsParams) {
  const query = new URLSearchParams(
    Object.entries(params).reduce((acc, [k, v]) => {
      if (v !== undefined && v !== null) acc[k] = String(v)
      return acc
    }, {} as Record<string, string>)
  ).toString()

  return fetchJsonWithFallback("/analytics/date-bounds", query)
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

export async function fetchGraphInsights(payload: GraphInsightsPayload): Promise<GraphInsightsResponse> {
  return fetchJsonWithFallback("/insights/graph", "", {
    method: "POST",
    body: JSON.stringify(payload),
  })
}
