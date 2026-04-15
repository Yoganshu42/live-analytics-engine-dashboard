"use client"

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react"
import { Shield } from "lucide-react"
import {
  applyAdminFilterFile,
  AdminFilterAnalyzeResponse,
  AdminFileItem,
  AdminUser,
  LiveUsersResponse,
  analyzeAdminFilterFile,
  createAdminUser,
  deleteAdminFile,
  deleteAdminUser,
  fetchLiveUsers,
  filterAndDownloadAdminFile,
  downloadAdminFile,
  fetchAdminFiles,
  fetchAdminUsers,
  revertAdminFilterApply,
  transformChatbotFile,
  updateAdminFile,
  updateAdminUserPassword,
} from "@/app/lib/api"

type Props = {
  isAdmin: boolean
  compact?: boolean
}

type AdminSection = "data" | "users" | "filter"

function notifyDashboardDataRefresh() {
  if (typeof window === "undefined") return
  const refreshedAt = new Date().toISOString()
  localStorage.setItem("dashboard_data_refresh_at", refreshedAt)
  window.dispatchEvent(
    new CustomEvent("dashboard-data-refreshed", {
      detail: {
        refreshedAt,
      },
    })
  )
}

const FILTER_AI_INSTRUCTION_EXAMPLES = [
  "keep rows from 2025-01-01 to 2025-03-31",
  "keep only rows for Jan 2026",
  "fill missing Plan Category with ADLD",
  "remove duplicates from column Policy Number",
]

const ADMIN_SOURCE_LABELS: Record<string, string> = {
  samsung: "Samsung Overview",
  samsung_vs: "Samsung Vijay Sales",
  samsung_croma: "Samsung Croma",
  samsung_reliance_digital: "Samsung Reliance Digital",
  reliance: "Reliance ResQ",
  godrej: "Godrej",
  hitachi: "Hitachi",
}

const formatAdminSourceLabel = (source: string) => (
  ADMIN_SOURCE_LABELS[source] || source.replace(/_/g, " ")
)

function formatAdminTimestamp(value?: string | null): string {
  if (!value) return "Legacy / not tracked"
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return value
  return parsed.toLocaleString("en-IN", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  })
}

function formatUploader(value?: string | null): string {
  const cleaned = String(value || "").trim()
  if (!cleaned) return "Legacy / not tracked"
  if (cleaned === "legacy-unknown") return "Legacy / unknown"
  return cleaned
}

function inferTransformOutputFormat(file: File): "csv" | "xlsx" | undefined {
  const name = String(file.name || "").trim().toLowerCase()
  if (name.endsWith(".csv")) return "csv"
  if (name.endsWith(".xlsx") || name.endsWith(".xls")) return "xlsx"
  return undefined
}

export default function AdminFileAccess({ isAdmin, compact = false }: Props) {
  const [open, setOpen] = useState(false)
  const [section, setSection] = useState<AdminSection>("data")

  if (!isAdmin) return null

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className={`w-full rounded-xl border border-indigo-200 bg-indigo-50 px-3 py-2 ${compact ? "flex items-center justify-center" : "text-left"}`}
        title="Admin Manual Access"
      >
        {compact ? (
          <Shield size={16} className="text-indigo-700" />
        ) : (
          <>
            <div className="text-[11px] font-bold uppercase tracking-wider text-indigo-700">
              Admin Manual Access
            </div>
            <div className="mt-1 text-[10px] text-indigo-500">Open admin tools</div>
          </>
        )}
      </button>

      {open && (
        <div className="fixed inset-0 z-[120] overflow-y-auto bg-black/40 backdrop-blur-[1px] p-4 sm:p-8">
          <div className="mx-auto my-4 flex min-h-[calc(100vh-2rem)] max-h-[calc(100vh-2rem)] max-w-6xl flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl">
            <div className="flex items-center justify-between border-b px-5 py-4">
              <div>
                <h2 className="text-sm font-black uppercase tracking-wider text-slate-700">Admin Manual Access</h2>
                <p className="text-xs text-slate-500">Choose one option below</p>
              </div>
              <button
                type="button"
                onClick={() => setOpen(false)}
                className="rounded-lg border border-slate-300 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-50"
              >
                Close
              </button>
            </div>

            <div className="border-b px-5 py-3 flex gap-2">
              <button
                type="button"
                onClick={() => setSection("data")}
                className={`rounded-lg px-3 py-2 text-xs font-bold ${section === "data" ? "bg-indigo-600 text-white" : "border border-slate-300 bg-white text-slate-700"}`}
              >
                Data Updation
              </button>
              <button
                type="button"
                onClick={() => setSection("users")}
                className={`rounded-lg px-3 py-2 text-xs font-bold ${section === "users" ? "bg-indigo-600 text-white" : "border border-slate-300 bg-white text-slate-700"}`}
              >
                Create User
              </button>
              <button
                type="button"
                onClick={() => setSection("filter")}
                className={`rounded-lg px-3 py-2 text-xs font-bold ${section === "filter" ? "bg-indigo-600 text-white" : "border border-slate-300 bg-white text-slate-700"}`}
              >
                Filter File
              </button>
            </div>

            <div className="flex-1 overflow-auto p-5">
              {section === "data" ? <DataUpdationPanel /> : section === "users" ? <UserManagementPanel /> : <FilterFilePanel />}
            </div>
          </div>
        </div>
      )}
    </>
  )
}

function DataUpdationPanel() {
  const [items, setItems] = useState<AdminFileItem[]>([])
  const [loading, setLoading] = useState(false)
  const [message, setMessage] = useState<string>("")
  const [error, setError] = useState<string>("")

  const [source, setSource] = useState("samsung")
  const [datasetType, setDatasetType] = useState("sales")
  const [jobId, setJobId] = useState("")
  const [file, setFile] = useState<File | null>(null)
  const [submitting, setSubmitting] = useState(false)

  const knownSources = useMemo(() => ["samsung", "samsung_vs", "samsung_croma", "samsung_reliance_digital", "reliance", "godrej", "hitachi"], [])
  const scopedItems = useMemo(
    () => items
      .filter((item) => item.source === source && item.dataset_type === datasetType)
      .sort((a, b) => (b.latest_row_id || 0) - (a.latest_row_id || 0)),
    [datasetType, items, source]
  )
  const scopedJobIds = useMemo(
    () => Array.from(new Set(scopedItems.map((item) => item.job_id).filter(Boolean))) as string[],
    [scopedItems]
  )
  const suggestedJobId = useMemo(() => scopedItems[0]?.job_id || "", [scopedItems])

  useEffect(() => {
    const hasCurrentJob = jobId ? scopedJobIds.includes(jobId) : false
    if (suggestedJobId) {
      if (!hasCurrentJob) {
        setJobId(suggestedJobId)
      }
      return
    }
    if (jobId && !hasCurrentJob) {
      setJobId("")
    }
  }, [datasetType, jobId, scopedJobIds, source, suggestedJobId])

  const refreshDashboard = useCallback(() => {
    if (typeof window === "undefined") return
    notifyDashboardDataRefresh()
    window.setTimeout(() => {
      window.location.reload()
    }, 350)
  }, [])

  const loadItems = useCallback(async () => {
    setLoading(true)
    setError("")
    try {
      const res = await fetchAdminFiles()
      setItems(res.items || [])
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load file tags")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadItems()
  }, [loadItems])

  const handleUpdate = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    if (!file) {
      setError("Select a file to update data")
      return
    }

    setSubmitting(true)
    setError("")
    setMessage("")
    try {
      const res = await updateAdminFile({
        file,
        source,
        dataset_type: datasetType,
        job_id: jobId || undefined,
      })
      const resolvedJobId = res.job_id || jobId || ""
      if (resolvedJobId) {
        setJobId(resolvedJobId)
      }
      const details = [
        res.auto_generated_job_id && resolvedJobId ? `Auto-generated job_id: ${resolvedJobId}.` : "",
        res.uploaded_by ? `Uploaded by ${formatUploader(res.uploaded_by)} on ${formatAdminTimestamp(res.uploaded_at)}.` : "",
      ].filter(Boolean).join(" ")
      setMessage(
        `Updated data. Inserted ${res.rows_inserted} rows for ${source}:${datasetType}:${resolvedJobId || "untagged"}. ${details}`.trim()
      )
      setFile(null)
      await loadItems()
      refreshDashboard()
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update data")
    } finally {
      setSubmitting(false)
    }
  }

  const handleDelete = async (item: AdminFileItem) => {
    const ok = window.confirm(`Delete rows for tag ${item.tag}? This cannot be undone.`)
    if (!ok) return

    setError("")
    setMessage("")
    try {
      const res = await deleteAdminFile({
        source: item.source,
        dataset_type: item.dataset_type,
        job_id: item.job_id || undefined,
      })
      setMessage(`Deleted ${res.deleted_rows} rows from ${item.tag}`)
      await loadItems()
      refreshDashboard()
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete file tag")
    }
  }

  const handleDownload = async (item: AdminFileItem) => {
    setError("")
    setMessage("")
    try {
      const { blob, filename } = await downloadAdminFile({
        source: item.source,
        dataset_type: item.dataset_type,
        job_id: item.job_id || undefined,
        format: "csv",
      })
      const href = URL.createObjectURL(blob)
      const link = document.createElement("a")
      link.href = href
      link.download = filename
      document.body.appendChild(link)
      link.click()
      link.remove()
      URL.revokeObjectURL(href)
      setMessage(`Downloaded ${filename}`)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to download file tag")
    }
  }

  const applyTagToForm = (item: AdminFileItem) => {
    setSource(item.source)
    setDatasetType(item.dataset_type)
    setJobId(item.job_id || "")
    setMessage(`Selected tag ${item.tag}. Choose a file and click Update Data.`)
    setError("")
  }

  return (
    <div className="rounded-xl border border-gray-200 bg-gray-50 p-3">
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-[11px] font-bold uppercase tracking-wider text-gray-700">Data Updation</h3>
        <button
          type="button"
          onClick={loadItems}
          className="rounded border border-gray-300 bg-white px-2 py-1 text-[10px] font-semibold text-gray-700 hover:bg-gray-100"
        >
          Refresh
        </button>
      </div>

      <form onSubmit={handleUpdate} className="mb-3 space-y-2">
        <div className="grid grid-cols-2 gap-2">
          <select
            value={source}
            onChange={(e) => setSource(e.target.value)}
            className="rounded border border-gray-300 bg-white px-2 py-1 text-[11px]"
          >
            {knownSources.map((src) => (
              <option key={src} value={src}>
                {formatAdminSourceLabel(src)}
              </option>
            ))}
          </select>
          <select
            value={datasetType}
            onChange={(e) => setDatasetType(e.target.value)}
            className="rounded border border-gray-300 bg-white px-2 py-1 text-[11px]"
          >
            <option value="sales">sales</option>
            <option value="claims">claims</option>
          </select>
        </div>
        <input
          type="text"
          value={jobId}
          onChange={(e) => setJobId(e.target.value)}
          list="data-job-id-suggestions"
          placeholder="job_id tag (optional, defaults to the latest live tag)"
          className="w-full rounded border border-gray-300 bg-white px-2 py-1 text-[11px]"
        />
        <datalist id="data-job-id-suggestions">
          {scopedJobIds.map((tag) => (
            <option key={tag} value={tag} />
          ))}
        </datalist>
        <p className="text-[10px] text-gray-500">
          Leave this blank to merge into the latest live tag for this source and dataset. Enter a custom job_id only when you intentionally want a separate bucket.
        </p>
        {scopedJobIds.length > 0 && (
          <div className="flex flex-wrap gap-1">
            {scopedJobIds.slice(0, 8).map((tag) => (
              <button
                key={tag}
                type="button"
                onClick={() => setJobId(tag)}
                className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold ${
                  jobId === tag
                    ? "border-indigo-300 bg-indigo-50 text-indigo-700"
                    : "border-gray-200 bg-white text-gray-700 hover:bg-gray-100"
                }`}
              >
                {tag}
              </button>
            ))}
            {suggestedJobId && jobId !== suggestedJobId && (
              <button
                type="button"
                onClick={() => setJobId(suggestedJobId)}
                className="rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-[10px] font-semibold text-emerald-700 hover:bg-emerald-100"
              >
                Use latest live tag
              </button>
            )}
          </div>
        )}
        <input
          type="file"
          accept=".csv,.xlsx,.xls"
          onChange={(e) => setFile(e.target.files?.[0] || null)}
          className="w-full text-[11px] text-gray-600 file:mr-2 file:rounded file:border file:border-gray-300 file:bg-white file:px-2 file:py-1 file:text-[10px] file:font-semibold"
        />
        <button
          type="submit"
          disabled={submitting}
          className="w-full rounded bg-indigo-600 px-2 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {submitting ? "Updating..." : "Update Data"}
        </button>
      </form>

      {error && <p className="mb-2 text-[10px] font-semibold text-red-600">{error}</p>}
      {message && <p className="mb-2 text-[10px] font-semibold text-emerald-700">{message}</p>}

      <div className="max-h-80 overflow-auto rounded border border-gray-200 bg-white">
        <table className="w-full text-left text-[10px]">
          <thead className="sticky top-0 bg-gray-100 text-gray-700">
            <tr>
              <th className="px-2 py-1 font-bold">Source</th>
              <th className="px-2 py-1 font-bold">Dataset</th>
              <th className="px-2 py-1 font-bold">Job Tag</th>
              <th className="px-2 py-1 font-bold">Rows</th>
              <th className="px-2 py-1 font-bold">Last Upload</th>
              <th className="px-2 py-1 font-bold">Uploaded By</th>
              <th className="px-2 py-1 font-bold">Actions</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={7} className="px-2 py-2 text-gray-500">Loading...</td>
              </tr>
            ) : items.length === 0 ? (
              <tr>
                <td colSpan={7} className="px-2 py-2 text-gray-500">No tagged files found.</td>
              </tr>
            ) : (
              items.map((item) => (
                <tr key={item.tag} className="border-t border-gray-100">
                  <td className="px-2 py-1.5 text-gray-700">{formatAdminSourceLabel(item.source)}</td>
                  <td className="px-2 py-1.5 text-gray-700">{item.dataset_type}</td>
                  <td className="px-2 py-1.5 text-gray-700">{item.job_id || "untagged"}</td>
                  <td className="px-2 py-1.5 font-semibold text-gray-800">{item.rows}</td>
                  <td className="px-2 py-1.5 text-gray-700">
                    <div>{formatAdminTimestamp(item.uploaded_at)}</div>
                    <div className="text-[9px] uppercase tracking-wide text-gray-400">{item.action || "legacy"}</div>
                    {item.file_name && <div className="max-w-[180px] truncate text-[9px] text-gray-400">{item.file_name}</div>}
                  </td>
                  <td className="px-2 py-1.5 text-gray-700">
                    <div>{formatUploader(item.uploaded_by)}</div>
                    {item.notes && <div className="max-w-[220px] text-[9px] text-gray-400">{item.notes}</div>}
                  </td>
                  <td className="px-2 py-1.5">
                    <div className="flex flex-wrap gap-1">
                      <button type="button" onClick={() => applyTagToForm(item)} className="rounded border border-gray-300 bg-white px-1.5 py-0.5 text-[10px] font-semibold text-gray-700 hover:bg-gray-100">Use Tag</button>
                      <button type="button" onClick={() => handleDownload(item)} className="rounded border border-indigo-200 bg-indigo-50 px-1.5 py-0.5 text-[10px] font-semibold text-indigo-700 hover:bg-indigo-100">Download</button>
                      <button type="button" onClick={() => handleDelete(item)} className="rounded border border-red-200 bg-red-50 px-1.5 py-0.5 text-[10px] font-semibold text-red-700 hover:bg-red-100">Delete</button>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function UserManagementPanel() {
  const [users, setUsers] = useState<AdminUser[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState("")
  const [message, setMessage] = useState("")
  const [liveUsers, setLiveUsers] = useState<LiveUsersResponse | null>(null)
  const [liveLoading, setLiveLoading] = useState(false)
  const [liveError, setLiveError] = useState("")

  const [search, setSearch] = useState("")
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [role, setRole] = useState<"admin" | "employee">("employee")
  const [submitting, setSubmitting] = useState(false)

  const refreshDashboard = useCallback(() => {
    if (typeof window === "undefined") return
    notifyDashboardDataRefresh()
    window.setTimeout(() => {
      window.location.reload()
    }, 350)
  }, [])

  const loadUsers = useCallback(async (q?: string) => {
    setLoading(true)
    setError("")
    try {
      const res = await fetchAdminUsers({ search: q || undefined, limit: 200 })
      setUsers(res)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load users")
    } finally {
      setLoading(false)
    }
  }, [])

  const loadLiveUsers = useCallback(async () => {
    setLiveLoading(true)
    setLiveError("")
    try {
      const res = await fetchLiveUsers()
      setLiveUsers(res)
    } catch (err) {
      setLiveError(err instanceof Error ? err.message : "Failed to load live users")
    } finally {
      setLiveLoading(false)
    }
  }, [])

  const roleLabel = (roleValue: "admin" | "employee") =>
    roleValue === "employee" ? "Business user" : "admin"

  useEffect(() => {
    loadUsers("")
  }, [loadUsers])

  const handleCreateUser = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    setSubmitting(true)
    setError("")
    setMessage("")
    try {
      await createAdminUser({ email, password, role })
      setMessage(`User created: ${email}`)
      setEmail("")
      setPassword("")
      setRole("employee")
      await loadUsers(search)
      refreshDashboard()
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create user")
    } finally {
      setSubmitting(false)
    }
  }

  const handleDeleteUser = async (targetEmail: string) => {
    const ok = window.confirm(`Delete user ${targetEmail}?`)
    if (!ok) return
    setError("")
    setMessage("")
    try {
      await deleteAdminUser(targetEmail)
      setMessage(`Deleted user: ${targetEmail}`)
      await loadUsers(search)
      refreshDashboard()
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete user")
    }
  }

  const handleResetPassword = async (targetEmail: string) => {
    const newPassword = window.prompt(`Enter new password for ${targetEmail}`)
    if (!newPassword) return
    setError("")
    setMessage("")
    try {
      await updateAdminUserPassword(targetEmail, newPassword)
      setMessage(`Password updated for ${targetEmail}`)
      refreshDashboard()
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update password")
    }
  }

  return (
    <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
      <div className="mb-3 flex items-center justify-between gap-2">
        <h3 className="text-[11px] font-bold uppercase tracking-wider text-slate-700">Create User / Manage Users</h3>
        <button
          type="button"
          onClick={() => loadUsers(search)}
          className="rounded border border-slate-300 bg-white px-2 py-1 text-[10px] font-semibold text-slate-700 hover:bg-slate-100"
        >
          Refresh
        </button>
      </div>

      <form onSubmit={handleCreateUser} className="mb-3 grid grid-cols-1 md:grid-cols-4 gap-2">
        <input
          type="email"
          required
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          placeholder="User ID (email)"
          className="rounded border border-slate-300 bg-white px-2 py-1 text-[11px]"
        />
        <input
          type="text"
          required
          minLength={6}
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Password"
          className="rounded border border-slate-300 bg-white px-2 py-1 text-[11px]"
        />
        <select
          value={role}
          onChange={(e) => setRole(e.target.value as "admin" | "employee")}
          className="rounded border border-slate-300 bg-white px-2 py-1 text-[11px]"
        >
          <option value="employee">Business user</option>
          <option value="admin">admin</option>
        </select>
        <button
          type="submit"
          disabled={submitting}
          className="rounded bg-indigo-600 px-2 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {submitting ? "Creating..." : "Create User"}
        </button>
      </form>

      <div className="mb-2 flex gap-2">
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search by email"
          className="flex-1 rounded border border-slate-300 bg-white px-2 py-1 text-[11px]"
        />
        <button
          type="button"
          onClick={() => loadUsers(search)}
          className="rounded border border-slate-300 bg-white px-2 py-1 text-[11px] font-semibold text-slate-700 hover:bg-slate-100"
        >
          Search
        </button>
      </div>

      {error && <p className="mb-2 text-[10px] font-semibold text-red-600">{error}</p>}
      {message && <p className="mb-2 text-[10px] font-semibold text-emerald-700">{message}</p>}

      <div className="max-h-80 overflow-auto rounded border border-slate-200 bg-white">
        <table className="w-full text-left text-[10px]">
          <thead className="sticky top-0 bg-slate-100 text-slate-700">
            <tr>
              <th className="px-2 py-1 font-bold">User ID</th>
              <th className="px-2 py-1 font-bold">Role</th>
              <th className="px-2 py-1 font-bold">Status</th>
              <th className="px-2 py-1 font-bold">Actions</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={4} className="px-2 py-2 text-slate-500">Loading...</td>
              </tr>
            ) : users.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-2 py-2 text-slate-500">No users found.</td>
              </tr>
            ) : (
              users.map((user) => (
                <tr key={user.email} className="border-t border-slate-100">
                  <td className="px-2 py-1.5 text-slate-700">{user.email}</td>
                  <td className="px-2 py-1.5 text-slate-700">{roleLabel(user.role)}</td>
                  <td className="px-2 py-1.5 text-slate-700">{user.is_active ? "active" : "inactive"}</td>
                  <td className="px-2 py-1.5">
                    <div className="flex flex-wrap gap-1">
                      <button
                        type="button"
                        onClick={() => handleResetPassword(user.email)}
                        className="rounded border border-indigo-200 bg-indigo-50 px-1.5 py-0.5 text-[10px] font-semibold text-indigo-700 hover:bg-indigo-100"
                      >
                        Change Password
                      </button>
                      <button
                        type="button"
                        onClick={() => handleDeleteUser(user.email)}
                        className="rounded border border-red-200 bg-red-50 px-1.5 py-0.5 text-[10px] font-semibold text-red-700 hover:bg-red-100"
                      >
                        Delete
                      </button>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <div className="mt-4 rounded-xl border border-slate-200 bg-white p-3">
        <div className="mb-2 flex items-center justify-between gap-2">
          <div>
            <h4 className="text-[11px] font-bold uppercase tracking-wider text-slate-700">Live Users</h4>
            <p className="text-[10px] text-slate-500">Shows sessions active within the last few minutes.</p>
          </div>
          <button
            type="button"
            onClick={loadLiveUsers}
            className="rounded border border-slate-300 bg-white px-2 py-1 text-[10px] font-semibold text-slate-700 hover:bg-slate-100"
          >
            {liveLoading ? "Refreshing..." : "Refresh Live Users"}
          </button>
        </div>

        {liveError && <p className="mb-2 text-[10px] font-semibold text-red-600">{liveError}</p>}

        {!liveUsers && !liveLoading && (
          <p className="text-[10px] text-slate-500">Click refresh to load live user sessions.</p>
        )}

        {liveUsers && (
          <div className="max-h-64 overflow-auto rounded border border-slate-200">
            <table className="w-full text-left text-[10px]">
              <thead className="sticky top-0 bg-slate-50 text-slate-700">
                <tr>
                  <th className="px-2 py-1 font-bold">User ID</th>
                  <th className="px-2 py-1 font-bold">Role</th>
                  <th className="px-2 py-1 font-bold">Last Seen</th>
                  <th className="px-2 py-1 font-bold">TTL (sec)</th>
                </tr>
              </thead>
              <tbody>
                {liveUsers.count === 0 ? (
                  <tr>
                    <td colSpan={4} className="px-2 py-2 text-slate-500">No active sessions found.</td>
                  </tr>
                ) : (
                  liveUsers.users.map((user) => (
                    <tr key={`${user.email}-${user.last_seen_at}`} className="border-t border-slate-100">
                      <td className="px-2 py-1.5 text-slate-700">{user.email}</td>
                      <td className="px-2 py-1.5 text-slate-700">{user.role}</td>
                      <td className="px-2 py-1.5 text-slate-700">{user.last_seen_at}</td>
                      <td className="px-2 py-1.5 text-slate-700">{user.ttl_seconds}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

function FilterFilePanel() {
  const [source, setSource] = useState("samsung")
  const [datasetType, setDatasetType] = useState<"sales" | "claims">("sales")
  const [outputFormat, setOutputFormat] = useState<"csv" | "xlsx">("csv")
  const [jobId, setJobId] = useState("")
  const [applyToDb, setApplyToDb] = useState(false)
  const [file, setFile] = useState<File | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [message, setMessage] = useState("")
  const [error, setError] = useState("")
  const [analysis, setAnalysis] = useState<AdminFilterAnalyzeResponse | null>(null)
  const [analysisLoading, setAnalysisLoading] = useState(false)
  const [analysisError, setAnalysisError] = useState("")
  const [lastRevisionId, setLastRevisionId] = useState<number | null>(null)
  const [applyLoading, setApplyLoading] = useState(false)
  const [aiInstruction, setAiInstruction] = useState("")
  const [instructionSummary, setInstructionSummary] = useState("")
  const [tagItems, setTagItems] = useState<AdminFileItem[]>([])
  const [tagLoading, setTagLoading] = useState(false)
  const [tagError, setTagError] = useState("")

  const knownSources = useMemo(() => ["samsung", "samsung_vs", "samsung_croma", "samsung_reliance_digital", "reliance", "godrej", "hitachi"], [])

  const loadTagItems = useCallback(async () => {
    setTagLoading(true)
    setTagError("")
    try {
      const res = await fetchAdminFiles({
        source,
        dataset_type: datasetType,
      })
      setTagItems(res.items || [])
    } catch (err) {
      setTagItems([])
      setTagError(
        err instanceof Error
          ? `Existing job tags are unavailable right now. You can still upload by entering the job_id manually. ${err.message}`
          : "Existing job tags are unavailable right now. You can still upload by entering the job_id manually."
      )
    } finally {
      setTagLoading(false)
    }
  }, [datasetType, source])

  useEffect(() => {
    loadTagItems()
  }, [loadTagItems])

  const matchingTags = useMemo(
    () => tagItems.filter((item) => item.source === source && item.dataset_type === datasetType),
    [datasetType, source, tagItems]
  )

  const jobIdOptions = useMemo(() => (
    Array.from(new Set(matchingTags.map((item) => item.job_id).filter(Boolean))) as string[]
  ), [matchingTags])

  const suggestedJobId = useMemo(() => {
    const candidates = matchingTags.filter((item) => item.job_id)
    if (!candidates.length) return ""
    const sorted = [...candidates].sort((a, b) => (b.latest_row_id || 0) - (a.latest_row_id || 0))
    return sorted[0]?.job_id || ""
  }, [matchingTags])

  useEffect(() => {
    const hasCurrentJob = jobId ? jobIdOptions.includes(jobId) : false
    if (suggestedJobId) {
      if (!hasCurrentJob) {
        setJobId(suggestedJobId)
      }
      return
    }
    if (jobId && !hasCurrentJob) {
      setJobId("")
    }
  }, [datasetType, jobId, jobIdOptions, source, suggestedJobId])

  const refreshDashboard = useCallback(() => {
    if (!applyToDb || typeof window === "undefined") return
    notifyDashboardDataRefresh()
    window.setTimeout(() => {
      window.location.reload()
    }, 350)
  }, [applyToDb])
  const selectedTag = useMemo(
    () => matchingTags.find((item) => (item.job_id || "") === jobId) || null,
    [jobId, matchingTags]
  )

  const triggerAutoRefresh = useCallback((delayMs: number = 1800) => {
    if (typeof window === "undefined") return
    window.setTimeout(() => {
      window.location.reload()
    }, delayMs)
  }, [])

  const handleManualRefresh = useCallback(() => {
    if (typeof window === "undefined") return
    notifyDashboardDataRefresh()
    window.location.reload()
  }, [])

  const prepareFileForWorkflow = useCallback(async (currentFile: File) => {
    const trimmedInstruction = aiInstruction.trim()
    if (!trimmedInstruction) {
      setInstructionSummary("")
      return currentFile
    }

    const transformed = await transformChatbotFile({
      file: currentFile,
      instruction: trimmedInstruction,
      source,
      dataset_type: datasetType,
      output_format: inferTransformOutputFormat(currentFile),
    })
    setInstructionSummary(transformed.summary || "AI instruction applied to uploaded file.")
    return new File([transformed.blob], transformed.filename || currentFile.name, {
      type: transformed.blob.type || currentFile.type,
      lastModified: Date.now(),
    })
  }, [aiInstruction, datasetType, source])

  const handleAnalyze = useCallback(async () => {
    if (!file) {
      setAnalysisError("Select a file before running AI analysis.")
      return
    }
    setAnalysisLoading(true)
    setAnalysisError("")
    try {
      const preparedFile = await prepareFileForWorkflow(file)
      const result = await analyzeAdminFilterFile({
        file: preparedFile,
        source,
        dataset_type: datasetType,
        job_id: jobId || undefined,
      })
      setAnalysis(result)
      setMessage(result.ai_mapping?.message || "AI analysis completed.")
    } catch (err) {
      setAnalysis(null)
      setAnalysisError(err instanceof Error ? err.message : "Failed to analyze file")
    } finally {
      setAnalysisLoading(false)
    }
  }, [file, source, datasetType, jobId, prepareFileForWorkflow])

  const handleRevert = useCallback(async () => {
    setError("")
    setMessage("")
    try {
      const res = await revertAdminFilterApply({
        source,
        dataset_type: datasetType,
        job_id: jobId || undefined,
        revision_id: lastRevisionId || undefined,
      })
      setMessage(`Reverted filter apply (revision ${res.revision_id}). Restored ${res.rows_inserted} rows.`)
      setLastRevisionId(null)
      if (typeof window !== "undefined") {
        notifyDashboardDataRefresh()
      }
      triggerAutoRefresh(350)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to revert previous apply")
    }
  }, [source, datasetType, jobId, lastRevisionId, triggerAutoRefresh])

  const handleApplyChangesToDb = useCallback(async () => {
    if (!file) {
      setError("Select a file to apply filtered changes into DB.")
      return
    }
    setApplyLoading(true)
    setError("")
    setMessage("")
    try {
      const preparedFile = await prepareFileForWorkflow(file)
      const res = await applyAdminFilterFile({
        file: preparedFile,
        source,
        dataset_type: datasetType,
        job_id: jobId || undefined,
      })
      const resolvedJobId = res.job_id || jobId || ""
      if (resolvedJobId) {
        setJobId(resolvedJobId)
      }
      setLastRevisionId(res.revision_id ? Number(res.revision_id) : null)
      await loadTagItems()
      const details = [
        resolvedJobId ? `job_id: ${resolvedJobId}.` : "",
        res.auto_generated_job_id && resolvedJobId ? "This tag was auto-generated." : "",
        res.uploaded_by ? `Uploaded by ${formatUploader(res.uploaded_by)} on ${formatAdminTimestamp(res.uploaded_at)}.` : "",
      ].filter(Boolean).join(" ")
      setMessage([res.summary || `Applied ${res.rows_inserted} rows to database.`, details].filter(Boolean).join(" "))
      if (typeof window !== "undefined") {
        notifyDashboardDataRefresh()
      }
      triggerAutoRefresh(500)
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Failed to apply filtered data to DB"
      setError(msg)
      if (msg.includes("HTTP 504")) {
        setMessage("Request timed out while applying changes. Auto-refreshing page to sync latest state.")
        triggerAutoRefresh(1800)
      }
    } finally {
      setApplyLoading(false)
    }
  }, [file, source, datasetType, jobId, triggerAutoRefresh, prepareFileForWorkflow, loadTagItems])

  const handleFilterDownload = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    if (!file) {
      setError("Select a file to filter")
      return
    }

    setSubmitting(true)
    setError("")
    setMessage("")
    try {
      const preparedFile = await prepareFileForWorkflow(file)
      const { blob, filename, summary, revision_id, job_id: resolvedJobId, auto_generated_job_id, uploaded_by, uploaded_at } = await filterAndDownloadAdminFile({
        file: preparedFile,
        source,
        dataset_type: datasetType,
        output_format: outputFormat,
        apply_to_db: applyToDb,
        job_id: jobId || undefined,
      })

      const href = URL.createObjectURL(blob)
      const link = document.createElement("a")
      link.href = href
      link.download = filename
      document.body.appendChild(link)
      link.click()
      link.remove()
      URL.revokeObjectURL(href)

      if (applyToDb && resolvedJobId) {
        setJobId(resolvedJobId)
        await loadTagItems()
      }
      const details = applyToDb
        ? [
            resolvedJobId ? `job_id: ${resolvedJobId}.` : "",
            auto_generated_job_id && resolvedJobId ? "This tag was auto-generated." : "",
            uploaded_by ? `Uploaded by ${formatUploader(uploaded_by)} on ${formatAdminTimestamp(uploaded_at)}.` : "",
          ].filter(Boolean).join(" ")
        : ""
      setMessage([summary || `Downloaded ${filename}`, details].filter(Boolean).join(" "))
      setLastRevisionId(revision_id || null)
      if (applyToDb) {
        refreshDashboard()
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Failed to filter and download file"
      setError(msg)
      if (msg.includes("HTTP 504")) {
        setMessage("Filtering request timed out. Auto-refreshing page now to keep latest state.")
        triggerAutoRefresh(1800)
      }
    } finally {
      setSubmitting(false)
    }
  }

  useEffect(() => {
    setAnalysis(null)
    setAnalysisError("")
    setLastRevisionId(null)
    setInstructionSummary("")
  }, [source, datasetType, jobId, file, aiInstruction])

  return (
    <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
      <div className="mb-3 flex items-start justify-between gap-2">
        <div>
          <h3 className="text-[11px] font-bold uppercase tracking-wider text-slate-700">Filter File And Download</h3>
          <p className="mt-1 text-[10px] text-slate-500">
            Applies smart partner mapping (columns, plan/device taxonomy, city-state normalization) and downloads filtered output.
          </p>
        </div>
        <button
          type="button"
          onClick={handleManualRefresh}
          className="rounded border border-slate-300 bg-white px-2 py-1 text-[10px] font-semibold text-slate-700 hover:bg-slate-100"
        >
          Refresh
        </button>
      </div>

      <form onSubmit={handleFilterDownload} className="space-y-2">
        <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
          <select
            value={source}
            onChange={(e) => setSource(e.target.value)}
            className="rounded border border-slate-300 bg-white px-2 py-1 text-[11px]"
          >
            {knownSources.map((src) => (
              <option key={src} value={src}>
                {formatAdminSourceLabel(src)}
              </option>
            ))}
          </select>
          <select
            value={datasetType}
            onChange={(e) => setDatasetType(e.target.value as "sales" | "claims")}
            className="rounded border border-slate-300 bg-white px-2 py-1 text-[11px]"
          >
            <option value="sales">sales</option>
            <option value="claims">claims</option>
          </select>
        </div>

        <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
          <select
            value={outputFormat}
            onChange={(e) => setOutputFormat(e.target.value as "csv" | "xlsx")}
            className="rounded border border-slate-300 bg-white px-2 py-1 text-[11px]"
          >
            <option value="csv">csv</option>
            <option value="xlsx">xlsx</option>
          </select>
          <input
            type="text"
            value={jobId}
            onChange={(e) => setJobId(e.target.value)}
            list="job-id-suggestions"
            placeholder="job_id tag (optional, defaults to the latest live tag when applied)"
            className="rounded border border-slate-300 bg-white px-2 py-1 text-[11px]"
          />
        </div>

        <datalist id="job-id-suggestions">
          {jobIdOptions.map((tag) => (
            <option key={tag} value={tag} />
          ))}
        </datalist>

        {tagError && (
          <p className="text-[10px] font-semibold text-red-600">{tagError}</p>
        )}

        {jobIdOptions.length > 0 && (
          <div className="flex flex-wrap gap-1">
            {jobIdOptions.slice(0, 8).map((tag) => (
              <button
                key={tag}
                type="button"
                onClick={() => setJobId(tag)}
                className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold ${
                  jobId === tag
                    ? "border-indigo-300 bg-indigo-50 text-indigo-700"
                    : "border-slate-200 bg-white text-slate-600 hover:bg-slate-50"
                }`}
              >
                {tag}
              </button>
            ))}
            {suggestedJobId && jobId !== suggestedJobId && (
              <button
                type="button"
                onClick={() => setJobId(suggestedJobId)}
                className="rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-[10px] font-semibold text-emerald-700 hover:bg-emerald-100"
              >
                Use latest tag
              </button>
            )}
          </div>
        )}

        {tagLoading && (
          <p className="text-[10px] text-slate-500">Loading tag suggestions...</p>
        )}

        <p className="text-[10px] text-slate-500">
          Leave job_id blank to merge into the latest live tag for this source and dataset. Enter a custom job_id only if you intentionally want a separate dataset bucket.
        </p>

        {selectedTag && (
          <div className="rounded border border-slate-200 bg-white p-2 text-[10px] text-slate-700">
            <div className="font-bold uppercase tracking-wider text-slate-700">Selected Tag</div>
            <div className="mt-1">{selectedTag.job_id}</div>
            <div className="mt-1">Rows: {selectedTag.rows}</div>
            <div className="mt-1">Last upload: {formatAdminTimestamp(selectedTag.uploaded_at)}</div>
            <div className="mt-1">Uploaded by: {formatUploader(selectedTag.uploaded_by)}</div>
            {selectedTag.notes && (
              <div className="mt-1 text-slate-500">{selectedTag.notes}</div>
            )}
          </div>
        )}

        <label className="flex items-center gap-2 text-[11px] text-slate-700">
          <input
            type="checkbox"
            checked={applyToDb}
            onChange={(e) => setApplyToDb(e.target.checked)}
            className="h-3.5 w-3.5"
          />
          Apply filtered rows to database immediately
        </label>

        <input
          type="file"
          accept=".csv,.xlsx,.xls"
          onChange={(e) => setFile(e.target.files?.[0] || null)}
          className="w-full text-[11px] text-slate-600 file:mr-2 file:rounded file:border file:border-slate-300 file:bg-white file:px-2 file:py-1 file:text-[10px] file:font-semibold"
        />

        <div className="rounded border border-slate-200 bg-white p-2">
          <div className="text-[10px] font-bold uppercase tracking-wider text-slate-700">AI Instructions (Optional)</div>
          <p className="mt-1 text-[10px] text-slate-500">
            Applied before analysis and DB upsert. Use this to keep a period, remove duplicates, or fill missing fields.
          </p>
          <textarea
            value={aiInstruction}
            onChange={(e) => setAiInstruction(e.target.value)}
            placeholder="Example: keep rows from 2025-01-01 to 2025-03-31"
            rows={3}
            className="mt-2 w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-[11px] text-slate-700 outline-none ring-indigo-500 focus:border-indigo-300 focus:ring-2"
          />
          <div className="mt-2 flex flex-wrap gap-1">
            {FILTER_AI_INSTRUCTION_EXAMPLES.map((example) => (
              <button
                key={example}
                type="button"
                onClick={() => setAiInstruction(example)}
                className="rounded-full border border-cyan-200 bg-cyan-50 px-2 py-1 text-[10px] font-semibold text-cyan-800 hover:bg-cyan-100"
              >
                {example}
              </button>
            ))}
          </div>
          {instructionSummary && (
            <p className="mt-2 text-[10px] font-semibold text-cyan-700">{instructionSummary}</p>
          )}
        </div>

        <button
          type="submit"
          disabled={submitting}
          className="w-full rounded bg-indigo-600 px-2 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {submitting ? "Filtering..." : "Filter & Download"}
        </button>

        <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
          <button
            type="button"
            onClick={handleAnalyze}
            disabled={analysisLoading}
            className="w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-[11px] font-semibold text-slate-700 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {analysisLoading ? "Analyzing..." : "Analyze File With AI"}
          </button>
          <button
            type="button"
            onClick={handleRevert}
            className="w-full rounded border border-amber-300 bg-amber-50 px-2 py-1.5 text-[11px] font-semibold text-amber-800 hover:bg-amber-100"
          >
            Revert Last Apply
          </button>
        </div>
      </form>

      {error && <p className="mt-2 text-[10px] font-semibold text-red-600">{error}</p>}
      {message && <p className="mt-2 text-[10px] font-semibold text-emerald-700">{message}</p>}
      {analysisError && <p className="mt-2 text-[10px] font-semibold text-red-600">{analysisError}</p>}

      {analysis && (
        <div className="mt-3 rounded-xl border border-slate-200 bg-white p-3">
          <div className="flex items-center justify-between gap-2">
            <h4 className="text-[11px] font-bold uppercase tracking-wider text-slate-700">AI Filter Diagnostics</h4>
            <button
              type="button"
              onClick={handleApplyChangesToDb}
              disabled={applyLoading || !file}
              className="rounded border border-indigo-300 bg-indigo-50 px-2 py-1 text-[10px] font-bold text-indigo-700 hover:bg-indigo-100 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {applyLoading ? "Applying..." : "Apply Changes & Upsert To DB"}
            </button>
          </div>
          <p className="mt-1 text-[10px] text-slate-600">
            Coverage: {analysis.mapping_quality.required_found}/{analysis.mapping_quality.required_total} required fields
            {" "}({Math.round((analysis.mapping_quality.coverage || 0) * 100)}%)
          </p>
          <p className="mt-1 text-[10px] text-slate-600">
            Uploaded rows: {analysis.rows_in} | Rows prepared for DB: {analysis.rows_after_filter}
          </p>
          <p className="mt-1 text-[10px] text-slate-600">
            Primary key: {analysis.key_detection.primary_key_name}
            {analysis.key_detection.key_column
              ? ` via ${analysis.key_detection.key_column}`
              : analysis.key_detection.key_columns?.length
                ? ` via ${analysis.key_detection.key_columns.join(", ")}`
                : " via fallback hash"}
          </p>
          <p className="mt-1 text-[10px] text-slate-600">
            Candidate key columns: {(analysis.key_detection.key_candidates || []).slice(0, 6).join(", ") || "none"}
          </p>
          {analysis.db_match && (
            <div className="mt-2 rounded border border-blue-100 bg-blue-50 p-2">
              <div className="text-[10px] font-bold uppercase tracking-wider text-blue-700">DB Match Preview</div>
              <div className="mt-1 text-[10px] text-blue-900">
                Existing rows in scope: {analysis.db_match.rows_in_scope} | Matched existing rows: {analysis.db_match.existing_rows_matched} | New rows: {analysis.db_match.new_rows_detected}
              </div>
              <div className="mt-1 text-[10px] text-blue-800">
                Match ratio: {Math.round((analysis.db_match.match_ratio || 0) * 100)}%
              </div>
            </div>
          )}
          {aiInstruction.trim() && (
            <div className="mt-2 rounded border border-cyan-100 bg-cyan-50 p-2 text-[10px] text-cyan-900">
              <div className="font-bold uppercase tracking-wider text-cyan-800">Active AI Instruction</div>
              <div className="mt-1 whitespace-pre-wrap">{aiInstruction.trim()}</div>
            </div>
          )}

          <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-2">
            <div className="rounded border border-red-100 bg-red-50 p-2">
              <div className="text-[10px] font-bold uppercase tracking-wider text-red-700">What Is Wrong</div>
              {analysis.issues.length ? (
                <ul className="mt-1 space-y-1 text-[10px] text-red-800">
                  {analysis.issues.map((item, idx) => (
                    <li key={`issue-${idx}`}>- {item}</li>
                  ))}
                </ul>
              ) : (
                <p className="mt-1 text-[10px] text-emerald-700">No blocking issue detected.</p>
              )}
            </div>
            <div className="rounded border border-emerald-100 bg-emerald-50 p-2">
              <div className="text-[10px] font-bold uppercase tracking-wider text-emerald-700">Changes AI Will Make</div>
              <ul className="mt-1 space-y-1 text-[10px] text-emerald-800">
                {analysis.planned_changes.map((item, idx) => (
                  <li key={`plan-${idx}`}>- {item}</li>
                ))}
              </ul>
            </div>
          </div>

          <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-2">
            <div className="rounded border border-slate-200 bg-slate-50 p-2">
              <div className="text-[10px] font-bold uppercase tracking-wider text-slate-700">Right Mappings</div>
              {analysis.right_mappings.length ? (
                <ul className="mt-1 space-y-1 text-[10px] text-slate-700">
                  {analysis.right_mappings.slice(0, 6).map((item, idx) => (
                    <li key={`right-${idx}`}>- {item.field}: {item.column} ({Math.round((item.confidence || 0) * 100)}%)</li>
                  ))}
                </ul>
              ) : (
                <p className="mt-1 text-[10px] text-slate-500">No high-confidence mappings yet.</p>
              )}
            </div>
            <div className="rounded border border-slate-200 bg-slate-50 p-2">
              <div className="text-[10px] font-bold uppercase tracking-wider text-slate-700">Wrong / Low Confidence</div>
              {analysis.wrong_mappings.length ? (
                <ul className="mt-1 space-y-1 text-[10px] text-slate-700">
                  {analysis.wrong_mappings.slice(0, 6).map((item, idx) => (
                    <li key={`wrong-${idx}`}>- {item.field}: {item.issue || "Needs review"}</li>
                  ))}
                </ul>
              ) : (
                <p className="mt-1 text-[10px] text-emerald-700">No missing required mapping found.</p>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
