"use client"

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react"
import {
  AdminFileItem,
  AdminUser,
  createAdminUser,
  deleteAdminFile,
  deleteAdminUser,
  downloadAdminFile,
  fetchAdminFiles,
  fetchAdminUsers,
  replaceAdminFile,
  updateAdminUserPassword,
} from "@/app/lib/api"

type Props = {
  isAdmin: boolean
}

type AdminSection = "data" | "users"

export default function AdminFileAccess({ isAdmin }: Props) {
  const [open, setOpen] = useState(false)
  const [section, setSection] = useState<AdminSection>("data")

  if (!isAdmin) return null

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="w-full rounded-xl border border-indigo-200 bg-indigo-50 px-3 py-2 text-left"
      >
        <div className="text-[11px] font-bold uppercase tracking-wider text-indigo-700">
          Admin Manual Access
        </div>
        <div className="text-[10px] text-indigo-500 mt-1">Open admin tools</div>
      </button>

      {open && (
        <div className="fixed inset-0 z-[120] bg-black/40 backdrop-blur-[1px] p-4 sm:p-8">
          <div className="mx-auto h-full max-w-6xl rounded-2xl border border-slate-200 bg-white shadow-2xl flex flex-col overflow-hidden">
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
            </div>

            <div className="flex-1 overflow-auto p-5">
              {section === "data" ? <DataUpdationPanel /> : <UserManagementPanel />}
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

  const knownSources = useMemo(() => ["samsung", "samsung_vs", "samsung_croma", "reliance", "godrej"], [])

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

  const handleReplace = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    if (!file) {
      setError("Select a file to update the selected tag")
      return
    }

    setSubmitting(true)
    setError("")
    setMessage("")
    try {
      const res = await replaceAdminFile({
        file,
        source,
        dataset_type: datasetType,
        job_id: jobId || undefined,
      })
      setMessage(`Updated tag. Deleted ${res.deleted_rows} rows, inserted ${res.rows_inserted} rows.`)
      setFile(null)
      await loadItems()
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update file tag")
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
    setMessage(`Selected tag ${item.tag}. Choose a file and click Replace Tag.`)
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

      <form onSubmit={handleReplace} className="mb-3 space-y-2">
        <div className="grid grid-cols-2 gap-2">
          <select
            value={source}
            onChange={(e) => setSource(e.target.value)}
            className="rounded border border-gray-300 bg-white px-2 py-1 text-[11px]"
          >
            {knownSources.map((src) => (
              <option key={src} value={src}>
                {src}
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
          placeholder="job_id tag (optional)"
          className="w-full rounded border border-gray-300 bg-white px-2 py-1 text-[11px]"
        />
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
          {submitting ? "Replacing..." : "Replace Tag Data (Update)"}
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
              <th className="px-2 py-1 font-bold">Actions</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={5} className="px-2 py-2 text-gray-500">Loading...</td>
              </tr>
            ) : items.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-2 py-2 text-gray-500">No tagged files found.</td>
              </tr>
            ) : (
              items.map((item) => (
                <tr key={item.tag} className="border-t border-gray-100">
                  <td className="px-2 py-1.5 text-gray-700">{item.source}</td>
                  <td className="px-2 py-1.5 text-gray-700">{item.dataset_type}</td>
                  <td className="px-2 py-1.5 text-gray-700">{item.job_id || "untagged"}</td>
                  <td className="px-2 py-1.5 font-semibold text-gray-800">{item.rows}</td>
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

  const [search, setSearch] = useState("")
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [role, setRole] = useState<"admin" | "employee">("employee")
  const [submitting, setSubmitting] = useState(false)

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
          <option value="employee">employee</option>
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
                  <td className="px-2 py-1.5 text-slate-700">{user.role}</td>
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
    </div>
  )
}
