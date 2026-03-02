"use client"

type GoogleTokenResponse = {
  access_token?: string
  expires_in?: number
  error?: string
  error_description?: string
}

type GoogleTokenClient = {
  requestAccessToken: (options?: { prompt?: string }) => void
}

type GoogleInitTokenClient = (config: {
  client_id: string
  scope: string
  callback: (response: GoogleTokenResponse) => void
}) => GoogleTokenClient

type GoogleUploadResult = {
  id: string
  name?: string
  mimeType?: string
  webViewLink?: string
}

declare global {
  interface Window {
    google?: {
      accounts?: {
        oauth2?: {
          initTokenClient?: GoogleInitTokenClient
        }
      }
    }
  }
}

const GOOGLE_IDENTITY_SRC = "https://accounts.google.com/gsi/client"
const GOOGLE_DRIVE_FILE_SCOPE = "https://www.googleapis.com/auth/drive.file"

let googleScriptPromise: Promise<void> | null = null
let cachedToken = ""
let cachedTokenExpiresAt = 0

const nowMs = () => Date.now()

const isTokenUsable = () => cachedToken && cachedTokenExpiresAt > nowMs() + 15_000

function loadGoogleIdentityScript(): Promise<void> {
  if (typeof window === "undefined") {
    return Promise.reject(new Error("Google identity is only available in browser context."))
  }
  if (window.google?.accounts?.oauth2?.initTokenClient) {
    return Promise.resolve()
  }
  if (googleScriptPromise) return googleScriptPromise

  googleScriptPromise = new Promise<void>((resolve, reject) => {
    const existing = document.querySelector(`script[src="${GOOGLE_IDENTITY_SRC}"]`) as HTMLScriptElement | null
    if (existing) {
      existing.addEventListener("load", () => resolve(), { once: true })
      existing.addEventListener("error", () => reject(new Error("Failed to load Google identity script.")), {
        once: true,
      })
      return
    }

    const script = document.createElement("script")
    script.src = GOOGLE_IDENTITY_SRC
    script.async = true
    script.defer = true
    script.onload = () => resolve()
    script.onerror = () => reject(new Error("Failed to load Google identity script."))
    document.head.appendChild(script)
  })

  return googleScriptPromise
}

export async function getGoogleDriveAccessToken(clientId: string): Promise<string> {
  const normalizedClientId = String(clientId || "").trim()
  if (!normalizedClientId) {
    throw new Error("Google Slides integration is not configured. Missing Google Client ID.")
  }
  if (isTokenUsable()) return cachedToken

  await loadGoogleIdentityScript()
  const initTokenClient = window.google?.accounts?.oauth2?.initTokenClient
  if (!initTokenClient) {
    throw new Error("Google OAuth client is unavailable in this browser.")
  }

  return new Promise<string>((resolve, reject) => {
    try {
      const tokenClient = initTokenClient({
        client_id: normalizedClientId,
        scope: GOOGLE_DRIVE_FILE_SCOPE,
        callback: (response) => {
          if (response?.error) {
            const detail = response.error_description || response.error
            reject(new Error(`Google authorization failed: ${detail}`))
            return
          }
          const token = String(response?.access_token || "").trim()
          if (!token) {
            reject(new Error("Google authorization did not return an access token."))
            return
          }
          const expiresInSec = Number(response?.expires_in || 0)
          cachedToken = token
          cachedTokenExpiresAt = nowMs() + Math.max(expiresInSec, 300) * 1000
          resolve(token)
        },
      })
      tokenClient.requestAccessToken({ prompt: "" })
    } catch (err: unknown) {
      reject(err instanceof Error ? err : new Error("Failed to initialize Google token client."))
    }
  })
}

export async function uploadPptxAsGoogleSlides(args: {
  accessToken: string
  blob: Blob
  filename: string
  title?: string
}): Promise<GoogleUploadResult> {
  const accessToken = String(args.accessToken || "").trim()
  if (!accessToken) {
    throw new Error("Missing Google access token.")
  }

  const baseName = String(args.title || args.filename || "Partner Deck")
    .replace(/\.pptx$/i, "")
    .trim()

  const metadata = {
    name: baseName || "Partner Deck",
    mimeType: "application/vnd.google-apps.presentation",
  }

  const boundary = `codex_${Date.now()}_${Math.random().toString(16).slice(2)}`
  const body = new Blob(
    [
      `--${boundary}\r\n`,
      "Content-Type: application/json; charset=UTF-8\r\n\r\n",
      JSON.stringify(metadata),
      "\r\n",
      `--${boundary}\r\n`,
      "Content-Type: application/vnd.openxmlformats-officedocument.presentationml.presentation\r\n\r\n",
      args.blob,
      "\r\n",
      `--${boundary}--`,
    ],
    { type: `multipart/related; boundary=${boundary}` }
  )

  const response = await fetch(
    "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,mimeType,webViewLink",
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": `multipart/related; boundary=${boundary}`,
      },
      body,
    }
  )

  if (!response.ok) {
    const raw = await response.text()
    let detail = raw
    try {
      const parsed = JSON.parse(raw)
      const msg = parsed?.error?.message || parsed?.error_description || parsed?.message
      if (typeof msg === "string" && msg.trim()) {
        detail = msg.trim()
      }
    } catch {
      // keep raw text
    }
    throw new Error(`Google Slides upload failed (HTTP ${response.status}): ${detail}`)
  }

  const payload = (await response.json()) as GoogleUploadResult
  if (!payload?.id) {
    throw new Error("Google Slides upload succeeded but no file id was returned.")
  }
  return payload
}

export function buildGoogleSlidesEditUrl(fileId: string): string {
  const id = String(fileId || "").trim()
  if (!id) return ""
  return `https://docs.google.com/presentation/d/${encodeURIComponent(id)}/edit`
}

