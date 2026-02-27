"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { createPortal } from "react-dom"
import {
  Bot,
  Download,
  FileUp,
  Loader2,
  Maximize2,
  MessageCircle,
  Minimize2,
  Send,
  X,
} from "lucide-react"
import {
  sendChatbotMessage,
  transformChatbotFile,
  type ChatbotTurn,
} from "@/app/lib/api"

type ChatMessage = {
  role: "user" | "assistant"
  content: string
}

type Props = {
  variant?: "floating" | "card"
}

type AssistantBlock =
  | { kind: "paragraph"; text: string }
  | { kind: "ordered"; items: string[] }
  | { kind: "unordered"; items: string[] }

const INITIAL_ASSISTANT_MESSAGE: ChatMessage = {
  role: "assistant",
  content: "AI Sahyogi is ready. Ask dashboard questions, or upload a CSV/XLS/XLSX file and tell me what to fill.",
}

const FILE_INSTRUCTION_EXAMPLES = [
  "Plan Price will be Total Billing Amount",
  "Brand is Article_Brand",
  "fill missing Plan Category with ADLD",
  "remove duplicates from column Item_Serial_Number",
]

const normalizeDate = (value: string) => {
  const cleaned = (value || "").trim()
  return /^\d{4}-\d{2}-\d{2}$/.test(cleaned) ? cleaned : ""
}

const normalizeJobId = (value: string) => {
  const cleaned = (value || "").trim()
  if (!cleaned) return ""
  if (cleaned.toLowerCase() === "all" || cleaned.toLowerCase() === "null" || cleaned.toLowerCase() === "undefined") {
    return ""
  }
  return cleaned
}

const normalizeSource = (value: string) => {
  const key = (value || "").trim().toLowerCase()
  if (!key) return ""
  if (key === "goodrej" || key === "goddrej") return "godrej"
  if (key === "reliance resq" || key === "reliance_resq" || key === "reliance-resq" || key === "resq") {
    return "reliance"
  }
  return key
}

const normalizeDatasetType = (value: string) => {
  return (value || "").trim().toLowerCase() === "claims" ? "claims" : "sales"
}

const splitToSentences = (text: string) =>
  text
    .replace(/([.!?])\s+(?=[A-Z0-9])/g, "$1\n")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)

const formatAssistantText = (content: string) => {
  if (!content) return ""
  let text = content.replace(/\r/g, "\n").replace(/\t/g, " ").trim()
  text = text.replace(/[ \u00A0]{2,}/g, " ")
  text = text.replace(/([A-Za-z])\s*:\s*(?=\d+[.)]\s)/g, "$1:\n")
  text = text.replace(/([.?!])\s+(?=\d+[.)]\s)/g, "$1\n")
  text = text.replace(/([.?!])\s+(?=[-•]\s)/g, "$1\n")
  text = text.replace(/\n{3,}/g, "\n\n")
  return text.trim()
}

const parseAssistantBlocks = (content: string): AssistantBlock[] => {
  const normalized = formatAssistantText(content)
  if (!normalized) return []

  const lines = normalized
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)

  const blocks: AssistantBlock[] = []
  let idx = 0

  while (idx < lines.length) {
    const ordered = lines[idx]?.match(/^\d+[.)]\s+(.*)$/)
    if (ordered) {
      const items: string[] = []
      while (idx < lines.length) {
        const match = lines[idx]?.match(/^\d+[.)]\s+(.*)$/)
        if (!match) break
        items.push(match[1].trim())
        idx += 1
      }
      if (items.length) {
        blocks.push({ kind: "ordered", items })
      }
      continue
    }

    const bullet = lines[idx]?.match(/^[-•]\s+(.*)$/)
    if (bullet) {
      const items: string[] = []
      while (idx < lines.length) {
        const match = lines[idx]?.match(/^[-•]\s+(.*)$/)
        if (!match) break
        items.push(match[1].trim())
        idx += 1
      }
      if (items.length) {
        blocks.push({ kind: "unordered", items })
      }
      continue
    }

    const line = lines[idx]
    if (line.length > 260) {
      const sentences = splitToSentences(line)
      if (sentences.length >= 3) {
        blocks.push({ kind: "unordered", items: sentences })
      } else {
        blocks.push({ kind: "paragraph", text: line })
      }
    } else {
      blocks.push({ kind: "paragraph", text: line })
    }
    idx += 1
  }

  if (!blocks.length) {
    return [{ kind: "paragraph", text: normalized }]
  }
  return blocks
}

const renderInlineRichText = (text: string) => {
  const parts = text.split(/(\*\*[^*]+\*\*)/g).filter(Boolean)
  return parts.map((part, index) => {
    if (part.startsWith("**") && part.endsWith("**")) {
      return (
        <strong key={`${part}-${index}`} className="font-semibold text-slate-900">
          {part.slice(2, -2)}
        </strong>
      )
    }
    return <span key={`${part}-${index}`}>{part}</span>
  })
}

export default function RightSideChatbot({ variant = "floating" }: Props) {
  const isCard = variant === "card"
  const [isMounted, setIsMounted] = useState(false)
  const [isOpen, setIsOpen] = useState(false)
  const [isExpanded, setIsExpanded] = useState(false)
  const [messages, setMessages] = useState<ChatMessage[]>([INITIAL_ASSISTANT_MESSAGE])
  const [input, setInput] = useState("")
  const [isSending, setIsSending] = useState(false)
  const [isTransforming, setIsTransforming] = useState(false)
  const [transformFile, setTransformFile] = useState<File | null>(null)
  const [downloadUrl, setDownloadUrl] = useState("")
  const [downloadName, setDownloadName] = useState("")
  const [transformSummary, setTransformSummary] = useState("")

  const cardListRef = useRef<HTMLDivElement | null>(null)
  const panelListRef = useRef<HTMLDivElement | null>(null)
  const fullListRef = useRef<HTMLDivElement | null>(null)
  const contextKeyRef = useRef("")

  useEffect(() => {
    setIsMounted(true)
  }, [])

  useEffect(() => {
    const target = isExpanded
      ? fullListRef.current
      : isCard
        ? cardListRef.current
        : isOpen
          ? panelListRef.current
          : null

    if (!target) return
    target.scrollTop = target.scrollHeight
  }, [messages, isCard, isOpen, isExpanded])

  useEffect(() => {
    if (!isExpanded) return

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setIsExpanded(false)
      }
    }

    window.addEventListener("keydown", onKeyDown)
    return () => window.removeEventListener("keydown", onKeyDown)
  }, [isExpanded])

  useEffect(() => {
    return () => {
      if (downloadUrl) {
        URL.revokeObjectURL(downloadUrl)
      }
    }
  }, [downloadUrl])

  useEffect(() => {
    if (typeof window === "undefined") return

    const readContextKey = () => {
      const source = normalizeSource(localStorage.getItem("dashboard_brand") || "")
      const datasetType = normalizeDatasetType(localStorage.getItem("dashboard_mode") || "")
      return `${source}|${datasetType}`
    }

    const resetForContextShift = () => {
      const nextKey = readContextKey()
      if (!contextKeyRef.current) {
        contextKeyRef.current = nextKey
        return
      }
      if (contextKeyRef.current === nextKey) return

      contextKeyRef.current = nextKey
      setMessages([INITIAL_ASSISTANT_MESSAGE])
      setInput("")
      setTransformFile(null)
      setTransformSummary("")
      setDownloadName("")
      setDownloadUrl((prev) => {
        if (prev) URL.revokeObjectURL(prev)
        return ""
      })
    }

    contextKeyRef.current = readContextKey()
    window.addEventListener("dashboard-context-changed", resetForContextShift as EventListener)
    // Fallback for any context changes that do not dispatch the custom event.
    const pollId = window.setInterval(resetForContextShift, 1000)

    return () => {
      window.removeEventListener("dashboard-context-changed", resetForContextShift as EventListener)
      window.clearInterval(pollId)
    }
  }, [])

  const history: ChatbotTurn[] = useMemo(
    () =>
      messages
        .slice(-10)
        .map((msg) => ({ role: msg.role, content: msg.content })),
    [messages]
  )

  const openFullChat = () => {
    if (!isCard) {
      setIsOpen(true)
    }
    setIsExpanded(true)
  }

  const appendAssistantMessage = (content: string) => {
    setMessages((prev) => [...prev, { role: "assistant", content }])
  }

  const getAuthToken = () =>
    typeof window !== "undefined" ? (localStorage.getItem("auth_token") || "").trim() : ""

  const ensureAuthorized = () => {
    const token = getAuthToken()
    if (token) return token
    appendAssistantMessage("Sign in first to use the chatbot.")
    return ""
  }

  const sendChatMessage = async () => {
    const text = input.trim()
    if (!text || isSending || isTransforming) return

    setInput("")
    setMessages((prev) => [...prev, { role: "user", content: text }])

    if (!ensureAuthorized()) {
      return
    }

    setIsSending(true)
    try {
      const compactText = text.replace(/\s+/g, " ").trim()
      const maxTokens = Math.max(900, Math.min(4096, Math.ceil(compactText.length * 4.5)))

      const useJobFilter =
        typeof window !== "undefined" && localStorage.getItem("use_job_filter") === "1"
      const jobId = normalizeJobId(
        typeof window !== "undefined" && useJobFilter ? localStorage.getItem("job_id") || "" : ""
      )
      const fromDate = normalizeDate(
        typeof window !== "undefined" ? localStorage.getItem("dashboard_from_date") || "" : ""
      )
      const toDate = normalizeDate(
        typeof window !== "undefined" ? localStorage.getItem("dashboard_to_date") || "" : ""
      )
      const source = normalizeSource(
        typeof window !== "undefined" ? localStorage.getItem("dashboard_brand") || "" : ""
      )
      const datasetType = normalizeDatasetType(
        typeof window !== "undefined" ? localStorage.getItem("dashboard_mode") || "" : ""
      )

      const result = await sendChatbotMessage({
        message: text,
        history,
        temperature: 0.14,
        max_tokens: maxTokens,
        source: source || undefined,
        dataset_type: datasetType,
        job_id: jobId || undefined,
        from_date: fromDate || undefined,
        to_date: toDate || undefined,
      })
      const reply = (result.response || "").trim() || "No response generated."
      appendAssistantMessage(reply)
    } catch (err) {
      const detail =
        err instanceof Error
          ? err.message
          : "Chatbot is unavailable right now. Check Sarvam service configuration."
      appendAssistantMessage(detail)
    } finally {
      setIsSending(false)
    }
  }

  const applyFileInstruction = async () => {
    const instruction = input.trim()
    if (!transformFile || !instruction || isTransforming || isSending) return

    setMessages((prev) => [...prev, { role: "user", content: `File task: ${instruction}` }])
    setInput("")

    if (!ensureAuthorized()) {
      return
    }

    const source = normalizeSource(
      typeof window !== "undefined" ? localStorage.getItem("dashboard_brand") || "" : ""
    )
    const datasetType = normalizeDatasetType(
      typeof window !== "undefined" ? localStorage.getItem("dashboard_mode") || "" : ""
    )

    setIsTransforming(true)
    try {
      const result = await transformChatbotFile({
        file: transformFile,
        instruction,
        source: source || undefined,
        dataset_type: datasetType,
      })

      if (downloadUrl) {
        URL.revokeObjectURL(downloadUrl)
      }
      const nextUrl = URL.createObjectURL(result.blob)
      setDownloadUrl(nextUrl)
      setDownloadName(result.filename)
      setTransformSummary(result.summary || "File updated successfully.")
      appendAssistantMessage(
        `${result.summary || "File updated."} Download is ready below as ${result.filename}.`
      )
    } catch (err) {
      const detail = err instanceof Error ? err.message : "Failed to update the uploaded file."
      appendAssistantMessage(detail)
    } finally {
      setIsTransforming(false)
    }
  }

  const onInputKeyDown = (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault()
      void sendChatMessage()
    }
  }

  const renderChatWindow = (mode: "card" | "panel" | "full") => {
    const isFullMode = mode === "full"
    const isPanelMode = mode === "panel"

    const listRef =
      mode === "full"
        ? fullListRef
        : mode === "card"
          ? cardListRef
          : panelListRef

    const containerClass = isFullMode
      ? "flex h-[min(92dvh,760px)] w-[min(98vw,960px)] flex-col overflow-hidden rounded-2xl border border-slate-200/90 bg-white shadow-[0_24px_80px_-28px_rgba(15,23,42,0.45)] sm:rounded-3xl"
      : mode === "card"
        ? "flex h-[380px] w-full flex-col overflow-hidden rounded-2xl border border-slate-200/90 bg-white shadow-[0_16px_50px_-30px_rgba(15,23,42,0.4)] sm:h-[440px]"
        : "pointer-events-auto flex h-[min(76dvh,700px)] w-[min(95vw,420px)] flex-col overflow-hidden rounded-2xl border border-slate-200/90 bg-white shadow-2xl sm:mr-3"

    return (
      <aside className={containerClass}>
        <header className={`flex items-center justify-between border-b border-slate-200/80 bg-gradient-to-r from-white via-slate-50 to-indigo-50/40 ${isFullMode ? "px-4 py-3 sm:px-5 sm:py-4" : "px-3 py-2.5 sm:px-4 sm:py-3"}`}>
          <div className="flex items-center gap-2.5">
            <span className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-indigo-100 bg-indigo-50 text-indigo-600">
              <Bot size={15} />
            </span>
            <div className="leading-tight">
              <h3 className="text-sm font-bold text-slate-800">AI Sahyogi</h3>
              <p className="text-[11px] font-medium text-slate-500">Dashboard Assistant</p>
            </div>
          </div>

          <div className="flex items-center gap-1.5">
            <span className="hidden items-center gap-1 rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-[10px] font-semibold text-emerald-700 sm:inline-flex">
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
              Live
            </span>
            {!isFullMode && (
              <button
                type="button"
                onClick={openFullChat}
                className={mode === "card"
                  ? "inline-flex items-center gap-1 rounded-lg border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600 hover:bg-slate-50"
                  : "rounded-full p-1 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
                }
                aria-label="Open full chat"
              >
                <Maximize2 size={14} />
                {mode === "card" && <span>Full Chat</span>}
              </button>
            )}

            {isFullMode && (
              <button
                type="button"
                onClick={() => setIsExpanded(false)}
                className="rounded-full p-1 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
                aria-label="Exit full chat"
              >
                <Minimize2 size={16} />
              </button>
            )}

            {isPanelMode && (
              <button
                type="button"
                onClick={() => {
                  setIsOpen(false)
                  setIsExpanded(false)
                }}
                className="rounded-full p-1 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
                aria-label="Close chatbot"
              >
                <X size={16} />
              </button>
            )}
          </div>
        </header>

        <div
          ref={listRef}
          className={`flex-1 space-y-4 overflow-y-auto bg-gradient-to-b from-slate-50/85 via-white to-slate-50/65 ${isFullMode ? "px-4 py-4 sm:px-5 sm:py-5" : "px-3 py-3.5"}`}
        >
          {messages.map((message, idx) => {
            const isUser = message.role === "user"
            const assistantBlocks = !isUser ? parseAssistantBlocks(message.content) : []
            return (
              <div
                key={`${message.role}-${idx}-${message.content.slice(0, 24)}`}
                className={`flex w-full ${isUser ? "justify-end" : "justify-start"}`}
              >
                <div className={`flex max-w-[96%] items-end gap-2 ${isUser ? "flex-row-reverse" : "flex-row"}`}>
                  <span
                    className={`inline-flex h-7 w-7 shrink-0 items-center justify-center rounded-full text-[11px] font-bold ${
                      isUser
                        ? "bg-slate-900 text-white"
                        : "border border-indigo-100 bg-indigo-50 text-indigo-600"
                    }`}
                  >
                    {isUser ? "You" : <Bot size={14} />}
                  </span>

                  <div
                    className={`rounded-2xl border px-3.5 py-2.5 text-sm leading-relaxed ${
                      isUser
                        ? "rounded-br-md border-slate-900 bg-slate-900 text-white shadow-[0_14px_30px_-18px_rgba(15,23,42,0.65)]"
                        : "rounded-bl-md border-slate-200 bg-white text-slate-800 shadow-[0_12px_24px_-18px_rgba(15,23,42,0.35)]"
                    }`}
                  >
                    {isUser ? (
                      <p className="whitespace-pre-wrap break-words">{message.content}</p>
                    ) : (
                      <div className="space-y-2.5 text-[13px] text-slate-700">
                        {assistantBlocks.map((block, blockIdx) => {
                          if (block.kind === "paragraph") {
                            return (
                              <p key={`p-${blockIdx}`} className="whitespace-pre-wrap break-words leading-6">
                                {renderInlineRichText(block.text)}
                              </p>
                            )
                          }
                          if (block.kind === "ordered") {
                            return (
                              <ol key={`o-${blockIdx}`} className="ml-4 list-decimal space-y-1.5 pr-1 leading-6 marker:font-semibold marker:text-slate-500">
                                {block.items.map((item, itemIdx) => (
                                  <li key={`oi-${itemIdx}`} className="break-words">
                                    {renderInlineRichText(item)}
                                  </li>
                                ))}
                              </ol>
                            )
                          }
                          return (
                            <ul key={`u-${blockIdx}`} className="ml-4 list-disc space-y-1.5 pr-1 leading-6 marker:text-slate-500">
                              {block.items.map((item, itemIdx) => (
                                <li key={`ui-${itemIdx}`} className="break-words">
                                  {renderInlineRichText(item)}
                                </li>
                              ))}
                            </ul>
                          )
                        })}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )
          })}
          {isSending && (
            <div className="flex w-full justify-start">
              <div className="flex items-end gap-2">
                <span className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-indigo-100 bg-indigo-50 text-indigo-600">
                  <Bot size={14} />
                </span>
                <div className="inline-flex items-center gap-2 rounded-2xl rounded-bl-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm">
                  <Loader2 size={14} className="animate-spin" />
                  Thinking...
                </div>
              </div>
            </div>
          )}
          {isTransforming && (
            <div className="flex w-full justify-start">
              <div className="flex items-end gap-2">
                <span className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-cyan-100 bg-cyan-50 text-cyan-700">
                  <FileUp size={14} />
                </span>
                <div className="inline-flex items-center gap-2 rounded-2xl rounded-bl-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm">
                  <Loader2 size={14} className="animate-spin" />
                  Updating file...
                </div>
              </div>
            </div>
          )}
        </div>

        <div className={`border-t border-slate-200/80 bg-white/95 ${isFullMode ? "p-3 sm:p-4" : "p-3"}`}>
          <div className="mb-2 flex flex-wrap items-center gap-2">
            <label className="inline-flex cursor-pointer items-center gap-1.5 rounded-lg border border-slate-300 bg-white px-2.5 py-1.5 text-[11px] font-semibold text-slate-700 hover:bg-slate-50">
              <FileUp size={13} />
              {transformFile ? "Change File" : "Upload File"}
              <input
                type="file"
                accept=".csv,.xlsx,.xls"
                className="hidden"
                onChange={(event) => {
                  const next = event.target.files?.[0] || null
                  setTransformFile(next)
                  setDownloadName("")
                  setTransformSummary("")
                  if (downloadUrl) {
                    URL.revokeObjectURL(downloadUrl)
                    setDownloadUrl("")
                  }
                }}
              />
            </label>
            {transformFile && (
              <button
                type="button"
                onClick={() => {
                  setTransformFile(null)
                  setDownloadName("")
                  setTransformSummary("")
                  if (downloadUrl) {
                    URL.revokeObjectURL(downloadUrl)
                    setDownloadUrl("")
                  }
                }}
                className="rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-[11px] font-semibold text-slate-600 hover:bg-slate-50"
              >
                Clear
              </button>
            )}
            {transformFile && (
              <span className="truncate text-[11px] text-slate-500" title={transformFile.name}>
                {transformFile.name}
              </span>
            )}
          </div>
          {transformFile && (
            <div className="mb-2 flex flex-wrap gap-1.5">
              {FILE_INSTRUCTION_EXAMPLES.map((example) => (
                <button
                  key={example}
                  type="button"
                  onClick={() => setInput(example)}
                  className="rounded-full border border-cyan-200 bg-cyan-50 px-2 py-1 text-[10px] font-semibold text-cyan-800 hover:bg-cyan-100"
                  title={example}
                >
                  {example}
                </button>
              ))}
            </div>
          )}

          <div className="flex items-end gap-2">
            <textarea
              value={input}
              onChange={(event) => setInput(event.target.value)}
              onKeyDown={onInputKeyDown}
              rows={isFullMode ? 3 : 2}
              placeholder={transformFile ? "Describe what to fill in the uploaded file..." : "Ask about dashboard insights..."}
              className="min-h-[44px] flex-1 resize-none rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm text-slate-800 outline-none ring-indigo-500 transition focus:border-indigo-300 focus:ring-2"
            />
            <button
              type="button"
              onClick={() => void sendChatMessage()}
              disabled={!input.trim() || isSending || isTransforming}
              className={`flex items-center justify-center rounded-xl bg-indigo-600 text-white transition hover:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-50 ${isFullMode ? "h-11 w-11" : "h-10 w-10"}`}
              aria-label="Send message"
            >
              <Send size={15} />
            </button>
            <button
              type="button"
              onClick={() => void applyFileInstruction()}
              disabled={!transformFile || !input.trim() || isSending || isTransforming}
              className={`flex items-center justify-center rounded-xl bg-cyan-600 px-3 text-white transition hover:bg-cyan-700 disabled:cursor-not-allowed disabled:opacity-50 ${isFullMode ? "h-11" : "h-10"}`}
              aria-label="Apply instruction to file"
              title="Apply instruction to uploaded file"
            >
              <FileUp size={14} />
            </button>
          </div>
          {downloadUrl && downloadName && (
            <a
              href={downloadUrl}
              download={downloadName}
              className="mt-2 inline-flex items-center gap-1.5 rounded-lg border border-emerald-200 bg-emerald-50 px-2.5 py-1.5 text-[11px] font-semibold text-emerald-700 hover:bg-emerald-100"
            >
              <Download size={13} />
              Download Updated File
            </a>
          )}
          {transformSummary && (
            <p className="mt-2 text-[11px] font-medium text-slate-500">{transformSummary}</p>
          )}
          <p className="mt-2 text-[11px] font-medium text-slate-400">
            Press Enter to send chat. Upload a file and click the cyan button to apply your fill instruction.
          </p>
        </div>
      </aside>
    )
  }

  const overlayContent = isExpanded ? (
    <div className="fixed inset-0 z-[220] flex items-center justify-center bg-slate-950/35 p-2 sm:p-4 backdrop-blur-sm">
      {renderChatWindow("full")}
    </div>
  ) : null

  const expandedOverlay =
    isMounted && overlayContent
      ? createPortal(overlayContent, document.body)
      : null

  if (isCard) {
    return (
      <>
        {renderChatWindow("card")}
        {expandedOverlay}
      </>
    )
  }

  return (
    <>
      <div className="pointer-events-none fixed bottom-4 right-3 z-[120] sm:bottom-auto sm:right-0 sm:top-1/2 sm:-translate-y-1/2">
        {!isOpen ? (
          <button
            type="button"
            onClick={() => setIsOpen(true)}
            className="pointer-events-auto rounded-2xl border border-slate-200 bg-white px-3 py-2.5 shadow-xl transition hover:bg-slate-50 sm:mr-0 sm:rounded-l-2xl sm:rounded-r-none sm:px-3 sm:py-4"
            aria-label="Open chatbot"
          >
            <span className="flex items-center gap-2">
              <span className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-indigo-50 text-indigo-600">
                <MessageCircle size={16} />
              </span>
              <span className="text-xs font-bold text-slate-700">AI Sahyogi</span>
            </span>
          </button>
        ) : (
          renderChatWindow("panel")
        )}
      </div>

      {expandedOverlay}
    </>
  )
}
