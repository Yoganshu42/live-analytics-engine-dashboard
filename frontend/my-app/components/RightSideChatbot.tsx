"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { createPortal } from "react-dom"
import {
  Bot,
  Loader2,
  Maximize2,
  MessageCircle,
  Minimize2,
  Send,
  X,
} from "lucide-react"
import { sendChatbotMessage, type ChatbotTurn } from "@/app/lib/api"

type ChatMessage = {
  role: "user" | "assistant"
  content: string
}

type Props = {
  variant?: "floating" | "card"
}

const INITIAL_ASSISTANT_MESSAGE: ChatMessage = {
  role: "assistant",
  content: "AI Sahyogi is ready. Ask about trends, anomalies, or actions from your dashboard data.",
}

const normalizeSource = (value: string) => {
  const key = (value || "").trim().toLowerCase()
  if (!key) return ""
  if (key === "reliance resq" || key === "reliance-resq" || key === "reliance_resq" || key === "resq") {
    return "reliance"
  }
  if (key === "goodrej" || key === "goddrej") return "godrej"
  if (key === "samsung_vijay_sales" || key === "samsung vijay sales" || key === "samsung vs" || key === "vijay sales") {
    return "samsung_vs"
  }
  if (key === "samsung croma" || key === "croma") return "samsung_croma"
  return key
}

const normalizeDatasetType = (value: string) => ((value || "").trim().toLowerCase() === "claims" ? "claims" : "sales")

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

export default function RightSideChatbot({ variant = "floating" }: Props) {
  const isCard = variant === "card"
  const [isMounted, setIsMounted] = useState(false)
  const [isOpen, setIsOpen] = useState(false)
  const [isExpanded, setIsExpanded] = useState(false)
  const [messages, setMessages] = useState<ChatMessage[]>([INITIAL_ASSISTANT_MESSAGE])
  const [input, setInput] = useState("")
  const [isSending, setIsSending] = useState(false)

  const cardListRef = useRef<HTMLDivElement | null>(null)
  const panelListRef = useRef<HTMLDivElement | null>(null)
  const fullListRef = useRef<HTMLDivElement | null>(null)

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

  const send = async () => {
    const text = input.trim()
    if (!text || isSending) return

    setInput("")
    setMessages((prev) => [...prev, { role: "user", content: text }])

    const token =
      typeof window !== "undefined" ? (localStorage.getItem("auth_token") || "").trim() : ""
    if (!token) {
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: "Sign in first to use the chatbot." },
      ])
      return
    }

    setIsSending(true)
    try {
      const compactText = text.replace(/\s+/g, " ").trim()
      const maxTokens = Math.max(900, Math.min(4096, Math.ceil(compactText.length * 4.5)))

      const source = normalizeSource(
        typeof window !== "undefined" ? localStorage.getItem("dashboard_brand") || "" : ""
      )
      const datasetType = normalizeDatasetType(
        typeof window !== "undefined" ? localStorage.getItem("dashboard_mode") || "sales" : "sales"
      )
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
      setMessages((prev) => [...prev, { role: "assistant", content: reply }])
    } catch (err) {
      const detail =
        err instanceof Error
          ? err.message
          : "Chatbot is unavailable right now. Check Sarvam service configuration."
      setMessages((prev) => [...prev, { role: "assistant", content: detail }])
    } finally {
      setIsSending(false)
    }
  }

  const onInputKeyDown = (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault()
      void send()
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
      ? "flex h-[min(92dvh,760px)] w-[min(98vw,960px)] flex-col rounded-2xl border border-slate-200 bg-white shadow-2xl sm:rounded-3xl"
      : mode === "card"
        ? "flex h-[360px] w-full flex-col rounded-2xl border border-slate-200 bg-white shadow-sm sm:h-[420px]"
        : "pointer-events-auto flex h-[min(74dvh,680px)] w-[min(95vw,380px)] flex-col rounded-2xl border border-slate-200 bg-white shadow-2xl sm:mr-3"

    return (
      <aside className={containerClass}>
        <header className={`flex items-center justify-between border-b border-slate-200 ${isFullMode ? "px-4 py-3 sm:px-5 sm:py-4" : "px-3 py-2.5 sm:px-4 sm:py-3"}`}>
          <div className="flex items-center gap-2">
            <Bot size={16} className="text-indigo-600" />
            <h3 className="text-sm font-bold text-slate-800">AI Sahyogi</h3>
          </div>

          <div className="flex items-center gap-1">
            {!isFullMode && (
              <button
                type="button"
                onClick={openFullChat}
                className={mode === "card"
                  ? "inline-flex items-center gap-1 rounded-lg border border-slate-200 px-2 py-1 text-[11px] font-semibold text-slate-600 hover:bg-slate-50"
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

        <div ref={listRef} className={`flex-1 space-y-3 overflow-y-auto ${isFullMode ? "px-4 py-3 sm:px-5 sm:py-4" : "px-3 py-3"}`}>
          {messages.map((message, idx) => (
            <div
              key={`${message.role}-${idx}-${message.content.slice(0, 24)}`}
              className={`max-w-[90%] rounded-2xl px-3 py-2 text-sm leading-relaxed ${
                message.role === "user"
                  ? "ml-auto bg-slate-900 text-white"
                  : "bg-slate-100 text-slate-800"
              }`}
            >
              {message.content}
            </div>
          ))}
          {isSending && (
            <div className="inline-flex items-center gap-2 rounded-2xl bg-slate-100 px-3 py-2 text-sm text-slate-700">
              <Loader2 size={14} className="animate-spin" />
              Thinking...
            </div>
          )}
        </div>

        <div className={`border-t border-slate-200 ${isFullMode ? "p-3 sm:p-4" : "p-3"}`}>
          <div className="flex items-end gap-2">
            <textarea
              value={input}
              onChange={(event) => setInput(event.target.value)}
              onKeyDown={onInputKeyDown}
              rows={isFullMode ? 3 : 2}
              placeholder="Ask about dashboard insights..."
              className="min-h-[44px] flex-1 resize-none rounded-xl border border-slate-300 px-3 py-2 text-sm text-slate-800 outline-none ring-indigo-500 transition focus:ring-2"
            />
            <button
              type="button"
              onClick={() => void send()}
              disabled={!input.trim() || isSending}
              className={`flex items-center justify-center rounded-xl bg-indigo-600 text-white transition hover:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-50 ${isFullMode ? "h-11 w-11" : "h-10 w-10"}`}
              aria-label="Send message"
            >
              <Send size={15} />
            </button>
          </div>
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
              <MessageCircle size={18} className="text-indigo-600" />
              <span className="text-xs font-bold text-slate-700">AI Chat</span>
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
