"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { Bot, Loader2, MessageCircle, Send, X } from "lucide-react"
import { sendChatbotMessage, type ChatbotTurn } from "@/app/lib/api"

type ChatMessage = {
  role: "user" | "assistant"
  content: string
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
  if (key === "samsung_vijay_sales") return "samsung_vs"
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

export default function RightSideChatbot() {
  const [isOpen, setIsOpen] = useState(false)
  const [messages, setMessages] = useState<ChatMessage[]>([INITIAL_ASSISTANT_MESSAGE])
  const [input, setInput] = useState("")
  const [isSending, setIsSending] = useState(false)
  const listRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (!listRef.current) return
    listRef.current.scrollTop = listRef.current.scrollHeight
  }, [messages, isOpen])

  const history: ChatbotTurn[] = useMemo(
    () =>
      messages
        .slice(-6)
        .map((msg) => ({ role: msg.role, content: msg.content })),
    [messages]
  )

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
      const maxTokens =
        compactText.length <= 80 ? 120 :
        compactText.length <= 220 ? 180 :
        260

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
        temperature: 0.15,
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
          : "Chatbot is unavailable right now. Check Ollama/Gemma service."
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

  return (
    <div className="pointer-events-none fixed right-0 top-1/2 z-[80] -translate-y-1/2">
      {!isOpen ? (
        <button
          type="button"
          onClick={() => setIsOpen(true)}
          className="pointer-events-auto mr-0 rounded-l-2xl border border-slate-200 bg-white px-3 py-4 shadow-xl transition hover:bg-slate-50"
          aria-label="Open chatbot"
        >
          <span className="flex items-center gap-2">
            <MessageCircle size={18} className="text-indigo-600" />
            <span className="text-xs font-bold text-slate-700">AI Chat</span>
          </span>
        </button>
      ) : (
        <aside className="pointer-events-auto mr-3 flex h-[70vh] max-h-[680px] w-[min(92vw,380px)] flex-col rounded-2xl border border-slate-200 bg-white shadow-2xl">
          <header className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
            <div className="flex items-center gap-2">
              <Bot size={16} className="text-indigo-600" />
              <h3 className="text-sm font-bold text-slate-800">AI Sahyogi</h3>
            </div>
            <button
              type="button"
              onClick={() => setIsOpen(false)}
              className="rounded-full p-1 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
              aria-label="Close chatbot"
            >
              <X size={16} />
            </button>
          </header>

          <div ref={listRef} className="flex-1 space-y-3 overflow-y-auto px-3 py-3">
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

          <div className="border-t border-slate-200 p-3">
            <div className="flex items-end gap-2">
              <textarea
                value={input}
                onChange={(event) => setInput(event.target.value)}
                onKeyDown={onInputKeyDown}
                rows={2}
                placeholder="Ask about dashboard insights..."
                className="min-h-[44px] flex-1 resize-none rounded-xl border border-slate-300 px-3 py-2 text-sm text-slate-800 outline-none ring-indigo-500 transition focus:ring-2"
              />
              <button
                type="button"
                onClick={() => void send()}
                disabled={!input.trim() || isSending}
                className="flex h-10 w-10 items-center justify-center rounded-xl bg-indigo-600 text-white transition hover:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-50"
                aria-label="Send message"
              >
                <Send size={15} />
              </button>
            </div>
          </div>
        </aside>
      )}
    </div>
  )
}
