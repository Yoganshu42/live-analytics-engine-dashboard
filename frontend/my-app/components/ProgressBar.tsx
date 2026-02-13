"use client"

export default function ProgressBar({ progress }: { progress: number }) {
  return (
    <div className="w-full bg-gray-200 rounded-full h-3 overflow-hidden">
      <div
        className="h-3 bg-green-500 rounded-full transition-all duration-300 ease-out"
        style={{ width: `${progress}%` }}
      />
    </div>
  )
}
