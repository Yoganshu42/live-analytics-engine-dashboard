"use client"

import type { ReactNode } from "react"
import { MotionConfig } from "framer-motion"

type Props = {
  children: ReactNode
}

export default function MotionProvider({ children }: Props) {
  return (
    <MotionConfig
      reducedMotion="user"
      transition={{ duration: 0.32, ease: [0.22, 1, 0.36, 1] }}
    >
      {children}
    </MotionConfig>
  )
}
