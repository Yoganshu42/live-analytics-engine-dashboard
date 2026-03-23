"use client"

import { useEffect, useRef } from "react"

type Props = {
  reducedMotion?: boolean
  quality?: "low" | "high"
}

type Particle = {
  x: number
  y: number
  vx: number
  vy: number
  radius: number
  alpha: number
  phase: number
  color: string
}

const PARTICLE_COLORS = [
  "rgba(108,181,255,0.3)",
  "rgba(155,220,255,0.24)",
  "rgba(189,174,255,0.22)",
  "rgba(255,255,255,0.38)",
]

export default function HomeParticleField({ reducedMotion = false, quality = "high" }: Props) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)

  useEffect(() => {
    const canvas = canvasRef.current
    const context = canvas?.getContext("2d")

    if (!canvas || !context) return

    let animationFrame = 0
    let width = 0
    let height = 0
    let time = 0
    let lastFrameTime = 0
    let particles: Particle[] = []
    const FRAME_INTERVAL = 1000 / 60

    const pointer = {
      x: 0,
      y: 0,
      targetX: 0,
      targetY: 0,
      active: false,
    }

    const randomBetween = (min: number, max: number) =>
      min + Math.random() * (max - min)

    const renderFrame = (motionScale: number) => {
      context.clearRect(0, 0, width, height)
      time += 0.008 * motionScale

      if (!pointer.active) {
        pointer.targetX += (width * 0.5 - pointer.targetX) * Math.min(0.018 * motionScale, 0.08)
        pointer.targetY += (height * 0.42 - pointer.targetY) * Math.min(0.018 * motionScale, 0.08)
      }

      pointer.x += (pointer.targetX - pointer.x) * Math.min(0.05 * motionScale, 0.18)
      pointer.y += (pointer.targetY - pointer.y) * Math.min(0.05 * motionScale, 0.18)

      for (let index = 0; index < particles.length; index += 1) {
        const particle = particles[index]

        if (!reducedMotion) {
          particle.x += particle.vx * motionScale
          particle.y += particle.vy * motionScale

          if (particle.x < -24 || particle.x > width + 24) particle.vx *= -1
          if (particle.y < -24 || particle.y > height + 24) particle.vy *= -1
        }

        const distanceX = pointer.x - particle.x
        const distanceY = pointer.y - particle.y
        const distance = Math.hypot(distanceX, distanceY)
        const pointerInfluence = Math.max(0, 1 - distance / 240)
        const driftX = reducedMotion ? 0 : Math.sin(time + particle.phase) * 7
        const driftY = reducedMotion ? 0 : Math.cos(time * 0.92 + particle.phase) * 6
        const renderX = particle.x + driftX + distanceX * pointerInfluence * 0.025
        const renderY = particle.y + driftY + distanceY * pointerInfluence * 0.02

        const maxConnectionDistance = quality === "low" ? 120 : 160

        for (let nextIndex = index + 1; nextIndex < particles.length; nextIndex += 1) {
          const nextParticle = particles[nextIndex]
          const pairDistance = Math.hypot(particle.x - nextParticle.x, particle.y - nextParticle.y)
          if (pairDistance > maxConnectionDistance) continue

          context.beginPath()
          context.strokeStyle = `rgba(95, 146, 255, ${0.085 * (1 - pairDistance / maxConnectionDistance)})`
          context.lineWidth = 1
          context.moveTo(renderX, renderY)
          context.lineTo(nextParticle.x, nextParticle.y)
          context.stroke()
        }

        context.beginPath()
        context.fillStyle = particle.color
        context.globalAlpha = particle.alpha + pointerInfluence * 0.12
        context.arc(renderX, renderY, particle.radius + pointerInfluence * 1.4, 0, Math.PI * 2)
        context.fill()

        context.beginPath()
        context.fillStyle = "rgba(255,255,255,0.78)"
        context.globalAlpha = 0.28 + pointerInfluence * 0.12
        context.arc(renderX, renderY, Math.max(1, particle.radius * 0.36), 0, Math.PI * 2)
        context.fill()
      }

      context.globalAlpha = 1
    }

    const draw = (now: number) => {
      if (lastFrameTime && now - lastFrameTime < FRAME_INTERVAL * 0.85) {
        animationFrame = window.requestAnimationFrame(draw)
        return
      }

      const deltaMs = lastFrameTime
        ? Math.min(now - lastFrameTime, FRAME_INTERVAL * 2)
        : FRAME_INTERVAL

      lastFrameTime = now
      renderFrame(deltaMs / FRAME_INTERVAL)
      animationFrame = window.requestAnimationFrame(draw)
    }

    const buildParticles = () => {
      const baseCount = quality === "low" ? 8 : 14
      const maxCount = quality === "low" ? 16 : 24
      const particleCount = Math.max(baseCount, Math.min(maxCount, Math.round((width * height) / 72000)))
      particles = Array.from({ length: particleCount }, (_, index) => ({
        x: randomBetween(0, width),
        y: randomBetween(0, height),
        vx: randomBetween(-0.1, 0.1),
        vy: randomBetween(-0.08, 0.08),
        radius: randomBetween(1.8, 4.2),
        alpha: randomBetween(0.18, 0.34),
        phase: randomBetween(0, Math.PI * 2),
        color: PARTICLE_COLORS[index % PARTICLE_COLORS.length],
      }))
    }

    const resize = () => {
      const devicePixelRatio = Math.min(window.devicePixelRatio || 1, 2)
      width = window.innerWidth
      height = window.innerHeight

      canvas.width = Math.floor(width * devicePixelRatio)
      canvas.height = Math.floor(height * devicePixelRatio)
      canvas.style.width = `${width}px`
      canvas.style.height = `${height}px`
      context.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0)

      pointer.x = width * 0.5
      pointer.y = height * 0.42
      pointer.targetX = pointer.x
      pointer.targetY = pointer.y

      buildParticles()
      lastFrameTime = 0
      window.cancelAnimationFrame(animationFrame)
      if (reducedMotion) {
        renderFrame(1)
      } else {
        animationFrame = window.requestAnimationFrame(draw)
      }
    }

    const handlePointerMove = (event: PointerEvent) => {
      pointer.active = true
      pointer.targetX = event.clientX
      pointer.targetY = event.clientY
    }

    const handlePointerLeave = () => {
      pointer.active = false
    }

    resize()
    window.addEventListener("resize", resize)
    window.addEventListener("pointermove", handlePointerMove, { passive: true })
    window.addEventListener("pointerleave", handlePointerLeave)

    return () => {
      window.cancelAnimationFrame(animationFrame)
      window.removeEventListener("resize", resize)
      window.removeEventListener("pointermove", handlePointerMove)
      window.removeEventListener("pointerleave", handlePointerLeave)
    }
  }, [quality, reducedMotion])

  return <canvas ref={canvasRef} className="absolute inset-0 h-full w-full" aria-hidden="true" />
}
