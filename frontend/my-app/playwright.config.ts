import { defineConfig } from "@playwright/test"

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  reporter: "line",
  use: {
    headless: true,
  },
  webServer: {
    command: "npm run dev -- --hostname 127.0.0.1 --port 3000",
    url: "http://127.0.0.1:3000",
    timeout: 180000,
    reuseExistingServer: false,
  },
})
