import { test, expect } from "@playwright/test"

test.setTimeout(120000)

test("rapid tab switching stays responsive", async ({ page }) => {
  const errors: string[] = []
  page.on("pageerror", (err) => errors.push(err.message))

  await page.addInitScript(() => {
    localStorage.setItem("auth_token", "test-token")
    localStorage.setItem("auth_role", "admin")
    localStorage.setItem("dashboard_view", "dashboard")
    localStorage.setItem("dashboard_brand", "samsung")
    localStorage.setItem("dashboard_mode", "sales")
  })

  await page.route("**/auth/me", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ email: "qa.user@zopper.com", role: "admin", is_active: true }),
    })
  })

  await page.route("**/analytics/date-bounds**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ min_date: "2025-01-01", max_date: "2025-12-31" }),
    })
  })

  await page.route("**/analytics/summary**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        gross_premium: 12345000,
        earned_premium: 9988000,
        zopper_earned_premium: 4567000,
        units_sold: 2244,
      }),
    })
  })

  await page.route("**/analytics/last-updated**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ data_upto: "2025-12-01" }),
    })
  })

  await page.route("**/analytics/by-dimension**", async (route) => {
    const url = new URL(route.request().url())
    const dimension = url.searchParams.get("dimension") ?? "month"
    const metric = url.searchParams.get("metric") ?? "gross_premium"

    const rows = [
      { [dimension]: "2025-09-01", [metric]: 101, ew_count: 10 },
      { [dimension]: "2025-10-01", [metric]: 121, ew_count: 12 },
      { [dimension]: "2025-11-01", [metric]: 141, ew_count: 14 },
    ]

    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(rows),
    })
  })

  await page.goto("http://127.0.0.1:3000", { waitUntil: "domcontentloaded" })

  await expect(page.getByText("Analysis Workspace")).toBeVisible({ timeout: 15000 })

  const salesTab = page.getByRole("button", { name: "SALES ANALYSIS" })
  const claimsTab = page.getByRole("button", { name: "CLAIMS ANALYSIS" })

  for (let i = 0; i < 20; i += 1) {
    await salesTab.click({ timeout: 5000 })
    await claimsTab.click({ timeout: 5000 })
  }

  await salesTab.click({ timeout: 5000 })

  await expect(page.getByText("Analysis Workspace")).toBeVisible()
  await expect(salesTab).toBeVisible()
  await expect(claimsTab).toBeVisible()
  await expect(errors, `Page errors: ${errors.join(" | ")}`).toEqual([])
})
