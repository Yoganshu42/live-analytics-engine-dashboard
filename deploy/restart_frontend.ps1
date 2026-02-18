$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$logs = Join-Path $PSScriptRoot "logs"
New-Item -ItemType Directory -Force -Path $logs | Out-Null

$frontendWd = Join-Path $root "frontend\\my-app"
$frontendOut = Join-Path $logs "frontend.out.log"
$frontendErr = Join-Path $logs "frontend.err.log"
$standaloneServer = Join-Path $frontendWd ".next\\standalone\\server.js"
$standaloneRoot = Join-Path $frontendWd ".next\\standalone"
$nextStaticSrc = Join-Path $frontendWd ".next\\static"
$nextStaticDest = Join-Path $standaloneRoot ".next\\static"
$publicSrc = Join-Path $frontendWd "public"
$publicDest = Join-Path $standaloneRoot "public"

function Stop-PortListener([int]$port) {
  $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
  if (-not $conn) { return }
  $procId = $conn.OwningProcess
  if (-not $procId) { return }
  Stop-Process -Id $procId -Force
  Start-Sleep -Seconds 1
}

Stop-PortListener -port 3000

# Uses the build created by `npm run build`.
if (Test-Path $standaloneServer) {
  if (Test-Path $nextStaticSrc) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $nextStaticDest) | Out-Null
    if (Test-Path $nextStaticDest) { Remove-Item -Recurse -Force $nextStaticDest }
    Copy-Item -Recurse -Force $nextStaticSrc $nextStaticDest
  }
  if (Test-Path $publicSrc) {
    if (Test-Path $publicDest) { Remove-Item -Recurse -Force $publicDest }
    Copy-Item -Recurse -Force $publicSrc $publicDest
  }
  $env:HOSTNAME = "0.0.0.0"
  $env:PORT = "3000"
  Start-Process -FilePath "node.exe" `
    -ArgumentList @("""$standaloneServer""") `
    -WorkingDirectory $frontendWd `
    -RedirectStandardOutput $frontendOut `
    -RedirectStandardError $frontendErr `
    -WindowStyle Hidden
} else {
  Start-Process -FilePath "npm.cmd" `
    -ArgumentList @("run","start") `
    -WorkingDirectory $frontendWd `
    -RedirectStandardOutput $frontendOut `
    -RedirectStandardError $frontendErr `
    -WindowStyle Hidden
}
