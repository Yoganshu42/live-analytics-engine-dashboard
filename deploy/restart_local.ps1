$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$logs = Join-Path $PSScriptRoot "logs"
New-Item -ItemType Directory -Force -Path $logs | Out-Null

$py = Join-Path $root "backend\\.venv\\Scripts\\python.exe"
$backendWd = Join-Path $root "backend"
$backendOut = Join-Path $logs "backend.out.log"
$backendErr = Join-Path $logs "backend.err.log"

function Stop-PortListener([int]$port) {
  $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
  if (-not $conn) { return }
  $procId = $conn.OwningProcess
  if (-not $procId) { return }

  $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
  Write-Host ("Stopping listener on port {0}: pid={1} name={2}" -f $port, $procId, ($proc.ProcessName))
  Stop-Process -Id $procId -Force
  Start-Sleep -Seconds 1
}

Stop-PortListener -port 8000

Write-Host "Starting backend (uvicorn) on http://0.0.0.0:8000 ..."
Start-Process -FilePath $py `
  -ArgumentList @("-m","uvicorn","main:app","--host","0.0.0.0","--port","8000") `
  -WorkingDirectory $backendWd `
  -RedirectStandardOutput $backendOut `
  -RedirectStandardError $backendErr `
  -WindowStyle Hidden

Start-Sleep -Seconds 2
try {
  $health = Invoke-RestMethod -Uri "http://127.0.0.1:8000/health" -Method GET -TimeoutSec 10
  Write-Host ("Backend health: {0}" -f ($health | ConvertTo-Json -Compress))
} catch {
  Write-Warning "Backend health check failed. See deploy\\logs\\backend.err.log"
  throw
}
