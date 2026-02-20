$ErrorActionPreference = "Stop"

$serverIP = "43.205.134.36"
$sshKey = Join-Path $PSScriptRoot "ssh_key.pem"
$remoteUser = "ubuntu"
$remotePath = "/home/ubuntu/live-dashboard"

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "PARALLEL BUILD DEPLOYMENT (FASTEST)" -ForegroundColor Cyan  
Write-Host "========================================" -ForegroundColor Cyan

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

# Step 1: Upload only source code (no node_modules, no .venv)
Write-Host "`n[1/4] Uploading source code..." -ForegroundColor Yellow

# Create minimal backend package
Write-Host "  Backend..." -ForegroundColor Gray
ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "mkdir -p $remotePath/backend"
scp -i $sshKey -o StrictHostKeyChecking=no -r backend/*.py backend/requirements.txt backend/Dockerfile "${remoteUser}@${serverIP}:${remotePath}/backend/" 2>$null
scp -i $sshKey -o StrictHostKeyChecking=no -r backend/authentication backend/chatcards backend/core backend/db backend/models backend/routers backend/services "${remoteUser}@${serverIP}:${remotePath}/backend/" 2>$null

# Create minimal frontend package  
Write-Host "  Frontend..." -ForegroundColor Gray
ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "mkdir -p $remotePath/frontend/my-app"
scp -i $sshKey -o StrictHostKeyChecking=no frontend/my-app/package*.json frontend/my-app/Dockerfile frontend/my-app/*.config.* frontend/my-app/tsconfig.json frontend/my-app/next-env.d.ts "${remoteUser}@${serverIP}:${remotePath}/frontend/my-app/" 2>$null
scp -i $sshKey -o StrictHostKeyChecking=no -r frontend/my-app/app frontend/my-app/components frontend/my-app/utils frontend/my-app/public "${remoteUser}@${serverIP}:${remotePath}/frontend/my-app/" 2>$null

# Upload docker-compose
Write-Host "  Docker compose..." -ForegroundColor Gray
scp -i $sshKey -o StrictHostKeyChecking=no deploy/docker-compose.prod.yml "${remoteUser}@${serverIP}:${remotePath}/docker-compose.yml"

Write-Host "  Upload complete!" -ForegroundColor Green

# Step 2: Stop old containers
Write-Host "`n[2/4] Stopping old containers..." -ForegroundColor Yellow
ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "cd $remotePath && docker compose down 2>/dev/null || true"

# Step 3: Build in PARALLEL (this is the key optimization!)
Write-Host "`n[3/4] Building containers IN PARALLEL..." -ForegroundColor Yellow
Write-Host "  This will save 5-8 minutes!" -ForegroundColor Cyan

# Start backend build in background
$backendBuild = Start-Job -ScriptBlock {
    param($key, $user, $ip, $path)
    ssh -i $key -o StrictHostKeyChecking=no "${user}@${ip}" "cd $path && docker compose build backend"
} -ArgumentList $sshKey, $remoteUser, $serverIP, $remotePath

# Start frontend build in background
$frontendBuild = Start-Job -ScriptBlock {
    param($key, $user, $ip, $path)
    ssh -i $key -o StrictHostKeyChecking=no "${user}@${ip}" "cd $path && docker compose build frontend"
} -ArgumentList $sshKey, $remoteUser, $serverIP, $remotePath

Write-Host "  Backend build: STARTED" -ForegroundColor Yellow
Write-Host "  Frontend build: STARTED" -ForegroundColor Yellow
Write-Host "  Waiting for both to complete..." -ForegroundColor Gray

# Wait for both builds
$backendBuild, $frontendBuild | Wait-Job | Out-Null

if ($backendBuild.State -eq "Completed") {
    Write-Host "  Backend build: DONE" -ForegroundColor Green
}
else {
    Write-Host "  Backend build: FAILED" -ForegroundColor Red
}

if ($frontendBuild.State -eq "Completed") {
    Write-Host "  Frontend build: DONE" -ForegroundColor Green
}
else {
    Write-Host "  Frontend build: FAILED" -ForegroundColor Red
}

$backendBuild, $frontendBuild | Remove-Job

# Step 4: Start services
Write-Host "`n[4/4] Starting services..." -ForegroundColor Yellow
ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "cd $remotePath && docker compose up -d && sleep 10 && docker compose ps"
Write-Host "  Ensuring Gemma model is available..." -ForegroundColor Gray
ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "cd $remotePath && (docker compose exec -T ollama ollama list | grep -q 'gemma2:2b' || docker compose exec -T ollama ollama pull gemma2:2b)"

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "DEPLOYMENT COMPLETE!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

# Verify
Start-Sleep -Seconds 5
try {
    $null = Invoke-RestMethod -Uri "http://${serverIP}:8000/health" -TimeoutSec 10
    Write-Host "Backend: http://${serverIP}:8000 ✓" -ForegroundColor Green
}
catch {
    Write-Host "Backend: http://${serverIP}:8000 ✗" -ForegroundColor Red
}

try {
    $null = Invoke-WebRequest -Uri "http://${serverIP}:3000" -TimeoutSec 10 -UseBasicParsing
    Write-Host "Frontend: http://${serverIP}:3000 ✓" -ForegroundColor Green
}
catch {
    Write-Host "Frontend: http://${serverIP}:3000 (starting...)" -ForegroundColor Yellow
}

Write-Host "`nDocs: http://${serverIP}:8000/docs" -ForegroundColor Cyan
Write-Host "Admin: admin.user@zopper.com / admin123" -ForegroundColor White
