param(
    [string]$ServerIP = "",
    [string]$RemoteUser = "ubuntu",
    [string]$RemotePath = "/home/ubuntu/live-dashboard",
    [int]$SshConnectTimeoutSec = 12
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ServerIP)) {
    $ServerIP = $env:LIVE_DASHBOARD_SERVER_IP
}
if ([string]::IsNullOrWhiteSpace($ServerIP)) {
    $ServerIP = "65.2.116.200"
}

$sshKey = Join-Path $PSScriptRoot "ssh_key.pem"
$serverIP = $ServerIP
$remoteUser = $RemoteUser
$remotePath = $RemotePath
$sshCommonArgs = @(
    "-i", $sshKey,
    "-o", "StrictHostKeyChecking=no",
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=$SshConnectTimeoutSec"
)

function Invoke-Ssh {
    param(
        [Parameter(Mandatory = $true)][string]$CommandText,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )
    & ssh @sshCommonArgs "${remoteUser}@${serverIP}" $CommandText
    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }
}

function Invoke-Scp {
    param(
        [Parameter(Mandatory = $true)][string[]]$SourcePaths,
        [Parameter(Mandatory = $true)][string]$Destination,
        [switch]$Recursive,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )
    $scpArgs = @()
    $scpArgs += $sshCommonArgs
    if ($Recursive) {
        $scpArgs += "-r"
    }
    $scpArgs += $SourcePaths
    $scpArgs += $Destination
    & scp @scpArgs
    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }
}

if (-not (Test-Path $sshKey)) {
    throw "SSH key not found: $sshKey"
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "PARALLEL BUILD DEPLOYMENT (FASTEST)" -ForegroundColor Cyan  
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Target: ${remoteUser}@${serverIP}" -ForegroundColor Gray

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

# Step 0: Preflight SSH connectivity
Write-Host "`n[0/4] Verifying SSH connectivity..." -ForegroundColor Yellow
Invoke-Ssh -CommandText "echo connected" -FailureMessage "Cannot connect to ${serverIP}:22. Check EC2 status/security group and server IP."

# Step 1: Upload only source code (no node_modules, no .venv)
Write-Host "`n[1/4] Uploading source code..." -ForegroundColor Yellow

# Create minimal backend package
Write-Host "  Backend..." -ForegroundColor Gray
Invoke-Ssh -CommandText "mkdir -p $remotePath/backend" -FailureMessage "Failed to create remote backend directory."
Invoke-Scp -Recursive `
    -SourcePaths @("backend/*.py", "backend/requirements.txt", "backend/Dockerfile") `
    -Destination "${remoteUser}@${serverIP}:${remotePath}/backend/" `
    -FailureMessage "Failed to upload backend root files."
Invoke-Scp -Recursive `
    -SourcePaths @("backend/authentication", "backend/chatcards", "backend/core", "backend/db", "backend/models", "backend/routers", "backend/services") `
    -Destination "${remoteUser}@${serverIP}:${remotePath}/backend/" `
    -FailureMessage "Failed to upload backend package directories."

# Create minimal frontend package  
Write-Host "  Frontend..." -ForegroundColor Gray
Invoke-Ssh -CommandText "mkdir -p $remotePath/frontend/my-app" -FailureMessage "Failed to create remote frontend directory."
Invoke-Scp `
    -SourcePaths @("frontend/my-app/package*.json", "frontend/my-app/Dockerfile", "frontend/my-app/*.config.*", "frontend/my-app/tsconfig.json", "frontend/my-app/next-env.d.ts") `
    -Destination "${remoteUser}@${serverIP}:${remotePath}/frontend/my-app/" `
    -FailureMessage "Failed to upload frontend root files."
Invoke-Scp -Recursive `
    -SourcePaths @("frontend/my-app/app", "frontend/my-app/components", "frontend/my-app/lib", "frontend/my-app/utils", "frontend/my-app/public") `
    -Destination "${remoteUser}@${serverIP}:${remotePath}/frontend/my-app/" `
    -FailureMessage "Failed to upload frontend app directories."

# Upload docker-compose
Write-Host "  Docker compose..." -ForegroundColor Gray
Invoke-Scp `
    -SourcePaths @("deploy/docker-compose.prod.yml") `
    -Destination "${remoteUser}@${serverIP}:${remotePath}/docker-compose.yml" `
    -FailureMessage "Failed to upload docker-compose file."

Write-Host "  Upload complete!" -ForegroundColor Green

# Step 2: Stop old containers
Write-Host "`n[2/4] Stopping old containers..." -ForegroundColor Yellow
Invoke-Ssh -CommandText "cd $remotePath && docker compose down 2>/dev/null || true" -FailureMessage "Failed while stopping existing containers."

# Step 3: Build in PARALLEL (this is the key optimization!)
Write-Host "`n[3/4] Building containers IN PARALLEL..." -ForegroundColor Yellow
Write-Host "  This will save 5-8 minutes!" -ForegroundColor Cyan

# Start backend build in background
$backendBuild = Start-Job -ScriptBlock {
    param($key, $user, $ip, $path, $timeout)
    $ErrorActionPreference = "Continue"
    & ssh -i $key -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=$timeout "${user}@${ip}" "cd $path && docker compose build backend" 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Backend build failed with exit code $LASTEXITCODE"
    }
} -ArgumentList $sshKey, $remoteUser, $serverIP, $remotePath, $SshConnectTimeoutSec

# Start frontend build in background
$frontendBuild = Start-Job -ScriptBlock {
    param($key, $user, $ip, $path, $timeout)
    $ErrorActionPreference = "Continue"
    & ssh -i $key -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=$timeout "${user}@${ip}" "cd $path && docker compose build frontend" 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Frontend build failed with exit code $LASTEXITCODE"
    }
} -ArgumentList $sshKey, $remoteUser, $serverIP, $remotePath, $SshConnectTimeoutSec

Write-Host "  Backend build: STARTED" -ForegroundColor Yellow
Write-Host "  Frontend build: STARTED" -ForegroundColor Yellow
Write-Host "  Waiting for both to complete..." -ForegroundColor Gray

# Wait for both builds
$backendBuild, $frontendBuild | Wait-Job | Out-Null
$backendOutput = Receive-Job -Job $backendBuild -Keep -ErrorAction SilentlyContinue
$frontendOutput = Receive-Job -Job $frontendBuild -Keep -ErrorAction SilentlyContinue
if ($backendOutput) { $backendOutput | ForEach-Object { Write-Host "  [backend] $_" -ForegroundColor DarkGray } }
if ($frontendOutput) { $frontendOutput | ForEach-Object { Write-Host "  [frontend] $_" -ForegroundColor DarkGray } }

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

$parallelFailed = ($backendBuild.State -ne "Completed" -or $frontendBuild.State -ne "Completed")
$backendBuild, $frontendBuild | Remove-Job
if ($parallelFailed) {
    throw "Parallel build failed. See job output above."
}


# Step 4: Start services
Write-Host "`n[4/4] Starting services..." -ForegroundColor Yellow
Invoke-Ssh -CommandText "cd $remotePath && docker compose up -d && sleep 10 && docker compose ps" -FailureMessage "Failed to start docker services."

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "DEPLOYMENT COMPLETE!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

# Verify
Start-Sleep -Seconds 5
try {
    $null = Invoke-RestMethod -Uri "http://${serverIP}/health" -TimeoutSec 10
    Write-Host "Backend (via nginx): http://${serverIP}/health ✓" -ForegroundColor Green
}
catch {
    Write-Host "Backend (via nginx): http://${serverIP}/health ✗" -ForegroundColor Red
}

try {
    $null = Invoke-WebRequest -Uri "http://${serverIP}" -TimeoutSec 10 -UseBasicParsing
    Write-Host "Frontend: http://${serverIP} ✓" -ForegroundColor Green
}
catch {
    Write-Host "Frontend: http://${serverIP} (starting...)" -ForegroundColor Yellow
}

Write-Host "`nDashboard: http://${serverIP}" -ForegroundColor Cyan
Write-Host "Docs: http://${serverIP}/docs" -ForegroundColor Cyan
Write-Host "Admin: admin.user@zopper.com / admin123" -ForegroundColor White
