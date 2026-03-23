$ErrorActionPreference = "Stop"

$serverIP = "13.235.56.166"
$sshKey = Join-Path $PSScriptRoot "ssh_key.pem"
$remoteUser = "ubuntu"
$remotePath = "/home/ubuntu/live-dashboard"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "ULTRA FAST DEPLOYMENT to EC2" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

# Step 1: Create deployment package (exclude large files)
Write-Host "`n[1/6] Creating deployment package..." -ForegroundColor Yellow

$tempDir = Join-Path $env:TEMP "dashboard-deploy-$(Get-Date -Format 'yyyyMMddHHmmss')"
New-Item -ItemType Directory -Path $tempDir -Force | Out-Null

# Copy only necessary backend files
Write-Host "  Packaging backend..." -ForegroundColor Gray
$backendDest = Join-Path $tempDir "backend"
New-Item -ItemType Directory -Path $backendDest -Force | Out-Null

# Copy backend files excluding .venv and __pycache__
Get-ChildItem -Path "backend" -Recurse -File | Where-Object {
    $_.FullName -notmatch '\\\.venv\\' -and
    $_.FullName -notmatch '\\__pycache__\\' -and
    $_.Extension -ne '.pyc' -and
    $_.Name -ne '.env'
} | ForEach-Object {
    $relativePath = $_.FullName.Substring((Get-Item "backend").FullName.Length + 1)
    $destPath = Join-Path $backendDest $relativePath
    $destDir = Split-Path $destPath -Parent
    if (-not (Test-Path $destDir)) {
        New-Item -ItemType Directory -Path $destDir -Force | Out-Null
    }
    Copy-Item $_.FullName -Destination $destPath -Force
}

# Copy only necessary frontend files
Write-Host "  Packaging frontend..." -ForegroundColor Gray
$frontendDest = Join-Path $tempDir "frontend/my-app"
New-Item -ItemType Directory -Path $frontendDest -Force | Out-Null

# Copy frontend files excluding node_modules and .next
Get-ChildItem -Path "frontend/my-app" -Recurse -File | Where-Object {
    $_.FullName -notmatch '\\node_modules\\' -and
    $_.FullName -notmatch '\\.next\\' -and
    $_.Name -ne '.env.local'
} | ForEach-Object {
    $relativePath = $_.FullName.Substring((Get-Item "frontend/my-app").FullName.Length + 1)
    $destPath = Join-Path $frontendDest $relativePath
    $destDir = Split-Path $destPath -Parent
    if (-not (Test-Path $destDir)) {
        New-Item -ItemType Directory -Path $destDir -Force | Out-Null
    }
    Copy-Item $_.FullName -Destination $destPath -Force
}

# Copy docker-compose
Copy-Item "deploy/docker-compose.prod.yml" -Destination (Join-Path $tempDir "docker-compose.yml")

Write-Host "  Package size: $((Get-ChildItem $tempDir -Recurse | Measure-Object -Property Length -Sum).Sum / 1MB | ForEach-Object {$_.ToString('F2')}) MB" -ForegroundColor Green

# Step 2: Create tarball for faster transfer
Write-Host "`n[2/6] Creating tarball..." -ForegroundColor Yellow
$tarFile = Join-Path $env:TEMP "dashboard-deploy.tar.gz"
if (Test-Path $tarFile) { Remove-Item $tarFile -Force }

# Use tar if available (Windows 10+)
tar -czf $tarFile -C $tempDir .
Write-Host "  Tarball created: $(((Get-Item $tarFile).Length / 1MB).ToString('F2')) MB" -ForegroundColor Green

# Step 3: Prepare remote directory
Write-Host "`n[3/6] Preparing remote..." -ForegroundColor Yellow
ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "mkdir -p $remotePath"

# Step 4: Upload tarball (much faster than individual files)
Write-Host "`n[4/6] Uploading (this should be FAST)..." -ForegroundColor Yellow
scp -i $sshKey -o StrictHostKeyChecking=no $tarFile "${remoteUser}@${serverIP}:${remotePath}/deploy.tar.gz"
Write-Host "  Upload complete!" -ForegroundColor Green

# Step 5: Extract and deploy on server
Write-Host "`n[5/6] Deploying on server..." -ForegroundColor Yellow

$deployScript = @"
cd $remotePath
echo 'Extracting files...'
tar -xzf deploy.tar.gz
rm deploy.tar.gz
echo 'Stopping old containers...'
docker-compose down --remove-orphans 2>/dev/null || true
echo 'Building backend...'
docker-compose build backend
echo 'Building frontend...'
docker-compose build frontend
echo 'Starting services...'
docker-compose up -d
echo 'Waiting for services...'
sleep 15
docker-compose ps
"@

ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" $deployScript

Write-Host "  Deployment complete!" -ForegroundColor Green

# Cleanup
Remove-Item $tempDir -Recurse -Force
Remove-Item $tarFile -Force

# Step 6: Verify
Write-Host "`n[6/6] Verifying..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

try {
    $response = Invoke-RestMethod -Uri "http://${serverIP}:8000/health" -Method GET -TimeoutSec 10
    Write-Host "  Backend: OK" -ForegroundColor Green
    
    try {
        Invoke-WebRequest -Uri "http://${serverIP}:3000" -Method GET -TimeoutSec 10 -UseBasicParsing | Out-Null
        Write-Host "  Frontend: OK" -ForegroundColor Green
    }
    catch {
        Write-Host "  Frontend: Starting..." -ForegroundColor Yellow
    }
}
catch {
    Write-Host "  Backend: FAILED" -ForegroundColor Red
    ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "cd $remotePath; docker-compose logs --tail=30"
}

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "DEPLOYMENT COMPLETE!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "`nFrontend: http://${serverIP}:3000" -ForegroundColor Cyan
Write-Host "Backend: http://${serverIP}:8000" -ForegroundColor Cyan
Write-Host "Docs: http://${serverIP}:8000/docs" -ForegroundColor Cyan
