$ErrorActionPreference = "Stop"

$serverIP = "13.235.66.217"
$sshKey = Join-Path $PSScriptRoot "ssh_key.pem"
$remoteUser = "ubuntu"
$remotePath = "/home/ubuntu/live-dashboard"

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "LIGHTNING FAST DEPLOYMENT" -ForegroundColor Cyan  
Write-Host "========================================" -ForegroundColor Cyan

$projectRoot = Split-Path -Parent $PSScriptRoot

# Step 1: Create .dockerignore files
Write-Host "`n[1/5] Creating .dockerignore files..." -ForegroundColor Yellow

"node_modules
.next
.git
*.log
.env.local
.DS_Store" | Out-File -FilePath "$projectRoot/frontend/my-app/.dockerignore" -Encoding utf8

".venv
__pycache__
*.pyc
.git
*.log
.env
.pytest_cache
scripts" | Out-File -FilePath "$projectRoot/backend/.dockerignore" -Encoding utf8

Write-Host "  Done!" -ForegroundColor Green

# Step 2: Sync only source files using SSH + tar
Write-Host "`n[2/5] Syncing source files..." -ForegroundColor Yellow

# Prepare remote
ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "mkdir -p $remotePath"

# Create tar excluding large directories, pipe directly to remote
Write-Host "  Creating and uploading package..." -ForegroundColor Gray
Set-Location $projectRoot

tar --exclude='node_modules' `
    --exclude='.next' `
    --exclude='.venv' `
    --exclude='__pycache__' `
    --exclude='*.pyc' `
    --exclude='.git' `
    --exclude='analytics.sql' `
    --exclude='analytics.dump' `
    --exclude='*.tar.gz' `
    --exclude='*.log' `
    -czf - backend frontend/my-app deploy/docker-compose.prod.yml | `
    ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "cd $remotePath && tar -xzf - && mv deploy/docker-compose.prod.yml docker-compose.yml"

Write-Host "  Upload complete!" -ForegroundColor Green

# Step 3: Build and deploy
Write-Host "`n[3/5] Building containers..." -ForegroundColor Yellow

ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" @"
cd $remotePath
echo 'Stopping old containers...'
docker-compose down 2>/dev/null || true
echo 'Building images...'
docker-compose build
echo 'Starting services...'
docker-compose up -d
sleep 10
docker-compose ps
"@

Write-Host "  Done!" -ForegroundColor Green

# Step 4: Verify
Write-Host "`n[4/5] Verifying deployment..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

try {
    $null = Invoke-RestMethod -Uri "http://${serverIP}/api/health" -Method GET -TimeoutSec 10
    Write-Host "  Backend: HEALTHY" -ForegroundColor Green
}
catch {
    Write-Host "  Backend: FAILED - Checking logs..." -ForegroundColor Red
    ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "cd $remotePath && docker-compose logs backend --tail=50"
}

try {
        $null = Invoke-WebRequest -Uri "http://${serverIP}/" -Method GET -TimeoutSec 10 -UseBasicParsing
    Write-Host "  Frontend: ACCESSIBLE" -ForegroundColor Green
}
catch {
    Write-Host "  Frontend: Still starting..." -ForegroundColor Yellow
}

# Step 5: Summary
Write-Host "`n[5/5] Deployment Summary" -ForegroundColor Yellow
Write-Host "`n========================================" -ForegroundColor Green
Write-Host "DEPLOYMENT COMPLETE!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "`nAccess your application:" -ForegroundColor Cyan
Write-Host "  Frontend:  http://${serverIP}/" -ForegroundColor White
Write-Host "  Backend:   http://${serverIP}/api" -ForegroundColor White
Write-Host "  API Docs:  http://${serverIP}/docs" -ForegroundColor White
Write-Host "`nCredentials:" -ForegroundColor Cyan
Write-Host "  Admin:    admin.user@zopper.com / admin123" -ForegroundColor White
Write-Host "  Employee: employee.user@zopper.com / employee123" -ForegroundColor White
Write-Host "`nView logs: ssh -i deploy/ssh_key.pem ubuntu@${serverIP} 'cd $remotePath && docker-compose logs -f'" -ForegroundColor Gray
