$ErrorActionPreference = "Stop"

$serverIP = "43.205.134.36"
$sshKey = Join-Path $PSScriptRoot "ssh_key.pem"
$remoteUser = "ubuntu"
$remotePath = "/home/ubuntu/app"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Deploying to EC2: $serverIP" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Navigate to project root
$projectRoot = Split-Path -Parent $PSScriptRoot

# Step 1: Prepare remote directory
Write-Host "`n[1/5] Preparing remote directory..." -ForegroundColor Yellow
ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "mkdir -p $remotePath/backend $remotePath/frontend"

# Step 2: Upload backend files (excluding .venv and __pycache__)
Write-Host "`n[2/5] Uploading backend code..." -ForegroundColor Yellow
scp -i $sshKey -o StrictHostKeyChecking=no -r "$projectRoot/backend" "${remoteUser}@${serverIP}:${remotePath}/"

# Step 3: Upload docker-compose
Write-Host "`n[3/5] Uploading docker-compose file..." -ForegroundColor Yellow
scp -i $sshKey -o StrictHostKeyChecking=no "$projectRoot/deploy/docker-compose.prod.yml" "${remoteUser}@${serverIP}:${remotePath}/docker-compose.yml"

# Step 4: Install dependencies and start backend
Write-Host "`n[4/5] Starting backend service..." -ForegroundColor Yellow
$deployScript = @'
cd /home/ubuntu/app/backend
pkill -f 'uvicorn main:app' || true
python3 -m venv .venv || true
.venv/bin/pip install -q -r requirements.txt
nohup .venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 > /tmp/backend.log 2>&1 &
sleep 3
echo 'Backend started'
'@

ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" $deployScript

# Step 5: Verify deployment
Write-Host "`n[5/5] Verifying deployment..." -ForegroundColor Yellow
Start-Sleep -Seconds 5
try {
    $response = Invoke-RestMethod -Uri "http://${serverIP}:8000/health" -Method GET -TimeoutSec 10
    Write-Host "✓ Backend health check passed: $($response | ConvertTo-Json -Compress)" -ForegroundColor Green
}
catch {
    Write-Host "✗ Backend health check failed" -ForegroundColor Red
    Write-Host "Checking remote logs..." -ForegroundColor Yellow
    ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "tail -30 /tmp/backend.log"
    throw "Deployment verification failed"
}

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "Deployment Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "`nBackend API: http://${serverIP}:8000" -ForegroundColor Cyan
Write-Host "API Docs: http://${serverIP}:8000/docs" -ForegroundColor Cyan
Write-Host "`nLogin with:" -ForegroundColor Cyan
Write-Host "  Username: admin.user@zopper.com" -ForegroundColor White
Write-Host "  Password: admin123" -ForegroundColor White
