$ErrorActionPreference = "Stop"

$serverIP = "13.235.56.166"
$sshKey = Join-Path $PSScriptRoot "ssh_key.pem"
$remoteUser = "ubuntu"
$remotePath = "/home/ubuntu/live-dashboard"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "FAST DEPLOYMENT to EC2: $serverIP" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Navigate to project root
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

# Step 1: Create .dockerignore files to speed up builds
Write-Host "`n[1/7] Creating .dockerignore files..." -ForegroundColor Yellow

"node_modules
.next
.git
*.log
.env.local
.DS_Store" | Out-File -FilePath "frontend/my-app/.dockerignore" -Encoding utf8

".venv
__pycache__
*.pyc
.git
*.log
.env
.pytest_cache" | Out-File -FilePath "backend/.dockerignore" -Encoding utf8

Write-Host "  Done!" -ForegroundColor Green

# Step 2: Create optimized docker-compose with build caching
Write-Host "`n[2/7] Creating optimized docker-compose..." -ForegroundColor Yellow

'version: "3.9"
services:
  backend:
    build:
      context: ./backend
    image: live-dashboard-backend:latest
    environment:
      DATABASE_URL: postgresql://postgres:DBaaKhprViUUu2F@database-1.cr4kiosyiszs.ap-south-1.rds.amazonaws.com:5432/analytics_db
      JWT_SECRET: change-me
      ACCESS_TOKEN_EXPIRE_MINUTES: "720"
      BOOTSTRAP_TOKEN: change-me
      ADMIN_USERNAME: admin.user@zopper.com
      ADMIN_PASSWORD: admin123
      EMPLOYEE_USERNAME: employee.user@zopper.com
      EMPLOYEE_PASSWORD: employee123
      UVICORN_WORKERS: "2"
    ports:
      - "8000:8000"
    restart: unless-stopped

  frontend:
    build:
      context: ./frontend/my-app
      args:
        NEXT_PUBLIC_API_BASE: http://13.235.56.166:8000
    image: live-dashboard-frontend:latest
    environment:
      NEXT_PUBLIC_API_BASE: http://13.235.56.166:8000
    ports:
      - "3000:3000"
    depends_on:
      - backend
    restart: unless-stopped' | Out-File -FilePath "deploy/docker-compose.fast.yml" -Encoding utf8

Write-Host "  Done!" -ForegroundColor Green

# Step 3: Prepare remote directory
Write-Host "`n[3/7] Preparing remote directory..." -ForegroundColor Yellow
ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "mkdir -p $remotePath"
Write-Host "  Done!" -ForegroundColor Green

# Step 4: Sync files efficiently
Write-Host "`n[4/7] Syncing files..." -ForegroundColor Yellow

Write-Host "  Uploading backend..." -ForegroundColor Gray
scp -i $sshKey -o StrictHostKeyChecking=no -r backend "${remoteUser}@${serverIP}:${remotePath}/"

Write-Host "  Uploading frontend..." -ForegroundColor Gray
scp -i $sshKey -o StrictHostKeyChecking=no -r frontend "${remoteUser}@${serverIP}:${remotePath}/"

Write-Host "  Uploading docker-compose..." -ForegroundColor Gray
scp -i $sshKey -o StrictHostKeyChecking=no deploy/docker-compose.fast.yml "${remoteUser}@${serverIP}:${remotePath}/docker-compose.yml"

Write-Host "  Done!" -ForegroundColor Green

# Step 5: Stop old containers
Write-Host "`n[5/7] Stopping old containers..." -ForegroundColor Yellow
ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "cd $remotePath; docker-compose down --remove-orphans 2>/dev/null || true"
Write-Host "  Done!" -ForegroundColor Green

# Step 6: Build and start services
Write-Host "`n[6/7] Building and starting services..." -ForegroundColor Yellow
Write-Host "  (This may take 5-10 minutes on first build)" -ForegroundColor Gray

ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "cd $remotePath; echo 'Building backend...'; docker-compose build backend; echo 'Building frontend...'; docker-compose build frontend; echo 'Starting services...'; docker-compose up -d; echo 'Waiting for services...'; sleep 15; docker-compose ps"

Write-Host "  Done!" -ForegroundColor Green

# Step 7: Verify deployment
Write-Host "`n[7/7] Verifying deployment..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

try {
    $response = Invoke-RestMethod -Uri "http://${serverIP}:8000/health" -Method GET -TimeoutSec 10
    Write-Host "  Backend health check passed!" -ForegroundColor Green
    
    try {
        $frontendCheck = Invoke-WebRequest -Uri "http://${serverIP}:3000" -Method GET -TimeoutSec 10 -UseBasicParsing
        Write-Host "  Frontend is accessible!" -ForegroundColor Green
    }
    catch {
        Write-Host "  Frontend might still be starting..." -ForegroundColor Yellow
    }
}
catch {
    Write-Host "  Backend health check failed" -ForegroundColor Red
    Write-Host "  Checking logs..." -ForegroundColor Yellow
    ssh -i $sshKey -o StrictHostKeyChecking=no "${remoteUser}@${serverIP}" "cd $remotePath; docker-compose logs --tail=50"
    throw "Deployment verification failed"
}

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "DEPLOYMENT COMPLETE!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "`nFrontend: http://${serverIP}:3000" -ForegroundColor Cyan
Write-Host "Backend API: http://${serverIP}:8000" -ForegroundColor Cyan
Write-Host "API Docs: http://${serverIP}:8000/docs" -ForegroundColor Cyan
Write-Host "`nLogin credentials:" -ForegroundColor Cyan
Write-Host "  Admin: admin.user@zopper.com / admin123" -ForegroundColor White
Write-Host "  Employee: employee.user@zopper.com / employee123" -ForegroundColor White
