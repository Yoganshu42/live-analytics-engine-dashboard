$ErrorActionPreference = "Stop"

$serverIP = "43.205.134.36"
$sshKey = Join-Path $PSScriptRoot "ssh_key.pem"
$remoteUser = "ubuntu"
$remotePath = "/home/ubuntu/live-dashboard"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Deploying to EC2: $serverIP" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Navigate to project root
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

# Use rsync to sync files (excluding unnecessary directories)
Write-Host "`nSyncing files to EC2..." -ForegroundColor Yellow
ssh -i $sshKey -o StrictHostKeyChecking=no ${remoteUser}@${serverIP} "mkdir -p $remotePath"

# Rsync the code
Write-Host "Uploading backend..." -ForegroundColor Yellow
scp -i $sshKey -o StrictHostKeyChecking=no -r backend ${remoteUser}@${serverIP}:${remotePath}/

Write-Host "Uploading frontend..." -ForegroundColor Yellow  
scp -i $sshKey -o StrictHostKeyChecking=no -r frontend ${remoteUser}@${serverIP}:${remotePath}/

Write-Host "Uploading docker-compose..." -ForegroundColor Yellow
scp -i $sshKey -o StrictHostKeyChecking=no deploy/docker-compose.prod.yml ${remoteUser}@${serverIP}:${remotePath}/docker-compose.yml

Write-Host "`nRestarting services on EC2..." -ForegroundColor Yellow
ssh -i $sshKey -o StrictHostKeyChecking=no ${remoteUser}@${serverIP} @"
cd $remotePath
docker-compose down
docker-compose build
docker-compose up -d --remove-orphans
echo 'Waiting for services to start...'
sleep 10
docker-compose ps
echo 'Checking backend health...'
curl -f http://localhost:8000/health || echo 'Backend health check failed!'
"@

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "Deployment Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "`nFrontend: http://${serverIP}:3000" -ForegroundColor Cyan
Write-Host "Backend API: http://${serverIP}:8000" -ForegroundColor Cyan
Write-Host "Backend Docs: http://${serverIP}:8000/docs" -ForegroundColor Cyan
