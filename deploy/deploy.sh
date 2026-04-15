#!/bin/bash
set -e

SERVER_IP="13.202.80.63"
REMOTE_USER="ubuntu"
REMOTE_PATH="/home/ubuntu/app"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "========================================"
echo "Deploying to EC2: $SERVER_IP"
echo "========================================"

# Step 1: Prepare remote directory
echo ""
echo "[1/5] Preparing remote directory..."
ssh -i deploy/ssh_key.pem -o StrictHostKeyChecking=no ${REMOTE_USER}@${SERVER_IP} "mkdir -p $REMOTE_PATH/backend"

# Step 2: Upload backend code
echo ""
echo "[2/5] Uploading backend code..."
scp -i deploy/ssh_key.pem -o StrictHostKeyChecking=no -r backend ${REMOTE_USER}@${SERVER_IP}:${REMOTE_PATH}/

# Step 3: Upload docker-compose
echo ""
echo "[3/5] Uploading docker-compose file..."
scp -i deploy/ssh_key.pem -o StrictHostKeyChecking=no deploy/docker-compose.prod.yml ${REMOTE_USER}@${SERVER_IP}:${REMOTE_PATH}/docker-compose.yml

# Step 4: Start backend
echo ""
echo "[4/5] Starting backend service..."
ssh -i deploy/ssh_key.pem -o StrictHostKeyChecking=no ${REMOTE_USER}@${SERVER_IP} << 'EOF'
cd /home/ubuntu/app/backend
pkill -f 'uvicorn main:app' || true
python3 -m venv .venv || true
.venv/bin/pip install -q -r requirements.txt
nohup .venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 > /tmp/backend.log 2>&1 &
sleep 3
echo 'Backend started'
EOF

# Step 5: Verify
echo ""
echo "[5/5] Verifying deployment..."
sleep 5
curl -f http://${SERVER_IP}/api/health && echo "✓ Backend health check passed" || (echo "✗ Backend health check failed" && ssh -i deploy/ssh_key.pem -o StrictHostKeyChecking=no ${REMOTE_USER}@${SERVER_IP} "tail -30 /tmp/backend.log" && exit 1)

echo ""
echo "========================================"
echo "Deployment Complete!"
echo "========================================"
echo ""
echo "Frontend: http://${SERVER_IP}/"
echo "Backend API: http://${SERVER_IP}/api"
echo "API Docs: http://${SERVER_IP}/docs"
