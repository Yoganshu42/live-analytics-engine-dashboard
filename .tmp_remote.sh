#!/usr/bin/env bash
set -e

docker exec -i live-dashboard-backend-1 python - <<'PY'
import json, subprocess
login_payload = json.dumps({"email": "yoganshu.sharma@zopper.com", "password": "yoganshu@1234", "role": "admin"})
res = subprocess.check_output([
    "curl", "-s", "-X", "POST", "http://127.0.0.1:8000/auth/login",
    "-H", "Content-Type: application/json", "-d", login_payload,
])
print(res.decode())
PY