# Live Dashboard

A modern analytics dashboard with a FastAPI backend, a Next.js frontend, CSV/XLSX ingest, and a branded landing experience.

**Stack**
- `FastAPI` + `SQLAlchemy` + `PostgreSQL`
- `Next.js` + `Tailwind`
- CSV/XLSX ingest via Swagger

**Monorepo Layout**
- `backend/` — API service
- `frontend/my-app/` — Web app

---

**Quick Start**

Backend:
```powershell
cd backend
uvicorn main:app --reload
```

Frontend:
```powershell
cd frontend/my-app
npm install
npm run dev
```

---

**Configuration**

Create `backend/.env` and set:
- `DATABASE_URL`
- `JWT_SECRET`
- `BOOTSTRAP_TOKEN`
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- `EMPLOYEE_USERNAME`
- `EMPLOYEE_PASSWORD`

The backend loads `.env` automatically and creates required tables (`users`, `data_rows`) on startup.

---

**Upload Data (Swagger)**

1. Open: `http://127.0.0.1:8000/docs`
2. Login: `POST /auth/login`
3. Authorize: `Bearer <access_token>`
4. Upload: `POST /upload`

Required fields for upload:
- `file` (CSV/XLSX)
- `source` (e.g., `samsung`, `godrej`, `reliance`)
- `dataset_type` (`sales` or `claims`)
- `job_id` (optional)

---

**Deploy**

Frontend (Vercel):
```powershell
cd frontend/my-app
vercel --prod --yes
```

Backend (Vercel, testing only):
```powershell
cd backend
vercel --prod --yes
```

Note: Vercel backend requires a publicly reachable database. Private VPC RDS will fail.

Backend (AWS ECS/Fargate, production):
- Recommended for private VPC RDS

---

**Notes**

- Landing video: `frontend/my-app/public/Business_Analytics_Video_Generation_Prompt.mp4`
- `/events` provides SSE (no WebSockets on Vercel)
