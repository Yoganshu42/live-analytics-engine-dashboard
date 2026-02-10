# Live Dashboard

FastAPI + Next.js analytics dashboard with CSV/XLSX ingest and a branded landing experience.

## Structure

- `backend/` — FastAPI API service
- `frontend/my-app/` — Next.js frontend

## Local Setup

### Backend

1. Create `backend/.env` (see `backend/.env.example` if present).
2. Install Python deps.
3. Run:

```powershell
cd backend
uvicorn main:app --reload
```

### Frontend

```powershell
cd frontend/my-app
npm install
npm run dev
```

## Database

The backend loads `DATABASE_URL` from `backend/.env` (via `python-dotenv`).
On startup, it creates required tables (e.g., `users`, `data_rows`).

## Upload Data (Swagger)

1. Open Swagger: `http://127.0.0.1:8000/docs`
2. Login: `POST /auth/login`
3. Click **Authorize** and paste `Bearer <access_token>`
4. Use `POST /upload` with:
   - `file` (CSV/XLSX)
   - `source` (e.g., `samsung`, `godrej`, `reliance`)
   - `dataset_type` (`sales` or `claims`)
   - `job_id` (optional)

## Deployment

### Frontend on Vercel

Set:

- `NEXT_PUBLIC_API_BASE` = your backend URL

Deploy:

```powershell
cd frontend/my-app
vercel --prod --yes
```

### Backend on Vercel (testing only)

Works only if the database is publicly reachable. If your DB is in a private VPC,
Vercel cannot reach it and the API will fail.

Deploy:

```powershell
cd backend
vercel --prod --yes
```

### Backend on AWS ECS/Fargate (production)

Recommended when using RDS in a private VPC.

## Notes

- Video background for landing page lives in `frontend/my-app/public/Business_Analytics_Video_Generation_Prompt.mp4`.
- `/events` provides SSE (server-sent events) instead of WebSockets for compatibility.
