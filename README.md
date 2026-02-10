# Live Dashboard: Business Control Centre

<p align="center">
  <img src="https://img.shields.io/badge/Next.js-000000?style=for-the-badge&logo=nextdotjs&logoColor=white" alt="Next.js" />
  <img src="https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL" />
  <img src="https://img.shields.io/badge/Tailwind_CSS-38B2AC?style=for-the-badge&logo=tailwind-css&logoColor=white" alt="Tailwind CSS" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Framer%20Motion-000000?style=for-the-badge&logo=framer&logoColor=white" alt="Framer Motion" />
  <img src="https://img.shields.io/badge/Vercel-000000?style=for-the-badge&logo=vercel&logoColor=white" alt="Vercel" />
</p>

<p align="center"><b>FastAPI + Next.js analytics dashboard</b> with CSV/XLSX ingestion and branded data visualization.</p>

---

## Overview

**Stack**
- `FastAPI` + `SQLAlchemy` + `PostgreSQL`
- `Next.js` + `Tailwind`

**Repository**
- 🧠 `backend/` — API service
- 🎛️ `frontend/my-app/` — Web app

---

## Directory Structure

```text
Live Dashboard
├─ backend                # 🧠 FastAPI service
│  ├─ authentication
│  ├─ core
│  ├─ db
│  ├─ models
│  ├─ routers
│  ├─ services
│  ├─ main.py
│  └─ requirements.txt
├─ frontend               # 🎛️ Next.js app
│  └─ my-app
│     ├─ app              # 🧭 routes
│     ├─ components       # 🧩 UI components
│     ├─ public           # 🖼️ assets
│     └─ package.json
├─ docker-compose.yml     # 🐳 local orchestration
└─ README.md              # 📘 documentation
```

---

## Quick Start

**Backend**
```powershell
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
```

**Frontend**
```powershell
cd frontend/my-app
npm install
npm run dev
```

---

## Configuration

Create `backend/.env` with:
- `DATABASE_URL`
- `JWT_SECRET`
- `BOOTSTRAP_TOKEN`
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- `EMPLOYEE_USERNAME`
- `EMPLOYEE_PASSWORD`

The backend loads `.env` automatically and creates required tables (`users`, `data_rows`) on startup.

---

## Data Ingestion (Swagger)

1. Open: `http://127.0.0.1:8000/docs`
2. Login: `POST /auth/login`
3. Authorize: `Bearer <access_token>`
4. Upload: `POST /upload`

**Required fields**
- `file` (CSV/XLSX)
- `source` (e.g., `samsung`, `godrej`, `reliance`)
- `dataset_type` (`sales` or `claims`)
- `job_id` (optional)

---

## Deployment

**Frontend (Vercel)**
```powershell
cd frontend/my-app
vercel --prod --yes
```

**Backend (Vercel, testing only)**
```powershell
cd backend
vercel --prod --yes
```

Note: Vercel backend requires a publicly reachable database. Private VPC RDS will fail.

**Backend (AWS ECS/Fargate, production)**
- Recommended for private VPC RDS

---

## Notes

- Landing video: `frontend/my-app/public/Business_Analytics_Video_Generation_Prompt.mp4`
- `/events` provides SSE (no WebSockets on Vercel)
