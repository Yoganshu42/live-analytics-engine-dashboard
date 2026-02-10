# AWS Deployment Guide (FastAPI + Next.js + Postgres)

This setup is optimized for AWS ECS Fargate with an Application Load Balancer and RDS Postgres.

## Components
- **Backend**: FastAPI container (from `backend/Dockerfile`).
- **Frontend**: Next.js container (from `frontend/my-app/Dockerfile`).
- **Database**: RDS Postgres.

## 1) Build and Push Images (ECR)
1. Create two ECR repositories: `live-dashboard-backend` and `live-dashboard-frontend`.
2. Build and push images:
   - Backend:
     - `docker build -t live-dashboard-backend ./backend`
     - `docker tag live-dashboard-backend:latest <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/live-dashboard-backend:latest`
     - `docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/live-dashboard-backend:latest`
   - Frontend:
     - `docker build -t live-dashboard-frontend --build-arg NEXT_PUBLIC_API_BASE=https://<API_DOMAIN> ./frontend/my-app`
     - `docker tag live-dashboard-frontend:latest <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/live-dashboard-frontend:latest`
     - `docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/live-dashboard-frontend:latest`

## 2) Create RDS Postgres
- Create a Postgres instance and a database `analytics_db`.
- Note the connection string for `DATABASE_URL`.

## 3) ECS Services
Create two ECS Fargate services (backend + frontend) behind an ALB.

### Backend Task Definition
Set environment variables:
- `DATABASE_URL`
- `JWT_SECRET`
- `ACCESS_TOKEN_EXPIRE_MINUTES`
- `BOOTSTRAP_TOKEN`
- `ADMIN_USERNAME` (format: `name.surname@zopper.com`)
- `ADMIN_PASSWORD`
- `EMPLOYEE_USERNAME` (format: `name.surname@zopper.com`)
- `EMPLOYEE_PASSWORD`

Expose container port `8000`.

### Frontend Task Definition
Set environment variable:
- `NEXT_PUBLIC_API_BASE=https://<API_DOMAIN>`

Expose container port `3000`.

## 4) DNS and Routing
- Point `https://<API_DOMAIN>` to the backend target group.
- Point `https://<APP_DOMAIN>` to the frontend target group.

## 5) Bootstrap Users (one-time)
After backend is running, call:
- `POST https://<API_DOMAIN>/auth/bootstrap`
- Header: `X-Bootstrap-Token: <BOOTSTRAP_TOKEN>`

This will create the default admin/employee users.

## 6) Health Check
- Backend health: `GET /health`

## Notes
- If you want managed auth later (Cognito), the backend can be adapted to validate Cognito JWTs.
