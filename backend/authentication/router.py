import os
from fastapi import APIRouter, Depends, HTTPException, status, Header
from sqlalchemy.orm import Session

from db.deps import get_db
from authentication.models import User
from authentication.schemas import (
    LoginRequest,
    LoginResponse,
    UserResponse,
    CreateUserRequest,
)
from authentication.security import verify_password, create_access_token, hash_password
from authentication.local_users import use_local_auth, verify_local_user
from authentication.deps import get_current_user, require_admin

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)):
    if use_local_auth():
        user = verify_local_user(payload.email, payload.password, payload.role)
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    else:
        user = db.query(User).filter(User.username == payload.email).first()
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

        if user.role != payload.role:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

        if not verify_password(payload.password, user.password_hash):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    token = create_access_token({"sub": user.username, "role": user.role})
    return {
        "access_token": token,
        "token_type": "bearer",
        "role": user.role,
        "email": user.username,
    }


@router.get("/me", response_model=UserResponse)
def me(current_user: User = Depends(get_current_user)):
    return {
        "email": current_user.username,
        "role": current_user.role,
        "is_active": current_user.is_active,
    }


@router.post("/users", response_model=UserResponse)
def create_user(
    payload: CreateUserRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    if use_local_auth():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Local auth enabled. Edit backend/authentication/local_users.py to add users.",
        )
    existing = db.query(User).filter(User.username == payload.email).first()
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User already exists")

    user = User(
        username=payload.email,
        password_hash=hash_password(payload.password),
        role=payload.role,
        is_active=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    return {
        "email": user.username,
        "role": user.role,
        "is_active": user.is_active,
    }


@router.post("/bootstrap")
def bootstrap_users(
    db: Session = Depends(get_db),
    token: str | None = Header(None, alias="X-Bootstrap-Token"),
):
    bootstrap_token = os.getenv("BOOTSTRAP_TOKEN")
    if not bootstrap_token:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="BOOTSTRAP_TOKEN not set")

    if token != bootstrap_token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid bootstrap token")

    admin_username = os.getenv("ADMIN_USERNAME", "admin.user@zopper.com")
    admin_password = os.getenv("ADMIN_PASSWORD", "admin123")
    employee_username = os.getenv("EMPLOYEE_USERNAME", "employee.user@zopper.com")
    employee_password = os.getenv("EMPLOYEE_PASSWORD", "employee123")

    created = []

    def ensure_user(username: str, password: str, role: str):
        user = db.query(User).filter(User.username == username).first()
        if user is None:
            user = User(
                username=username,
                password_hash=hash_password(password),
                role=role,
                is_active=True,
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            created.append(username)

    ensure_user(admin_username, admin_password, "admin")
    ensure_user(employee_username, employee_password, "employee")

    return {"created": created, "admin": admin_username, "employee": employee_username}
