import os
from typing import Any
from fastapi import APIRouter, Depends, HTTPException, status, Header
from sqlalchemy.orm import Session

from db.deps import get_db
from authentication.schemas import (
    LoginRequest,
    LoginResponse,
    UserResponse,
    CreateUserRequest,
    UpdatePasswordRequest,
)
from authentication.security import verify_password, create_access_token, hash_password
from authentication.local_users import use_local_auth, verify_local_user
from authentication.deps import get_current_user, require_admin
from authentication.repository import (
    get_user_by_identifier,
    create_user as create_user_record,
    list_users as list_user_records,
    delete_user as delete_user_record,
    update_user_password as update_user_password_record,
)

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)):
    if use_local_auth():
        user = verify_local_user(payload.email, payload.password, payload.role)
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    else:
        user = get_user_by_identifier(db, payload.email)
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
def me(current_user: Any = Depends(get_current_user)):
    return {
        "email": current_user.username,
        "role": current_user.role,
        "is_active": current_user.is_active,
    }


@router.post("/users", response_model=UserResponse)
def create_user(
    payload: CreateUserRequest,
    db: Session = Depends(get_db),
    _: Any = Depends(require_admin),
):
    if use_local_auth():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Local auth enabled. Edit backend/authentication/local_users.py to add users.",
        )

    existing = get_user_by_identifier(db, payload.email)
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User already exists")

    user = create_user_record(
        db,
        identifier=payload.email,
        password_hash=hash_password(payload.password),
        role=payload.role,
    )
    db.commit()

    if user is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create user")

    return {
        "email": user.username,
        "role": user.role,
        "is_active": user.is_active,
    }


@router.get("/users", response_model=list[UserResponse])
def list_users(
    search: str | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
    _: Any = Depends(require_admin),
):
    if use_local_auth():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Local auth enabled. User management API is unavailable.",
        )

    users = list_user_records(db, search=search, limit=limit)
    return [
        {
            "email": user.username,
            "role": user.role,
            "is_active": user.is_active,
        }
        for user in users
    ]


@router.delete("/users/{email}")
def delete_user(
    email: str,
    db: Session = Depends(get_db),
    _: Any = Depends(require_admin),
):
    if use_local_auth():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Local auth enabled. User management API is unavailable.",
        )

    current = get_user_by_identifier(db, email)
    if current is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    deleted = delete_user_record(db, email)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete user")
    db.commit()
    return {"deleted": True, "email": email}


@router.patch("/users/{email}/password")
def update_user_password(
    email: str,
    payload: UpdatePasswordRequest,
    db: Session = Depends(get_db),
    _: Any = Depends(require_admin),
):
    if use_local_auth():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Local auth enabled. User management API is unavailable.",
        )

    current = get_user_by_identifier(db, email)
    if current is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    updated = update_user_password_record(
        db,
        identifier=email,
        password_hash=hash_password(payload.password),
    )
    if not updated:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update password")
    db.commit()
    return {"updated": True, "email": email}


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
        user = get_user_by_identifier(db, username)
        if user is None:
            user = create_user_record(
                db,
                identifier=username,
                password_hash=hash_password(password),
                role=role,
            )
            if user is not None:
                created.append(username)

    ensure_user(admin_username, admin_password, "admin")
    ensure_user(employee_username, employee_password, "employee")
    db.commit()

    return {"created": created, "admin": admin_username, "employee": employee_username}
