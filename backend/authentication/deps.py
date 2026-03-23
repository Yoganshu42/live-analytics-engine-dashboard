import os
import threading
import time
from datetime import datetime
from types import SimpleNamespace

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from jose import JWTError

from db.deps import get_db
from authentication.security import decode_token
from authentication.local_users import get_local_user, use_local_auth
from authentication.repository import get_user_by_identifier

security = HTTPBearer(auto_error=False)
AUTH_CACHE_TTL_SECONDS = int(os.getenv("AUTH_CACHE_TTL_SECONDS", "300"))
_auth_cache_lock = threading.Lock()
_auth_cache: dict[str, tuple[float, float, SimpleNamespace]] = {}


def _cache_get(token: str) -> SimpleNamespace | None:
    now = time.time()
    with _auth_cache_lock:
        cached = _auth_cache.get(token)
        if cached is None:
            return None
        expires_at, _cached_at, principal = cached
        if expires_at < now:
            _auth_cache.pop(token, None)
            return None
        return principal


def _cache_set(token: str, principal: SimpleNamespace) -> None:
    with _auth_cache_lock:
        now = time.time()
        _auth_cache[token] = (now + AUTH_CACHE_TTL_SECONDS, now, principal)


def list_live_users() -> dict[str, object]:
    now = time.time()
    users: list[dict[str, object]] = []
    with _auth_cache_lock:
        for token, cached in list(_auth_cache.items()):
            expires_at, cached_at, principal = cached
            if expires_at < now:
                _auth_cache.pop(token, None)
                continue
            users.append(
                {
                    "email": str(getattr(principal, "username", "") or ""),
                    "role": str(getattr(principal, "role", "") or ""),
                    "is_active": bool(getattr(principal, "is_active", False)),
                    "last_seen_at": datetime.fromtimestamp(cached_at).isoformat(),
                    "expires_at": datetime.fromtimestamp(expires_at).isoformat(),
                    "ttl_seconds": int(max(0, expires_at - now)),
                }
            )
    users.sort(key=lambda item: item.get("last_seen_at", ""), reverse=True)
    return {
        "ttl_seconds": int(AUTH_CACHE_TTL_SECONDS),
        "count": len(users),
        "users": users,
    }


def _principal_from_user(user) -> SimpleNamespace:
    return SimpleNamespace(
        username=getattr(user, "username", ""),
        role=getattr(user, "role", ""),
        is_active=bool(getattr(user, "is_active", False)),
    )


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
    db: Session = Depends(get_db),
):
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    token = credentials.credentials
    cached_user = _cache_get(token)
    if cached_user is not None:
        return cached_user
    try:
        payload = decode_token(token)
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    username = payload.get("sub")
    if not username:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    if use_local_auth():
        user = get_local_user(username)
        if user is None or not user.is_active:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User inactive")
        principal = _principal_from_user(user)
        _cache_set(token, principal)
        return principal

    user = get_user_by_identifier(db, username)
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User inactive")

    principal = _principal_from_user(user)
    _cache_set(token, principal)
    return principal


def require_admin(user=Depends(get_current_user)):
    if user.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user
