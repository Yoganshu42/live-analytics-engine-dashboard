import os
from dataclasses import dataclass


@dataclass
class LocalUser:
    username: str
    role: str
    is_active: bool = True


# Manual user dictionary for local/dev usage.
# Format: email must be name.surname@zopper.com
USERS = [
    {"email": "yoganshu.sharma@zopper.com", "password": "admin123", "role": "admin"},
    {"email": "employee.user@zopper.com", "password": "employee123", "role": "employee"},
]


def use_local_auth() -> bool:
    return os.getenv("USE_LOCAL_AUTH", "0") == "1"


def get_local_user(email: str) -> LocalUser | None:
    for item in USERS:
        if item["email"] == email:
            return LocalUser(username=item["email"], role=item["role"], is_active=True)
    return None


def verify_local_user(email: str, password: str, role: str) -> LocalUser | None:
    for item in USERS:
        if (
            item["email"] == email
            and item["password"] == password
            and item["role"] == role
        ):
            return LocalUser(username=item["email"], role=item["role"], is_active=True)
    return None
