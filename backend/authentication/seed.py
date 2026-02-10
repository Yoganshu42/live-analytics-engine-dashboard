import os

from db.session import SessionLocal
from db.base import Base
from db.session import engine
from authentication.models import User
from authentication.security import hash_password


def ensure_user(db, username: str, password: str, role: str):
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
        return True
    return False


def main():
    Base.metadata.create_all(bind=engine)
    admin_username = os.getenv("ADMIN_USERNAME", "admin.user@zopper.com")
    admin_password = os.getenv("ADMIN_PASSWORD", "admin123")
    employee_username = os.getenv("EMPLOYEE_USERNAME", "employee.user@zopper.com")
    employee_password = os.getenv("EMPLOYEE_PASSWORD", "employee123")

    db = SessionLocal()
    try:
        created_admin = ensure_user(db, admin_username, admin_password, "admin")
        created_employee = ensure_user(db, employee_username, employee_password, "employee")
        print(
            f"Seed complete. admin_created={created_admin} employee_created={created_employee}"
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
