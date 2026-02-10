import argparse
import re

from db.session import SessionLocal
from db.base import Base
from db.session import engine
from authentication.models import User
from authentication.security import hash_password


EMAIL_RE = re.compile(r"^[a-z]+\.[a-z]+@zopper\.com$")


def main():
    parser = argparse.ArgumentParser(description="Create a user (production-safe).")
    parser.add_argument("--email", required=True, help="name.surname@zopper.com")
    parser.add_argument("--password", required=True)
    parser.add_argument("--role", required=True, choices=["admin", "employee"])
    args = parser.parse_args()

    if not EMAIL_RE.match(args.email):
        raise SystemExit("Email must be in the format name.surname@zopper.com")

    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        existing = db.query(User).filter(User.username == args.email).first()
        if existing:
            raise SystemExit("User already exists")

        user = User(
            username=args.email,
            password_hash=hash_password(args.password),
            role=args.role,
            is_active=True,
        )
        db.add(user)
        db.commit()
        print(f"Created user: {args.email} ({args.role})")
    finally:
        db.close()


if __name__ == "__main__":
    main()
