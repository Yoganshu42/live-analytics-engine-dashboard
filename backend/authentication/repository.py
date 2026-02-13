from types import SimpleNamespace

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session


_ALLOWED_IDENTIFIER_COLUMNS = ("username", "email")
_ALLOWED_PASSWORD_COLUMNS = ("password_hash", "hashed_password", "password")


def _users_columns(db: Session) -> set[str]:
    return {c["name"] for c in inspect(db.bind).get_columns("users")}


def _pick_column(available: set[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in available:
            return candidate
    return None


def _identifier_column(available: set[str]) -> str | None:
    col = _pick_column(available, _ALLOWED_IDENTIFIER_COLUMNS)
    if col in _ALLOWED_IDENTIFIER_COLUMNS:
        return col
    return None


def _password_column(available: set[str]) -> str | None:
    col = _pick_column(available, _ALLOWED_PASSWORD_COLUMNS)
    if col in _ALLOWED_PASSWORD_COLUMNS:
        return col
    return None


def _user_projection(available: set[str], identifier_col: str, password_col: str) -> str:
    role_expr = '"role"' if "role" in available else "'employee' AS role"
    active_expr = '"is_active"' if "is_active" in available else "TRUE AS is_active"
    created_expr = '"created_at"' if "created_at" in available else "NULL AS created_at"
    updated_expr = '"updated_at"' if "updated_at" in available else "NULL AS updated_at"

    return (
        f'"id", "{identifier_col}" AS username, "{password_col}" AS password_hash, '
        f"{role_expr}, {active_expr}, {created_expr}, {updated_expr}"
    )


def get_user_by_identifier(db: Session, identifier: str):
    cols = _users_columns(db)
    identifier_col = _identifier_column(cols)
    password_col = _password_column(cols)

    if not identifier_col or not password_col:
        return None

    projection = _user_projection(cols, identifier_col, password_col)

    row = (
        db.execute(
            text(
                f"""
                SELECT {projection}
                FROM users
                WHERE "{identifier_col}" = :identifier
                LIMIT 1
                """
            ),
            {"identifier": identifier},
        )
        .mappings()
        .first()
    )

    if not row:
        return None

    return SimpleNamespace(**row)


def create_user(db: Session, identifier: str, password_hash: str, role: str):
    cols = _users_columns(db)
    identifier_col = _identifier_column(cols)
    password_col = _password_column(cols)

    if not identifier_col or not password_col:
        raise RuntimeError("Unsupported users schema")

    existing = get_user_by_identifier(db, identifier)
    if existing is not None:
        return None

    insert_columns = [f'"{identifier_col}"', f'"{password_col}"']
    insert_values = [":identifier", ":password_value"]
    params = {
        "identifier": identifier,
        "password_value": password_hash,
    }

    if "role" in cols:
        insert_columns.append('"role"')
        insert_values.append(":role")
        params["role"] = role

    if "is_active" in cols:
        insert_columns.append('"is_active"')
        insert_values.append(":is_active")
        params["is_active"] = True

    projection = _user_projection(cols, identifier_col, password_col)

    created = (
        db.execute(
            text(
                f"""
                INSERT INTO users ({", ".join(insert_columns)})
                VALUES ({", ".join(insert_values)})
                RETURNING {projection}
                """
            ),
            params,
        )
        .mappings()
        .first()
    )

    if not created:
        return None

    return SimpleNamespace(**created)


def list_users(db: Session, search: str | None = None, limit: int = 100):
    cols = _users_columns(db)
    identifier_col = _identifier_column(cols)
    password_col = _password_column(cols)
    if not identifier_col or not password_col:
        return []

    projection = _user_projection(cols, identifier_col, password_col)
    where_clause = ""
    params: dict[str, object] = {"limit": max(1, min(limit, 500))}

    if search:
        where_clause = f'WHERE "{identifier_col}" ILIKE :search'
        params["search"] = f"%{search.strip()}%"

    rows = (
        db.execute(
            text(
                f"""
                SELECT {projection}
                FROM users
                {where_clause}
                ORDER BY "{identifier_col}" ASC
                LIMIT :limit
                """
            ),
            params,
        )
        .mappings()
        .all()
    )
    return [SimpleNamespace(**row) for row in rows]


def delete_user(db: Session, identifier: str) -> bool:
    cols = _users_columns(db)
    identifier_col = _identifier_column(cols)
    if not identifier_col:
        return False

    deleted = db.execute(
        text(
            f"""
            DELETE FROM users
            WHERE "{identifier_col}" = :identifier
            """
        ),
        {"identifier": identifier},
    )
    return bool((deleted.rowcount or 0) > 0)


def update_user_password(db: Session, identifier: str, password_hash: str) -> bool:
    cols = _users_columns(db)
    identifier_col = _identifier_column(cols)
    password_col = _password_column(cols)
    if not identifier_col or not password_col:
        return False

    set_parts = [f'"{password_col}" = :password_hash']
    if "updated_at" in cols:
        set_parts.append('"updated_at" = NOW()')

    updated = db.execute(
        text(
            f"""
            UPDATE users
            SET {", ".join(set_parts)}
            WHERE "{identifier_col}" = :identifier
            """
        ),
        {"identifier": identifier, "password_hash": password_hash},
    )
    return bool((updated.rowcount or 0) > 0)
