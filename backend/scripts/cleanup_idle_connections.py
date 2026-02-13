import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()
load_dotenv("backend/.env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL is not set")

MIN_IDLE_MINUTES = int(os.getenv("DB_IDLE_CLEAN_MINUTES", "10"))
KILL_USER = os.getenv("DB_IDLE_CLEAN_USER", "")
APP_NAME_PREFIX = os.getenv("DB_IDLE_CLEAN_APP_PREFIX", "")

base_sql = """
SELECT pid
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND usename <> 'rdsadmin'
  AND state = 'idle'
  AND now() - state_change > (%s || ' minutes')::interval
"""

params = [str(MIN_IDLE_MINUTES)]

if KILL_USER:
    base_sql += " AND usename = %s"
    params.append(KILL_USER)

if APP_NAME_PREFIX:
    base_sql += " AND application_name LIKE %s"
    params.append(f"{APP_NAME_PREFIX}%")

terminate_sql = f"SELECT pg_terminate_backend(pid) FROM ({base_sql}) t"

with psycopg2.connect(DATABASE_URL) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SHOW max_connections")
        max_conn = cur.fetchone()[0]

        cur.execute("SELECT count(*) FROM pg_stat_activity")
        before = cur.fetchone()[0]

        cur.execute(base_sql, params)
        candidates = [r[0] for r in cur.fetchall()]

        killed = 0
        if candidates:
            cur.execute(terminate_sql, params)
            killed = sum(1 for (ok,) in cur.fetchall() if ok)

        cur.execute("SELECT count(*) FROM pg_stat_activity")
        after = cur.fetchone()[0]

print(f"max_connections={max_conn}")
print(f"before={before}")
print(f"candidates={len(candidates)}")
print(f"terminated={killed}")
print(f"after={after}")
