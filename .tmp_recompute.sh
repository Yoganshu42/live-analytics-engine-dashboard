docker exec -i live-dashboard-backend-1 python - <<'PY'
from db.session import SessionLocal
from services.precompute_service import rebuild_precomputed_analytics

db = SessionLocal()
try:
    rebuild_precomputed_analytics(db=db, source='reliance', dataset_type='sales', job_id=None)
    db.commit()
    print('recompute_done')
finally:
    db.close()
PY
