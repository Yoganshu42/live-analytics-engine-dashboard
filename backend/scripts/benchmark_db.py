import time
import sys
import os
from sqlalchemy import text

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.session import SessionLocal
from services.analytics_repository import get_dataframe

def benchmark():
    db = SessionLocal()
    try:
        print("Checking sources...")
        result = db.execute(text("SELECT DISTINCT source, dataset_type FROM data_rows")).fetchall()
        print(f"Sources found: {result}")
        
        for source, dataset_type in result:
            print(f"\nBenchmarking source='{source}', dataset_type='{dataset_type}'...")
            start = time.time()
            df = get_dataframe(db, None, source, dataset_type)
            duration = time.time() - start
            print(f"  Rows: {len(df)}")
            print(f"  Time: {duration:.4f}s")
            
    finally:
        db.close()

if __name__ == "__main__":
    benchmark()
