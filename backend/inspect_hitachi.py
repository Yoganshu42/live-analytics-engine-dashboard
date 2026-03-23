import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import json

load_dotenv()
db_url = os.getenv("DATABASE_URL")
if not db_url:
    print("DATABASE_URL not found")
    exit(1)

engine = create_engine(db_url)
with engine.connect() as conn:
    for ds in ["sales", "claims"]:
        result = conn.execute(text(f"SELECT data FROM data_rows WHERE source='hitachi' AND dataset_type='{ds}' LIMIT 50;"))
        rows = result.fetchall()
        if rows:
            all_keys = set()
            for row in rows:
                all_keys.update(row[0].keys())
            
            with open(f"hitachi_{ds}_keys.txt", "w") as f:
                for k in sorted(all_keys):
                    f.write(k + "\n")
            
            with open(f"hitachi_{ds}_sample.json", "w") as f:
                json.dump(rows[0][0], f, indent=2)
            
            print(f"Done writing {ds} info")
        else:
            print(f"No hitachi {ds} found")
