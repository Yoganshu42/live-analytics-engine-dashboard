import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv("DATABASE_URL")
if not db_url:
    print("DATABASE_URL not found")
    exit(1)

engine = create_engine(db_url)
with engine.connect() as conn:
    # Use jsonb_exists function instead of ? operator to avoid SQLAlchemy placeholder issues
    result = conn.execute(text("SELECT count(*) FROM data_rows WHERE source='hitachi' AND dataset_type='claims' AND jsonb_exists(data::jsonb, 'Item Name');"))
    count = result.scalar()
    print(f"Number of Hitachi claims rows with 'Item Name': {count}")
    
    if count > 0:
        result = conn.execute(text("SELECT data->'Item Name' FROM data_rows WHERE source='hitachi' AND dataset_type='claims' AND jsonb_exists(data::jsonb, 'Item Name') LIMIT 5;"))
        samples = result.fetchall()
        print("Sample 'Item Name' values:")
        for s in samples:
            print(f"- {s[0]}")
    else:
        # Check variants using jsonb_exists_any
        result = conn.execute(text("SELECT count(*) FROM data_rows WHERE source='hitachi' AND dataset_type='claims' AND jsonb_exists_any(data::jsonb, ARRAY['Item Name', 'item name', 'ITEM NAME', 'Item_Name']);"))
        count_variants = result.scalar()
        print(f"Number of Hitachi claims rows with any 'Item Name' variant: {count_variants}")
