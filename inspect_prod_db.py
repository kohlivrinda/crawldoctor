
import os
from sqlalchemy import create_engine, text
import json

# Connection string for the local proxy to production DB
DB_URL = "postgresql://postgres:postgres@localhost:15432/crawldoctor"

def inspect_db():
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            print("Checking VisitEvent types...")
            res = conn.execute(text("SELECT event_type, COUNT(*) FROM visit_events GROUP BY event_type")).all()
            for r in res:
                print(f"  {r[0]}: {r[1]}")
            
            print("\nChecking sample form_submit data...")
            res = conn.execute(text("SELECT event_data FROM visit_events WHERE event_type = 'form_submit' LIMIT 5")).all()
            for r in res:
                print(f"  Data: {json.dumps(r[0])[:200]}...")
                
            print("\nChecking Visit timestamp range...")
            res = conn.execute(text("SELECT MIN(timestamp), MAX(timestamp) FROM visits")).first()
            print(f"  Visits: {res[0]} to {res[1]}")

            print("\nChecking VisitEvent timestamp range...")
            res = conn.execute(text("SELECT MIN(timestamp), MAX(timestamp) FROM visit_events")).first()
            print(f"  Events: {res[0]} to {res[1]}")

    except Exception as e:
        print(f"Error connecting to DB: {e}")

if __name__ == "__main__":
    inspect_db()
