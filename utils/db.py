import os
import time
import psycopg2

def get_db_conn(retries=5):
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL is not set")

    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(db_url)
        except psycopg2.OperationalError as e:
            print(f"DB connection failed: {e} (attempt {attempt}/{retries})")
            time.sleep(attempt * 2)
            if attempt == retries:
                raise