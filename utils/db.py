import os
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL or SUPABASE_KEY is not set")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def insert_row(table: str, data: dict):
    """Insert a single row into a Supabase table."""
    response = supabase.table(table).insert(data).execute()
    return response

def upsert_row(table: str, data: dict, conflict_column: str):
    """Upsert a row based on a conflict column."""
    response = (
        supabase.table(table)
        .upsert(data, on_conflict=conflict_column)
        .execute()
    )
    return response

def insert_many(table: str, rows: list):
    """Insert multiple rows at once."""
    response = supabase.table(table).insert(rows).execute()
    return response