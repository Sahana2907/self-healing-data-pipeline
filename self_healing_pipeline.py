import datetime
import json
import os
import sqlite3
from typing import List, Optional
import pandas as pd
import requests

EXPECTED_SCHEMA = [
    "transaction_id",
    "customer_email",
    "purchase_amount",
    "purchase_date",
]

DB_PATH = "pipeline_metadata.db"
DLQ_DIR = "dead_letter_queue"


# -----------------------------------------------------------
# 1. Cache & Metadata Store (SQLite)
# -----------------------------------------------------------
def init_db():
    os.makedirs(DLQ_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_cache (
            incoming_columns TEXT PRIMARY KEY,
            mapping_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def get_cached_mapping(actual_cols: List[str]) -> Optional[dict]:
    key = ",".join(sorted(actual_cols))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT mapping_json FROM schema_cache WHERE incoming_columns = ?",
        (key,),
    )
    row = cursor.fetchone()
    conn.close()
    if row:
        return json.loads(row[0])
    return None


def save_cached_mapping(actual_cols: List[str], mapping: dict):
    key = ",".join(sorted(actual_cols))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR REPLACE INTO schema_cache (incoming_columns, mapping_json)
        VALUES (?, ?)
    """,
        (key, json.dumps(mapping)),
    )
    conn.commit()
    conn.close()


# -----------------------------------------------------------
# 2. Local LLM Schema Detective
# -----------------------------------------------------------
def heal_schema_with_llm(
    expected_cols: List[str], actual_cols: List[str]
) -> Optional[dict]:
    prompt = f"""You are a data engineer system. Your job is to map actual data columns to the expected schema.

Expected columns: {expected_cols}
Actual columns: {actual_cols}

Match the actual columns to the expected columns based on semantic meaning.
Return ONLY a valid JSON object where keys are actual columns and values are expected columns.
Do not include any markdown, explanations, or text outside the JSON.
"""
    url = "http://localhost:11434/api/generate"
    payload = {
        "model": "phi3",
        "prompt": prompt,
        "stream": False,
        "format": "json",
    }
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        result_text = response.json().get("response", "{}")
        return json.loads(result_text)
    except Exception as e:
        print(f"[ERROR] LLM healing failed: {e}")
        return None


# -----------------------------------------------------------
# 3. Dead-Letter Queue (DLQ) Writer
# -----------------------------------------------------------
def write_to_dlq(df: pd.DataFrame, reason: str):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(DLQ_DIR, f"failed_batch_{timestamp}.json")
    payload = {
        "timestamp": timestamp,
        "failure_reason": reason,
        "raw_data": df.to_dict(orient="records"),
    }
    with open(filepath, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"[DLQ] Corrupted batch isolated to: {filepath}")


# -----------------------------------------------------------
# 4. Ingestion Engine
# -----------------------------------------------------------
def process_data(
    df: pd.DataFrame, expected_schema: List[str]
) -> Optional[pd.DataFrame]:
    actual_cols = list(df.columns)

    # Fast path 1: Exact match
    if set(actual_cols) == set(expected_schema):
        print("[INFO] Schema matched target contract exactly.")
        return df[expected_schema]

    print("[WARN] Schema mismatch. Checking cache...")

    # Fast path 2: Local SQLite Cache
    mapping = get_cached_mapping(actual_cols)
    if mapping:
        print(f"[CACHE HIT] Applied cached mapping: {mapping}")
    else:
        print("[CACHE MISS] Invoking local Ollama (phi3)...")
        mapping = heal_schema_with_llm(expected_schema, actual_cols)
        if mapping:
            save_cached_mapping(actual_cols, mapping)
            print(f"[LLM HEALED] Mapping resolved and cached: {mapping}")

    if not mapping:
        write_to_dlq(df, "LLM failed to return a valid JSON mapping.")
        return None

    # Apply mapping & verify
    df_healed = df.rename(columns=mapping)
    missing_cols = [
        col for col in expected_schema if col not in df_healed.columns
    ]

    if missing_cols:
        write_to_dlq(df_healed, f"Missing required columns: {missing_cols}")
        return None

    print("[SUCCESS] Batch healed successfully.")
    return df_healed[expected_schema]


# -----------------------------------------------------------
# 5. Run & Test Scenarios
# -----------------------------------------------------------
if __name__ == "__main__":
    init_db()

    # Test Batch 1: Drifted keys (triggers LLM, then caches result)
    batch_1 = pd.DataFrame(
        {
            "txn_id": ["A1", "A2"],
            "email_address": ["alice@test.com", "bob@test.com"],
            "total_cost": [150.00, 89.50],
            "date": ["2026-05-26", "2026-05-26"],
        }
    )

    print("\n--- RUN 1: First time encountering drifted schema ---")
    result_1 = process_data(batch_1, EXPECTED_SCHEMA)
    print(result_1)

    print("\n--- RUN 2: Second batch with same schema (should hit cache) ---")
    result_2 = process_data(batch_1, EXPECTED_SCHEMA)
    print(result_2)

    # Test Batch 3: Completely invalid data (should trigger DLQ)
    batch_3 = pd.DataFrame(
        {
            "employee_id": ["E101"],
            "department": ["Engineering"],
            "salary": [120000],
        }
    )

    print("\n--- RUN 3: Irrecoverable schema (DLQ test) ---")
    result_3 = process_data(batch_3, EXPECTED_SCHEMA)