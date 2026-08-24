import datetime
import json
import os
import sqlite3
import time
from typing import List, Optional, Tuple
import pandas as pd
import requests
import streamlit as st

EXPECTED_SCHEMA = [
    "transaction_id",
    "customer_email",
    "purchase_amount",
    "purchase_date",
]
DB_PATH = "pipeline_metadata.db"
DLQ_DIR = "dead_letter_queue"


# -----------------------------------------------------------
# Database & Cache Utilities
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
    return json.loads(row[0]) if row else None


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


def fetch_all_cache() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT incoming_columns, mapping_json, created_at FROM schema_cache",
        conn,
    )
    conn.close()
    return df


# -----------------------------------------------------------
# LLM & DLQ Engine
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
    except Exception:
        return None


def write_to_dlq(df: pd.DataFrame, reason: str) -> str:
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(DLQ_DIR, f"failed_batch_{timestamp}.json")
    payload = {
        "timestamp": timestamp,
        "failure_reason": reason,
        "raw_data": df.to_dict(orient="records"),
    }
    with open(filepath, "w") as f:
        json.dump(payload, f, indent=2)
    return filepath


# -----------------------------------------------------------
# Streamlit UI
# -----------------------------------------------------------
st.set_page_config(
    page_title="Self-Healing Data Pipeline", page_icon="⚙️", layout="wide"
)

init_db()

st.title("⚙️ Self-Healing Data Pipeline (Local LLM + Cache)")
st.markdown(
    "Automated schema drift detection and semantic healing powered by **Ollama (`phi3`)** with an **LRU SQLite Cache** and **Dead-Letter Queue**."
)

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("1. Ingestion Source")
    target_schema_str = ", ".join([f"`{c}`" for c in EXPECTED_SCHEMA])
    st.info(f"**Target Contract Schema:** {target_schema_str}")

    sample_choice = st.selectbox(
        "Select a test scenario or upload your own CSV:",
        [
            "Scenario A: Drifted Column Names (Missing in Cache)",
            "Scenario B: Irrecoverable Schema (Triggers DLQ)",
            "Upload Custom CSV",
        ],
    )

    df_incoming = None
    if sample_choice == "Scenario A: Drifted Column Names (Missing in Cache)":
        df_incoming = pd.DataFrame(
            {
                "txn_id": ["TX-901", "TX-902", "TX-903"],
                "email_address": [
                    "user1@domain.com",
                    "user2@domain.com",
                    "user3@domain.com",
                ],
                "total_cost": [450.00, 120.75, 89.00],
                "date": ["2026-08-24", "2026-08-24", "2026-08-24"],
            }
        )
    elif sample_choice == "Scenario B: Irrecoverable Schema (Triggers DLQ)":
        df_incoming = pd.DataFrame(
            {
                "employee_name": ["John Doe", "Jane Smith"],
                "department": ["Finance", "Sales"],
                "badge_number": [1021, 1022],
            }
        )
    else:
        uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])
        if uploaded_file:
            df_incoming = pd.read_csv(uploaded_file)

    if df_incoming is not None:
        st.write("**Untrusted Raw Data Preview:**")
        st.dataframe(df_incoming, use_container_width=True)
        run_button = st.button("🚀 Process Batch Through Pipeline", type="primary")

with col2:
    st.subheader("2. Pipeline Execution & Observability")

    if df_incoming is not None and run_button:
        actual_cols = list(df_incoming.columns)
        start_time = time.time()

        with st.status("Executing pipeline stages...", expanded=True) as status:
            st.write("🔍 **Step 1: Validating against schema contract...**")
            time.sleep(0.3)

            if set(actual_cols) == set(EXPECTED_SCHEMA):
                elapsed = round((time.time() - start_time) * 1000, 2)
                st.success(f"Exact schema match. Zero drift. ({elapsed}ms)")
                status.update(label="Pipeline Succeeded", state="complete")
                st.dataframe(df_incoming[EXPECTED_SCHEMA], use_container_width=True)
            else:
                st.warning(
                    f"Schema drift detected! Found columns: `{actual_cols}`"
                )
                st.write("🗄️ **Step 2: Checking SQLite schema cache...**")

                cached_mapping = get_cached_mapping(actual_cols)
                mapping = None
                source = ""

                if cached_mapping:
                    mapping = cached_mapping
                    source = "Cache Hit (<1ms)"
                    st.info(f"⚡ Cache Hit: Retrieved mapping `{mapping}`")
                else:
                    st.write(
                        "🧠 **Step 3: Cache Miss. Querying Local LLM (`phi3` via Ollama)...**"
                    )
                    mapping = heal_schema_with_llm(EXPECTED_SCHEMA, actual_cols)
                    if mapping:
                        save_cached_mapping(actual_cols, mapping)
                        source = "Ollama Local LLM"
                        st.success(f"✨ LLM Healed & Cached Mapping: `{mapping}`")

                if not mapping:
                    dlq_path = write_to_dlq(df_incoming, "LLM failed mapping generation.")
                    st.error(f"❌ Pipeline failed. Isolated to DLQ: `{dlq_path}`")
                    status.update(label="Failed - Routed to DLQ", state="error")
                else:
                    st.write("🔄 **Step 4: Applying dynamic column alias mapping...**")
                    df_healed = df_incoming.rename(columns=mapping)
                    missing_cols = [c for c in EXPECTED_SCHEMA if c not in df_healed.columns]

                    elapsed = round((time.time() - start_time) * 1000, 2)

                    if missing_cols:
                        dlq_path = write_to_dlq(
                            df_healed, f"Missing required columns: {missing_cols}"
                        )
                        st.error(
                            f"❌ Unrecoverable schema. Missing: `{missing_cols}`"
                        )
                        st.warning(f"📦 Batch saved to DLQ: `{dlq_path}`")
                        status.update(label="Failed - Routed to DLQ", state="error")
                    else:
                        st.success(
                            f"✅ Batch successfully healed via {source} in {elapsed}ms!"
                        )
                        status.update(label="Pipeline Succeeded", state="complete")
                        st.write("**Clean Downstream Target Data:**")
                        st.dataframe(df_healed[EXPECTED_SCHEMA], use_container_width=True)

st.divider()

# -----------------------------------------------------------
# Metadata Observability Section
# -----------------------------------------------------------
st.subheader("📊 Ingestion Registry & Cache Status")
tab1, tab2 = st.tabs(["Active Schema Cache", "Dead-Letter Queue Explorer"])

with tab1:
    cache_data = fetch_all_cache()
    if not cache_data.empty:
        st.dataframe(cache_data, use_container_width=True)
    else:
        st.write("No schemas cached yet.")

with tab2:
    if os.path.exists(DLQ_DIR):
        dlq_files = os.listdir(DLQ_DIR)
        if dlq_files:
            selected_file = st.selectbox("Select failed batch log:", dlq_files)
            with open(os.path.join(DLQ_DIR, selected_file), "r") as f:
                st.json(json.load(f))
        else:
            st.write("Dead-letter queue is currently empty.")
    else:
        st.write("DLQ directory not initialized.")