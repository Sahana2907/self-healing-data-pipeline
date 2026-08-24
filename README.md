# ⚙️ Self-Healing Data Ingestion Pipeline (Local SLM + Cache + DLQ)

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![Ollama](https://img.shields.io/badge/Inference-Ollama%20(phi3)-black.svg)](https://ollama.com/)
[![Storage](https://img.shields.io/badge/Cache-SQLite-lightgrey.svg)](https://www.sqlite.org/)
[![UI](https://img.shields.io/badge/Dashboard-Streamlit-red.svg)](https://streamlit.io/)
[![License](https://img.shields.io/badge/License-Apache_2.0-green.svg)](LICENSE)

An enterprise-grade, privacy-first **Self-Healing Data Ingestion Engine** designed to solve upstream schema drift without pipeline halts. Powered by local Small Language Models (SLMs) via Ollama, an ultra-low-latency SQLite mapping cache ($<1\text{ms}$ resolution), and an isolated Dead-Letter Queue (DLQ) for unrecoverable structural anomalies.

---

## 📌 Problem Statement
In modern distributed data platforms, unexpected upstream schema shifts (e.g., column renaming like `txn_id` $\rightarrow$ `transaction_id`, or `date` $\rightarrow$ `purchase_date`) cause fatal `KeyError` crashes in downstream batch jobs. Engineering teams often spend up to 30% of engineering sprints debugging fragile ingestion transformations.

This project delivers a **fault-tolerant, self-recovering ingestion engine** that:
1. Validates incoming untrusted data contracts deterministically.
2. Resolves drifted column aliases dynamically using an edge-hosted SLM (`phi3`).
3. Caches resolved mappings locally to avoid recurring LLM invocation overhead.
4. Isolates structurally unrecoverable batches to a Dead-Letter Queue (DLQ) with zero data loss.

---

## 🏗️ System Architecture

```text
               [ Untrusted Incoming Batch (CSV / JSON) ]
                                  │
                                  ▼
               [ Exact Schema Contract Validation ]
                      │                       │
                 (Passes)                  (Fails)
                      │                       │
                      ▼                       ▼
            [ Standard Ingestion ]   [ Schema Cache Lookup ]
                                     (SQLite - O(1) Index)
                                              │
                              ┌───────────────┴───────────────┐
                          (Cache Hit)                    (Cache Miss)
                              │                               │
                              ▼                               ▼
                      [ Apply Mapping ]           [ Local SLM Inference ]
                              │                   (Ollama phi3 / JSON mode)
                              │                               │
                              │                      [ Cache New Mapping ]
                              │                               │
                              └───────────────┬───────────────┘
                                              ▼
                                 [ Post-Healing Validation ]
                                              │
                              ┌───────────────┴───────────────┐
                           (Valid)                        (Invalid)
                              │                               │
                              ▼                               ▼
                   [ Target Data Store ]          [ Dead-Letter Queue (DLQ) ]
                   (Downstream Warehouse)        (Isolated JSON Log + Alert)
                

## ⚡ Key Architectural Features

* **Zero-Cost & Air-Gapped Inference:** Uses local **Ollama (`phi3`)** via REST endpoints with zero third-party API token costs, minimal memory footprint, and strict data privacy compliance (raw customer/transaction row data is never transmitted to the model).
* **Deterministic Metadata Healing:** Constrains model output to strict JSON key-value schema mappings using Ollama's `format="json"`, eliminating conversational noise and hallucinations.
* **Tiered Low-Latency Resolution:**
  * **Tier 1 (Exact Match):** Direct ingestion ($0\text{ms}$ overhead).
  * **Tier 2 (Cache Hit):** Instant SQLite query ($<1\text{ms}$ resolution).
  * **Tier 3 (Cache Miss / Drift):** Local SLM semantic inference with automatic result caching.
* **Dead-Letter Queue (DLQ) Isolation:** Unrecoverable or corrupted payloads are safely logged to `dead_letter_queue/` with full diagnostic metadata, preventing pipeline crashes with zero data loss.

## 📊 End-to-End Latency & Performance

| Ingestion Path | Resolution Mechanism | Average Latency | Compute Cost |
| :--- | :--- | :--- | :--- |
| **Clean Batch** | Direct Contract Validation | `< 1 ms` | $0.00 |
| **Known Drift** | SQLite In-Memory / Disk Cache | `< 2 ms` | $0.00 |
| **New Drift** | Ollama (`phi3` 3.8B Quantized) | `~450 ms` | $0.00 (Local CPU/GPU) |
| **Irrecoverable** | DLQ Isolation + Logging | `< 5 ms` | $0.00 |

## 🛠️ Tech Stack & Dependencies

* **Language:** Python 3.9+
* **Inference Engine:** [Ollama](https://ollama.com/) running `phi3:mini` (Microsoft Phi-3)
* **Data Processing:** Pandas
* **Metadata & Cache:** SQLite3
* **Observability UI:** Streamlit

## 🚀 Getting Started

### 1. Prerequisites
Ensure Ollama is installed and the background daemon is running:
```bash
ollama pull phi3

## Setup Environment
# Create and activate virtual environment
python -m venv .venv

# On Windows (PowerShell):
.\.venv\Scripts\activate

# On macOS/Linux:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

##Run Pipeline via CLI
Execute the automated test script demonstrating cache hits, cache misses, and DLQ routing:
Bash
python self_healing_pipeline.py

##Launch Interactive Observability Dashboard
Bash
streamlit run app.py

## 📂 Repository Structure
Plaintext
.
├── app.py                      # Streamlit interactive dashboard & real-time trace UI
├── self_healing_pipeline.py    # Core pipeline logic, validation, & CLI demo
├── pipeline_metadata.db        # SQLite schema mapping registry (auto-generated)
├── dead_letter_queue/          # Isolated corrupted batch logs (auto-generated)
├── requirements.txt            # Project dependencies
├── .gitignore                  # Git ignore rules
└── README.md                   # System design & documentation

## 🔒 Security & Data Governance

* **No PII Transmission:** The pipeline sends only column metadata arrays (e.g., `['txn_id', 'total_cost']`) to the LLM context window. User records, personally identifiable information (PII), and financial transaction values remain strictly inside local runtime memory and are never exposed to the model.

![Pipeline Demo](assets/demo.gif)