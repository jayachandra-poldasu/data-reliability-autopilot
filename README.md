# 🚀 Data Reliability Autopilot
**An AI-driven self-healing pipeline for modern data infrastructure.**

### 🔍 The Problem
Data pipelines often crash due to "dirty" source data (e.g., strings in numeric columns, malformed dates). Traditional ETL tools like Informatica require manual intervention to fix these issues, leading to high **MTTR (Mean Time to Repair)** and broken downstream dashboards.

### 💡 The Solution
This Autopilot acts as a "First Responder" for data failures:
1. **Detects:** Catches SQL conversion errors in real-time.
2. **Analyzes:** Uses a local LLM (Llama 3 via Ollama) to interpret the error log.
3. **Validates:** Automatically generates a SQL fix and tests it in an **isolated DuckDB Sandbox**.
4. **Recovers:** Proposes a "Type-Safe" query to the engineer for one-click deployment.

### 🛠️ Tech Stack
- **Database:** DuckDB (OLAP engine for high-speed sandbox testing)
- **AI:** Llama 3 / Ollama (for SQL remediation logic)
- **UI:** Streamlit (Engineering Dashboard)
- **Language:** Python

### 📈 Reliability Impact
- **MTTR Reduction:** Moves from manual debugging to 5-second automated suggestions.
- **Data Quality:** Implements `TRY_CAST` patterns to ensure "Schema Purity" in production warehouses.