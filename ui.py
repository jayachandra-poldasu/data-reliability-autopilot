"""
Streamlit Dashboard — Data Reliability Autopilot UI.

Provides an interactive dashboard for pipeline health monitoring,
failure classification, remediation proposals, sandbox testing,
and approve/reject workflow controls.
"""

import json
import requests
import streamlit as st
import pandas as pd
import os

# ── Page Config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Data Reliability Autopilot",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

API_URL = os.environ.get("AUTOPILOT_API_URL", "http://localhost:8000")

# ── Custom CSS ───────────────────────────────────────────────────────────────

st.markdown("""
<style>
    .stApp {
        background: linear-gradient(135deg, #0f0f23 0%, #1a1a3e 50%, #0d1b2a 100%);
    }
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.5rem;
        font-weight: 800;
        margin-bottom: 0;
    }
    .subtitle {
        color: #8892b0;
        font-size: 1.1rem;
        margin-top: -10px;
    }
    .metric-card {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 20px;
        backdrop-filter: blur(10px);
    }
    .status-healthy { color: #64ffda; font-weight: bold; }
    .status-failed { color: #ff6b6b; font-weight: bold; }
    .status-degraded { color: #ffd93d; font-weight: bold; }
    .state-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    .badge-pending { background: #3d3d5c; color: #8892b0; }
    .badge-analyzing { background: #1a365d; color: #63b3ed; }
    .badge-awaiting { background: #553c00; color: #ffd93d; }
    .badge-approved { background: #1c4532; color: #68d391; }
    .badge-rejected { background: #4a1d1d; color: #fc8181; }
    .badge-applied { background: #1a4731; color: #64ffda; }
</style>
""", unsafe_allow_html=True)


# ── Helper Functions ─────────────────────────────────────────────────────────

def api_get(endpoint):
    """Make a GET request to the API."""
    try:
        resp = requests.get(f"{API_URL}{endpoint}", timeout=10)
        return resp.json() if resp.status_code == 200 else None
    except Exception:
        return None


def api_post(endpoint, data=None, params=None):
    """Make a POST request to the API."""
    try:
        resp = requests.post(f"{API_URL}{endpoint}", json=data, params=params, timeout=30)
        return resp.json() if resp.status_code == 200 else {"error": resp.text}
    except Exception as e:
        return {"error": str(e)}


def state_badge(state):
    """Return a styled HTML badge for a workflow state."""
    colors = {
        "pending": "badge-pending",
        "analyzing": "badge-analyzing",
        "awaiting_approval": "badge-awaiting",
        "sandbox_testing": "badge-awaiting",
        "approved": "badge-approved",
        "applied": "badge-applied",
        "rejected": "badge-rejected",
        "rolled_back": "badge-rejected",
    }
    css = colors.get(state, "badge-pending")
    return f'<span class="state-badge {css}">{state.upper().replace("_", " ")}</span>'


def severity_emoji(severity):
    """Return an emoji for a severity level."""
    return {
        "critical": "🔴",
        "high": "🟠",
        "medium": "🟡",
        "low": "🟢",
        "info": "🔵",
    }.get(severity, "⚪")


# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🛡️ Autopilot Controls")
    st.markdown("---")

    # Health check
    health = api_get("/health")
    if health:
        st.success(f"API: **{health['status'].upper()}**")
        st.caption(f"Version: {health['version']}")
        st.caption(f"AI Backend: {health['ai_backend']}")
        st.caption(f"AI Available: {'✅' if health['ai_available'] else '❌'}")
        st.caption(f"Active Failures: {health.get('active_failures', 0)}")
    else:
        st.error("⚠️ API Unreachable")
        st.info(f"Start the API server:\n```\nAUTOPILOT_AI_BACKEND=none uvicorn app.main:app --reload\n```")

    st.markdown("---")
    st.markdown("### 📖 Quick Reference")
    st.markdown("""
    **Failure Categories:**
    - 🔄 Schema Drift
    - 📊 Data Quality
    - 🔧 SQL Error
    - ⏱️ Timeout
    - 🔗 Dependency Failure
    - 💾 Resource Exhaustion
    - 🔐 Permission Error
    """)

    st.markdown("---")
    st.markdown("### 🏗️ Tech Stack")
    st.markdown("- **Engine:** DuckDB (OLAP)")
    st.markdown("- **LLM:** Ollama / OpenAI")
    st.markdown("- **Backend:** FastAPI")
    st.markdown("- **UI:** Streamlit")


# ── Main Content ─────────────────────────────────────────────────────────────

st.markdown('<p class="main-header">🛡️ Data Reliability Autopilot</p>', unsafe_allow_html=True)
st.markdown('<p class="subtitle">Autonomous Pipeline Failure Classification, Remediation & Sandbox Validation</p>', unsafe_allow_html=True)
st.markdown("---")

# ── Tab Layout ───────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs(["📊 Pipeline Dashboard", "🔍 Analyze Failure", "📋 Failure History"])


# ── Tab 1: Pipeline Dashboard ────────────────────────────────────────────────

with tab1:
    st.markdown("### 📊 Pipeline Health Overview")

    pipelines = api_get("/pipelines")

    if pipelines:
        cols = st.columns(len(pipelines))
        for i, pipeline in enumerate(pipelines):
            with cols[i]:
                status = pipeline.get("status", "unknown")
                emoji = {"healthy": "🟢", "degraded": "🟡", "failed": "🔴", "recovering": "🔵"}.get(status, "⚪")
                st.metric(
                    label=pipeline["name"].replace("_", " ").title(),
                    value=f"{emoji} {status.upper()}",
                )
                st.caption(f"Owner: {pipeline.get('owner', 'N/A')}")
                st.caption(f"Schedule: {pipeline.get('schedule', 'N/A')}")
    else:
        st.info("Start the API server to see pipeline data.")


# ── Tab 2: Analyze Failure ───────────────────────────────────────────────────

with tab2:
    st.markdown("### 🔍 Analyze Pipeline Failure")

    # Sample error scenarios for quick testing
    st.markdown("**Quick Load Sample Scenarios:**")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("📊 Data Quality", use_container_width=True):
            st.session_state["sample_pipeline"] = "daily_order_ingestion"
            st.session_state["sample_error"] = "Could not convert string 'invalid_number' to INTEGER"
            st.session_state["sample_context"] = "Row 15 in batch 42, column 'amount'"
            st.session_state["sample_sql"] = "INSERT INTO orders SELECT id, name, CAST(amount AS INTEGER) FROM source_table"
    with col2:
        if st.button("🔄 Schema Drift", use_container_width=True):
            st.session_state["sample_pipeline"] = "revenue_aggregation"
            st.session_state["sample_error"] = "Column 'revenue' not found in table 'daily_sales'"
            st.session_state["sample_context"] = "Schema mismatch after upstream table migration"
            st.session_state["sample_sql"] = "SELECT region, SUM(revenue) FROM daily_sales GROUP BY region"
    with col3:
        if st.button("🔧 SQL Error", use_container_width=True):
            st.session_state["sample_pipeline"] = "user_event_processing"
            st.session_state["sample_error"] = "Syntax error at or near 'FORM'"
            st.session_state["sample_context"] = "Typo in query deployed in latest release"
            st.session_state["sample_sql"] = "SELECT * FORM user_events WHERE event_type = 'purchase'"
    with col4:
        if st.button("⏱️ Timeout", use_container_width=True):
            st.session_state["sample_pipeline"] = "data_quality_checks"
            st.session_state["sample_error"] = "Query timed out after 300 seconds"
            st.session_state["sample_context"] = "Data volume increased 10x after marketing campaign"
            st.session_state["sample_sql"] = "SELECT * FROM user_events WHERE created_at > '2026-01-01'"

    st.markdown("---")

    # Input form
    with st.form("analyze_form"):
        pipeline_name = st.text_input(
            "Pipeline Name",
            value=st.session_state.get("sample_pipeline", ""),
            placeholder="e.g., daily_order_ingestion",
        )
        error_message = st.text_area(
            "Error Message",
            value=st.session_state.get("sample_error", ""),
            placeholder="Paste the error message from your pipeline logs...",
            height=80,
        )
        error_context = st.text_input(
            "Error Context (optional)",
            value=st.session_state.get("sample_context", ""),
            placeholder="Additional context, stack trace, etc.",
        )
        pipeline_sql = st.text_area(
            "Failing SQL (optional)",
            value=st.session_state.get("sample_sql", ""),
            placeholder="The SQL query that failed...",
            height=80,
        )
        submitted = st.form_submit_button("🔍 Analyze Failure", use_container_width=True)

    if submitted and pipeline_name and error_message:
        with st.spinner("🤖 Classifying failure and generating remediations..."):
            result = api_post("/failures/analyze", {
                "pipeline_name": pipeline_name,
                "error_message": error_message,
                "error_context": error_context,
                "pipeline_sql": pipeline_sql,
            })

        if "error" not in result:
            st.session_state["current_failure"] = result

            # Classification Results
            st.markdown("---")
            st.markdown("### 🎯 Classification Result")

            classification = result.get("classification", {})
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Category", classification.get("category", "unknown").replace("_", " ").title())
            with col2:
                conf = classification.get("confidence", 0)
                st.metric("Confidence", f"{conf:.0%}")
            with col3:
                st.markdown(state_badge(result.get("state", "pending")), unsafe_allow_html=True)

            st.info(f"**Reasoning:** {classification.get('reasoning', 'N/A')}")

            if classification.get("matched_patterns"):
                st.caption(f"Matched Patterns: {', '.join(classification['matched_patterns'])}")

            # AI Analysis
            if result.get("ai_analysis"):
                st.markdown("### 🤖 AI Analysis")
                st.markdown(result["ai_analysis"])

            # Remediation Proposals
            st.markdown("### 💊 Remediation Proposals")
            remediations = result.get("remediations", [])

            for i, rem in enumerate(remediations):
                with st.expander(
                    f"#{rem['rank']} — {rem['action'].replace('_', ' ').title()} "
                    f"{severity_emoji(rem.get('risk_level', 'low'))}",
                    expanded=(i == 0),
                ):
                    st.markdown(f"**Description:** {rem['description']}")
                    st.markdown(f"**Impact:** {rem['estimated_impact']}")
                    st.markdown(f"**Risk Level:** {severity_emoji(rem.get('risk_level', 'low'))} {rem.get('risk_level', 'low').title()}")
                    st.markdown(f"**Auto-executable:** {'✅ Yes' if rem.get('auto_executable') else '❌ No'}")

                    if rem.get("sql_fix"):
                        st.code(rem["sql_fix"], language="sql")

                        # Sandbox test button
                        if st.button(f"🧪 Sandbox Test", key=f"sandbox_{i}"):
                            with st.spinner("Running sandbox validation..."):
                                sandbox_result = api_post(
                                    f"/failures/{result['id']}/sandbox-test",
                                    params={"remediation_index": i},
                                )
                            if sandbox_result.get("success"):
                                st.success(f"✅ Sandbox passed! Rows affected: {sandbox_result.get('rows_affected', 0)}")
                                if sandbox_result.get("rows_quarantined", 0) > 0:
                                    st.warning(f"⚠️ {sandbox_result['rows_quarantined']} rows quarantined")
                                if sandbox_result.get("preview_data"):
                                    st.dataframe(pd.DataFrame(sandbox_result["preview_data"]), use_container_width=True)
                                for check in sandbox_result.get("validation_checks", []):
                                    st.caption(f"✓ {check}")
                            else:
                                st.error(f"❌ Sandbox failed: {sandbox_result.get('error_message', 'Unknown error')}")

            # Approve / Reject buttons
            st.markdown("### ⚡ Decision")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Approve & Apply", type="primary", use_container_width=True):
                    approve_result = api_post(f"/failures/{result['id']}/approve", {
                        "remediation_index": 0,
                        "approver": "dashboard_user",
                    })
                    if approve_result.get("state") == "applied":
                        st.success("🎉 Remediation approved and applied!")
                        st.balloons()
                    else:
                        st.error(f"Failed to approve: {approve_result}")
            with col2:
                if st.button("❌ Reject", use_container_width=True):
                    reject_result = api_post(f"/failures/{result['id']}/reject")
                    if reject_result.get("state") == "rejected":
                        st.warning("Remediation rejected. Manual investigation required.")
                    else:
                        st.error(f"Failed to reject: {reject_result}")
        else:
            st.error(f"Analysis failed: {result.get('error', 'Unknown error')}")


# ── Tab 3: Failure History ───────────────────────────────────────────────────

with tab3:
    st.markdown("### 📋 Recent Failures")
    st.caption("Showing failures analyzed in the current session.")

    current = st.session_state.get("current_failure")
    if current:
        with st.container():
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Failure ID", current["id"])
            with col2:
                st.metric("Pipeline", current["pipeline_name"])
            with col3:
                cat = current.get("classification", {}).get("category", "unknown")
                st.metric("Category", cat.replace("_", " ").title())
            with col4:
                st.markdown(state_badge(current.get("state", "pending")), unsafe_allow_html=True)

            st.json(current)
    else:
        st.info("No failures analyzed yet. Go to the **Analyze Failure** tab to get started.")
