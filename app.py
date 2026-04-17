import streamlit as st
import pandas as pd
import duckdb
import os
from engine import ask_ai_for_fix, run_in_sandbox

# 1. Page Configuration
st.set_page_config(
    page_title="Data Reliability Autopilot",
    page_icon="🚀",
    layout="wide"
)

# 2. Styling and Header
st.title("🚀 Data Reliability Autopilot")
st.subheader("Autonomous Pipeline Remediation & Sandbox Validation")
st.markdown("---")

# 3. Simulated Incident Context
# In a real environment, these would be pulled from your logs (e.g., CloudWatch or Datadog)
error_log = "Conversion Error: Could not convert string 'invalid_number' to INT32"
failing_sql = "INSERT INTO orders SELECT id, name, CAST(amount AS INTEGER) FROM read_csv_auto('data/raw_data.csv')"

# 4. Error Display
st.error(f"**🔴 Pipeline Failure Detected**\n\n**Error:** `{error_log}`\n\n**Failing Job:** `daily_order_ingestion_job`")

# 5. The "Autopilot" Logic
if st.button("Generate AI Remediation"):
    with st.spinner("🤖 AI is analyzing error logs and generating safe SQL..."):
        
        # Call our AI Engine
        suggested_fix = ask_ai_for_fix(error_log, failing_sql)
        
        st.markdown("### 🛠️ Suggested SQL Remediation")
        st.code(suggested_fix, language='sql')
        
        # 6. Sandbox Validation
        with st.status("🔍 Running Sandbox Safety Check...", expanded=True) as status:
            st.write("Isolating production state...")
            success, message = run_in_sandbox(suggested_fix)
            
            if success:
                status.update(label="✅ Sandbox Validation Passed!", state="complete", expanded=False)
                st.success(f"**Status:** {message}")
                
                # 7. Data Preview (The "Portfolio" Piece)
                st.markdown("---")
                st.write("### 📊 Preview: Post-Remediation Data")
                st.info("The following table shows how the AI fix handles the 'dirty' rows in your 50-row dataset.")
                
                try:
                    # We run the fix locally to show the user the results
                    con = duckdb.connect(database=':memory:')
                    # Load the raw 50 rows
                    con.execute("CREATE TABLE test_table AS SELECT * FROM read_csv_auto('data/raw_data.csv', all_varchar=True)")
                    # Apply the fix
                    preview_df = con.execute(suggested_fix).fetchdf()
                    
                    # Highlight the first few rows (where the bad data usually is)
                    st.dataframe(preview_df.head(15), use_container_width=True)
                    
                    st.balloons()
                    
                    # 8. Action Buttons
                    col1, col2 = st.columns([1, 4])
                    with col1:
                        if st.button("Apply to Production"):
                            st.warning("Executing change in production...")
                    with col2:
                        st.write("*(This would trigger a GitHub Action or Jenkins Job in a real SRE workflow)*")
                
                except Exception as e:
                    st.warning(f"Could not generate preview: {e}")
            
            else:
                status.update(label="❌ Sandbox Validation Failed", state="error", expanded=True)
                st.error(f"**Reason:** {message}")
                st.info("The AI-generated fix was rejected because it failed the safety check. Please retry or adjust the prompt.")

# 9. Sidebar for "Senior SRE" Context
with st.sidebar:
    st.header("Project Insights")
    st.write("**Strategy:** Self-Healing Infrastructure")
    st.write("**Stack:**")
    st.markdown("- **Engine:** DuckDB (OLAP)")
    st.markdown("- **LLM:** Llama 3 (via Ollama)")
    st.markdown("- **UI:** Streamlit")
    st.write("---")
    st.write("This tool reduces MTTR by validating schema fixes in a memory-isolated sandbox before human approval.")