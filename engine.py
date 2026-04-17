import duckdb
import requests
import os

def run_in_sandbox(suggested_sql):
    """
    Isolates the AI's logic in a memory-only DuckDB instance.
    Tests the logic against your actual 50-row messy dataset.
    """
    try:
        # Create a fresh, in-memory database
        con = duckdb.connect(database=':memory:') 
        
        # 1. Load the actual 50-row CSV into the sandbox
        # 'all_varchar=True' is key so DuckDB doesn't try to fix the data for us
        if os.path.exists('data/raw_data.csv'):
            con.execute("CREATE TABLE test_table AS SELECT * FROM read_csv_auto('data/raw_data.csv', all_varchar=True)")
        else:
            # Fallback for local testing if file is missing
            con.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR, amount VARCHAR, timestamp VARCHAR)")
            con.execute("INSERT INTO test_table VALUES (1, 'Test', 'invalid_number', '2026-04-16')")
        
        # 2. Validate the suggestion by trying to create a temporary table from it
        # This will fail if the SQL has syntax errors or includes forbidden commands like INSERT
        con.execute(f"CREATE TABLE validation_results AS {suggested_sql}")
        
        return True, "Sandbox Test Passed!"
    except Exception as e:
        return False, f"Sandbox Failed: {str(e)}"

def ask_ai_for_fix(error_message, failing_sql):
    """
    Queries local Llama 3 and strictly enforces a 'SELECT-only' response.
    Includes a 'Sanitizer' to strip out forbidden SQL commands.
    """
    prompt = f"""
    SRE DATA AUTOPILOT
    Pipeline failed with error: {error_message}
    Failing SQL context: {failing_sql}
    
    TASK: Provide a corrected SELECT statement to transform 'test_table'.
    
    STRICT INSTRUCTIONS:
    1. Handle 'amount' using TRY_CAST(amount AS INTEGER).
    2. Output ONLY the SELECT statement. 
    3. DO NOT include "INSERT INTO", "CREATE TABLE", or "UPDATE".
    4. The source table is 'test_table'.
    5. No markdown, no backticks, no explanations.

    Example output: SELECT id, name, TRY_CAST(amount AS INTEGER) FROM test_table
    """
    
    try:
        # Request to local Ollama instance
        response = requests.post("http://localhost:11434/api/generate", 
                                 json={"model": "llama3", "prompt": prompt, "stream": False})
        
        raw_sql = response.json().get("response", "").strip()
        
        # --- THE SANITIZER ---
        # 1. Strip Markdown backticks
        clean_sql = raw_sql.replace("```sql", "").replace("```", "").replace(";", "").strip()
        
        # 2. Force it to be a SELECT (removes 'INSERT INTO...' prefixes)
        if "SELECT" in clean_sql.upper():
            # Find where SELECT starts and ignore everything before it
            select_start = clean_sql.upper().find("SELECT")
            clean_sql = clean_sql[select_start:]
        
        # 3. Final catch-all if it still has 'INSERT' in it
        clean_sql = clean_sql.replace("INSERT INTO test_table", "").strip()
        
        return clean_sql
    except Exception:
        # Hardcoded safe fallback
        return "SELECT id, name, TRY_CAST(amount AS INTEGER) AS amount FROM test_table"