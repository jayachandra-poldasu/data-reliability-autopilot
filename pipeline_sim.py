import duckdb
import os

def run_failing_pipeline():
    if not os.path.exists('data'):
        os.makedirs('data')
        
    # Connect to the 'Production' database file
    con = duckdb.connect('data/warehouse.db')
    
    try:
        # Create table
        con.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER, name VARCHAR, amount INTEGER)")
        
        # This will crash because 'invalid_number' can't be CAST to INTEGER automatically
        sql = "INSERT INTO orders SELECT id, name, CAST(amount AS INTEGER) FROM read_csv_auto('data/raw_data.csv')"
        con.execute(sql)
        return "Success", None
    except Exception as e:
        return "Failed", str(e)

if __name__ == "__main__":
    status, error = run_failing_pipeline()
    print(f"--- Pipeline Status: {status} ---")
    if error:
        print(f"Error Log: {error}")