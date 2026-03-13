import json
import duckdb
import pandas as pd
from pathlib import Path

# Configuration
RAW_LOGS = "data/raw/telemetry_logs.jsonl"
EMPLOYEES_CSV = "data/raw/employees.csv"
DB_PATH = "data/processed/claude_analytics.db"

def initialize_db():
    """Create the database and load employee metadata."""
    conn = duckdb.connect(DB_PATH)
    # Ingest and structure Employee Metadata [cite: 7, 12]
    conn.execute(f"CREATE OR REPLACE TABLE dim_employees AS SELECT * FROM read_csv_auto('{EMPLOYEES_CSV}')")
    return conn

def process_telemetry():
    """Extract, clean, and structure the nested telemetry events."""
    all_events = []
    
    with open(RAW_LOGS, 'r') as f:
        for line in f:
            batch = json.loads(line)
            # Navigate nested structure: logEvents -> message
            for log_entry in batch.get('logEvents', []):
                event_data = json.loads(log_entry['message'])
                attrs = event_data.get('attributes', {})
                
                # Cleaning and mapping fields for efficient retrieval [cite: 12, 17]
                flattened_event = {
                    "event_id": log_entry.get('id'),
                    "timestamp": attrs.get('event.timestamp'),
                    "session_id": attrs.get('session.id'),
                    "user_email": attrs.get('user.email'),
                    "event_type": event_data.get('body'),
                    "model": attrs.get('model'),
                    "cost_usd": float(attrs.get('cost_usd', 0)),
                    "input_tokens": int(attrs.get('input_tokens', 0)),
                    "output_tokens": int(attrs.get('output_tokens', 0)),
                    "tool_name": attrs.get('tool_name'),
                    "success": attrs.get('success') == 'true'
                }
                all_events.append(flattened_event)

    # Convert to DataFrame for final cleaning
    df = pd.DataFrame(all_events)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Load into SQL fact table 
    conn = initialize_db()
    conn.execute("CREATE OR REPLACE TABLE fact_events AS SELECT * FROM df")
    
    # Validation check [cite: 17]
    row_count = conn.execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
    print(f"✅ Successfully ingested {row_count} events into {DB_PATH}")
    conn.close()

if __name__ == "__main__":
    # Ensure directories exist
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    process_telemetry()
