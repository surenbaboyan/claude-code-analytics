import duckdb
import pandas as pd

DB_PATH = "data/processed/claude_analytics.db"

def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)

def analyze_token_consumption_by_role():
    """Requirement: Token consumption trends by user role."""
    conn = get_connection()
    query = """
        SELECT 
            e.practice,
            e.level,
            SUM(f.input_tokens + f.output_tokens) as total_tokens,
            AVG(f.input_tokens + f.output_tokens) as avg_tokens_per_event,
            SUM(f.cost_usd) as total_spend
        FROM fact_events f
        JOIN dim_employees e ON f.user_email = e.email
        WHERE f.event_type = 'claude_code.api_request'
        GROUP BY 1, 2
        ORDER BY total_tokens DESC
    """
    return conn.execute(query).df()

def analyze_peak_usage_times():
    """Requirement: Peak usage times."""
    conn = get_connection()
    query = """
        SELECT 
            hour(timestamp) as hour_of_day,
            dayname(timestamp) as day_of_week,
            COUNT(*) as event_count
        FROM fact_events
        GROUP BY 1, 2
        ORDER BY event_count DESC
        LIMIT 10
    """
    return conn.execute(query).df()

def analyze_code_generation_behaviors():
    """Requirement: Common code generation behaviors[cite: 13, 21]."""
    conn = get_connection()
    query = """
        SELECT 
            tool_name,
            COUNT(*) as usage_count,
            AVG(CAST(success AS FLOAT)) as success_rate
        FROM fact_events
        WHERE event_type = 'claude_code.tool_result'
        GROUP BY 1
        ORDER BY usage_count DESC
    """
    return conn.execute(query).df()

if __name__ == "__main__":
    print("--- Token Consumption by Practice ---")
    print(analyze_token_consumption_by_role().head())
    
    print("\n--- Top Peak Usage Windows ---")
    print(analyze_peak_usage_times().head())
    
    print("\n--- Tool Usage Patterns ---")
    print(analyze_code_generation_behaviors())
