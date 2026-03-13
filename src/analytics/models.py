import os
import sys
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.linear_model import LinearRegression

# --- Path Configuration ---
# Ensures the script can find the database regardless of where it is executed
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = os.path.join(ROOT_DIR, "data", "processed", "claude_analytics.db")

def get_connection():
    """Establish a read-only connection to the DuckDB database."""
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found at {DB_PATH}. Run loader.py first.")
    return duckdb.connect(DB_PATH, read_only=True)

def forecast_token_usage(days_to_forecast=7):
    """
    Predicts future token consumption using a Linear Regression trend line.
    """
    try:
        conn = get_connection()
        
        # Aggregate total tokens by day
        query = """
            SELECT 
                CAST(timestamp AS DATE) as date,
                SUM(input_tokens + output_tokens) as daily_tokens
            FROM fact_events 
            WHERE timestamp IS NOT NULL
            GROUP BY 1 
            ORDER BY 1
        """
        df = conn.execute(query).df()
        conn.close()

        if df.empty:
            return None, "No data available for forecasting."

        # Prepare numerical index for time (X) and token counts (y)
        df['day_index'] = np.arange(len(df))
        X = df[['day_index']]
        y = df['daily_tokens']

        # Train the model
        model = LinearRegression()
        model.fit(X, y)

        # Generate future indices
        last_index = df['day_index'].max()
        future_indices = np.array([last_index + i for i in range(1, days_to_forecast + 1)]).reshape(-1, 1)
        
        # Predict
        predictions = model.predict(future_indices)
        
        # Create a DataFrame for the forecast
        last_date = pd.to_datetime(df['date'].max())
        future_dates = [last_date + pd.Timedelta(days=i) for i in range(1, days_to_forecast + 1)]
        
        forecast_df = pd.DataFrame({
            'date': future_dates,
            'predicted_tokens': predictions
        })
        
        return forecast_df, None

    except Exception as e:
        return None, str(e)

def detect_cost_anomalies(threshold_z=2.5):
    """
    Identifies API requests that are statistical outliers in terms of cost.
    Uses Z-Score: (Value - Mean) / Standard Deviation
    """
    try:
        conn = get_connection()
        query = """
            SELECT 
                timestamp, 
                user_email, 
                model, 
                cost_usd 
            FROM fact_events 
            WHERE event_type = 'claude_code.api_request'
        """
        df = conn.execute(query).df()
        conn.close()

        if df.empty:
            return None

        # Calculate Z-Scores
        mean_cost = df['cost_usd'].mean()
        std_cost = df['cost_usd'].std()
        
        # Avoid division by zero if all costs are identical
        if std_cost == 0:
            return pd.DataFrame()

        df['z_score'] = (df['cost_usd'] - mean_cost) / std_cost
        
        # Filter for anomalies
        anomalies = df[df['z_score'].abs() > threshold_z].sort_values(by='cost_usd', ascending=False)
        
        return anomalies

    except Exception as e:
        print(f"Error in anomaly detection: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    # Quick CLI test
    print("Running ML Model Test...")
    forecast, err = forecast_token_usage()
    if err:
        print(f"Forecast Error: {err}")
    else:
        print("\n--- 7-Day Token Forecast ---")
        print(forecast)

    anomalies = detect_cost_anomalies()
    print(f"\n--- Detected {len(anomalies)} Cost Anomalies ---")
    if not anomalies.empty:
        print(anomalies.head())
