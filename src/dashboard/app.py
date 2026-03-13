import os
import sys
from pathlib import Path
import time

# --- 1. BOOTSTRAP: Fix Python Path ---
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd
import src.analytics.models as ml

# --- 2. CONFIGURATION ---
DB_PATH = os.path.join(ROOT_DIR, "data", "processed", "claude_analytics.db")

st.set_page_config(
    page_title="Claude Code Analytics Platform", 
    layout="wide", 
    page_icon="📈"
)

# --- 3. DATABASE HELPER ---
def get_data(query):
    """Executes SQL and returns a DataFrame with safety checks."""
    if not os.path.exists(DB_PATH):
        st.error(f"Database file not found at: {DB_PATH}")
        return pd.DataFrame()
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute(query).df()
        conn.close()
        return df
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return pd.DataFrame()

# --- 4. SIDEBAR FILTERS ---
st.sidebar.title("📊 Control Panel")

# Check connectivity
if os.path.exists(DB_PATH):
    st.sidebar.success("Connected to DuckDB")
else:
    st.sidebar.error("Database Disconnected")

# Load practices for filtering
practices_df = get_data("SELECT DISTINCT practice FROM dim_employees")
if not practices_df.empty:
    all_practices = practices_df['practice'].tolist()
    selected_practice = st.sidebar.multiselect(
        "Select Departments", 
        options=all_practices, 
        default=all_practices
    )
else:
    selected_practice = []

# --- 5. MAIN DASHBOARD UI ---
st.title("🤖 Claude Code Telemetry Insights")
st.markdown("---")

if not selected_practice:
    st.warning("Please select at least one department in the sidebar to view data.")
else:
    # Format the SQL filter safely
    if len(selected_practice) == 1:
        practice_filter = f" = '{selected_practice[0]}'"
    else:
        practice_filter = f" IN {tuple(selected_practice)}"

    # --- KPI METRICS ---
    st.subheader("High-Level Performance")
    kpi_query = f"""
        SELECT 
            CAST(SUM(f.input_tokens + f.output_tokens) AS BIGINT) as total_tokens,
            SUM(f.cost_usd) as total_spend,
            COUNT(DISTINCT f.session_id) as total_sessions
        FROM fact_events f
        JOIN dim_employees e ON LOWER(f.user_email) = LOWER(e.email)
        WHERE e.practice {practice_filter}
    """
    kpi_df = get_data(kpi_query)

    if not kpi_df.empty and kpi_df['total_tokens'][0] is not None:
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Tokens Consumed", f"{int(kpi_df['total_tokens'][0]):,}")
        m2.metric("Accumulated Cost (USD)", f"${kpi_df['total_spend'][0]:,.2f}")
        m3.metric("Unique AI Sessions", f"{int(kpi_df['total_sessions'][0]):,}")
    else:
        st.info("No data matches the current filters.")

    st.markdown("---")

    # --- VISUAL DIAGRAMS ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Token Distribution by Practice")
        # Explicitly cast to BIGINT for Plotly rendering
        pie_query = f"""
            SELECT 
                e.practice, 
                CAST(SUM(f.input_tokens + f.output_tokens) AS BIGINT) as tokens
            FROM fact_events f
            JOIN dim_employees e ON LOWER(f.user_email) = LOWER(e.email)
            WHERE e.practice {practice_filter}
            GROUP BY 1 
            HAVING tokens > 0
            ORDER BY 2 DESC
        """
        pie_df = get_data(pie_query)
        if not pie_df.empty:
            fig_pie = px.pie(pie_df, names='practice', values='tokens', hole=0.4, 
                             color_discrete_sequence=px.colors.qualitative.Set3)
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.write("No data available for the distribution chart.")

    with col2:
        st.subheader("Daily Usage Trend")
        trend_query = f"""
            SELECT 
                CAST(timestamp AS DATE) as date,
                CAST(SUM(input_tokens + output_tokens) AS BIGINT) as tokens
            FROM fact_events f
            JOIN dim_employees e ON LOWER(f.user_email) = LOWER(e.email)
            WHERE e.practice {practice_filter}
            GROUP BY 1 ORDER BY 1
        """
        trend_df = get_data(trend_query)
        if not trend_df.empty:
            fig_trend = px.area(trend_df, x='date', y='tokens', 
                                title="Token Volume Over Time")
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.write("No data available for the trend chart.")

    # --- PREDICTIVE ML SECTION ---
    st.markdown("---")
    st.header("🔮 Predictive Analytics")
    
    t1, t2 = st.tabs(["Usage Forecasting", "Anomaly Detection"])
    
    with t1:
        forecast_df, err = ml.forecast_token_usage()
        if err:
            st.error(f"Forecasting Error: {err}")
        else:
            fig_f = px.line(forecast_df, x='date', y='predicted_tokens', markers=True,
                            title="Linear Regression: Next 7-Day Predicted Load")
            st.plotly_chart(fig_f, use_container_width=True)

    with t2:
        anomalies = ml.detect_cost_anomalies(threshold_z=2.5)
        if not anomalies.empty:
            st.warning(f"Detected {len(anomalies)} cost anomalies (outliers).")
            st.dataframe(anomalies[['timestamp', 'user_email', 'cost_usd', 'z_score']], use_container_width=True)
        else:
            st.success("No significant cost anomalies detected.")

# --- FOOTER ---
st.sidebar.markdown("---")
if st.sidebar.button("Force Dashboard Refresh"):
    st.rerun()
