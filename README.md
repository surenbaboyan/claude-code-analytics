## 📁 Project Documentation
The following documents are available in the `docs/` directory:
- **Presentation (PDF):** [Technical_Assignment_Claude_Analytic.pdf](./docs/Technical_Assignment_Claude_Analytic.pdf) — A summary of key metrics, forecasts, and anomalies.
- **AI Usage Log:** [ai_usage_log.md](./docs/ai_usage_log.md) — Documentation of how Google Gemini was used during development.

# Claude Code Analytics Platform
An end-to-end telemetry processing and visualization platform designed to monitor Claude Code usage, token consumption, and cost efficiency across engineering departments.

## Architecture Overview
* **Ingestion:** Python-based ETL pipeline that flattens nested JSONL telemetry logs.

* **Storage:** DuckDB (OLAP) for high-performance analytical SQL queries on 90k+ events.

* **Analytics:** Time-series forecasting (Linear Regression) and Z-score anomaly detection.

* **Visualization:** Interactive Streamlit dashboard with Plotly integration.

## Getting Started (Ubuntu)
1. #### Setup Environment

       python3 -m venv venv

       source venv/bin/activate

       pip install -r requirements.txt

 2. #### Process Data
      ##### Ingest raw telemetry and employee metadata into DuckDB
        python3 src/ingestion/loader.py
 
 3. #### Launch Dashboard
        export PYTHONPATH=$PYTHONPATH:.
        streamlit run src/dashboard/app.py

## Tech Stack

* **Database:** DuckDB

* **UI/UX:** Streamlit, Plotly

* **ML:** Scikit-learn, Numpy

* **Data:** Pandas