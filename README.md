# Fintech Payments Analytics (Bronze/Silver/Gold + Power BI)

Portfolio-grade payments analytics project simulating a fintech pipeline:
- **Bronze**: raw CSV ingestion into PostgreSQL
- **Silver**: cleaned/typed tables + KPI-ready flags
- **Gold**: analytics marts (daily KPIs, channel daily, merchant daily)
- **Power BI**: executive + channel + merchant insight dashboards
- **Python monitoring**: anomaly alerts on daily KPIs

## Tech
- Windows + VS Code
- PostgreSQL + DBeaver
- Python (pandas, psycopg2, python-dotenv)
- Power BI Desktop

## How to run
1. Generate data:
   - `python 02_python/01_generate_data.py`
2. Load Bronze:
   - `python 02_python/02_load_to_postgres.py`
3. Build Silver/Gold in DBeaver:
   - Run scripts in `01_sql/`
4. Run anomaly monitoring:
   - `python 02_python/03_kpi_anomaly_monitor.py`
   - Output: `00_docs/exports/alerts_daily.csv`

## Dashboards
See `00_docs/screenshots/` for final visuals.