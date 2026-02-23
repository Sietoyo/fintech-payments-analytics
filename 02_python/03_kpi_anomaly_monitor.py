import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / "02_python" / ".env"
EXPORT_DIR = PROJECT_ROOT / "00_docs" / "exports"

load_dotenv(ENV_PATH)

def get_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )

def fetch_daily_kpis(conn) -> pd.DataFrame:
    sql = """
    select
      txn_date,
      txn_cnt,
      total_value,
      success_cnt,
      failure_cnt,
      success_rate_pct,
      failure_rate_pct,
      success_value
    from gold.fact_payments_daily
    order by txn_date;
    """
    return pd.read_sql(sql, conn)

def add_anomaly_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["txn_date"] = pd.to_datetime(df["txn_date"])
    df = df.sort_values("txn_date")

    # Rolling baselines (7-day)
    window = 7
    for col in ["success_rate_pct", "txn_cnt", "total_value"]:
        df[f"{col}_roll_mean"] = df[col].rolling(window, min_periods=3).mean()
        df[f"{col}_roll_std"]  = df[col].rolling(window, min_periods=3).std(ddof=0)

        # z-score (handle std=0)
        std = df[f"{col}_roll_std"].replace(0, pd.NA)
        df[f"{col}_z"] = (df[col] - df[f"{col}_roll_mean"]) / std

    # Simple business rules (tuneable thresholds)
    # - Success rate drop: z <= -2 OR absolute drop >= 5 percentage points vs rolling mean
    df["success_rate_drop_pp"] = df["success_rate_pct"] - df["success_rate_pct_roll_mean"]
    df["alert_success_rate_drop"] = (
        (df["success_rate_pct_z"] <= -2) |
        (df["success_rate_drop_pp"] <= -5)
    )

    # Volume anomaly: z >= 2 or z <= -2
    df["alert_volume_anomaly"] = (df["txn_cnt_z"].abs() >= 2)

    # Value anomaly: z >= 2 or z <= -2
    df["alert_value_anomaly"] = (df["total_value_z"].abs() >= 2)

    # Combine alerts
    df["alert_any"] = df[["alert_success_rate_drop", "alert_volume_anomaly", "alert_value_anomaly"]].any(axis=1)

    # Human-readable severity (simple)
    def severity(row):
        score = 0
        if row["alert_success_rate_drop"]:
            score += 2
        if row["alert_volume_anomaly"]:
            score += 1
        if row["alert_value_anomaly"]:
            score += 1
        return "HIGH" if score >= 3 else ("MEDIUM" if score == 2 else ("LOW" if score == 1 else "NONE"))

    df["severity"] = df.apply(severity, axis=1)

    return df

def main():
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    conn = get_conn()
    try:
        df = fetch_daily_kpis(conn)
    finally:
        conn.close()

    if df.empty:
        print("No rows found in gold.fact_payments_daily. Load Gold first.")
        return

    flagged = add_anomaly_flags(df)

    alerts = flagged[flagged["alert_any"]].copy()
    out_csv = EXPORT_DIR / "alerts_daily.csv"
    alerts.to_csv(out_csv, index=False)

    # Console summary (last 30 days of alerts, or all if less)
    print("=== Payments KPI Monitoring ===")
    print(f"Source: gold.fact_payments_daily | Rows: {len(df)}")
    print(f"Alerts found: {len(alerts)}")
    print(f"Saved: {out_csv}")

    if len(alerts) > 0:
        show = alerts.tail(30)[
            ["txn_date", "severity", "txn_cnt", "total_value", "success_rate_pct",
             "alert_success_rate_drop", "alert_volume_anomaly", "alert_value_anomaly",
             "success_rate_drop_pp", "success_rate_pct_z", "txn_cnt_z", "total_value_z"]
        ]
        print("\n--- Recent alerts (tail) ---")
        print(show.to_string(index=False))
    else:
        print("No anomalies detected with current thresholds. (This is possible with synthetic data.)")

if __name__ == "__main__":
    main()