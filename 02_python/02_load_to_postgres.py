import os
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "generated"
ENV_PATH = PROJECT_ROOT / "02_python" / ".env"

load_dotenv(ENV_PATH)


def get_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )

def copy_csv(conn, csv_path: Path, table: str, columns: list[str]):
    with conn.cursor() as cur:
        with csv_path.open("r", encoding="utf-8") as f:
            copy_sql = f"""
                COPY {table} ({", ".join(columns)})
                FROM STDIN WITH (FORMAT csv, HEADER true)
            """
            cur.copy_expert(copy_sql, f)
    conn.commit()

def truncate_bronze(conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE bronze.raw_transactions;")
        cur.execute("TRUNCATE TABLE bronze.raw_merchants;")
        cur.execute("TRUNCATE TABLE bronze.raw_customers;")
    conn.commit()

def main():
    merchants_csv = DATA_DIR / "merchants.csv"
    customers_csv = DATA_DIR / "customers.csv"
    tx_csv = DATA_DIR / "transactions.csv"

    for p in [merchants_csv, customers_csv, tx_csv]:
        if not p.exists():
            raise FileNotFoundError(f"Missing file: {p}. Run 01_generate_data.py first.")

    conn = get_conn()
    try:
        print("Truncating bronze tables...")
        truncate_bronze(conn)

        print("Loading merchants...")
        copy_csv(
            conn,
            merchants_csv,
            "bronze.raw_merchants",
            ["merchant_id_raw", "merchant_name", "region_raw"]
        )

        print("Loading customers...")
        copy_csv(
            conn,
            customers_csv,
            "bronze.raw_customers",
            ["customer_id_raw", "signup_date_raw"]
        )

        print("Loading transactions...")
        copy_csv(
            conn,
            tx_csv,
            "bronze.raw_transactions",
            [
                "transaction_ref",
                "merchant_id_raw",
                "customer_id_raw",
                "amount_raw",
                "status_raw",
                "payment_method_raw",
                "channel_raw",
                "created_at_raw",
                "source_system",
            ],
        )

        print("Load complete ✅")

    finally:
        conn.close()

if __name__ == "__main__":
    main()