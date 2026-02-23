import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from faker import Faker

fake = Faker("en_NG")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT_ROOT / "data" / "generated"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SEED = 42
random.seed(SEED)
np.random.seed(SEED)
Faker.seed(SEED)

N_MERCHANTS = 500
N_CUSTOMERS = 2000
N_TRANSACTIONS = 10000  # start smaller for speed

SOURCE_SYSTEMS = ["switch_api", "pos_gateway", "mobile_app", "partner_feed"]
STATUS_VARIANTS = ["success", "SUCCESS", "failed", "FAILED", "error"]
PAYMENT_METHOD_VARIANTS = ["Card", "Transfer", "USSD"]
CHANNEL_VARIANTS = ["POS", "Web", "Mobile"]
REGIONS = ["Lagos", "Abuja", "Rivers", "Oyo"]

def main():
    print("Generating merchants...")
    merchants_path = OUT_DIR / "merchants.csv"

    with merchants_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["merchant_id_raw", "merchant_name", "region_raw"])

        for mid in range(1, N_MERCHANTS + 1):
            w.writerow([
                mid,
                fake.company(),
                random.choice(REGIONS)
            ])

    print("Generating customers...")
    customers_path = OUT_DIR / "customers.csv"

    with customers_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["customer_id_raw", "signup_date_raw"])

        for cid in range(1, N_CUSTOMERS + 1):
            signup_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 700))
            w.writerow([
                cid,
                signup_date.strftime("%Y-%m-%d")
            ])

    print("Generating transactions...")
    tx_path = OUT_DIR / "transactions.csv"

    with tx_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "transaction_ref",
            "merchant_id_raw",
            "customer_id_raw",
            "amount_raw",
            "status_raw",
            "payment_method_raw",
            "channel_raw",
            "created_at_raw",
            "source_system"
        ])

        start = datetime(2026, 1, 1)

        for i in range(1, N_TRANSACTIONS + 1):
            dt = start + timedelta(minutes=random.randint(0, 500000))

            w.writerow([
                f"TX-{i:08d}",
                random.randint(1, N_MERCHANTS),
                random.randint(1, N_CUSTOMERS),
                round(np.random.lognormal(8, 0.5), 2),
                random.choice(STATUS_VARIANTS),
                random.choice(PAYMENT_METHOD_VARIANTS),
                random.choice(CHANNEL_VARIANTS),
                dt.strftime("%Y-%m-%d %H:%M:%S"),
                random.choice(SOURCE_SYSTEMS)
            ])

            if i % 2000 == 0:
                print(f"  generated {i:,} transactions...")

    print("\nDone.")
    print(f"Files written to: {OUT_DIR}")

if __name__ == "__main__":
    main()