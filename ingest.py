import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def get_engine():
    """สร้าง SQLAlchemy engine จากตัวแปรสภาพแวดล้อม (มีค่าเริ่มต้นให้)"""
    load_dotenv()
    db_user = os.getenv("DB_USER", "car_user")
    db_password = os.getenv("DB_PASSWORD", "car_password")
    db_name = os.getenv("DB_NAME", "car_db")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")

    url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(url)


def ensure_schema(engine, schema_name: str):
    """สร้าง schema ถ้ายังไม่มี"""
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))


def ingest_csv_to_postgres(csv_path: str = "car_prices.csv"):
    """อ่าน CSV แล้วโหลดเข้า raw_data.vehicle_sales"""
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"[ingest] CSV file not found at path: {csv_path}")
        sys.exit(1)
    except Exception as exc:  # pragma: no cover - เผื่อจับข้อผิดพลาดขณะอ่านไฟล์
        print(f"[ingest] Unexpected error reading CSV: {exc}")
        sys.exit(1)

    print(f"[ingest] Loaded CSV with {len(df)} rows and {len(df.columns)} columns")

    engine = get_engine()
    ensure_schema(engine, "raw_data")

    try:
        df.to_sql(
            "vehicle_sales",
            engine,
            schema="raw_data",
            if_exists="replace",
            index=False,
            chunksize=10_000,
        )
        print("[ingest] Data written to raw_data.vehicle_sales")
    except Exception as exc:  # pragma: no cover - เผื่อจับข้อผิดพลาดขณะเขียน DB
        print(f"[ingest] Failed to load data into PostgreSQL: {exc}")
        sys.exit(1)


def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "car_prices.csv"
    ingest_csv_to_postgres(csv_path)


if __name__ == "__main__":
    main()
