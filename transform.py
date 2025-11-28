import sys
import numpy as np
import pandas as pd
from sqlalchemy import text
from ingest import get_engine


def ensure_schema(engine, schema_name: str):
    """สร้าง schema ถ้ายังไม่มี"""
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))


def _normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    """ตัดช่องว่างและจัดรูปสตริงให้มาตรฐานเพื่อใช้วิเคราะห์"""
    df = df.copy()
    string_cols = [
        "vin",
        "make",
        "model",
        "trim",
        "body",
        "transmission",
        "state",
        "color",
        "interior",
        "seller",
    ]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    if "state" in df.columns:
        df["state"] = df["state"].str.upper()
    title_cols = ["make", "model", "trim", "body", "transmission", "color", "interior", "seller"]
    for col in title_cols:
        if col in df.columns:
            df[col] = df[col].str.title()

    if "vin" in df.columns:
        df["vin"] = df["vin"].str.upper()
    return df


def _clean_and_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """ทำความสะอาดเข้ม + สร้างฟีเจอร์ให้พร้อมใช้ทำแดชบอร์ด"""
    df = _normalize_strings(df)
    original_rows = len(df)

    numeric_cols = ["year", "mmr", "sellingprice", "odometer", "condition"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["sale_datetime"] = pd.to_datetime(df.get("saledate"), errors="coerce", utc=True)

    current_year = pd.Timestamp.utcnow().year + 1  # อนุญาตปีโมเดลถัดไป
    df["condition"] = df["condition"].where(df["condition"].between(0, 5), pd.NA)

    # กรองความสมบูรณ์: ตัดแถวที่ขาด/ไม่สมเหตุผลในคอลัมน์หลัก
    df = df[
        (df["sale_datetime"].notna())
        & (df["sellingprice"].notna())
        & (df["mmr"].notna())
        & (df["odometer"].notna())
        & (df["year"].notna())
        & (df["vin"].notna())
        & (df["state"].notna())
    ]
    df = df[
        (df["sellingprice"] > 0)
        & (df["mmr"] > 0)
        & (df["year"].between(1950, current_year))
        & (df["odometer"] >= 0)
        & (df["odometer"] <= 1_500_000)
    ]

    # สร้างฟีเจอร์เพิ่ม
    df["sale_year"] = df["sale_datetime"].dt.year
    df["sale_month"] = df["sale_datetime"].dt.month
    df["sale_quarter"] = df["sale_datetime"].dt.quarter
    df["vehicle_age"] = df["sale_year"] - df["year"]
    df = df[df["vehicle_age"] > 0]

    df["price_diff"] = df["sellingprice"] - df["mmr"]
    df["price_diff_pct"] = df["price_diff"] / df["mmr"]
    df["is_above_mmr"] = df["sellingprice"] > df["mmr"]
    df["odometer_per_year"] = df["odometer"] / df["vehicle_age"]

    required_cols = [
        "vin",
        "year",
        "make",
        "model",
        "state",
        "sale_datetime",
        "sale_year",
        "sale_month",
        "sale_quarter",
        "sellingprice",
        "mmr",
        "odometer",
        "condition",
        "price_diff",
        "price_diff_pct",
        "is_above_mmr",
        "vehicle_age",
        "odometer_per_year",
    ]
    df = df.dropna(subset=[col for col in required_cols if col in df.columns])

    # ตัดซ้ำตาม (vin, sale_datetime) เพื่อไม่ให้นับรถคันเดียวกันซ้ำ
    df = df.sort_values("sale_datetime").drop_duplicates(subset=["vin", "sale_datetime"], keep="first")

    cleaned_rows = len(df)
    print(
        f"[transform] Cleaned dataset: {cleaned_rows} rows remaining "
        f"(dropped {original_rows - cleaned_rows} rows)"
    )
    return df


def _write_dataframe(df: pd.DataFrame, engine, table_name: str, schema: str):
    df.to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists="replace",
        index=False,
        chunksize=10_000,
    )


def build_summary_tables(enriched_df: pd.DataFrame, engine):
    """Create a couple of simple aggregate tables for quick analysis."""
    make_month = (
        enriched_df.groupby(["make", "sale_year", "sale_month"], dropna=False)
        .agg(
            avg_sellingprice=("sellingprice", "mean"),
            avg_mmr=("mmr", "mean"),
            avg_price_diff=("price_diff", "mean"),
            sale_count=("vin", "count"),
        )
        .reset_index()
    )
    _write_dataframe(make_month, engine, "sales_summary_by_make_month", "production")
    print("[transform] Wrote production.sales_summary_by_make_month")

    state_month = (
        enriched_df.groupby(["state", "sale_year", "sale_month"], dropna=False)
        .agg(
            avg_sellingprice=("sellingprice", "mean"),
            avg_mmr=("mmr", "mean"),
            avg_price_diff=("price_diff", "mean"),
            sale_count=("vin", "count"),
        )
        .reset_index()
    )
    _write_dataframe(state_month, engine, "sales_summary_by_state_month", "production")
    print("[transform] Wrote production.sales_summary_by_state_month")


def transform_data():
    """Read raw data, clean/enrich it, and write production tables."""
    engine = get_engine()

    try:
        raw_df = pd.read_sql_table("vehicle_sales", con=engine, schema="raw_data")
    except Exception as exc:  # pragma: no cover - runtime guard
        print(f"[transform] Unable to read raw_data.vehicle_sales: {exc}")
        sys.exit(1)

    print(f"[transform] Loaded {len(raw_df)} rows from raw_data.vehicle_sales")

    ensure_schema(engine, "production")
    enriched_df = _clean_and_enrich(raw_df)
    _write_dataframe(enriched_df, engine, "vehicle_sales_enriched", "production")
    print("[transform] Wrote production.vehicle_sales_enriched")

    build_summary_tables(enriched_df, engine)


def main():
    transform_data()


if __name__ == "__main__":
    main()
