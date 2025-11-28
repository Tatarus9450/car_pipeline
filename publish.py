import json
import os
import sys
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from ingest import get_engine

# Google Sheets จำกัด 10M เซลล์; กันสำรองไว้หน่อย
DEFAULT_MAX_SHEET_CELLS = 9_000_000
# vehicle_sales_enriched ใหญ่ ให้เพดานสูงขึ้นแต่ยังต่ำกว่า 10M
SHEET_CELL_LIMITS = {
    "vehicle_sales_enriched": 8_000_000,
}


def get_gspread_client():
    """ขอสิทธิ gspread จากไฟล์หรือ JSON ในตัวแปรสภาพแวดล้อม"""
    load_dotenv()
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")  # ตัวเลือก: ใส่ JSON ตรงๆ

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if creds_path:
        try:
            return gspread.service_account(filename=creds_path, scopes=scope)
        except Exception as exc:  # pragma: no cover - runtime guard
            print(f"[publish] Failed to load credentials from file: {exc}")
            sys.exit(1)

    if creds_json:
        try:
            creds_dict = json.loads(creds_json)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                creds_dict, scopes=scope
            )
            return gspread.authorize(credentials)
        except Exception as exc:  # pragma: no cover - runtime guard
            print(f"[publish] Failed to parse credentials JSON: {exc}")
            sys.exit(1)

    print(
        "[publish] Missing credentials. Set GOOGLE_APPLICATION_CREDENTIALS to a JSON file path "
        "or GOOGLE_CREDENTIALS_JSON to the JSON content."
    )
    sys.exit(1)


def _get_or_create_worksheet(spreadsheet, title: str, rows: int = 100, cols: int = 20):
    """คืน worksheet ตามชื่อ ถ้าไม่มีจะสร้างใหม่"""
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))


def _prepare_for_sheet(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """เลือก/จัดรูปคอลัมน์ก่อนอัปขึ้น Sheets"""
    if sheet_name == "vehicle_sales_enriched":
        desired_cols = [
            "vin",
            "year",
            "make",
            "model",
            "trim",
            "body",
            "transmission",
            "state",
            "condition",
            "odometer",
            "sale_datetime",
            "sale_year",
            "sale_month",
            "sale_quarter",
            "sellingprice",
            "mmr",
            "price_diff",
            "price_diff_pct",
            "is_above_mmr",
            "vehicle_age",
            "odometer_per_year",
        ]
        available_cols = [c for c in desired_cols if c in df.columns]
        df = df[available_cols].copy()
        if "sale_datetime" in df.columns:
            df["sale_datetime"] = pd.to_datetime(df["sale_datetime"], errors="coerce").dt.strftime(
                "%Y-%m-%d"
            )
    return df


def _enforce_cell_limit(df: pd.DataFrame, title: str) -> pd.DataFrame:
    """จำกัดจำนวนแถวให้อยู่ใต้ลิมิตเซลล์ของ Sheets"""
    limit = SHEET_CELL_LIMITS.get(title, DEFAULT_MAX_SHEET_CELLS)
    cells = len(df) * len(df.columns)
    if cells <= limit:
        return df

    max_rows = max(limit // max(len(df.columns), 1), 1)
    clipped = df.head(max_rows)
    print(
        f"[publish] {title}: {cells:,} cells exceeds limit; sending first {len(clipped):,} rows to stay under {limit:,} cells."
    )
    return clipped


def publish_tables_to_sheets():
    """อ่านตาราง production แล้วเขียนไป Google Sheets"""
    load_dotenv()
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        print("[publish] Please set GOOGLE_SHEETS_SPREADSHEET_ID in your .env")
        sys.exit(1)

    engine = get_engine()

    table_map = {
        "vehicle_sales_enriched": "vehicle_sales_enriched",
        "sales_summary_by_make_month": "sales_summary_by_make_month",
        "sales_summary_by_state_month": "sales_summary_by_state_month",
    }

    dataframes = {}
    for sheet_name, table_name in table_map.items():
        try:
            df = pd.read_sql_table(table_name, con=engine, schema="production")
            df = _prepare_for_sheet(df, sheet_name)
            df = _enforce_cell_limit(df, sheet_name)
            dataframes[sheet_name] = df
            print(f"[publish] Loaded {len(df)} rows from production.{table_name}")
        except Exception as exc:  # pragma: no cover - runtime guard
            print(f"[publish] Skipping {table_name}: {exc}")

    if not dataframes:
        print("[publish] No tables were loaded; aborting publish step.")
        sys.exit(1)

    client = get_gspread_client()
    spreadsheet = client.open_by_key(spreadsheet_id)

    for sheet_name, df in dataframes.items():
        worksheet = _get_or_create_worksheet(
            spreadsheet,
            title=sheet_name,
            rows=max(len(df) + 10, 100),
            cols=max(len(df.columns) + 2, 20),
        )
        worksheet.clear()
        set_with_dataframe(worksheet, df, include_index=False, resize=True)
        print(f"[publish] Wrote {len(df)} rows to sheet '{sheet_name}'")


def main():
    publish_tables_to_sheets()


if __name__ == "__main__":
    main()
