from ingest import ingest_csv_to_postgres
from transform import transform_data
from publish import publish_tables_to_sheets


def main():
    print("[pipeline] Starting ingest step")
    ingest_csv_to_postgres()

    print("[pipeline] Starting transform step")
    transform_data()

    print("[pipeline] Starting publish step")
    publish_tables_to_sheets()

    print("[pipeline] Pipeline completed successfully")


if __name__ == "__main__":
    main()
