import duckdb
from datetime import datetime
from config.settings import RAW_DIR, BRONZE_DIR


def run():

    con = duckdb.connect()

    # Read JSONL
    query = f"""
    CREATE TABLE delivery_events AS
    SELECT *
    FROM read_json_auto('{RAW_DIR / "delivery_events.json"}')
    """

    con.execute(query)

    # Add ingestion metadata
    con.execute("""
    ALTER TABLE delivery_events ADD COLUMN ingestion_ts TIMESTAMP;
    ALTER TABLE delivery_events ADD COLUMN source_file VARCHAR;
    ALTER TABLE delivery_events ADD COLUMN load_date DATE;
    """)

    con.execute(f"""
    UPDATE delivery_events
    SET ingestion_ts = '{datetime.now()}',
        source_file = 'delivery_events.json',
        load_date = ingestion_ts::DATE
    """)

    # Write parquet
    con.execute(f"""
    COPY delivery_events
    TO '{BRONZE_DIR / "delivery_events.parquet"}'
    (FORMAT PARQUET)
    """)

    con.close()

    print("Bronze delivery_events ingestion complete")


if __name__ == "__main__":
    run()