import duckdb
from datetime import datetime
from config.settings import RAW_DIR, BRONZE_DIR


def run():

    con = duckdb.connect()

    # Read raw CSV
    con.execute(f"""
        CREATE TABLE orders AS
        SELECT *
        FROM read_csv('{RAW_DIR / "orders.csv"}',
                      header=true,
                      auto_detect=true)
    """)

    # Add ingestion metadata
    con.execute("""
        ALTER TABLE orders ADD COLUMN ingestion_ts TIMESTAMP;
        ALTER TABLE orders ADD COLUMN source_file VARCHAR;
        ALTER TABLE orders ADD COLUMN load_date DATE;
    """)

    con.execute(f"""
        UPDATE orders
        SET ingestion_ts = '{datetime.now()}',
            source_file = 'orders.csv',
            load_date = ingestion_ts::DATE
    """)

    # Write to parquet
    con.execute(f"""
        COPY orders
        TO '{BRONZE_DIR / "orders.parquet"}'
        (FORMAT PARQUET)
    """)

    con.close()

    print("Bronze orders ingestion complete")


if __name__ == "__main__":
    run()