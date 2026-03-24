import duckdb
from datetime import datetime
from config.settings import RAW_DIR, BRONZE_DIR


def run():

    con = duckdb.connect()

    con.execute(f"""
    CREATE TABLE order_items AS
    SELECT *
    FROM read_csv('{RAW_DIR / "order_items.csv"}',
                  header=true,
                  auto_detect=true)
    """)

    con.execute("""
    ALTER TABLE order_items ADD COLUMN ingestion_ts TIMESTAMP;
    ALTER TABLE order_items ADD COLUMN source_file VARCHAR;
    ALTER TABLE order_items ADD COLUMN load_date DATE;
    """)

    con.execute(f"""
    UPDATE order_items
    SET ingestion_ts = '{datetime.now()}',
        source_file = 'order_items.csv',
        load_date = ingestion_ts::DATE
    """)

    con.execute(f"""
    COPY order_items
    TO '{BRONZE_DIR / "order_items.parquet"}'
    (FORMAT PARQUET)
    """)

    con.close()

    print("Bronze order_items ingestion complete")


if __name__ == "__main__":
    run()