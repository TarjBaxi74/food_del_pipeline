import duckdb
from datetime import datetime
from config.settings import RAW_DIR, BRONZE_DIR


def run():

    con = duckdb.connect()

    con.execute(f"""
    CREATE TABLE restaurants AS
    SELECT *
    FROM read_csv('{RAW_DIR / "restaurants.csv"}',
                  header=true,
                  auto_detect=true)
    """)

    con.execute("""
    ALTER TABLE restaurants ADD COLUMN ingestion_ts TIMESTAMP;
    ALTER TABLE restaurants ADD COLUMN source_file VARCHAR;
    ALTER TABLE restaurants ADD COLUMN load_date DATE;
    """)

    con.execute(f"""
    UPDATE restaurants
    SET ingestion_ts = '{datetime.now()}',
        source_file = 'restaurants.csv',
        load_date = ingestion_ts::DATE
    """)

    con.execute(f"""
    COPY restaurants
    TO '{BRONZE_DIR / "restaurants.parquet"}'
    (FORMAT PARQUET)
    """)

    con.close()

    print("Bronze restaurants ingestion complete")


if __name__ == "__main__":
    run()
    