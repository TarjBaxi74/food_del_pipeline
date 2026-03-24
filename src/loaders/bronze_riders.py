import duckdb
from datetime import datetime
from config.settings import RAW_DIR, BRONZE_DIR


def run():

    con = duckdb.connect()

    con.execute(f"""
    CREATE TABLE riders AS
    SELECT *
    FROM read_csv('{RAW_DIR / "riders.csv"}',
                  header=true,
                  auto_detect=true)
    """)

    con.execute("""
    ALTER TABLE riders ADD COLUMN ingestion_ts TIMESTAMP;
    ALTER TABLE riders ADD COLUMN source_file VARCHAR;
    ALTER TABLE riders ADD COLUMN load_date DATE;
    """)

    con.execute(f"""
    UPDATE riders
    SET ingestion_ts = '{datetime.now()}',
        source_file = 'riders.csv',
        load_date = ingestion_ts::DATE
    """)

    con.execute(f"""
    COPY riders
    TO '{BRONZE_DIR / "riders.parquet"}'
    (FORMAT PARQUET)
    """)

    con.close()

    print("Bronze riders ingestion complete")


if __name__ == "__main__":
    run()