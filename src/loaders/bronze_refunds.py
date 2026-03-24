import duckdb
from datetime import datetime
from config.settings import RAW_DIR, BRONZE_DIR


def run():

    con = duckdb.connect()

    con.execute(f"""
    CREATE TABLE refunds AS
    SELECT *
    FROM read_csv('{RAW_DIR / "refunds.csv"}',
                  header=true,
                  auto_detect=true)
    """)

    con.execute("""
    ALTER TABLE refunds ADD COLUMN ingestion_ts TIMESTAMP;
    ALTER TABLE refunds ADD COLUMN source_file VARCHAR;
    ALTER TABLE refunds ADD COLUMN load_date DATE;
    """)

    con.execute(f"""
    UPDATE refunds
    SET ingestion_ts = '{datetime.now()}',
        source_file = 'refunds.csv',
        load_date = ingestion_ts::DATE
    """)

    con.execute(f"""
    COPY refunds
    TO '{BRONZE_DIR / "refunds.parquet"}'
    (FORMAT PARQUET)
    """)

    con.close()

    print("Bronze refunds ingestion complete")


if __name__ == "__main__":
    run()