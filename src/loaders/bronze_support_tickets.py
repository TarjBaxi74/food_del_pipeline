import duckdb
from datetime import datetime
from config.settings import RAW_DIR, BRONZE_DIR


def run():

    con = duckdb.connect()

    con.execute(f"""
    CREATE TABLE support_tickets AS
    SELECT *
    FROM read_csv('{RAW_DIR / "support_tickets.csv"}',
                  header=true,
                  auto_detect=true)
    """)

    con.execute("""
    ALTER TABLE support_tickets ADD COLUMN ingestion_ts TIMESTAMP;
    ALTER TABLE support_tickets ADD COLUMN source_file VARCHAR;
    ALTER TABLE support_tickets ADD COLUMN load_date DATE;
    """)

    con.execute(f"""
    UPDATE support_tickets
    SET ingestion_ts = '{datetime.now()}',
        source_file = 'support_tickets.csv',
        load_date = ingestion_ts::DATE
    """)

    con.execute(f"""
    COPY support_tickets
    TO '{BRONZE_DIR / "support_tickets.parquet"}'
    (FORMAT PARQUET)
    """)

    con.close()

    print("Bronze support_tickets ingestion complete")


if __name__ == "__main__":
    run()