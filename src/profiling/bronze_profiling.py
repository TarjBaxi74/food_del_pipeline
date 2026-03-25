import duckdb
from config.settings import BRONZE_DIR


def run():

    con = duckdb.connect()

    orders = f"{BRONZE_DIR}/orders.parquet"
    events = f"{BRONZE_DIR}/delivery_events.parquet"
    refunds = f"{BRONZE_DIR}/refunds.parquet"
    restaurants = f"{BRONZE_DIR}/restaurants.parquet"

    print("\n================ ROW COUNTS ================\n")

    print("Orders:",
          con.execute(f"SELECT COUNT(*) FROM read_parquet('{orders}')").fetchone()[0])

    print("Events:",
          con.execute(f"SELECT COUNT(*) FROM read_parquet('{events}')").fetchone()[0])

    print("Refunds:",
          con.execute(f"SELECT COUNT(*) FROM read_parquet('{refunds}')").fetchone()[0])

    print("\n================ NULL % ================\n")

    res = con.execute(f"""
        SELECT 
            SUM(CASE WHEN payment_mode IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*) AS payment_null_pct,
            SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*) AS city_null_pct
        FROM read_parquet('{orders}')
    """).fetchone()

    print("Payment Mode Null %:", round(res[0], 2))
    print("City Null %:", round(res[1], 2))

    print("\n================ DUPLICATE ORDERS ================\n")

    dup = con.execute(f"""
        SELECT COUNT(*) 
        FROM (
            SELECT order_id
            FROM read_parquet('{orders}')
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    print("Duplicate order_ids:", dup)

    print("\n================ LATE DELIVERIES ================\n")

    late = con.execute(f"""
        WITH delivered AS (
            SELECT o.order_id,
                   MAX(CASE WHEN e.event_type='DELIVERED' THEN e.event_ts END) AS delivered_ts,
                   o.promised_delivery_ts
            FROM read_parquet('{orders}') o
            LEFT JOIN read_parquet('{events}') e
            ON o.order_id = e.order_id
            GROUP BY o.order_id, o.promised_delivery_ts
        )
        SELECT COUNT(*) 
        FROM delivered
        WHERE delivered_ts > promised_delivery_ts
    """).fetchone()[0]

    print("Late deliveries:", late)

    print("\n================ REFUND WITHOUT ORDER ================\n")

    orphan = con.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{refunds}') r
        LEFT JOIN read_parquet('{orders}') o
        ON r.order_id = o.order_id
        WHERE o.order_id IS NULL
    """).fetchone()[0]

    print("Orphan refunds:", orphan)

    print("\n================ CITY MISMATCH ================\n")

    mismatch = con.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{orders}') o
        JOIN read_parquet('{restaurants}') r
        ON o.restaurant_id = r.restaurant_id
        WHERE o.city IS NOT NULL
        AND o.city <> r.city
    """).fetchone()[0]

    print("City mismatch:", mismatch)

    con.close()


if __name__ == "__main__":
    run()