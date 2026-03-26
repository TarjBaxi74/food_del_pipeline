import duckdb
from config.settings import WAREHOUSE_DIR


def run():

    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(WAREHOUSE_DIR / "dev.duckdb"))

    marts = {
        "01_SLA_breach_analysis": "sla_breach_analysis.csv",
        "02_restaurants_prep_delay": "restaurant_prep_delays.csv",
        "03_refund_drivers": "refund_drivers.csv",
        "04_riders_performance": "rider_performance.csv",
        "05_weekly_trends": "weekly_trends.csv"
    }

    for table, file in marts.items():
        df = con.execute(f'SELECT * FROM "{table}"').df()
        df.to_csv(WAREHOUSE_DIR / file, index=False)
        print(f"{file} exported")

    con.close()

    print("✅ All marts exported to warehouse")


if __name__ == "__main__":
    run()