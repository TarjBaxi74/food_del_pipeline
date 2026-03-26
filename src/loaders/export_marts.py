import duckdb
from config.settings import WAREHOUSE_DIR


def run():

    con = duckdb.connect("dbt_project/dev.duckdb")

    marts = {
        "01_SLA_breach_analysis": "sla_breach_analysis.csv",
        "02_restaurants_prep_delay": "restaurant_prep_delays.csv",
        "03_refund_drivers": "refund_drivers.csv",
        "04_riders_performance": "rider_performance.csv",
        "05_weekly_trends": "weekly_trends.csv"
    }

    for table, file in marts.items():

        query = f"""
        COPY (SELECT * FROM "{table}")
        TO '{WAREHOUSE_DIR / file}'
        (HEADER, DELIMITER ',')
        """

        con.execute(query)

        print(f"{file} exported")

    con.close()

    print("✅ All marts exported to warehouse")


if __name__ == "__main__":
    run()