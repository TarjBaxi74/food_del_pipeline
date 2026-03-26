import time

from src.spark_jobs.silver.clean_orders import run as run_orders
from src.spark_jobs.silver.clean_delivery_events import run as run_delivery
from src.spark_jobs.silver.build_orders_facts import run as run_facts


def run():

    start = time.time()

    print("\n========== SILVER LAYER START ==========\n")

    print("➡ Running Silver Orders Job")
    run_orders()
    print("✔ Silver Orders Completed\n")

    print("➡ Running Silver Delivery Timeline Job")
    run_delivery()
    print("✔ Silver Delivery Timeline Completed\n")

    print("➡ Running Silver Order Facts Job")
    run_facts()
    print("✔ Silver Order Facts Completed\n")

    end = time.time()

    print("========== SILVER LAYER COMPLETED ==========")
    print(f"Total Time Taken: {round(end - start, 2)} seconds\n")


if __name__ == "__main__":
    run()