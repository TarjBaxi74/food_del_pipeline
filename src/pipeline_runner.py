import time

from src.generators.orchestrator import run_all_generators as run_raw
from src.loaders.run_bronze import run_all as run_bronze
from src.spark_jobs.run_silver import run as run_silver
from src.dbt_runner import run as run_dbt
from src.loaders.export_marts import run as run_export


def run():

    start = time.time()

    print("\n================ PIPELINE START ================\n")

    print("🚀 STEP-1 RAW DATA GENERATION")
    run_raw()

    print("\n🚀 STEP-2 BRONZE INGESTION")
    run_bronze()

    print("\n🚀 STEP-3 SILVER TRANSFORMATIONS")
    run_silver()

    print("\n🚀 STEP-4 DBT ANALYTICS MODELS")
    run_dbt()

    print("\n🚀 STEP-5 WAREHOUSE EXPORT")
    run_export()

    end = time.time()

    print("\n================ PIPELINE COMPLETED ================\n")
    print(f"Total Pipeline Time: {round(end - start, 2)} seconds")


if __name__ == "__main__":
    run()