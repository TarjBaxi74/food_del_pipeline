import os
import time
import sys


def run():

    start = time.time()

    print("\n========== DBT LAYER START ==========\n")

    # save original working directory
    original_cwd = os.getcwd()
    
    try:
        # run from the dbt_project folder to ensure dbt_project.yml is found
        os.chdir('dbt_project')

        dbt_run_cmd = 'dbt run'
        dbt_test_cmd = 'dbt test'

        print("➡ Running dbt models")
        if os.system(dbt_run_cmd) != 0:
            raise Exception("❌ dbt run failed")

        print("✔ dbt models completed\n")

        print("➡ Running dbt tests")
        if os.system(dbt_test_cmd) != 0:
            raise Exception("❌ dbt test failed")

        print("✔ dbt tests completed\n")

    finally:
        # restore original working directory
        os.chdir(original_cwd)

    end = time.time()

    print("========== DBT LAYER COMPLETED ==========")
    print(f"Total Time Taken: {round(end - start, 2)} seconds\n")


if __name__ == "__main__":
    run()