from src.loaders.bronze_delivery_events import run as run_delivery
from src.loaders.bronze_order_items import run as run_items
from src.loaders.bronze_orders import run as run_orders
from src.loaders.bronze_refunds import run as run_refunds
from src.loaders.bronze_support_tickets import run as run_tickets
from src.loaders.bronze_restaurants import run as run_restaurants
from src.loaders.bronze_riders import run as run_riders


def run_all():

    print("Running Bronze Ingestion Pipeline...\n")

    run_orders()
    run_delivery()
    run_items()
    run_refunds()
    run_tickets()
    run_restaurants()
    run_riders()

    print("\nAll Bronze datasets ingested successfully.")


if __name__ == "__main__":
    run_all()