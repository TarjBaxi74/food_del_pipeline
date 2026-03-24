from src.generators.restaurants import RestaurantsGenerator
from src.generators.riders import RidersGenerator
from src.generators.orders import OrdersGenerator
from src.generators.delivery_items import DeliveryEventsGenerator
from src.generators.order_items import OrderItemsGenerator
from src.generators.refunds import RefundsGenerator
from src.generators.support_tickets import SupportTicketsGenerator


def run_all_generators():

    print("Generating restaurants...")
    RestaurantsGenerator().generate()

    print("Generating riders...")
    RidersGenerator().generate()

    print("Generating orders...")
    OrdersGenerator().generate()

    print("Generating delivery events...")
    DeliveryEventsGenerator().generate()

    print("Generating order items...")
    OrderItemsGenerator().generate()

    print("Generating refunds...")
    RefundsGenerator().generate()

    print("Generating support tickets...")
    SupportTicketsGenerator().generate()

    print("All raw datasets generated successfully.")


if __name__ == "__main__":
    run_all_generators()