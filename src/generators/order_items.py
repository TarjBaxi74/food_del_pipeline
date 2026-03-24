import random
import pandas as pd

from src.generators.base import BaseGenerator


CUISINES = [
    "North Indian", "South Indian", "Chinese",
    "Pizza", "Biryani", "Street Food", "Cafe"
]


class OrderItemsGenerator(BaseGenerator):

    def __init__(self):
        super().__init__("order_items.csv")

    def generate(self):

        orders = pd.read_csv("data/raw/orders.csv")

        items = []
        item_id = 1

        for _, row in orders.iterrows():

            basket_size = random.randint(1, 4)

            for _ in range(basket_size):

                items.append({
                    "order_id": int(row["order_id"]),
                    "item_id": item_id,
                    "quantity": random.randint(1, 3),
                    "item_price": round(random.uniform(80, 300), 2),
                    "cuisine_type": random.choice(CUISINES)
                })

                item_id += 1

        df = pd.DataFrame(items)

        self.save(df)
        print("order_items.csv generated")