import random
import pandas as pd
from datetime import datetime, timedelta

from config.settings import START_DATE, END_DATE
from src.generators.base import BaseGenerator


STATUSES = ["COMPLETED", "CANCELLED"]
PAYMENT_MODES = ["COD", "UPI", "CARD"]


class OrdersGenerator(BaseGenerator):

    def __init__(self):
        super().__init__("orders.csv")

    def generate(self):

        orders = []
        order_id = 1

        current = START_DATE

        while current <= END_DATE:

            daily_orders = random.randint(120, 220)

            for _ in range(daily_orders):

                order_time = datetime.combine(
                    current,
                    datetime.min.time()
                ) + timedelta(
                    minutes=random.randint(480, 1380)
                )

                sla_minutes = random.randint(25, 45)

                promised_time = order_time + timedelta(
                    minutes=sla_minutes
                )

                orders.append({
                    "order_id": order_id,
                    "customer_id": random.randint(1000, 5000),
                    "restaurant_id": random.randint(1, 120),
                    "city": None,   # we will fix later in silver
                    "order_ts": order_time,
                    "promised_delivery_ts": promised_time,
                    "status": random.choice(STATUSES),
                    "order_value": round(random.uniform(150, 800), 2),
                    "payment_mode": random.choice(PAYMENT_MODES)
                })

                order_id += 1

            current += timedelta(days=1)

        df = pd.DataFrame(orders)

        self.save(df)
        print("orders.csv generated")