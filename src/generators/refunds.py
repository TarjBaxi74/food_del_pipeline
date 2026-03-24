import random
import pandas as pd
from datetime import timedelta

from src.generators.base import BaseGenerator


REASONS = ["DELAY", "MISSING_ITEM", "CUSTOMER_CANCEL"]


class RefundsGenerator(BaseGenerator):

    def __init__(self):
        super().__init__("refunds.csv")

    def generate(self):

        orders = pd.read_csv("data/raw/orders.csv")

        refunds = []
        refund_id = 1

        for _, row in orders.iterrows():

            if random.random() < 0.15:

                order_time = pd.to_datetime(row["order_ts"])

                refund_time = order_time + timedelta(
                    minutes=random.randint(40, 180)
                )

                refund_amt = round(
                    row["order_value"] * random.uniform(0.3, 1.0),
                    2
                )

                refunds.append({
                    "refund_id": refund_id,
                    "order_id": int(row["order_id"]),
                    "refund_ts": refund_time,
                    "refund_reason": random.choice(REASONS),
                    "refund_amount": refund_amt
                })

                refund_id += 1

        df = pd.DataFrame(refunds)

        self.save(df)
        print("refunds.csv generated")