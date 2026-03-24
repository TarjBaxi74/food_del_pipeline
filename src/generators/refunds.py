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
                    minutes=random.randint(20, 200)
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

        # ---- REALISM INJECTION ----

        # refund without order (~10 rows)
        extra = []

        for i in range(10):
            extra.append({
                "refund_id": refund_id + i,
                "order_id": random.randint(50000, 60000),
                "refund_ts": pd.Timestamp.now(),
                "refund_reason": random.choice(REASONS),
                "refund_amount": round(random.uniform(100, 500), 2)
            })

        df = pd.concat([df, pd.DataFrame(extra)])

        self.save(df)
        print("refunds.csv generated with realism")