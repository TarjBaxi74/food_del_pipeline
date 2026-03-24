import random
import pandas as pd
from datetime import timedelta

from src.generators.base import BaseGenerator


TICKET_TYPES = [
    "DELAY", "MISSING_ITEM",
    "WRONG_ORDER", "PAYMENT_ISSUE"
]

STATUS = ["RESOLVED", "OPEN"]


class SupportTicketsGenerator(BaseGenerator):

    def __init__(self):
        super().__init__("support_tickets.csv")

    def generate(self):

        orders = pd.read_csv("data/raw/orders.csv")

        tickets = []
        ticket_id = 1

        for _, row in orders.iterrows():

            if random.random() < 0.10:

                order_time = pd.to_datetime(row["order_ts"])

                created_time = order_time + timedelta(
                    minutes=random.randint(20, 200)
                )

                tickets.append({
                    "ticket_id": ticket_id,
                    "order_id": int(row["order_id"]),
                    "ticket_type": random.choice(TICKET_TYPES),
                    "created_ts": created_time,
                    "resolution_status": random.choice(STATUS)
                })

                ticket_id += 1

        df = pd.DataFrame(tickets)

        self.save(df)
        print("support_tickets.csv generated")