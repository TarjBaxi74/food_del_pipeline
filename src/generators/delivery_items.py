import random
import pandas as pd
import json
from datetime import timedelta

from src.generators.base import BaseGenerator


EVENT_FLOW = ["ASSIGNED", "PICKED_UP", "DELIVERED"]


class DeliveryEventsGenerator(BaseGenerator):

    def __init__(self):
        super().__init__("delivery_events.json")

    def generate(self):

        orders = pd.read_csv("data/raw/orders.csv")

        events = []

        for _, row in orders.iterrows():

            base_time = pd.to_datetime(row["order_ts"])

            rider_id = random.randint(1, 200)

            delay = random.choice([0, 5, 10, 15, 25])

            event_times = [
                base_time + timedelta(minutes=5),
                base_time + timedelta(minutes=15),
                base_time + timedelta(minutes=30 + delay)
            ]

            for etype, ets in zip(EVENT_FLOW, event_times):

                events.append({
                    "order_id": int(row["order_id"]),
                    "rider_id": rider_id,
                    "event_type": etype,
                    "event_ts": str(ets),
                    "latitude": round(random.uniform(18.5, 19.5), 6),
                    "longitude": round(random.uniform(72.5, 73.5), 6)
                })

        with open(self.file_path, "w") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")

        print("delivery_events.json generated")