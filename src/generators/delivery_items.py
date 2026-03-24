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

            # Invalid rider (~1%)
            if random.random() < 0.01:
                rider_id = random.randint(1000, 2000)

            delay = random.choice([0, 5, 10, 20, 35])

            assigned_time = base_time + timedelta(minutes=5)
            pickup_time = base_time + timedelta(minutes=15)
            delivered_time = base_time + timedelta(minutes=30 + delay)

            event_list = [
                ("ASSIGNED", assigned_time),
                ("PICKED_UP", pickup_time),
                ("DELIVERED", delivered_time)
            ]

            # Missing delivered event (~5%)
            if random.random() < 0.05:
                event_list = event_list[:-1]

            # Out-of-order events (~2%)
            if random.random() < 0.02:
                event_list.reverse()

            for etype, ets in event_list:

                record = {
                    "order_id": int(row["order_id"]),
                    "rider_id": rider_id,
                    "event_type": etype,
                    "event_ts": str(ets),
                    "latitude": round(random.uniform(18.5, 19.5), 6),
                    "longitude": round(random.uniform(72.5, 73.5), 6)
                }

                events.append(record)

                # Duplicate event (~1%)
                if random.random() < 0.01:
                    events.append(record.copy())

        with open(self.file_path, "w") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")

        print("delivery_events.json generated with realism")