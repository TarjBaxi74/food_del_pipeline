import random
import pandas as pd
from datetime import date, timedelta

from config.settings import CITIES
from src.generators.base import BaseGenerator


SHIFT_TYPES = ["DAY", "NIGHT", "FLEX"]


class RidersGenerator(BaseGenerator):

    def __init__(self):
        super().__init__("riders.csv")

    def generate(self):

        riders = []

        for i in range(1, 201):

            joining = date.today() - timedelta(
                days=random.randint(10, 800)
            )

            riders.append({
                "rider_id": i,
                "city": random.choice(CITIES),
                "shift_type": random.choice(SHIFT_TYPES),
                "joining_date": joining
            })

        df = pd.DataFrame(riders)

        self.save(df)
        print("riders.csv generated")