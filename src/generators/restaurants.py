import random
import pandas as pd
from datetime import date, timedelta

from config.settings import CITIES
from src.generators.base import BaseGenerator


CUISINES = [
    "North Indian", "South Indian", "Chinese",
    "Pizza", "Biryani", "Street Food", "Cafe"
]

RATING_BANDS = ["LOW", "MEDIUM", "HIGH"]


class RestaurantsGenerator(BaseGenerator):

    def __init__(self):
        super().__init__("restaurants.csv")

    def generate(self):

        restaurants = []

        for i in range(1, 121):

            onboarding = date.today() - timedelta(
                days=random.randint(30, 900)
            )

            restaurants.append({
                "restaurant_id": i,
                "city": random.choice(CITIES),
                "cuisine_type": random.choice(CUISINES),
                "rating_band": random.choice(RATING_BANDS),
                "onboarding_date": onboarding
            })

        df = pd.DataFrame(restaurants)

        self.save(df)
        print("restaurants.csv generated")