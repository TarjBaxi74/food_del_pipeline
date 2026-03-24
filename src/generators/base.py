import random
from pathlib import Path
import pandas as pd
from faker import Faker

from config.settings import RAW_DIR, RANDOM_SEED

fake = Faker()
random.seed(RANDOM_SEED)


class BaseGenerator:

    def __init__(self, file_name: str):
        self.file_path = Path(RAW_DIR) / file_name

    def save(self, df: pd.DataFrame):
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.file_path, index=False)

    def generate(self):
        raise NotImplementedError("Subclasses must implement generate()")