from pathlib import Path
from datetime import date, timedelta

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
WAREHOUSE_DIR = DATA_DIR / "warehouse"

START_DATE = date.today() - timedelta(days=60)
END_DATE = date.today()

CITIES = ["Rajkot", "Pune", "Gandhinagar", "Ahmedabad", "Surat"]
RANDOM_SEED = 42