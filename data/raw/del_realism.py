import pandas as pd

df = pd.read_json("data/raw/delivery_events.json", lines=True)

print("Total rows:", len(df))
print("Invalid riders:", (df["rider_id"] > 200).sum())
print("Duplicate rows:", df.duplicated().sum())

print("Events per order distribution:")
print(df.groupby("order_id").size().value_counts().head())