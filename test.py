import pandas as pd
import os

path = r"D:\Final Project Data arc"

files = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "product_category_name_translation.csv",
]

for f in files:
    df = pd.read_csv(os.path.join(path, f), nrows=0)
    print(f"\n=== {f} ===")
    for col in df.columns:
        print(f"  {col}")