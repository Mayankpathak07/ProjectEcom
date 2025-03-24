import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#import seaborn as sns

import json
import random
from faker import Faker

# Load Olist datasets
olist_products_df = pd.read_csv("olist_products_cleaned_dataset.csv")
olist_orders_df = pd.read_csv("olist_orders_cleaned_dataset.csv")
olist_order_items_df = pd.read_csv("olist_order_items_cleaned_dataset.csv")
olist_customers_df = pd.read_csv("olist_customers_cleaned_dataset.csv")


# Extract lists of real data
product_ids = olist_products_df["product_id"].tolist()
order_ids = olist_orders_df["order_id"].tolist()
customer_ids = olist_customers_df["customer_id"].tolist()
seller_ids = olist_order_items_df["seller_id"].tolist()

fake = Faker()
Faker.seed(42)

# Define event types
event_types = ["purchase"]
payment_types = ["credit_card", "debit_card", "voucher", "boleto"]
payment_weights = [0.6, 0.2, 0.1, 0.1]  # Adjusted distribution

def generate_event():
    event_type = random.choice(event_types)

    # Select a real product, order, customer, and seller
    product_id = random.choice(product_ids)
    order_id = random.choice(order_ids) if event_type in ["add_to_cart", "remove_from_cart", "purchase"] else None
    customer_id = random.choice(customer_ids)
    seller_id = random.choice(seller_ids)

    event = {
        "event_id": fake.uuid4(),
        "timestamp": fake.iso8601(),
        "customer_id": customer_id, 
        "session_id": fake.uuid4(),
        "order_id": order_id,
        "product_id": product_id,
        "seller_id": seller_id,
        "product_category": fake.word(),
        "product_price": round(random.uniform(10, 1000), 2),
        "quantity": (
            random.randint(1, 5) if event_type in ["add_to_cart", "remove_from_cart", "purchase"]
            else (1 if event_type == "product_view" else 0)
        ),
        "payment_type": (
            random.choices(payment_types, weights=payment_weights, k=1)[0]
            if event_type == "purchase"
            else None
        ),
        "customer_state": fake.state_abbr() if event_type == "purchase" else "Unknown",
        "customer_city": fake.city() if event_type == "purchase" else "Unknown"
    }
    return event

if __name__ == "__main__":
    num_events = 30000  
    events = [generate_event() for _ in range(num_events)]
    
    with open("synthetic_ecommerce_events.json", "w") as f:
        json.dump(events, f, indent=4)

    print("Synthetic e-commerce events with real product, order, customer, and seller IDs generated successfully!")


 
data = pd.read_json("synthetic_ecommerce_events.json")

 
# data.info()

# ### Handling Missing values in Synthetic Event Generator data

 
print(data.isna().sum())

 
print(data.duplicated().sum())

 
data["quantity"] = data["quantity"].astype("int8")

