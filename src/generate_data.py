import pandas as pd
import random
from faker import Faker

fake = Faker("en_IN")   # Indian locale

def generate_sales_data(num_rows=100000, output_path="data/raw/sales.csv"):
    products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headphones", "Charger"]
    statuses = ["completed", "completed", "completed", "returned", "pending", None]
    # completed appears 3x so it's most common — realistic

    rows = []
    for _ in range(num_rows):
        qty   = random.randint(1, 10)
        price = round(random.uniform(199, 89999), 2)
        rows.append({
            "order_id":    fake.uuid4(),
            "date":        fake.date_between(start_date="-1y", end_date="today"),
            "customer_id": random.randint(1000, 9999),
            "product":     random.choice(products),
            "quantity":    qty,
            "price":       price,
            "status":      random.choice(statuses),
            "city":        fake.city(),
        })

    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False)
    print(f"Generated {len(df)} rows → {output_path}")

if __name__ == "__main__":
    generate_sales_data()