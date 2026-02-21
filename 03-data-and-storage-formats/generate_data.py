"""Generate a synthetic e-commerce orders dataset.

Produces a CSV file with ~10M rows (~2GB).
Run with: python generate_data.py [--rows N] [--output PATH]
"""

import argparse
import csv
import random
import sys
from datetime import datetime, timedelta

CATEGORIES = [
    "Electronics", "Clothing", "Books", "Home & Garden", "Sports",
    "Toys", "Food & Drink", "Health", "Automotive", "Music",
]

PRODUCTS = {
    "Electronics": [
        ("Laptop", 600, 1500), ("Smartphone", 300, 1200), ("Headphones", 20, 350),
        ("Tablet", 200, 800), ("Monitor", 150, 600), ("Keyboard", 15, 150),
        ("Mouse", 10, 80), ("Webcam", 30, 200), ("Speaker", 25, 400),
        ("Smartwatch", 100, 500),
    ],
    "Clothing": [
        ("T-Shirt", 10, 50), ("Jeans", 30, 120), ("Jacket", 50, 300),
        ("Sneakers", 40, 200), ("Dress", 25, 150), ("Hat", 10, 40),
        ("Socks", 5, 20), ("Sweater", 30, 100),
    ],
    "Books": [
        ("Novel", 8, 25), ("Textbook", 30, 100), ("Cookbook", 15, 40),
        ("Biography", 10, 30), ("Comic", 5, 15), ("Art Book", 20, 60),
    ],
    "Home & Garden": [
        ("Lamp", 20, 150), ("Plant Pot", 5, 40), ("Rug", 30, 200),
        ("Pillow", 10, 60), ("Candle", 5, 30), ("Toolset", 25, 120),
    ],
    "Sports": [
        ("Running Shoes", 50, 200), ("Yoga Mat", 15, 60), ("Dumbbell Set", 30, 150),
        ("Bicycle", 200, 1000), ("Tennis Racket", 30, 200), ("Football", 15, 50),
    ],
    "Toys": [
        ("Board Game", 15, 60), ("Puzzle", 10, 30), ("Action Figure", 8, 40),
        ("LEGO Set", 20, 150), ("Doll", 10, 50),
    ],
    "Food & Drink": [
        ("Coffee Beans", 8, 30), ("Olive Oil", 5, 25), ("Chocolate Box", 10, 40),
        ("Wine", 8, 60), ("Tea Set", 5, 20),
    ],
    "Health": [
        ("Vitamins", 10, 40), ("Sunscreen", 8, 25), ("First Aid Kit", 15, 50),
        ("Thermometer", 10, 40),
    ],
    "Automotive": [
        ("Car Charger", 10, 40), ("Floor Mats", 20, 80), ("Dash Cam", 40, 200),
        ("Air Freshener", 3, 15),
    ],
    "Music": [
        ("Guitar", 100, 600), ("Ukulele", 30, 100), ("Drum Sticks", 5, 20),
        ("Vinyl Record", 15, 40), ("MIDI Controller", 50, 300),
    ],
}

COUNTRIES = [
    ("US", 0.35), ("UK", 0.12), ("DE", 0.10), ("FR", 0.08), ("ES", 0.07),
    ("IT", 0.06), ("JP", 0.05), ("BR", 0.05), ("CA", 0.04), ("AU", 0.04),
    ("MX", 0.02), ("IN", 0.02),
]

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
PAYMENT_WEIGHTS = [0.40, 0.25, 0.20, 0.10, 0.05]

STATUS_OPTIONS = ["completed", "completed", "completed", "completed",
                  "returned", "cancelled"]

COLUMNS = [
    "order_id", "order_date", "customer_id", "country", "category",
    "product", "quantity", "unit_price", "total_amount", "payment_method",
    "status",
]


def weighted_choice(options_weights):
    options, weights = zip(*options_weights)
    return random.choices(options, weights=weights, k=1)[0]


def generate_rows(num_rows, seed=42):
    random.seed(seed)

    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)
    date_range_days = (end_date - start_date).days

    for i in range(num_rows):
        order_date = start_date + timedelta(days=random.randint(0, date_range_days))
        country = weighted_choice(COUNTRIES)
        category = random.choice(CATEGORIES)
        product_name, price_min, price_max = random.choice(PRODUCTS[category])
        unit_price = round(random.uniform(price_min, price_max), 2)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3], k=1)[0]
        total_amount = round(unit_price * quantity, 2)
        payment_method = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0]
        status = random.choice(STATUS_OPTIONS)
        customer_id = random.randint(1, 500_000)

        yield [
            i + 1,
            order_date.strftime("%Y-%m-%d"),
            customer_id,
            country,
            category,
            product_name,
            quantity,
            unit_price,
            total_amount,
            payment_method,
            status,
        ]


def main():
    parser = argparse.ArgumentParser(description="Generate e-commerce orders dataset")
    parser.add_argument("--rows", type=int, default=10_000_000, help="Number of rows (default: 10M)")
    parser.add_argument("--output", type=str, default="data/orders.csv", help="Output CSV path")
    args = parser.parse_args()

    print(f"Generating {args.rows:,} rows to {args.output}...")

    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        for i, row in enumerate(generate_rows(args.rows)):
            writer.writerow(row)
            if (i + 1) % 1_000_000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"Done! Written to {args.output}")


if __name__ == "__main__":
    main()
