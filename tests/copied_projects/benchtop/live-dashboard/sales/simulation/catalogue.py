"""Fixed data the shop starts from: its products, and where and how its users arrive.

The products come from a fixed seed, so every run has the same catalogue. The country
weights are the combined population of each country's 10 largest cities in the theLook
eCommerce location data.
"""

import random

from sales.core.models import Product

COUNTRIES = {
    "China": 7_319_934,
    "Brasil": 7_294_477,
    "South Korea": 1_493_470,
    "France": 1_458_801,
    "United States": 1_157_308,
    "United Kingdom": 1_024_009,
}
TRAFFIC_SOURCES = {
    "Search": 0.7,
    "Organic": 0.15,
    "Facebook": 0.06,
    "Email": 0.05,
    "Display": 0.04,
}
ITEM_COUNTS = {1: 0.7, 2: 0.2, 3: 0.05, 4: 0.05}  # how many items an order holds

# The usual price range of each category in the source catalogue, in dollars.
PRICE_RANGES = {
    "Accessories": (7, 108),
    "Active": (15, 88),
    "Blazers & Jackets": (12, 207),
    "Clothing Sets": (34, 147),
    "Dresses": (13, 160),
    "Fashion Hoodies & Sweatshirts": (24, 85),
    "Intimates": (10, 62),
    "Jeans": (35, 189),
    "Jumpsuits & Rompers": (11, 98),
    "Leggings": (8, 56),
    "Maternity": (19, 94),
    "Outerwear & Coats": (41, 280),
    "Pants": (22, 109),
    "Pants & Capris": (19, 100),
    "Plus": (8, 87),
    "Shorts": (16, 70),
    "Skirts": (15, 98),
    "Sleep & Lounge": (18, 90),
    "Socks": (9, 30),
    "Socks & Hosiery": (8, 27),
    "Suits": (56, 170),
    "Suits & Sport Coats": (28, 260),
    "Sweaters": (25, 148),
    "Swim": (25, 99),
    "Tops & Tees": (15, 73),
    "Underwear": (15, 38),
}
BRANDS = ["Northfield", "Harbor & Pine", "Kestrel", "Marlow", "Upland"]
DEPARTMENTS = ["Men", "Women"]


def products() -> list[Product]:
    """
    Generates the catalogue: one product per brand, category and department.

    Returns:
        list[Product]: 260 products, the same on every call.
    """
    rng = random.Random(0)
    rows: list[Product] = []
    for department in DEPARTMENTS:
        for category, (low, high) in PRICE_RANGES.items():
            for brand in BRANDS:
                price = round(rng.uniform(low, high), 2)
                rows.append(
                    Product(
                        id=len(rows) + 1,
                        name=f"{brand} {department}'s {category}",
                        category=category,
                        department=department,
                        retail_price=price,
                        cost=round(price * rng.uniform(0.39, 0.59), 2),
                    )
                )
    return rows
