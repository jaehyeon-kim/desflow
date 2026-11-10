"""The shop's fixed data: distribution centres, the cities users live in, and products.

The cities are the 10 largest in each of the 6 largest countries of the theLook location
data, weighted by population. The 260 products are generated from a fixed seed.
"""

import random
from typing import NamedTuple

from ecommerce.core.models import DistCenter, Product


class City(NamedTuple):
    """
    A city users live in.

    Attributes:
        country (str): The country.
        state (str): The state or province.
        city (str): The city.
        postal_code (str): A postal code in the city.
        latitude (float): Its latitude.
        longitude (float): Its longitude.
        population (int): Its population, the weight for choosing it.
    """

    country: str
    state: str
    city: str
    postal_code: str
    latitude: float
    longitude: float
    population: int


DIST_CENTERS = [
    DistCenter(id=1, name="Memphis TN", latitude=35.1174, longitude=-89.9711),
    DistCenter(id=2, name="Chicago IL", latitude=41.8369, longitude=-87.6847),
    DistCenter(id=3, name="Houston TX", latitude=29.7604, longitude=-95.3698),
    DistCenter(id=4, name="Los Angeles CA", latitude=34.05, longitude=-118.25),
    DistCenter(id=5, name="New Orleans LA", latitude=29.95, longitude=-90.0667),
    DistCenter(id=6, name="Port Authority of New York/New Jersey NY/NJ", latitude=40.634, longitude=-73.7834),
    DistCenter(id=7, name="Philadelphia PA", latitude=39.95, longitude=-75.1667),
    DistCenter(id=8, name="Mobile AL", latitude=30.6944, longitude=-88.0431),
    DistCenter(id=9, name="Charleston SC", latitude=32.7833, longitude=-79.9333),
    DistCenter(id=10, name="Savannah GA", latitude=32.0167, longitude=-81.1167),
]  # fmt: skip

CITIES = [
    City('China', 'Fujian', 'Dongguan', '361006', 24.5095, 118.1006, 1298368),
    City('China', 'Guangdong', 'Shenzhen', '523850', 22.8263, 113.7858, 772125),
    City('China', 'Guangdong', 'Dongguan', '518172', 22.7164, 114.239, 710307),
    City('China', 'Guangdong', 'Baoding', '523900', 22.8105, 113.6835, 681780),
    City('China', 'Hebei', 'Shenzhen', '71051', 38.8668, 115.4801, 673684),
    City('China', 'Guangdong', 'Jilin', '518109', 22.6662, 114.0052, 657969),
    City('China', 'Jilin', 'Shenzhen', '132001', 43.8496, 126.6119, 636311),
    City('China', 'Guangdong', 'Yinchuan', '518024', 22.5916, 114.1003, 636152),
    City('China', 'Ningxia Hui Autonomous Region', 'Shenzhen', '750000', 38.5076, 106.3158, 631948),
    City('China', 'Guangdong', 'Beijing', '518102', 22.5887, 113.8823, 621290),
    City('United States', 'Texas', 'El Paso', '79936', 31.7762, -106.2978, 129566),
    City('United States', 'California', 'Riverside', '92503', 33.8858, -117.4479, 120768),
    City('United States', 'Texas', 'Brownsville', '78521', 25.9624, -97.3255, 118948),
    City('United States', 'Texas', 'Houston', '77084', 29.8247, -95.6502, 117503),
    City('United States', 'Texas', 'Sugar Land', '77479', 29.5668, -95.6334, 116497),
    City('United States', 'California', 'Bakersfield', '93307', 35.2567, -118.9304, 114019),
    City('United States', 'California', 'Fontana', '92335', 34.0873, -117.4649, 113685),
    City('United States', 'California', 'Jurupa Valley', '92509', 34.003, -117.4456, 109868),
    City('United States', 'New York', 'New York', '11368', 40.7498, -73.854, 108647),
    City('United States', 'Washington', 'Pasco', '99301', 46.4183, -118.9075, 107807),
    City('Brasil', 'São Paulo', 'São Paulo', '02675-031', -23.4726, -46.7466, 2049839),
    City('Brasil', 'Distrito Federal', 'Brasília', '70297-400', -15.8268, -47.9226, 1640936),
    City('Brasil', 'Bahia', 'Salvador', '40301-110', -12.629, -38.6999, 1189323),
    City('Brasil', 'Paraná', 'Curitiba', '81020-490', -25.5031, -49.3314, 465169),
    City('Brasil', 'Maranhão', 'São Luís', '65137-000', -2.5481, -44.1893, 390034),
    City('Brasil', 'Rio de Janeiro', 'Duque de Caxias', '25265-008', -22.7245, -43.2869, 370859),
    City('Brasil', 'Pará', 'Belém', '68447-000', -1.4909, -48.6117, 346681),
    City('Brasil', 'Pará', 'Parauapebas', '68515-000', -6.1549, -50.4863, 331051),
    City('Brasil', 'Maranhão', 'São José de Ribamar', '65110-000', -2.5663, -44.0889, 268899),
    City('Brasil', 'Maranhão', 'Paço do Lumiar', '65060-000', -2.514, -44.1447, 241686),
    City('South Korea', 'Seoul', 'Seoul', '151-015', 37.4647, 126.9357, 303915),
    City('South Korea', 'Gyeonggi-do', 'Siheung City', '429-450', 37.3434, 126.7109, 170543),
    City('South Korea', 'Gyeonggi-do', 'Anyang City', '430-010', 37.3899, 126.9168, 141826),
    City('South Korea', 'Gyeonggi-do', 'Hwaseong City', '445-810', 37.1854, 127.1157, 139337),
    City('South Korea', 'Gyeongsangnam-do', 'Gimhae City', '621-830', 35.1845, 128.8, 133656),
    City('South Korea', 'Gyeonggi-do', 'Namyangju City', '472-840', 37.6532, 127.3158, 124309),
    City('South Korea', 'Busan', 'Busan', '611-080', 35.1785, 129.0913, 124238),
    City('South Korea', 'Gyeonggi-do', 'Yongin City', '446-913', 37.3095, 127.107, 121294),
    City('South Korea', 'Incheon Metropolitan City', 'Incheon Metropolitan City', '405-310', 37.3896, 126.6984, 118090),
    City('South Korea', 'Gyeonggi-do', 'Bucheon City', '420-020', 37.5006, 126.7693, 116262),
    City('France', 'Île-de-France', 'Paris', '75015', 48.8401, 2.2929, 226701),
    City('France', 'Auvergne-Rhône-Alpes', 'Villeurbanne', '69100', 45.771, 4.889, 162658),
    City('France', 'Grand Est', 'Reims', '51100', 49.2517, 4.0401, 162563),
    City('France', 'Bourgogne-Franche-Comté', 'Dijon', '21000', 47.3229, 5.0376, 147000),
    City('France', "Provence-Alpes-Côte d'Azur", 'Nice', '6200', 43.7045, 7.2083, 134356),
    City('France', 'Bretagne', 'Rennes', '35000', 48.1112, -1.6984, 128534),
    City('France', 'Occitanie', 'Toulouse', '31200', 43.6381, 1.4385, 125106),
    City('France', 'Occitanie', 'Montpellier', '34000', 43.606, 3.9007, 124632),
    City('France', 'Bretagne', 'Brest', '29200', 48.4001, -4.5023, 123821),
    City('France', 'Pays de la Loire', 'Le Mans', '72100', 47.985, 0.2072, 123430),
    City('United Kingdom', 'England', 'Croydon', 'CR0', 51.3655, -0.0656, 166862),
    City('United Kingdom', 'England', 'Leicester', 'LE2', 52.6027, -1.0956, 124740),
    City('United Kingdom', 'England', 'London', 'E17', 51.5875, -0.0232, 121162),
    City('United Kingdom', 'England', 'Brighton', 'BN1', 50.8681, -0.1338, 91427),
    City('United Kingdom', 'England', 'Coventry', 'CV6', 52.4372, -1.5082, 89781),
    City('United Kingdom', 'Wales', 'Cardiff', 'CF14', 51.5305, -3.1951, 88602),
    City('United Kingdom', 'England', 'Nottingham', 'NG5', 53.0122, -1.1398, 88317),
    City('United Kingdom', 'England', 'Maidenhead', 'SL6', 51.5188, -0.7406, 86084),
    City('United Kingdom', 'England', 'Hove', 'BN3', 50.8421, -0.1811, 85204),
    City('United Kingdom', 'England', 'Newcastle-under-Lyme', 'ST5', 52.9901, -2.2725, 81830),
]  # fmt: skip

# The usual price range of each category in the source catalogue, in dollars.
PRICE_RANGES = {
    "Accessories": (7, 108), "Active": (15, 88), "Blazers & Jackets": (12, 207),
    "Clothing Sets": (34, 147), "Dresses": (13, 160),
    "Fashion Hoodies & Sweatshirts": (24, 85), "Intimates": (10, 62),
    "Jeans": (35, 189), "Jumpsuits & Rompers": (11, 98), "Leggings": (8, 56),
    "Maternity": (19, 94), "Outerwear & Coats": (41, 280), "Pants": (22, 109),
    "Pants & Capris": (19, 100), "Plus": (8, 87), "Shorts": (16, 70),
    "Skirts": (15, 98), "Sleep & Lounge": (18, 90), "Socks": (9, 30),
    "Socks & Hosiery": (8, 27), "Suits": (56, 170), "Suits & Sport Coats": (28, 260),
    "Sweaters": (25, 148), "Swim": (25, 99), "Tops & Tees": (15, 73),
    "Underwear": (15, 38),
}  # fmt: skip
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
                        brand=brand,
                        category=category,
                        department=department,
                        retail_price=price,
                        cost=round(price * rng.uniform(0.39, 0.59), 2),
                        sku=f"SKU-{len(rows) + 1:05d}",
                        distribution_center_id=rng.choice(DIST_CENTERS).id,
                    )
                )
    return rows
