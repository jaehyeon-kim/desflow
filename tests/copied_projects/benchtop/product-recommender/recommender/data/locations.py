"""Where the simulated users live: the postal areas of one city, weighted by population."""

import csv
from dataclasses import dataclass

from faker import Faker

from recommender.core.config import CITY, LOCATIONS

_FIELDS = ("city", "state", "postal_code", "country", "latitude", "longitude")


@dataclass
class Locations:
    """
    The postal areas of one city, and the chance of a user living in each.

    Attributes:
        areas (list[dict]): The postal areas, as rows of `world_pop.csv`.
        weights (list[float]): Each area's share of the city's population.
    """

    areas: list[dict]
    weights: list[float]

    def pick(self, fake: Faker) -> dict:
        """
        Draws one postal area, weighted by population.

        Args:
            fake (Faker): The seeded Faker, whose random generator draws the area.

        Returns:
            dict: The area's city, state, postal code, country, latitude and longitude.
        """
        area = fake.random.choices(self.areas, weights=self.weights, k=1)[0]
        return {k: area[k] for k in _FIELDS}


def load_locations() -> Locations:
    """
    Reads the postal areas of the configured city from `world_pop.csv`.

    Returns:
        Locations: The city's postal areas and their population weights.
    """
    country, city = CITY
    with open(LOCATIONS, encoding="utf-8") as f:
        areas = [
            r
            for r in csv.DictReader(f)
            if r["country"] == country and r["city"] == city
        ]
    total = sum(int(a["population"]) for a in areas)
    return Locations(areas, [int(a["population"]) / total for a in areas])
