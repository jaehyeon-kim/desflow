"""The rows of the six tables, one pydantic model each, with bounds on their values.

Times are ISO 8601 text in UTC: desflow publishes rows as JSON-friendly values, and
asyncpg rejects a string for a timestamp column.
"""

from typing import Literal

from pydantic import BaseModel, Field

Status = Literal["Processing", "Shipped", "Delivered", "Cancelled", "Returned"]


class DistCenter(BaseModel):
    """
    A distribution centre products ship from.

    Attributes:
        id (int): The centre's id.
        name (str): The centre's name.
        latitude (float): Its latitude, from -90 to 90.
        longitude (float): Its longitude, from -180 to 180.
    """

    id: int
    name: str
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)


class Product(BaseModel):
    """
    A product in the catalogue.

    Attributes:
        id (int): The product's id.
        name (str): The product's name.
        brand (str): Its brand.
        category (str): Its category, such as "Jeans".
        department (str): "Men" or "Women".
        retail_price (float): Its price, above 0.
        cost (float): What it costs the shop, above 0 and below the price.
        sku (str): Its stock keeping unit.
        distribution_center_id (int): The centre it ships from.
    """

    id: int
    name: str
    brand: str
    category: str
    department: Literal["Men", "Women"]
    retail_price: float = Field(gt=0)
    cost: float = Field(gt=0)
    sku: str
    distribution_center_id: int


class User(BaseModel):
    """
    A registered user.

    Attributes:
        id (str): The user's id.
        first_name (str): First name.
        last_name (str): Last name.
        email (str): Email address.
        age (int): Age, from 12 to 70.
        gender (str): "M" or "F".
        street_address (str): Street address.
        postal_code (str): Postal code.
        city (str): City.
        state (str): State or province.
        country (str): Country.
        latitude (float): Latitude of the city.
        longitude (float): Longitude of the city.
        traffic_source (str): How the user first arrived.
        created_at (str): When the user registered.
        updated_at (str): When the row last changed, such as after a move.
    """

    id: str
    first_name: str
    last_name: str
    email: str
    age: int = Field(ge=12, le=70)
    gender: Literal["M", "F"]
    street_address: str
    postal_code: str
    city: str
    state: str
    country: str
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    traffic_source: str
    created_at: str
    updated_at: str


class Order(BaseModel):
    """
    An order, whose status moves from Processing onwards.

    Attributes:
        id (str): The order's id.
        user_id (str): The user who placed it.
        status (str): Processing, Shipped, Delivered, Cancelled or Returned.
        num_of_items (int): How many items it has, from 1 to 4.
        created_at (str): When it was placed.
        updated_at (str): When its status last changed.
        shipped_at (str | None): When it shipped.
        delivered_at (str | None): When it was delivered.
        cancelled_at (str | None): When it was cancelled.
        returned_at (str | None): When it was returned.
    """

    id: str
    user_id: str
    status: Status
    num_of_items: int = Field(ge=1, le=4)
    created_at: str
    updated_at: str
    shipped_at: str | None = None
    delivered_at: str | None = None
    cancelled_at: str | None = None
    returned_at: str | None = None


class OrderItem(BaseModel):
    """
    One product in an order. It carries the order's status and times.

    Attributes:
        id (str): The item's id.
        order_id (str): The order it belongs to.
        product_id (int): The product.
        status (str): The order's status.
        quantity (int): How many, from 1 to 3.
        sale_price (float): The price paid for one, above 0.
        created_at (str): When the order was placed.
        updated_at (str): When the order's status last changed.
        shipped_at (str | None): When the order shipped.
        delivered_at (str | None): When the order was delivered.
        cancelled_at (str | None): When the order was cancelled.
        returned_at (str | None): When the order was returned.
    """

    id: str
    order_id: str
    product_id: int
    status: Status
    quantity: int = Field(ge=1, le=3)
    sale_price: float = Field(gt=0)
    created_at: str
    updated_at: str
    shipped_at: str | None = None
    delivered_at: str | None = None
    cancelled_at: str | None = None
    returned_at: str | None = None


class Event(BaseModel):
    """
    One page view in a web session.

    Attributes:
        id (str): The event's id.
        user_id (str | None): The user, or None for an anonymous visitor.
        session_id (str): The session the view belongs to.
        sequence_number (int): Its position in the session, from 1.
        event_type (str): The page, such as "product", "cart" or "purchase".
        uri (str): The page's address.
        city (str): The visitor's city.
        state (str): The visitor's state or province.
        postal_code (str): The visitor's postal code.
        browser (str): The visitor's browser.
        traffic_source (str): How the visitor arrived.
        ip_address (str): The visitor's IP address.
        created_at (str): When the page was viewed.
    """

    id: str
    user_id: str | None
    session_id: str
    sequence_number: int = Field(ge=1)
    event_type: str
    uri: str
    city: str
    state: str
    postal_code: str
    browser: str
    traffic_source: str
    ip_address: str
    created_at: str


# The table each model's rows go to. Every table is keyed on `id`, and the simulation
# upserts on it, so a changed order or user reaches PostgreSQL as an update.
TABLES: dict[type[BaseModel], str] = {
    DistCenter: "dist_centers",
    Product: "products",
    User: "users",
    Order: "orders",
    OrderItem: "order_items",
    Event: "events",
}
KEY = ["id"]
