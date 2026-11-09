"""Rows of the four tables the dashboards read, one pydantic model each.

Bounds check every row when it is built. Each model names its table in `table`.
Timestamps are ISO strings in UTC, because desflow publishes rows as JSON.
"""

from typing import ClassVar, Literal

from pydantic import BaseModel, Field

Status = Literal["Processing", "Shipped", "Complete", "Cancelled", "Returned"]


class Product(BaseModel):
    """
    A product in the catalogue.

    Attributes:
        id (int): The product's id.
        name (str): The product's name.
        category (str): Its category, such as "Jeans".
        department (str): "Men" or "Women".
        retail_price (float): Its price, above 0.
        cost (float): What it costs the shop, above 0.
    """

    table: ClassVar[str] = "products"
    id: int
    name: str
    category: str
    department: Literal["Men", "Women"]
    retail_price: float = Field(gt=0)
    cost: float = Field(gt=0)


class User(BaseModel):
    """
    A customer.

    Attributes:
        id (str): The user's id.
        age (int): Age, from 12 to 70.
        gender (str): "M" or "F".
        country (str): The country the user lives in.
        traffic_source (str): How the user found the shop, such as "Search".
        created_at (str): When the user signed up.
    """

    table: ClassVar[str] = "users"
    id: str
    age: int = Field(ge=12, le=70)
    gender: Literal["M", "F"]
    country: str
    traffic_source: str
    created_at: str


class Order(BaseModel):
    """
    An order of one or more items.

    Attributes:
        id (str): The order's id.
        user_id (str): The user who placed it.
        status (str): Its status, such as "Shipped".
        num_of_item (int): How many items it holds, from 1 to 4.
        created_at (str): When it was placed.
    """

    table: ClassVar[str] = "orders"
    id: str
    user_id: str
    status: Status
    num_of_item: int = Field(ge=1, le=4)
    created_at: str


class OrderItem(BaseModel):
    """
    One product in an order.

    Attributes:
        id (str): The item's id.
        order_id (str): The order it belongs to.
        user_id (str): The user who placed the order.
        product_id (int): The product.
        status (str): The order's status.
        sale_price (float): The price paid, above 0.
        created_at (str): When the order was placed.
    """

    table: ClassVar[str] = "order_items"
    id: str
    order_id: str
    user_id: str
    product_id: int
    status: Status
    sale_price: float = Field(gt=0)
    created_at: str
