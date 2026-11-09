"""Builds the user and product features, and keeps the fitted transforms for later users.

User features are one-hot gender and traffic source, and scaled age and location.
Product features are a text embedding of the name and description, one-hot category,
a coffee flag and scaled price.
"""

import dataclasses
import logging
import pickle
from typing import Any

import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from textwiser import Embedding, TextWiser, Transformation

from recommender.core.config import (
    ARTIFACTS,
    PRODUCT_FEATURES,
    PRODUCTS,
    TEXT_COMPONENTS,
    USER_FEATURES,
    USERS,
)
from recommender.core.models import User

logger = logging.getLogger(__name__)

_USER_CATEGORIES = ["gender", "traffic_source"]
_USER_NUMBERS = ["age", "latitude", "longitude"]
# Personal details, and the address fields that latitude and longitude already cover.
_USER_DROPPED = ["user_id", "first_name", "last_name", "email", "street_address", "city", "state", "country", "postal_code"]  # fmt: skip
_COFFEE_IDS = [189, 190, 191, 192, 193, 194]


def _user_features() -> tuple[pd.DataFrame, MinMaxScaler, list[str]]:
    """Returns the users' features, the fitted age and location scaler, and the columns."""
    df = pd.read_csv(USERS)
    encoded = pd.get_dummies(
        df.drop(columns=_USER_DROPPED), columns=_USER_CATEGORIES, dtype=int
    )
    scaler = MinMaxScaler()
    encoded[_USER_NUMBERS] = scaler.fit_transform(encoded[_USER_NUMBERS])
    return pd.concat([df["user_id"], encoded], axis=1), scaler, encoded.columns.tolist()


def _product_features() -> tuple[pd.DataFrame, TextWiser, MinMaxScaler, list[str]]:
    """Returns the products' features, the fitted text model and price scaler, and the columns."""
    df = pd.read_csv(PRODUCTS)
    text = df["name"].fillna("") + " " + df["description"].fillna("")
    # TF-IDF word counts, reduced by SVD to the most meaningful patterns.
    model = TextWiser(
        Embedding.TfIdf(), Transformation.SVD(n_components=TEXT_COMPONENTS)
    )
    vectors = model.fit_transform(text)
    text_df = pd.DataFrame(vectors, columns=[f"txt_{i}" for i in range(vectors.shape[1])], index=df.index)  # fmt: skip
    categories = pd.get_dummies(df[["category"]], prefix="cat", dtype=int)
    df["is_coffee"] = df["product_id"].isin(_COFFEE_IDS).astype(int)
    scaler = MinMaxScaler()
    price = pd.DataFrame(
        scaler.fit_transform(df[["price"]]), columns=["price"], index=df.index
    )
    features = pd.concat(
        [df["product_id"], text_df, categories, df["is_coffee"], price], axis=1
    )
    return features, model, scaler, features.columns.tolist()


def build_features() -> None:
    """Writes `user_features.csv`, `product_features.csv` and the fitted transforms."""
    logger.info("Starting Feature Engineering...")
    users, user_scaler, user_columns = _user_features()
    users.to_csv(USER_FEATURES, index=False)
    logger.info(f"Saved User Features: {users.shape}")
    products, text_model, price_scaler, product_columns = _product_features()
    products.to_csv(PRODUCT_FEATURES, index=False)
    logger.info(f"Saved Product Features: {products.shape}")
    with open(ARTIFACTS, "wb") as f:
        pickle.dump(
            {
                "user_scaler": user_scaler,
                "user_columns": user_columns,
                "product_text_model": text_model,
                "product_price_scaler": price_scaler,
                "product_columns": product_columns,
            },
            f,
        )
    logger.info(f"Saved Pipeline Artifacts to: {ARTIFACTS}")


def load_artifacts() -> dict[str, Any]:
    """
    Reads the transforms that `build_features` fitted.

    Returns:
        dict[str, Any]: The user scaler and columns, and the product text model, price
            scaler and columns.
    """
    with open(ARTIFACTS, "rb") as f:
        return pickle.load(f)


def user_features(user: User, artifacts: dict[str, Any]) -> dict:
    """
    Turns one user into the features the models were trained on.

    Args:
        user (User): The user.
        artifacts (dict[str, Any]): The fitted transforms, from `load_artifacts`.

    Returns:
        dict: The user's id, then the features in the training columns' order.
    """
    df = pd.get_dummies(
        pd.DataFrame([dataclasses.asdict(user)]), columns=_USER_CATEGORIES, dtype=int
    )
    # Missing one-hot columns are added as 0, and columns not trained on are dropped.
    df = df.reindex(columns=artifacts["user_columns"], fill_value=0)
    df[_USER_NUMBERS] = artifacts["user_scaler"].transform(df[_USER_NUMBERS])
    df.insert(0, "user_id", user.user_id)
    return df.to_dict(orient="records")[0]


def load_products() -> dict[int, dict]:
    """
    Reads the product features that `build_features` wrote.

    Returns:
        dict[int, dict]: Each product's features, by product id, in file order.
    """
    return pd.read_csv(PRODUCT_FEATURES).set_index("product_id").to_dict("index")
