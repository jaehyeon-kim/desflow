import runpy
import uuid
from pathlib import Path

import pyarrow.fs as pafs
import pytest

from desflow.connectors.egress.storage import _build_filesystem
from tests.integration.helpers import check_round_trip, write_and_read_back

GUIDES = Path(__file__).resolve().parents[2] / "docs" / "snippets" / "guides"

pytestmark = pytest.mark.integration


@pytest.mark.floci
def test_parquet_and_jsonl_round_trip_on_s3_through_floci(floci):
    """Both storage sinks write through the AWS S3 protocol to Floci and read back."""
    check_round_trip(
        write_and_read_back(
            {
                "type": "s3",
                "endpoint_override": floci,
                "access_key": "test",
                "secret_key": "test",
                "region": "us-east-1",
                "allow_bucket_creation": True,
            },
            "desflow-storage",
        )
    )


@pytest.mark.gcp
def test_parquet_and_jsonl_round_trip_on_gcs(fake_gcs):
    """Both storage sinks write to fake-gcs-server through a `gcs` mapping and read back."""
    check_round_trip(
        write_and_read_back(
            {
                "type": "gcs",
                "anonymous": True,
                "project_id": "desflow-test",
                "endpoint_override": f"http://{fake_gcs}",
            },
            "desflow-storage",
        )
    )


@pytest.mark.azure
def test_parquet_and_jsonl_round_trip_on_azure(azurite):
    """Both storage sinks write to Azurite through an `azure` mapping and read back."""
    check_round_trip(write_and_read_back(azurite, "desflow-storage"))


def _run_the_page_shop(filesystem: dict) -> tuple:
    """Runs the Write to Cloud Storage shop for 10 simulated minutes into a new folder.

    Returns the Parquet and the JSON Lines tables, read back with the page's
    read_events through the same filesystem.
    """
    page = runpy.run_path(str(GUIDES / "cloud_storage.py"))

    store = _build_filesystem(filesystem, pafs)
    store.create_dir("desflow-storage")
    # The page's bucket argument is a prefix, so a folder of its own keeps runs apart
    folder = f"desflow-storage/page-{uuid.uuid4().hex[:8]}"
    try:
        page["build_shop"](page["storage_sinks"](filesystem, folder)).run(until=600)
        return (
            page["read_events"](store, f"{folder}/parquet"),
            page["read_events"](store, f"{folder}/jsonl", format="json"),
        )
    finally:
        store.delete_dir_contents(folder, missing_dir_ok=True)


def _check_the_page_shop(tables: tuple) -> None:
    parquet, jsonl = tables
    assert parquet.num_rows == jsonl.num_rows > 0
    assert {"key", "status", "order_id", "amount"} <= set(parquet.column_names)


@pytest.mark.floci
def test_the_page_shop_writes_to_s3_through_floci(floci):
    """The Write to Cloud Storage shop writes both formats to S3 and reads them back."""
    _check_the_page_shop(
        _run_the_page_shop(
            {
                "type": "s3",
                "endpoint_override": floci,
                "access_key": "test",
                "secret_key": "test",
                "region": "us-east-1",
                "allow_bucket_creation": True,
            }
        )
    )


@pytest.mark.gcp
def test_the_page_shop_writes_to_gcs(fake_gcs):
    """The Write to Cloud Storage shop writes both formats to GCS and reads them back."""
    _check_the_page_shop(
        _run_the_page_shop(
            {
                "type": "gcs",
                "anonymous": True,
                "project_id": "desflow-test",
                "endpoint_override": f"http://{fake_gcs}",
            }
        )
    )


@pytest.mark.azure
def test_the_page_shop_writes_to_azure(azurite):
    """The Write to Cloud Storage shop writes both formats to Azure and reads them back."""
    _check_the_page_shop(_run_the_page_shop(azurite))
