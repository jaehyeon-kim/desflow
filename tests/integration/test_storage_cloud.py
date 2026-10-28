import pytest

from tests.integration.helpers import check_round_trip, write_and_read_back

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
