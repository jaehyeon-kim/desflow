import pytest

from tests.integration.helpers import check_round_trip, write_and_read_back

pytestmark = [pytest.mark.integration, pytest.mark.storage]


def test_parquet_and_jsonl_round_trip_on_seaweedfs(seaweedfs):
    """Both storage sinks write two batches to SeaweedFS's S3 API and read back whole."""
    rows = write_and_read_back(
        {
            "type": "s3",
            "endpoint_override": seaweedfs["endpoint"],
            "access_key": seaweedfs["access_key"],
            "secret_key": seaweedfs["secret_key"],
            "region": seaweedfs["region"],
            "allow_bucket_creation": True,
        },
        "desflow-storage",
    )

    check_round_trip(rows)
