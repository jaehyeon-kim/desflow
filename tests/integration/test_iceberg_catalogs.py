import json
import queue
import uuid
from uuid import UUID

import boto3
import pytest

from desflow.connectors.egress.iceberg import IcebergStorageEgress
from desflow.connectors.egress.kafka import GlueAvroSerializer
from tests.integration.helpers import drain

pytestmark = pytest.mark.integration

# The catalogs keep their metadata in a database or a service, and the warehouse is a
# temporary folder or bucket, so these tests check the catalog and not the storage.


async def _write_three_flushes(catalog: dict, table: str) -> None:
    """Writes three batches of four rows through an egress built from `catalog`."""
    egress = IcebergStorageEgress(catalog=catalog, default_table=table)
    q: queue.Queue = queue.Queue()
    for flush in range(3):
        q.put([{"key": f"task-{flush}-{i}", "n": i} for i in range(4)])
    await drain(egress, q)


@pytest.mark.storage
@pytest.mark.asyncio
async def test_sql_catalog_on_postgres_writes_one_snapshot_per_flush(
    postgres_container, tmp_path
):
    """A SQL catalog on the odctl PostgreSQL commits once per flush and reads back."""
    from pyiceberg.catalog import load_catalog

    properties = {
        "type": "sql",
        "uri": "postgresql+psycopg2://user:password@127.0.0.1:5432/odctl",
        "warehouse": f"file://{tmp_path}/warehouse",
    }
    namespace = f"des_test_{uuid.uuid4().hex[:8]}"
    catalog = load_catalog("postgres", **properties)
    try:
        await _write_three_flushes(
            {"name": "postgres", **properties}, f"{namespace}.events"
        )

        table = catalog.load_table(f"{namespace}.events")
        assert len(table.metadata.snapshots) == 3
        assert table.scan().to_arrow().num_rows == 12
    finally:
        for identifier in catalog.list_tables(namespace):
            catalog.drop_table(identifier)
        catalog.drop_namespace(namespace)


def _floci_client(service: str, floci: str):
    return boto3.client(
        service,
        endpoint_url=floci,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.mark.floci
@pytest.mark.asyncio
async def test_glue_catalog_writes_one_snapshot_per_flush(floci):
    """A Glue catalog on Floci, with its warehouse on Floci's S3, commits once per flush."""
    from pyiceberg.catalog import load_catalog

    bucket = f"desflow-{uuid.uuid4().hex[:8]}"
    _floci_client("s3", floci).create_bucket(Bucket=bucket)
    properties = {
        "type": "glue",
        "glue.endpoint": floci,
        "glue.region": "us-east-1",
        "glue.access-key-id": "test",
        "glue.secret-access-key": "test",
        "s3.endpoint": floci,
        "s3.access-key-id": "test",
        "s3.secret-access-key": "test",
        "s3.region": "us-east-1",
        "warehouse": f"s3://{bucket}/",
    }
    namespace = f"des_test_{uuid.uuid4().hex[:8]}"
    catalog = load_catalog("glue", **properties)
    try:
        await _write_three_flushes(
            {"name": "glue", **properties}, f"{namespace}.events"
        )

        table = catalog.load_table(f"{namespace}.events")
        assert len(table.metadata.snapshots) == 3
        assert table.scan().to_arrow().num_rows == 12
    finally:
        for identifier in catalog.list_tables(namespace):
            catalog.drop_table(identifier)
        catalog.drop_namespace(namespace)


@pytest.mark.floci
def test_glue_serializer_registers_the_schema_and_frames_a_record(floci):
    """GlueAvroSerializer reaches Glue on Floci, and the schema version reads back."""
    registry = f"desflow-{uuid.uuid4().hex[:8]}"
    schema = json.dumps(
        {
            "type": "record",
            "name": "Probe",
            "fields": [{"name": "value", "type": "int"}],
        }
    )
    glue = _floci_client("glue", floci)
    glue.create_registry(RegistryName=registry)

    serializer = GlueAvroSerializer(
        registry,
        schema,
        endpoint_url=floci,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    framed = serializer.serialize("events", {"value": 3})

    assert framed[:2] == b"\x03\x00"
    version = glue.get_schema_version(SchemaVersionId=str(UUID(bytes=framed[2:18])))
    assert json.loads(version["SchemaDefinition"]) == json.loads(schema)
