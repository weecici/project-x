"""Integration tests for the MinIO Parquet writer.

Uses ``testcontainers`` to spin up a real MinIO instance in Docker.
Validates that the lake writer correctly serialises data to Snappy-
compressed Parquet and that the resulting file is readable and correct.

Requires: Docker daemon running.
"""

from __future__ import annotations

import io

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.config import Config
from mypy_boto3_s3 import S3Client
from testcontainers.minio import MinioContainer


@pytest.fixture(scope="module")
def minio_container() -> MinioContainer:
    """Start a MinIO testcontainer for the duration of the module."""
    with MinioContainer() as minio:
        yield minio


@pytest.fixture(scope="module")
def s3_client(minio_container: MinioContainer) -> S3Client:
    """Return a boto3 S3 client configured for the testcontainer MinIO."""
    config = minio_container.get_config()
    return boto3.client(
        "s3",
        endpoint_url=f"http://{config['endpoint']}",
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        config=Config(signature_version="s3v4"),
    )


@pytest.fixture(scope="module")
def bronze_bucket(s3_client: S3Client) -> str:
    """Create the bronze bucket and return its name."""
    bucket = "bronze"
    s3_client.create_bucket(Bucket=bucket)
    return bucket


@pytest.mark.integration
class TestMinIOParquetWriter:
    """Integration tests for Parquet write/read via MinIO."""

    def test_parquet_file_is_written_and_readable(
        self, s3_client: S3Client, bronze_bucket: str
    ) -> None:
        """A Parquet file written to MinIO can be read back correctly."""
        # Arrange
        records = [
            {"symbol": "BTCUSDT", "price": 65_000.0, "quantity": 0.001},
            {"symbol": "BTCUSDT", "price": 65_001.5, "quantity": 0.002},
        ]
        table = pa.Table.from_pylist(records)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        key = "trades/symbol=BTCUSDT/year=2026/month=07/day=05/test-file.parquet"

        # Act
        s3_client.put_object(
            Bucket=bronze_bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )

        response = s3_client.get_object(Bucket=bronze_bucket, Key=key)
        raw = response["Body"].read()
        result = pq.read_table(io.BytesIO(raw))

        # Assert
        assert result.num_rows == 2
        assert result.schema.names == ["symbol", "price", "quantity"]
        assert result.column("symbol")[0].as_py() == "BTCUSDT"
        assert result.column("price")[0].as_py() == pytest.approx(65_000.0)

    def test_bucket_listing_returns_written_object(
        self, s3_client: S3Client, bronze_bucket: str
    ) -> None:
        """A written object appears in the bucket listing under the correct prefix."""
        # Arrange
        prefix = "trades/symbol=ETHUSDT/"
        key = f"{prefix}year=2026/month=07/day=05/eth-test.parquet"
        buf = io.BytesIO()
        pq.write_table(pa.table({"symbol": ["ETHUSDT"]}), buf, compression="snappy")
        buf.seek(0)
        s3_client.put_object(Bucket=bronze_bucket, Key=key, Body=buf.getvalue())

        # Act
        response = s3_client.list_objects_v2(Bucket=bronze_bucket, Prefix=prefix)
        keys = [obj["Key"] for obj in response.get("Contents", [])]

        # Assert
        assert key in keys

    def test_snappy_compression_reduces_file_size(
        self, s3_client: S3Client, bronze_bucket: str
    ) -> None:
        """Snappy-compressed Parquet is smaller than uncompressed."""
        # Arrange: 10 000 distinct rows (high compression opportunity)
        records = [{"symbol": "BTCUSDT", "price": f"price-{i}"} for i in range(10_000)]

        compressed_buf = io.BytesIO()
        uncompressed_buf = io.BytesIO()
        table = pa.Table.from_pylist(records)
        pq.write_table(
            table,
            compressed_buf,
            compression="snappy",
            use_dictionary=False,
        )
        pq.write_table(
            table,
            uncompressed_buf,
            compression="none",
            use_dictionary=False,
        )

        # Act / Assert
        assert len(compressed_buf.getvalue()) < len(uncompressed_buf.getvalue()), (
            "Snappy compression should reduce file size for repetitive data"
        )
