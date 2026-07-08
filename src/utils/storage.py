"""Shared S3/MinIO client factory.

Provides a single, consistently configured ``boto3`` S3 client for all
phase packages. Avoids duplicating connection setup (endpoint, credentials,
SigV4) across ``ingestion``, ``batch``, and future consumers.
"""

from __future__ import annotations

import boto3
from botocore.config import Config
from mypy_boto3_s3 import S3Client


def make_s3_client(
    *,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> S3Client:
    """Build a boto3 S3 client configured for S3-compatible storage.

    Suitable for both MinIO (local dev) and AWS S3 (cloud). Always uses
    SigV4 request signing and path-style addressing for MinIO compatibility.

    Args:
        endpoint: Full URL of the S3 endpoint (e.g. ``http://localhost:9000``).
        access_key: AWS/MinIO access key ID.
        secret_key: AWS/MinIO secret access key.

    Returns:
        A fully configured ``boto3`` S3 client.
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )
