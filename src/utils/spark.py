"""PySpark session builder helper.

Provides a unified interface for initializing configured PySpark sessions
with MinIO/S3A storage connectors.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark_session(
    *,
    app_name: str,
    master: str = "local[*]",
    driver_memory: str = "2g",
    executor_memory: str = "2g",
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    shuffle_partitions: int = 4,
    additional_packages: list[str] | None = None,
    spark_config: dict[str, str] | None = None,
) -> SparkSession:
    """Build and return a PySpark session configured for MinIO access.

    Args:
        app_name: The application name.
        master: Spark master URL (e.g. local[*]).
        driver_memory: Memory size for the driver process.
        executor_memory: Memory size for each executor process.
        minio_endpoint: Endpoint URL for MinIO storage.
        minio_access_key: Access key for MinIO storage.
        minio_secret_key: Secret key for MinIO storage.
        shuffle_partitions: Number of shuffle partitions to configure.
        additional_packages: Additional maven packages to load.
        spark_config: Optional dictionary of extra config overrides.

    Returns:
        A configured SparkSession instance.
    """
    packages = ["org.apache.hadoop:hadoop-aws:3.4.2"]
    if additional_packages:
        packages.extend(additional_packages)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=WARN,console")
    )

    if spark_config:
        for key, value in spark_config.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()
