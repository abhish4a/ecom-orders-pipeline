"""SparkSession factory and per-mode tuning switches."""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession


def build_spark(
    app_name: str = "ecom-orders-pipeline",
    master: str = "local[*]",
    ui_enabled: bool = False,
    driver_memory: str = "4g",
) -> SparkSession:
    # Python workers must run the same interpreter as the driver (e.g. the project venv),
    # otherwise PySpark fails with PYTHON_VERSION_MISMATCH on machines with several Pythons.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        # All timestamps are interpreted and partitioned in UTC.
        .config("spark.sql.session.timeZone", "UTC")
        # Strict java.time parser: impossible dates such as 2026-02-30 become NULL, not an error.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        # Overwrite only the partitions present in the written DataFrame (idempotent reruns).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.enabled", str(ui_enabled).lower())
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def apply_mode(spark: SparkSession, mode: str, shuffle_partitions: int) -> None:
    """Set runtime SQL confs for the chosen job variant.

    baseline : what you get with "naive" settings -- no AQE, no auto-broadcast,
               the default 200 shuffle partitions.
    optimized: AQE on, broadcast allowed, shuffle partitions sized for the data/cores.
    """
    if mode == "baseline":
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        spark.conf.set("spark.sql.shuffle.partitions", "200")
    else:
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
