"""Ingest: list source files, check each file's header (schema), read rows as strings.

Schema policy (per file, decided from the CSV header):
  * all required columns present                -> accepted (extra columns are ignored, logged)
  * one or more required columns missing/renamed -> every row of the file is quarantined
                                                  with reject_reason SCHEMA_MISMATCH
  * zero-byte file                               -> skipped with a warning (nothing to read)
"""
from __future__ import annotations

import csv
import glob
import logging
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from ecom_pipeline.schemas import (
    CORRUPT_COL, CUSTOMERS_SCHEMA, ORDER_COLUMNS, PRODUCTS_SCHEMA, RAW_ORDERS_SCHEMA,
)

log = logging.getLogger(__name__)

# Columns of a schema-mismatch reject before run-level fields are added.
SCHEMA_REJECT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("reject_reason", StringType(), True),
    StructField("reject_detail", StringType(), True),
    StructField("raw_record", StringType(), True),
])


@dataclass
class FileCheck:
    path: str
    header: list[str]
    missing: list[str]
    extra: list[str]

    @property
    def accepted(self) -> bool:
        return bool(self.header) and not self.missing


@dataclass
class IngestResult:
    raw: DataFrame                # rows from accepted files (all strings + lineage)
    schema_rejects: DataFrame     # rows from files whose header failed the schema check
    files: list[FileCheck] = field(default_factory=list)


def list_order_files(input_dir: str, pattern: str) -> list[str]:
    """Sorted list so processing order (and therefore lineage) is deterministic."""
    return sorted(glob.glob(str(Path(input_dir) / pattern)))


def read_header(path: str) -> list[str]:
    with open(path, newline="", encoding="utf-8") as fh:
        first = next(csv.reader(fh), [])
    return [h.strip() for h in first]


def check_file(path: str) -> FileCheck:
    header = read_header(path)
    missing = [c for c in ORDER_COLUMNS if c not in header] if header else []
    extra = [c for c in header if c not in ORDER_COLUMNS]
    return FileCheck(path=path, header=header, missing=missing, extra=extra)


def _file_name_col() -> Column:
    # input_file_name() is a full URI; keep only the base name (portable, stable in tests)
    return F.element_at(F.split(F.input_file_name(), "/"), -1)


def _json_of(cols: list[str]) -> Column:
    # ignoreNullFields=false keeps missing values visible as null in the raw record
    return F.to_json(F.struct(*[F.col(f"`{c}`") for c in cols]), {"ignoreNullFields": "false"})


def _read_csv_strings(spark: SparkSession, paths: list[str], header: list[str]) -> DataFrame:
    """Read files that share an identical header, every column as STRING.

    The schema is built from the file's own header so columns are matched by name, and a
    _corrupt_record column captures lines with the wrong number of fields.
    """
    schema = StructType(
        [StructField(c, StringType(), True) for c in header]
        + [StructField(CORRUPT_COL, StringType(), True)]
    )
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .schema(schema)
        .csv(paths)
    )


def read_orders(spark: SparkSession, files: list[str]) -> IngestResult:
    checks = [check_file(p) for p in files]
    raw_parts: list[DataFrame] = []
    reject_parts: list[DataFrame] = []

    # Group files by identical header so each group is one Spark read.
    groups: dict[tuple[str, ...], list[FileCheck]] = {}
    for chk in checks:
        if not chk.header:
            log.warning("skipping empty file (no header): %s", chk.path)
            continue
        groups.setdefault(tuple(chk.header), []).append(chk)

    for header_t, group in groups.items():
        header = list(header_t)
        paths = [g.path for g in group]
        df = _read_csv_strings(spark, paths, header)
        if group[0].accepted:
            if group[0].extra:
                log.warning("ignoring extra columns %s in %s", group[0].extra, paths)
            raw_parts.append(df.select(
                *[F.col(c) for c in ORDER_COLUMNS],
                _file_name_col().alias("source_file"),
                F.col(CORRUPT_COL),
                F.coalesce(F.col(CORRUPT_COL), _json_of(header)).alias("raw_record"),
            ))
        else:
            detail = (f"missing columns: {','.join(group[0].missing)}"
                      + (f"; unexpected columns: {','.join(group[0].extra)}" if group[0].extra else ""))
            log.error("schema mismatch in %s -> quarantining all rows (%s)", paths, detail)
            reject_parts.append(df.select(
                (F.col("event_id") if "event_id" in header else F.lit(None).cast("string")).alias("event_id"),
                (F.col("order_id") if "order_id" in header else F.lit(None).cast("string")).alias("order_id"),
                _file_name_col().alias("source_file"),
                F.lit("SCHEMA_MISMATCH").alias("reject_reason"),
                F.lit(detail).alias("reject_detail"),
                F.coalesce(F.col(CORRUPT_COL), _json_of(header)).alias("raw_record"),
            ))

    raw = _union_all(raw_parts) if raw_parts else spark.createDataFrame([], RAW_ORDERS_SCHEMA)
    rejects = (_union_all(reject_parts) if reject_parts
               else spark.createDataFrame([], SCHEMA_REJECT_SCHEMA))
    return IngestResult(raw=raw, schema_rejects=rejects, files=checks)


def _union_all(parts: list[DataFrame]) -> DataFrame:
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def read_customers(spark: SparkSession, path: str) -> DataFrame:
    """Small dimension. Assumed clean; duplicates by key are dropped defensively."""
    return (spark.read.option("header", True).schema(CUSTOMERS_SCHEMA).csv(path)
            .dropDuplicates(["customer_id"]))


def read_products(spark: SparkSession, path: str) -> DataFrame:
    return (spark.read.option("header", True).schema(PRODUCTS_SCHEMA).csv(path)
            .dropDuplicates(["product_id"]))
