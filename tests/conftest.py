"""Shared pytest fixtures: one local SparkSession for the whole test session."""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ecom_pipeline.spark_session import build_spark
from tests.helpers import make_input


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = build_spark(app_name="ecom-tests", master="local[2]", driver_memory="2g")
    yield session
    session.stop()


@pytest.fixture()
def workdir(tmp_path: Path) -> Path:
    """A temp project root with dimensions already written to <tmp>/input."""
    make_input(tmp_path)
    return tmp_path
