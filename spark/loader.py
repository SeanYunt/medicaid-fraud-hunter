"""
Spark-based ingestion layer for Medicaid claims data.

Mirrors data/loader.py but uses PySpark DataFrames instead of Polars.
Designed to run locally (SparkSession with local[*]) or on a cluster.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Same column map as data/loader.py — HHS dataset -> internal names
COLUMN_MAP = {
    "BILLING_PROVIDER_NPI_NUM": "npi",
    "SERVICING_PROVIDER_NPI_NUM": "servicing_npi",
    "HCPCS_CODE": "procedure_code",
    "CLAIM_FROM_MONTH": "service_month",
    "TOTAL_UNIQUE_BENEFICIARIES": "beneficiaries",
    "TOTAL_CLAIMS": "total_claims",
    "TOTAL_PAID": "total_paid",
}


def get_or_create_session(app_name: str = "MedicaidFraudHunter") -> SparkSession:
    """Return (or create) a local SparkSession."""
    # Point Spark workers at the current Python executable so the venv is used
    # (avoids the Windows Store Python alias intercepting 'python' calls)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.host", "127.0.0.1")      # avoid hostname resolution issues on Windows
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", sys.executable)
        .getOrCreate()
    )


def load_claims(spark: SparkSession, filepath: Path) -> DataFrame:
    """Load a claims CSV or Parquet file and normalize column names."""
    path_str = str(filepath)
    if filepath.suffix == ".parquet":
        df = spark.read.parquet(path_str)
    else:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path_str)

    return _normalize(df)


def _normalize(df: DataFrame) -> DataFrame:
    """Rename HHS columns to internal names; cast NPI columns to string."""
    existing = set(df.columns)
    rename_map = {raw: internal for raw, internal in COLUMN_MAP.items() if raw in existing}

    for raw, internal in rename_map.items():
        df = df.withColumnRenamed(raw, internal)

    for npi_col in ("npi", "servicing_npi"):
        if npi_col in df.columns:
            df = df.withColumn(npi_col, F.col(npi_col).cast(StringType()))

    return df


def build_monthly_summary(df: DataFrame) -> DataFrame:
    """Aggregate to one row per (npi, service_month)."""
    return (
        df.groupBy("npi", "service_month")
        .agg(
            F.sum("total_claims").alias("total_claims"),
            F.sum("total_paid").alias("total_paid"),
            F.sum("beneficiaries").alias("beneficiaries"),
        )
        .orderBy("npi", "service_month")
    )


def build_procedure_summary(df: DataFrame) -> DataFrame:
    """Aggregate to (npi, total_paid, row_count) for consistency detection."""
    return (
        df.groupBy("npi", "total_paid")
        .agg(F.count("*").alias("row_count"))
        .orderBy("npi", "total_paid")
    )
