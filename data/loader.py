from pathlib import Path

import polars as pl

# Column name mapping: HHS dataset -> internal names
# HHS dataset columns:
#   BILLING_PROVIDER_NPI_NUM, SERVICING_PROVIDER_NPI_NUM, HCPCS_CODE,
#   CLAIM_FROM_MONTH, TOTAL_UNIQUE_BENEFICIARIES, TOTAL_CLAIMS, TOTAL_PAID
COLUMN_MAP = {
    "npi": "BILLING_PROVIDER_NPI_NUM",
    "servicing_npi": "SERVICING_PROVIDER_NPI_NUM",
    "procedure_code": "HCPCS_CODE",
    "service_month": "CLAIM_FROM_MONTH",
    "beneficiaries": "TOTAL_UNIQUE_BENEFICIARIES",
    "total_claims": "TOTAL_CLAIMS",
    "total_paid": "TOTAL_PAID",
}

REVERSE_MAP = {v: k for k, v in COLUMN_MAP.items()}


def load_claims(filepath: Path) -> pl.LazyFrame:
    """Load claims data as a Polars LazyFrame for memory-efficient processing.

    Supports both CSV and Parquet files. Uses lazy evaluation so the
    dataset is never fully loaded into memory.
    """
    if filepath.suffix == ".parquet":
        lf = pl.scan_parquet(filepath)
    else:
        lf = pl.scan_csv(filepath, infer_schema_length=10000)

    # Rename columns if they match the HHS schema
    existing_cols = lf.collect_schema().names()
    rename_map = {raw: internal for raw, internal in REVERSE_MAP.items() if raw in existing_cols}

    if rename_map:
        lf = lf.rename(rename_map)

    # Ensure NPI is always a string
    names = lf.collect_schema().names()
    for npi_col in ["npi", "servicing_npi"]:
        if npi_col in names:
            lf = lf.with_columns(pl.col(npi_col).cast(pl.Utf8))

    return lf


def load_claims_for_provider(filepath: Path, npi: str) -> pl.DataFrame:
    """Load all rows for a specific billing provider (collected, not lazy)."""
    lf = load_claims(filepath)
    return lf.filter(pl.col("npi") == npi).collect()


def get_all_providers(filepath: Path) -> pl.DataFrame:
    """Get a unique list of billing provider NPIs from the dataset."""
    lf = load_claims(filepath)
    return lf.select("npi").unique().collect()
