from pathlib import Path

import polars as pl

# Column name mapping — update these once we see the actual dataset schema.
# Keys are our internal names, values are the column names in the raw CSV.
COLUMN_MAP = {
    "claim_id": "CLM_ID",
    "npi": "NPI",
    "provider_name": "PROVIDER_NAME",
    "specialty": "SPECIALTY",
    "state": "STATE",
    "city": "CITY",
    "zip_code": "ZIP",
    "service_date": "SRVC_DT",
    "procedure_code": "PROC_CD",
    "diagnosis_code": "DIAG_CD",
    "billed_amount": "BILLED_AMT",
    "paid_amount": "PAID_AMT",
    "place_of_service": "POS",
    "units": "UNITS",
}

# Reverse map for renaming raw columns to internal names
REVERSE_MAP = {v: k for k, v in COLUMN_MAP.items()}


def load_claims(filepath: Path) -> pl.LazyFrame:
    """Load claims data as a Polars LazyFrame for memory-efficient processing.

    Uses lazy evaluation so the 11GB dataset is never fully loaded into memory.
    """
    lf = pl.scan_csv(filepath, infer_schema_length=10000)

    # Rename columns if they match our expected schema
    existing_cols = lf.collect_schema().names()
    rename_map = {raw: internal for raw, internal in REVERSE_MAP.items() if raw in existing_cols}

    if rename_map:
        lf = lf.rename(rename_map)

    # Ensure NPI is always a string (Polars may infer as int for all-numeric values)
    npi_col = "npi" if "npi" in lf.collect_schema().names() else "NPI"
    if npi_col in lf.collect_schema().names():
        lf = lf.with_columns(pl.col(npi_col).cast(pl.Utf8))

    return lf


def load_claims_for_provider(filepath: Path, npi: str) -> pl.DataFrame:
    """Load all claims for a specific provider (collected, not lazy)."""
    lf = load_claims(filepath)

    npi_col = "npi" if "npi" in lf.collect_schema().names() else "NPI"
    return lf.filter(pl.col(npi_col) == npi).collect()


def get_all_providers(filepath: Path) -> pl.DataFrame:
    """Get a unique list of providers from the dataset."""
    lf = load_claims(filepath)

    names = lf.collect_schema().names()
    npi_col = "npi" if "npi" in names else "NPI"
    name_col = "provider_name" if "provider_name" in names else "PROVIDER_NAME"

    cols = [npi_col]
    if name_col in names:
        cols.append(name_col)

    return lf.select(cols).unique().collect()
