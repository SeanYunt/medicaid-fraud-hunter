from pathlib import Path

import click
import pandas as pd

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

PROCESSED_DIR = Path(__file__).parent / "processed"
PROVIDER_MONTHLY_FILE = PROCESSED_DIR / "provider_monthly.parquet"
PROVIDER_PROCEDURE_FILE = PROCESSED_DIR / "provider_procedure.parquet"


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Rename HHS columns to internal names and cast NPI to string."""
    rename_map = {raw: internal for raw, internal in REVERSE_MAP.items() if raw in df.columns}
    if rename_map:
        df = df.rename(columns=rename_map)
    for npi_col in ["npi", "servicing_npi"]:
        if npi_col in df.columns:
            df[npi_col] = df[npi_col].astype(str)
    return df


def load_claims(filepath: Path) -> pd.DataFrame:
    """Load claims data as a pandas DataFrame.

    Supports both CSV and Parquet files. Uses PyArrow engine for Parquet
    to support AVX-only hardware (unlike Polars which requires AVX2).
    """
    if filepath.suffix == ".parquet":
        df = pd.read_parquet(filepath, engine="pyarrow")
    else:
        df = pd.read_csv(filepath, dtype_backend="numpy_nullable", low_memory=False)

    return _normalize(df)


def load_claims_for_provider(filepath: Path, npi: str) -> pd.DataFrame:
    """Load all rows for a specific billing provider."""
    if filepath.suffix == ".parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        schema = pq.read_schema(filepath)
        npi_col = "BILLING_PROVIDER_NPI_NUM" if "BILLING_PROVIDER_NPI_NUM" in schema.names else "npi"
        # HHS parquet stores NPI as int64; cast the filter value to match
        filter_val: int | str = int(npi) if pa.types.is_integer(schema.field(npi_col).type) else npi
        table = pq.read_table(filepath, filters=[(npi_col, "=", filter_val)])
        return _normalize(table.to_pandas())

    df = load_claims(filepath)
    return df[df["npi"] == npi].copy().reset_index(drop=True)


def get_all_providers(filepath: Path) -> pd.DataFrame:
    """Get a unique list of billing provider NPIs from the dataset."""
    df = load_claims(filepath)
    return df[["npi"]].drop_duplicates().reset_index(drop=True)


def preprocess(raw_filepath: Path) -> tuple[Path, Path]:
    """Read the raw dataset once and write two small summary Parquet files.

    Creates:
      - provider_monthly.parquet: (npi, service_month, total_claims, total_paid,
        beneficiaries) — one row per provider per month.
      - provider_procedure.parquet: (npi, procedure_code, total_paid, row_count)
        — for consistency detection across procedure codes.

    Returns the paths to both files.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    click.echo(f"Reading raw dataset: {raw_filepath}")
    df = load_claims(raw_filepath)

    # --- Provider monthly summary ---
    click.echo("Aggregating provider monthly summaries...")
    agg_dict: dict = {"total_claims": "sum", "total_paid": "sum"}
    if "beneficiaries" in df.columns:
        agg_dict["beneficiaries"] = "sum"
    monthly = (
        df.groupby(["npi", "service_month"], as_index=False)
        .agg(agg_dict)
        .sort_values(["npi", "service_month"])
        .reset_index(drop=True)
    )
    monthly.to_parquet(PROVIDER_MONTHLY_FILE, engine="pyarrow", index=False)
    click.echo(f"  -> {PROVIDER_MONTHLY_FILE} ({PROVIDER_MONTHLY_FILE.stat().st_size / 1e6:.1f} MB, {len(monthly):,} rows)")

    # --- Provider procedure summary (for consistency detection) ---
    click.echo("Aggregating provider procedure summaries...")
    procedure = (
        df.groupby(["npi", "total_paid"])
        .size()
        .reset_index(name="row_count")
        .sort_values(["npi", "total_paid"])
        .reset_index(drop=True)
    )
    procedure.to_parquet(PROVIDER_PROCEDURE_FILE, engine="pyarrow", index=False)
    click.echo(f"  -> {PROVIDER_PROCEDURE_FILE} ({PROVIDER_PROCEDURE_FILE.stat().st_size / 1e6:.1f} MB, {len(procedure):,} rows)")

    return PROVIDER_MONTHLY_FILE, PROVIDER_PROCEDURE_FILE


def find_preprocessed() -> tuple[Path, Path] | None:
    """Return paths to preprocessed files if they exist."""
    if PROVIDER_MONTHLY_FILE.exists() and PROVIDER_PROCEDURE_FILE.exists():
        return PROVIDER_MONTHLY_FILE, PROVIDER_PROCEDURE_FILE
    return None
