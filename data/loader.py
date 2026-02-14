from pathlib import Path

import click
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

PROCESSED_DIR = Path(__file__).parent / "processed"
PROVIDER_MONTHLY_FILE = PROCESSED_DIR / "provider_monthly.parquet"
PROVIDER_PROCEDURE_FILE = PROCESSED_DIR / "provider_procedure.parquet"


def _normalize(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Rename HHS columns to internal names and cast NPI to string."""
    existing_cols = lf.collect_schema().names()
    rename_map = {raw: internal for raw, internal in REVERSE_MAP.items() if raw in existing_cols}

    if rename_map:
        lf = lf.rename(rename_map)

    names = lf.collect_schema().names()
    for npi_col in ["npi", "servicing_npi"]:
        if npi_col in names:
            lf = lf.with_columns(pl.col(npi_col).cast(pl.Utf8))

    return lf


def load_claims(filepath: Path) -> pl.LazyFrame:
    """Load claims data as a Polars LazyFrame for memory-efficient processing.

    Supports both CSV and Parquet files.
    """
    if filepath.suffix == ".parquet":
        lf = pl.scan_parquet(filepath)
    else:
        lf = pl.scan_csv(filepath, infer_schema_length=10000)

    return _normalize(lf)


def load_claims_for_provider(filepath: Path, npi: str) -> pl.DataFrame:
    """Load all rows for a specific billing provider (collected, not lazy)."""
    lf = load_claims(filepath)
    return lf.filter(pl.col("npi") == npi).collect()


def get_all_providers(filepath: Path) -> pl.DataFrame:
    """Get a unique list of billing provider NPIs from the dataset."""
    lf = load_claims(filepath)
    return lf.select("npi").unique().collect()


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
    lf = load_claims(raw_filepath)

    # --- Provider monthly summary ---
    click.echo("Aggregating provider monthly summaries...")
    monthly = (
        lf.group_by(["npi", "service_month"])
        .agg([
            pl.col("total_claims").sum().alias("total_claims"),
            pl.col("total_paid").sum().alias("total_paid"),
            pl.col("beneficiaries").sum().alias("beneficiaries"),
        ])
        .sort(["npi", "service_month"])
        .collect()
    )
    monthly.write_parquet(PROVIDER_MONTHLY_FILE)
    click.echo(f"  -> {PROVIDER_MONTHLY_FILE} ({PROVIDER_MONTHLY_FILE.stat().st_size / 1e6:.1f} MB, {len(monthly):,} rows)")

    # --- Provider procedure summary (for consistency detection) ---
    click.echo("Aggregating provider procedure summaries...")
    procedure = (
        lf.group_by(["npi", "total_paid"])
        .agg(pl.len().alias("row_count"))
        .sort(["npi", "total_paid"])
        .collect()
    )
    procedure.write_parquet(PROVIDER_PROCEDURE_FILE)
    click.echo(f"  -> {PROVIDER_PROCEDURE_FILE} ({PROVIDER_PROCEDURE_FILE.stat().st_size / 1e6:.1f} MB, {len(procedure):,} rows)")

    return PROVIDER_MONTHLY_FILE, PROVIDER_PROCEDURE_FILE


def find_preprocessed() -> tuple[Path, Path] | None:
    """Return paths to preprocessed files if they exist."""
    if PROVIDER_MONTHLY_FILE.exists() and PROVIDER_PROCEDURE_FILE.exists():
        return PROVIDER_MONTHLY_FILE, PROVIDER_PROCEDURE_FILE
    return None
