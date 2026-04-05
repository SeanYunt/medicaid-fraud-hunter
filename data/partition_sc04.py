"""
Partition the Medicaid spending dataset to SC-04 providers only.

SC-04 covers Greenville and Spartanburg counties (upstate SC).
  - Greenville County zips: 296xx
  - Spartanburg County zips: 293xx

Approach:
  1. Stream the NPPES full registry CSV (11GB uncompressed, inside zip)
     and collect NPIs whose practice location zip falls in SC-04.
  2. Filter the Medicaid spending parquet to those NPIs.
  3. Write data/raw/medicaid-provider-spending-sc04.parquet.
"""

import csv
import io
import sys
import zipfile
from pathlib import Path

import polars as pl

RAW_DIR = Path(__file__).parent / "raw"
NPPES_ZIP = RAW_DIR / "nppes_full.zip"
NPPES_CSV = "npidata_pfile_20050523-20260208.csv"
SPENDING_PARQUET = RAW_DIR / "medicaid-provider-spending.parquet"
OUTPUT_PARQUET = RAW_DIR / "medicaid-provider-spending-sc04.parquet"

# Zip code prefixes for SC-04 counties
SC04_ZIP_PREFIXES = ("293", "296")


def extract_sc04_npis() -> set[str]:
    """Stream NPPES CSV and return NPIs with practice location in SC-04."""
    print("Streaming NPPES registry (this may take a few minutes)...")

    sc04_npis: set[str] = set()
    rows_read = 0

    with zipfile.ZipFile(NPPES_ZIP) as z:
        with z.open(NPPES_CSV) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8"))
            headers = next(reader)

            npi_idx = headers.index("NPI")
            state_idx = headers.index(
                "Provider Business Practice Location Address State Name"
            )
            zip_idx = headers.index(
                "Provider Business Practice Location Address Postal Code"
            )
            deactivated_idx = headers.index("NPI Deactivation Date")

            for row in reader:
                rows_read += 1
                if rows_read % 1_000_000 == 0:
                    print(f"  {rows_read:,} rows scanned, {len(sc04_npis):,} SC-04 NPIs found...")

                # Skip deactivated providers
                if row[deactivated_idx].strip():
                    continue

                if row[state_idx].strip().upper() != "SC":
                    continue

                zip_code = row[zip_idx].strip()[:3]
                if zip_code in SC04_ZIP_PREFIXES:
                    sc04_npis.add(row[npi_idx].strip())

    print(f"Done. {rows_read:,} rows scanned, {len(sc04_npis):,} active SC-04 NPIs found.")
    return sc04_npis


def filter_spending(sc04_npis: set[str]) -> None:
    """Filter spending parquet to SC-04 NPIs and write output."""
    print(f"Loading spending data from {SPENDING_PARQUET}...")

    npi_list = list(sc04_npis)

    df = (
        pl.scan_parquet(SPENDING_PARQUET)
        .filter(pl.col("BILLING_PROVIDER_NPI_NUM").is_in(npi_list))
        .collect()
    )

    print(f"Filtered to {len(df):,} rows across {df['BILLING_PROVIDER_NPI_NUM'].n_unique():,} providers.")
    print(f"Writing {OUTPUT_PARQUET}...")
    df.write_parquet(OUTPUT_PARQUET)
    size_mb = OUTPUT_PARQUET.stat().st_size / 1e6
    print(f"Done. Output: {OUTPUT_PARQUET} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    if not NPPES_ZIP.exists():
        print(f"Error: {NPPES_ZIP} not found.", file=sys.stderr)
        sys.exit(1)

    if not SPENDING_PARQUET.exists():
        print(f"Error: {SPENDING_PARQUET} not found.", file=sys.stderr)
        sys.exit(1)

    sc04_npis = extract_sc04_npis()
    filter_spending(sc04_npis)
