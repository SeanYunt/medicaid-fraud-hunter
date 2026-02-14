from pathlib import Path

import click

RAW_DATA_DIR = Path(__file__).parent / "raw"


def find_dataset(data_dir: Path | None = None) -> Path:
    """Find the dataset file in the raw data directory.

    Prefers Parquet files over CSV for faster loading.
    """
    if data_dir is None:
        data_dir = RAW_DATA_DIR

    if not data_dir.exists():
        raise click.ClickException(
            f"Data directory {data_dir} not found. "
            "Place your Medicaid claims dataset in data/raw/ and try again."
        )

    # Prefer Parquet, fall back to CSV
    data_files = list(data_dir.glob("*.parquet")) or list(data_dir.glob("*.csv"))
    if not data_files:
        raise click.ClickException(
            f"No Parquet or CSV files found in {data_dir}. "
            "Place your Medicaid claims dataset in data/raw/ and try again."
        )

    # Return the largest file (most likely the main dataset)
    return max(data_files, key=lambda f: f.stat().st_size)
