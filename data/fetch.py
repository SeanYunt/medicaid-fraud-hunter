from pathlib import Path

import click

RAW_DATA_DIR = Path(__file__).parent / "raw"


def find_dataset(data_dir: Path | None = None) -> Path:
    """Find the dataset file in the raw data directory."""
    if data_dir is None:
        data_dir = RAW_DATA_DIR

    if not data_dir.exists():
        raise click.ClickException(
            f"Data directory {data_dir} not found. "
            "Place your Medicaid claims CSV in data/raw/ and try again."
        )

    csv_files = list(data_dir.glob("*.csv"))
    if not csv_files:
        raise click.ClickException(
            f"No CSV files found in {data_dir}. "
            "Place your Medicaid claims CSV in data/raw/ and try again."
        )

    # Return the largest CSV file (most likely the main dataset)
    return max(csv_files, key=lambda f: f.stat().st_size)
