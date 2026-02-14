import json
import urllib.request
from pathlib import Path

import click

RAW_DATA_DIR = Path(__file__).parent / "raw"
NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/?version=2.1&number="


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


def lookup_npi(npi: str) -> dict:
    """Look up provider details from the NPPES public registry.

    Returns a dict with name, address, specialty, and enumeration_type,
    or an empty dict if the lookup fails.
    """
    try:
        resp = urllib.request.urlopen(NPPES_API_URL + npi, timeout=10)
        data = json.loads(resp.read())
    except Exception:
        return {}

    if data.get("result_count", 0) == 0:
        return {}

    result = data["results"][0]
    basic = result.get("basic", {})
    info: dict = {"npi": npi, "enumeration_type": result.get("enumeration_type", "")}

    # Name: organization or individual
    if "organization_name" in basic:
        info["name"] = basic["organization_name"]
    else:
        parts = [basic.get("first_name", ""), basic.get("middle_name", ""),
                 basic.get("last_name", "")]
        info["name"] = " ".join(p for p in parts if p)
        if basic.get("credential"):
            info["name"] += f", {basic['credential']}"

    # Practice address (LOCATION type preferred)
    for addr in result.get("addresses", []):
        if addr.get("address_purpose") == "LOCATION":
            info["address"] = addr.get("address_1", "")
            info["city"] = addr.get("city", "")
            info["state"] = addr.get("state", "")
            info["zip"] = addr.get("postal_code", "")[:5]
            break

    # Primary taxonomy (specialty)
    for tax in result.get("taxonomies", []):
        if tax.get("primary"):
            info["specialty"] = tax.get("desc", "")
            break

    return info
