"""NPPES provider registry loader — efficient zip-based access.

Reads the NPPES full replacement data file from its zip without extracting,
using chunked CSV reads with usecols to minimise memory usage on the ~1 GB file.
"""

import io
import zipfile
from pathlib import Path

import pandas as pd

RAW_DATA_DIR = Path(__file__).parent / "raw"

# NPPES dissemination CSV column names (stable since 2013 format)
_NPI_COL = "NPI"
_ENTITY_COL = "Entity Type Code"
_ORG_NAME_COL = "Provider Organization Name (Legal Business Name)"
_LAST_NAME_COL = "Provider Last Name (Legal Name)"
_FIRST_NAME_COL = "Provider First Name"
_CRED_COL = "Provider Credential Text"
_ADDR_COL = "Provider Business Practice Location Address First Line"
_CITY_COL = "Provider Business Practice Location Address City Name"
_STATE_COL = "Provider Business Practice Location Address State Name"
_ZIP_COL = "Provider Business Practice Location Address Postal Code"
_TAXONOMY_COL = "Healthcare Provider Primary Taxonomy Code"
# Fallback: mailing address state when practice location state is absent
_MAIL_STATE_COL = "Provider Business Mailing Address State Name"

_STATE_COLS = {_NPI_COL, _STATE_COL, _MAIL_STATE_COL}
_ENTITY_COLS = {_NPI_COL, _ENTITY_COL}

_LOOKUP_COLS = {
    _NPI_COL, _ENTITY_COL, _ORG_NAME_COL, _LAST_NAME_COL,
    _FIRST_NAME_COL, _CRED_COL, _ADDR_COL, _CITY_COL,
    _STATE_COL, _ZIP_COL, _TAXONOMY_COL, _MAIL_STATE_COL,
}


def find_nppes_zip(data_dir: Path | None = None) -> Path:
    """Find the NPPES zip in the raw data directory."""
    if data_dir is None:
        data_dir = RAW_DATA_DIR

    zips = (
        list(data_dir.glob("nppes*.zip"))
        + list(data_dir.glob("NPPES*.zip"))
        + list(data_dir.glob("npidata*.zip"))
    )
    if not zips:
        raise FileNotFoundError(
            f"No NPPES zip found in {data_dir}. "
            "Expected a file matching nppes*.zip or npidata*.zip in data/raw/."
        )
    return max(zips, key=lambda f: f.stat().st_size)


def _open_main_csv(zip_path: Path) -> tuple[zipfile.ZipFile, io.IOBase]:
    """Return (ZipFile handle, file stream) for the main npidata CSV inside the zip."""
    zf = zipfile.ZipFile(zip_path)
    names = zf.namelist()

    # Prefer the canonical npidata_pfile_*.csv (skip fileheader, endpoint, pl files)
    candidates = [
        n for n in names
        if n.lower().endswith(".csv")
        and "fileheader" not in n.lower()
        and "endpoint" not in n.lower()
        and ("npidata" in n.lower() or n.lower().startswith("npi"))
    ]
    if not candidates:
        candidates = [
            n for n in names
            if n.lower().endswith(".csv") and "fileheader" not in n.lower()
        ]
    if not candidates:
        raise ValueError(
            f"Cannot find NPPES data CSV inside {zip_path}. "
            f"Zip contains: {names}"
        )

    # Pick the largest CSV — most likely the main data file
    largest = max(candidates, key=lambda n: zf.getinfo(n).file_size)
    return zf, zf.open(largest)


def load_organization_npis(zip_path: Path) -> set[str]:
    """Return NPIs whose NPPES entity type is 2 (Organization).

    Type 1 = Individual practitioner, Type 2 = Organization.
    The volume impossibility detector uses this to skip multi-staff providers
    for which the per-solo-practitioner claim threshold is meaningless.
    """
    org_npis: set[str] = set()
    zf, csv_stream = _open_main_csv(zip_path)
    try:
        for chunk in pd.read_csv(
            csv_stream,
            usecols=lambda c: c in _ENTITY_COLS,
            dtype=str,
            chunksize=200_000,
            low_memory=False,
        ):
            if _NPI_COL not in chunk.columns or _ENTITY_COL not in chunk.columns:
                continue
            chunk = chunk.copy()
            chunk[_NPI_COL] = chunk[_NPI_COL].fillna("").str.strip()
            chunk[_ENTITY_COL] = chunk[_ENTITY_COL].fillna("").str.strip()
            orgs = chunk.loc[chunk[_ENTITY_COL] == "2", _NPI_COL]
            org_npis.update(orgs[orgs != ""].tolist())
    finally:
        zf.close()
    return org_npis


def load_npi_state_map(zip_path: Path) -> dict[str, str]:
    """Return {npi: state} for every provider in the NPPES zip.

    Reads only the NPI and state columns for efficiency.
    State is the practice-location state; falls back to mailing state when absent.
    """
    result: dict[str, str] = {}
    zf, csv_stream = _open_main_csv(zip_path)
    try:
        for chunk in pd.read_csv(
            csv_stream,
            usecols=lambda c: c in _STATE_COLS,
            dtype=str,
            chunksize=200_000,
            low_memory=False,
        ):
            if _NPI_COL not in chunk.columns:
                continue
            chunk = chunk.copy()
            chunk[_NPI_COL] = chunk[_NPI_COL].fillna("").str.strip()

            # Prefer practice-location state; fall back to mailing state
            if _STATE_COL in chunk.columns:
                state_series = chunk[_STATE_COL].fillna("")
                if _MAIL_STATE_COL in chunk.columns:
                    state_series = state_series.where(
                        state_series != "", chunk[_MAIL_STATE_COL].fillna("")
                    )
            elif _MAIL_STATE_COL in chunk.columns:
                state_series = chunk[_MAIL_STATE_COL].fillna("")
            else:
                continue

            state_series = state_series.str.strip().str.upper()
            sub = chunk[[_NPI_COL]].copy()
            sub["state"] = state_series
            sub = sub[sub[_NPI_COL] != ""]
            result.update(zip(sub[_NPI_COL], sub["state"]))
    finally:
        zf.close()
    return result


def _build_name(row: pd.Series) -> str:
    """Return display name: org name for organisations, first+last for individuals."""
    entity = str(row.get(_ENTITY_COL, "")).strip()
    if entity == "2":
        name = str(row.get(_ORG_NAME_COL, "")).strip()
        return "" if name in ("", "nan") else name
    else:
        first = str(row.get(_FIRST_NAME_COL, "")).strip()
        last = str(row.get(_LAST_NAME_COL, "")).strip()
        parts = [p for p in [first, last] if p and p != "nan"]
        name = " ".join(parts)
        cred = str(row.get(_CRED_COL, "")).strip()
        if cred and cred != "nan":
            name += f", {cred}"
        return name


def _safe(row: pd.Series, col: str) -> str:
    v = str(row.get(col, "")).strip()
    return "" if v == "nan" else v


def search_providers(
    zip_path: Path,
    query: str,
    state: str | None = None,
    max_results: int = 10,
) -> list[dict]:
    """Search NPPES for providers by name or exact NPI.

    If query is a 10-digit number, performs an exact NPI lookup.
    Otherwise does a case-insensitive substring match against organisation name
    and individual last name.  Returns up to max_results records.
    """
    query = query.strip()
    is_npi = query.isdigit() and len(query) == 10
    state_upper = state.strip().upper() if state else None

    results: list[dict] = []
    zf, csv_stream = _open_main_csv(zip_path)
    try:
        for chunk in pd.read_csv(
            csv_stream,
            usecols=lambda c: c in _LOOKUP_COLS,
            dtype=str,
            chunksize=200_000,
            low_memory=False,
        ):
            if _NPI_COL not in chunk.columns:
                continue
            chunk = chunk.copy()
            chunk[_NPI_COL] = chunk[_NPI_COL].fillna("").str.strip()

            if is_npi:
                matches = chunk[chunk[_NPI_COL] == query]
            else:
                q_lower = query.lower()
                mask = pd.Series(False, index=chunk.index)
                if _ORG_NAME_COL in chunk.columns:
                    mask |= chunk[_ORG_NAME_COL].fillna("").str.lower().str.contains(
                        q_lower, regex=False
                    )
                if _LAST_NAME_COL in chunk.columns:
                    mask |= chunk[_LAST_NAME_COL].fillna("").str.lower().str.contains(
                        q_lower, regex=False
                    )
                matches = chunk[mask]

            if state_upper:
                state_col = _STATE_COL if _STATE_COL in matches.columns else _MAIL_STATE_COL
                if state_col in matches.columns:
                    matches = matches[
                        matches[state_col].fillna("").str.strip().str.upper() == state_upper
                    ]

            for _, row in matches.iterrows():
                zip_val = _safe(row, _ZIP_COL)
                state_val = _safe(row, _STATE_COL) or _safe(row, _MAIL_STATE_COL)
                results.append({
                    "npi": _safe(row, _NPI_COL),
                    "name": _build_name(row),
                    "address": _safe(row, _ADDR_COL),
                    "city": _safe(row, _CITY_COL),
                    "state": state_val,
                    "zip": zip_val[:5],
                    "taxonomy": _safe(row, _TAXONOMY_COL),
                })

            if is_npi and results:
                break  # NPI is unique — no need to read further
            if len(results) >= max_results:
                break
    finally:
        zf.close()

    return results[:max_results]
