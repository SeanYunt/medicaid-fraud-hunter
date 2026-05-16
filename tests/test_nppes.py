"""Tests for data/nppes.py — NPPES provider lookup and state filtering."""

import csv
import io
import zipfile
from pathlib import Path

import pytest

from data.nppes import load_npi_state_map, search_providers
from scanner.anomalies import scan_all
from tests.conftest import REVENUE_NPI


# ---------------------------------------------------------------------------
# Synthetic NPPES zip fixture
# ---------------------------------------------------------------------------

_NPPES_COLUMNS = [
    "NPI",
    "Entity Type Code",
    "Provider Organization Name (Legal Business Name)",
    "Provider Last Name (Legal Name)",
    "Provider First Name",
    "Provider Credential Text",
    "Provider Business Practice Location Address First Line",
    "Provider Business Practice Location Address City Name",
    "Provider Business Practice Location Address State Name",
    "Provider Business Practice Location Address Postal Code",
    "Healthcare Provider Primary Taxonomy Code",
    "Provider Business Mailing Address State Name",
]

_SAMPLE_PROVIDERS = [
    # NPI, entity, org_name, last, first, cred, addr, city, state, zip, taxonomy, mail_state
    ("1111111111", "2", "Acme Health Clinic", "", "", "", "100 Main St", "Charleston", "WV", "25301", "207Q00000X", "WV"),
    ("2222222222", "2", "Acme Medical Group", "", "", "", "200 Oak Ave", "Morgantown", "WV", "26501", "207R00000X", "WV"),
    ("3333333333", "1", "", "Smith", "John", "MD", "300 Pine Rd", "Roanoke", "VA", "24011", "208D00000X", "VA"),
    ("4444444444", "1", "", "Jones", "Jane", "DO", "400 Elm St", "Richmond", "VA", "23220", "207Q00000X", "VA"),
    # Provider with missing practice-location state — falls back to mailing state
    ("5555555555", "2", "Mountain Health", "", "", "", "500 Hill Dr", "Beckley", "", "25801", "207R00000X", "WV"),
]


def _make_nppes_zip(tmp_path: Path) -> Path:
    """Build a minimal synthetic NPPES zip for testing."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_NPPES_COLUMNS)
    for row in _SAMPLE_PROVIDERS:
        writer.writerow(row)

    zip_path = tmp_path / "npidata_test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("npidata_pfile_20240101-20240101.csv", buf.getvalue())
    return zip_path


@pytest.fixture
def nppes_zip(tmp_path: Path) -> Path:
    return _make_nppes_zip(tmp_path)


# ---------------------------------------------------------------------------
# load_npi_state_map tests
# ---------------------------------------------------------------------------

def test_load_npi_state_map_returns_dict(nppes_zip: Path):
    result = load_npi_state_map(nppes_zip)
    assert isinstance(result, dict)


def test_state_map_covers_all_providers(nppes_zip: Path):
    result = load_npi_state_map(nppes_zip)
    for npi, *_ in _SAMPLE_PROVIDERS:
        assert npi in result


def test_state_map_correct_state_values(nppes_zip: Path):
    result = load_npi_state_map(nppes_zip)
    assert result["1111111111"] == "WV"
    assert result["3333333333"] == "VA"


def test_state_map_fallback_to_mailing_state(nppes_zip: Path):
    # Provider 5555555555 has empty practice-location state → should fall back to mail state WV
    result = load_npi_state_map(nppes_zip)
    assert result["5555555555"] == "WV"


# ---------------------------------------------------------------------------
# search_providers tests
# ---------------------------------------------------------------------------

def test_search_by_npi(nppes_zip: Path):
    results = search_providers(nppes_zip, "1111111111")
    assert len(results) == 1
    assert results[0]["npi"] == "1111111111"


def test_search_by_name_substring(nppes_zip: Path):
    results = search_providers(nppes_zip, "Acme", max_results=10)
    npis = {r["npi"] for r in results}
    assert "1111111111" in npis
    assert "2222222222" in npis


def test_search_by_name_with_state_filter(nppes_zip: Path):
    results = search_providers(nppes_zip, "Acme", state="WV", max_results=10)
    assert all(r["state"] == "WV" for r in results)
    assert len(results) == 2


def test_search_individual_provider_last_name(nppes_zip: Path):
    results = search_providers(nppes_zip, "Smith", max_results=10)
    assert any(r["npi"] == "3333333333" for r in results)


def test_search_unknown_npi_returns_empty(nppes_zip: Path):
    results = search_providers(nppes_zip, "9999999999")
    assert results == []


def test_search_unknown_name_returns_empty(nppes_zip: Path):
    results = search_providers(nppes_zip, "Zzz_NoMatch_Xyz", max_results=10)
    assert results == []


def test_search_result_has_expected_fields(nppes_zip: Path):
    results = search_providers(nppes_zip, "1111111111")
    assert len(results) == 1
    r = results[0]
    for key in ("npi", "name", "address", "city", "state", "zip", "taxonomy"):
        assert key in r, f"Missing key: {key}"


# ---------------------------------------------------------------------------
# Integration: scan_all state_npis parameter
# ---------------------------------------------------------------------------

def test_scan_state_filter_restricts_output(sample_csv: Path, monthly_df, procedure_df):
    # Only include one provider in the state set
    state_npis = {"1000000001"}  # CLEAN_NPI
    results = scan_all(
        sample_csv,
        threshold=0.0,
        state_npis=state_npis,
    )
    result_npis = {r.npi for r in results}
    # Only NPIs in state_npis should appear
    assert result_npis.issubset(state_npis)


def test_revenue_outlier_national_baseline(sample_csv: Path):
    """Revenue outlier detection should still flag a provider using national baseline
    even when state_npis is provided to restrict output."""
    # Include only REVENUE_NPI in the state set — it should still be flagged
    # because national median/MAD uses all providers as baseline
    state_npis = {REVENUE_NPI}
    results = scan_all(
        sample_csv,
        threshold=0.0,
        state_npis=state_npis,
    )
    result_npis = {r.npi for r in results}
    assert REVENUE_NPI in result_npis
