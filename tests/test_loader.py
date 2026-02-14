"""Tests for data loader."""

from pathlib import Path

import polars as pl

from data.loader import load_claims, load_claims_for_provider, get_all_providers
from tests.conftest import CLEAN_NPI, VOLUME_NPI


def test_load_claims_returns_lazyframe(sample_csv: Path):
    lf = load_claims(sample_csv)
    assert isinstance(lf, pl.LazyFrame)


def test_load_claims_renames_columns(sample_csv: Path):
    lf = load_claims(sample_csv)
    names = lf.collect_schema().names()
    assert "npi" in names
    assert "service_month" in names
    assert "total_paid" in names
    assert "procedure_code" in names


def test_load_claims_for_provider(sample_csv: Path):
    df = load_claims_for_provider(sample_csv, CLEAN_NPI)
    # Clean provider has 6 months * 2 procedure codes = 12 rows
    assert len(df) == 12
    assert df["npi"].unique().to_list() == [CLEAN_NPI]


def test_load_claims_for_unknown_provider(sample_csv: Path):
    df = load_claims_for_provider(sample_csv, "9999999999")
    assert df.is_empty()


def test_get_all_providers(sample_csv: Path):
    df = get_all_providers(sample_csv)
    npis = df["npi"].to_list()
    assert CLEAN_NPI in npis
    assert VOLUME_NPI in npis
    # 5 archetype providers + 20 filler providers
    assert len(npis) == 25
