"""
Tests for the PySpark anomaly detection layer (spark/anomalies.py + spark/loader.py).

Uses the same synthetic dataset as conftest.py via a shared SparkSession fixture.
SparkSession is session-scoped so it is created once per test run (startup is ~5s).
"""

import csv
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")

from collections.abc import Iterator

from pyspark.sql import SparkSession

from spark.loader import (
    build_monthly_summary,
    build_procedure_summary,
    get_or_create_session,
    load_claims,
)
from spark.anomalies import (
    detect_billing_spikes,
    detect_revenue_outliers,
    detect_suspicious_consistency,
    detect_volume_impossibility,
    filter_qualifying_providers,
    run_all_detectors,
    MIN_TOTAL_PAID,
)
from tests.conftest import (
    CLEAN_NPI,
    CONSISTENCY_NPI,
    REVENUE_NPI,
    SPIKE_NPI,
    VOLUME_NPI,
    _generate_rows,
)


# ---------------------------------------------------------------------------
# Session-scoped Spark + DataFrames (created once for the whole test module)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = get_or_create_session("MedicaidFraudHunterTest")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_csv_session(tmp_path_factory) -> Path:
    tmp = tmp_path_factory.mktemp("spark_data")
    filepath = tmp / "test_claims.csv"
    rows = _generate_rows()
    fieldnames = list(rows[0].keys())
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return filepath


@pytest.fixture(scope="session")
def monthly(spark, sample_csv_session):
    df = load_claims(spark, sample_csv_session)
    return build_monthly_summary(df).cache()


@pytest.fixture(scope="session")
def procedure(spark, sample_csv_session):
    df = load_claims(spark, sample_csv_session)
    return build_procedure_summary(df).cache()


# ---------------------------------------------------------------------------
# loader tests
# ---------------------------------------------------------------------------

def test_load_claims_normalizes_columns(spark, sample_csv_session):
    df = load_claims(spark, sample_csv_session)
    assert "npi" in df.columns
    assert "total_claims" in df.columns
    assert "total_paid" in df.columns
    assert "service_month" in df.columns
    # Raw HHS column names should be gone
    assert "BILLING_PROVIDER_NPI_NUM" not in df.columns


def test_load_claims_npi_is_string(spark, sample_csv_session):
    df = load_claims(spark, sample_csv_session)
    npi_type = dict(df.dtypes)["npi"]
    assert npi_type == "string"


def test_monthly_summary_one_row_per_npi_month(monthly):
    # After groupBy there should be no duplicate (npi, service_month) pairs
    total_rows = monthly.count()
    distinct_rows = monthly.select("npi", "service_month").distinct().count()
    assert total_rows == distinct_rows


def test_procedure_summary_row_count_positive(procedure):
    from pyspark.sql import functions as F
    min_count = procedure.agg(F.min("row_count")).collect()[0][0]
    assert min_count >= 1


# ---------------------------------------------------------------------------
# filter_qualifying_providers
# ---------------------------------------------------------------------------

def test_filter_qualifying_removes_small_providers(monthly):
    filtered = filter_qualifying_providers(monthly)
    npis = {r["npi"] for r in filtered.select("npi").distinct().collect()}
    # All synthetic providers easily exceed MIN_TOTAL_PAID=100_000
    assert VOLUME_NPI in npis
    assert REVENUE_NPI in npis


# ---------------------------------------------------------------------------
# detect_volume_impossibility
# ---------------------------------------------------------------------------

def test_volume_flags_known_violator(monthly):
    flags = detect_volume_impossibility(monthly)
    npis = {r["npi"] for r in flags.collect()}
    assert VOLUME_NPI in npis


def test_volume_does_not_flag_clean_provider(monthly):
    flags = detect_volume_impossibility(monthly)
    npis = {r["npi"] for r in flags.collect()}
    assert CLEAN_NPI not in npis


def test_volume_severity_between_0_and_1(monthly):
    flags = detect_volume_impossibility(monthly)
    for row in flags.collect():
        assert 0.0 <= row["severity"] <= 1.0


def test_volume_flag_type_correct(monthly):
    flags = detect_volume_impossibility(monthly)
    for row in flags.collect():
        assert row["flag_type"] == "volume_impossibility"


# ---------------------------------------------------------------------------
# detect_revenue_outliers
# ---------------------------------------------------------------------------

def test_revenue_flags_known_outlier(monthly):
    flags = detect_revenue_outliers(monthly)
    npis = {r["npi"] for r in flags.collect()}
    assert REVENUE_NPI in npis


def test_revenue_does_not_flag_clean(monthly):
    flags = detect_revenue_outliers(monthly)
    npis = {r["npi"] for r in flags.collect()}
    assert CLEAN_NPI not in npis


def test_revenue_severity_between_0_and_1(monthly):
    flags = detect_revenue_outliers(monthly)
    for row in flags.collect():
        assert 0.0 <= row["severity"] <= 1.0


# ---------------------------------------------------------------------------
# detect_billing_spikes
# ---------------------------------------------------------------------------

def test_spike_flags_known_violator(monthly):
    flags = detect_billing_spikes(monthly)
    npis = {r["npi"] for r in flags.collect()}
    assert SPIKE_NPI in npis


def test_spike_does_not_flag_clean(monthly):
    flags = detect_billing_spikes(monthly)
    npis = {r["npi"] for r in flags.collect()}
    assert CLEAN_NPI not in npis


def test_spike_severity_between_0_and_1(monthly):
    flags = detect_billing_spikes(monthly)
    for row in flags.collect():
        assert 0.0 <= row["severity"] <= 1.0


# ---------------------------------------------------------------------------
# detect_suspicious_consistency
# ---------------------------------------------------------------------------

def test_consistency_flags_known_violator(procedure):
    flags = detect_suspicious_consistency(procedure)
    npis = {r["npi"] for r in flags.collect()}
    assert CONSISTENCY_NPI in npis


def test_consistency_does_not_flag_clean(procedure):
    flags = detect_suspicious_consistency(procedure)
    npis = {r["npi"] for r in flags.collect()}
    assert CLEAN_NPI not in npis


def test_consistency_severity_between_0_and_1(procedure):
    flags = detect_suspicious_consistency(procedure)
    for row in flags.collect():
        assert 0.0 <= row["severity"] <= 1.0


# ---------------------------------------------------------------------------
# run_all_detectors (integration)
# ---------------------------------------------------------------------------

def test_run_all_returns_dataframe(monthly, procedure):
    results = run_all_detectors(monthly, procedure, threshold=0.0)
    assert results is not None


def test_run_all_scores_between_0_and_1(monthly, procedure):
    results = run_all_detectors(monthly, procedure, threshold=0.0)
    for row in results.collect():
        assert 0.0 <= row["overall_score"] <= 1.0


def test_run_all_threshold_filters(monthly, procedure):
    all_results = run_all_detectors(monthly, procedure, threshold=0.0)
    high_results = run_all_detectors(monthly, procedure, threshold=0.9)
    assert all_results.count() >= high_results.count()


def test_run_all_catches_all_suspicious_npis(monthly, procedure):
    results = run_all_detectors(monthly, procedure, threshold=0.0)
    result_npis = {r["npi"] for r in results.collect()}
    # Each known bad NPI should appear in results
    assert VOLUME_NPI in result_npis
    assert REVENUE_NPI in result_npis
    assert SPIKE_NPI in result_npis
    assert CONSISTENCY_NPI in result_npis


def test_run_all_clean_npi_below_threshold(monthly, procedure):
    results = run_all_detectors(monthly, procedure, threshold=0.3)
    result_npis = {r["npi"] for r in results.collect()}
    assert CLEAN_NPI not in result_npis


def test_run_all_result_has_expected_columns(monthly, procedure):
    results = run_all_detectors(monthly, procedure, threshold=0.0)
    cols = set(results.columns)
    assert "npi" in cols
    assert "overall_score" in cols
    assert "num_flags" in cols
    assert "flag_types" in cols
