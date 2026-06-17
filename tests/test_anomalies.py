"""Tests for anomaly detection — each detector should fire on the right provider."""

from pathlib import Path

import pandas as pd

from data.models import RedFlagType
from scanner.anomalies import (
    _detect_volume_impossibility,
    _detect_revenue_outliers,
    _detect_billing_spikes,
    _detect_suspicious_consistency,
    scan_all,
)
from tests.conftest import (
    CLEAN_NPI,
    VOLUME_NPI,
    REVENUE_NPI,
    SPIKE_NPI,
    CONSISTENCY_NPI,
)


def test_volume_impossibility_flags_volume_provider(monthly_df: pd.DataFrame):
    flags = _detect_volume_impossibility(monthly_df)
    assert VOLUME_NPI in flags
    assert all(f.flag_type == RedFlagType.VOLUME_IMPOSSIBILITY for f in flags[VOLUME_NPI])


def test_volume_impossibility_skips_clean_provider(monthly_df: pd.DataFrame):
    flags = _detect_volume_impossibility(monthly_df)
    assert CLEAN_NPI not in flags


def test_volume_impossibility_skips_nppes_org_npis(monthly_df: pd.DataFrame):
    """NPIs in org_npis are excluded even when they exceed the volume threshold."""
    # VOLUME_NPI is flagged without the exclusion set
    assert VOLUME_NPI in _detect_volume_impossibility(monthly_df)
    # Passing it as an org NPI suppresses the flag
    flags = _detect_volume_impossibility(monthly_df, org_npis={VOLUME_NPI})
    assert VOLUME_NPI not in flags


def test_volume_impossibility_skips_org_billing_code_providers(monthly_df: pd.DataFrame):
    """A provider whose top code is in ORG_BILLING_CODES is excluded via code heuristic."""
    # H0043 (ACT per diem) is an org billing code — dominates VOLUME_NPI here
    org_code_df = pd.DataFrame([
        {"npi": VOLUME_NPI, "procedure_code": "H0043", "service_month": "2024-07-01",
         "total_claims": 6000, "total_paid": 120_000.0},
        {"npi": VOLUME_NPI, "procedure_code": "99214", "service_month": "2024-07-01",
         "total_claims": 100, "total_paid": 10_000.0},
    ])
    flags = _detect_volume_impossibility(monthly_df, code_df=org_code_df)
    assert VOLUME_NPI not in flags


def test_volume_impossibility_flags_when_org_code_not_dominant(monthly_df: pd.DataFrame):
    """Provider is still flagged when an org code is present but not the top code."""
    code_df = pd.DataFrame([
        {"npi": VOLUME_NPI, "procedure_code": "99214", "service_month": "2024-07-01",
         "total_claims": 5000, "total_paid": 500_000.0},
        {"npi": VOLUME_NPI, "procedure_code": "H0043", "service_month": "2024-07-01",
         "total_claims": 50, "total_paid": 1_000.0},
    ])
    flags = _detect_volume_impossibility(monthly_df, code_df=code_df)
    assert VOLUME_NPI in flags


def test_revenue_outlier_flags_revenue_provider(monthly_df: pd.DataFrame):
    flags = _detect_revenue_outliers(monthly_df)
    assert REVENUE_NPI in flags
    assert all(f.flag_type == RedFlagType.REVENUE_OUTLIER for f in flags[REVENUE_NPI])


def test_revenue_outlier_skips_clean_provider(monthly_df: pd.DataFrame):
    flags = _detect_revenue_outliers(monthly_df)
    assert CLEAN_NPI not in flags


def test_billing_spike_flags_spike_provider(monthly_df: pd.DataFrame):
    flags = _detect_billing_spikes(monthly_df)
    assert SPIKE_NPI in flags
    assert all(f.flag_type == RedFlagType.BILLING_SPIKE for f in flags[SPIKE_NPI])


def test_billing_spike_skips_clean_provider(monthly_df: pd.DataFrame):
    flags = _detect_billing_spikes(monthly_df)
    assert CLEAN_NPI not in flags


def test_suspicious_consistency_flags_consistency_provider(procedure_df: pd.DataFrame):
    flags = _detect_suspicious_consistency(procedure_df)
    assert CONSISTENCY_NPI in flags
    assert all(f.flag_type == RedFlagType.SUSPICIOUS_CONSISTENCY for f in flags[CONSISTENCY_NPI])


def test_suspicious_consistency_skips_clean_provider(procedure_df: pd.DataFrame):
    flags = _detect_suspicious_consistency(procedure_df)
    assert CLEAN_NPI not in flags


def test_scan_all_returns_sorted_results(sample_csv: Path):
    results = scan_all(sample_csv, threshold=0.0)
    assert len(results) > 0
    scores = [r.overall_score for r in results]
    assert scores == sorted(scores, reverse=True)


def test_scan_all_threshold_filters(sample_csv: Path):
    all_results = scan_all(sample_csv, threshold=0.0)
    high_results = scan_all(sample_csv, threshold=0.9)
    assert len(high_results) <= len(all_results)
    for r in high_results:
        assert r.overall_score >= 0.9


def test_clean_provider_not_in_scan_results(sample_csv: Path):
    results = scan_all(sample_csv, threshold=0.0)
    npis = [r.npi for r in results]
    assert CLEAN_NPI not in npis
