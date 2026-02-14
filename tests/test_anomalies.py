"""Tests for anomaly detection — each detector should fire on the right provider."""

from pathlib import Path

from data.loader import load_claims
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


def test_volume_impossibility_flags_volume_provider(sample_csv: Path):
    lf = load_claims(sample_csv)
    flags = _detect_volume_impossibility(lf)
    assert VOLUME_NPI in flags
    assert all(f.flag_type == RedFlagType.VOLUME_IMPOSSIBILITY for f in flags[VOLUME_NPI])


def test_volume_impossibility_skips_clean_provider(sample_csv: Path):
    lf = load_claims(sample_csv)
    flags = _detect_volume_impossibility(lf)
    assert CLEAN_NPI not in flags


def test_revenue_outlier_flags_revenue_provider(sample_csv: Path):
    lf = load_claims(sample_csv)
    flags = _detect_revenue_outliers(lf)
    assert REVENUE_NPI in flags
    assert all(f.flag_type == RedFlagType.REVENUE_OUTLIER for f in flags[REVENUE_NPI])


def test_revenue_outlier_skips_clean_provider(sample_csv: Path):
    lf = load_claims(sample_csv)
    flags = _detect_revenue_outliers(lf)
    assert CLEAN_NPI not in flags


def test_billing_spike_flags_spike_provider(sample_csv: Path):
    lf = load_claims(sample_csv)
    flags = _detect_billing_spikes(lf)
    assert SPIKE_NPI in flags
    assert all(f.flag_type == RedFlagType.BILLING_SPIKE for f in flags[SPIKE_NPI])


def test_billing_spike_skips_clean_provider(sample_csv: Path):
    lf = load_claims(sample_csv)
    flags = _detect_billing_spikes(lf)
    assert CLEAN_NPI not in flags


def test_suspicious_consistency_flags_consistency_provider(sample_csv: Path):
    lf = load_claims(sample_csv)
    flags = _detect_suspicious_consistency(lf)
    assert CONSISTENCY_NPI in flags
    assert all(f.flag_type == RedFlagType.SUSPICIOUS_CONSISTENCY for f in flags[CONSISTENCY_NPI])


def test_suspicious_consistency_skips_clean_provider(sample_csv: Path):
    lf = load_claims(sample_csv)
    flags = _detect_suspicious_consistency(lf)
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
