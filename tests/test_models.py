"""Tests for data models."""

from data.models import Dossier, Provider, RedFlag, RedFlagType, ScanResult


def test_red_flag_type_values():
    assert RedFlagType.VOLUME_IMPOSSIBILITY.value == "volume_impossibility"
    assert RedFlagType.SUSPICIOUS_CONSISTENCY.value == "suspicious_consistency"
    assert len(RedFlagType) == 4


def test_provider_defaults():
    p = Provider(npi="123")
    assert p.billing_npi == ""
    assert p.servicing_npi == ""


def test_scan_result_defaults():
    r = ScanResult(npi="123", provider_name="", overall_score=0.5)
    assert r.red_flags == []
    assert r.total_billed == 0.0


def test_dossier_construction():
    p = Provider(npi="123")
    sr = ScanResult(npi="123", provider_name="", overall_score=0.8)
    d = Dossier(provider=p, scan_result=sr)
    assert d.claims_summary == {}
    assert d.timeline == []
