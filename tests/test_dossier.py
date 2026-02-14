"""Tests for dossier builder."""

from pathlib import Path

from data.models import Dossier, ScanResult
from profiler.dossier import build_dossier
from tests.conftest import CLEAN_NPI, VOLUME_NPI


def test_build_dossier_returns_dossier(sample_csv: Path):
    dossier = build_dossier(sample_csv, CLEAN_NPI)
    assert isinstance(dossier, Dossier)
    assert dossier.provider.npi == CLEAN_NPI


def test_dossier_has_claims_summary(sample_csv: Path):
    dossier = build_dossier(sample_csv, CLEAN_NPI)
    # Clean provider: 6 months * (30 + 15) = 270 total claims
    assert dossier.claims_summary["total_claims"] == 270
    assert "total_paid" in dossier.claims_summary


def test_dossier_has_peer_comparison(sample_csv: Path):
    dossier = build_dossier(sample_csv, CLEAN_NPI)
    assert "peer_count" in dossier.peer_comparison


def test_dossier_has_timeline(sample_csv: Path):
    dossier = build_dossier(sample_csv, CLEAN_NPI)
    assert len(dossier.timeline) > 0
    assert "month" in dossier.timeline[0]
    assert "total_paid" in dossier.timeline[0]


def test_dossier_with_scan_result(sample_csv: Path):
    sr = ScanResult(npi=VOLUME_NPI, provider_name="", overall_score=0.85)
    dossier = build_dossier(sample_csv, VOLUME_NPI, scan_result=sr)
    assert dossier.scan_result.overall_score == 0.85
