"""Tests for PDF report generation."""

from pathlib import Path

from data.models import Dossier, Provider, RedFlag, RedFlagType, ScanResult
from reports.pdf import generate_dossier_pdf


def test_generate_pdf_creates_file(tmp_path: Path):
    provider = Provider(npi="1234567890", name="Dr. Test", specialty="Cardiology",
                        state="TX", city="Houston", zip_code="77001")
    scan_result = ScanResult(
        npi="1234567890",
        provider_name="Dr. Test",
        overall_score=0.75,
        red_flags=[
            RedFlag(
                flag_type=RedFlagType.VOLUME_IMPOSSIBILITY,
                description="Billed 80 procedures on 2024-01-15",
                severity=0.8,
                evidence={"date": "2024-01-15", "count": 80},
            ),
        ],
    )
    dossier = Dossier(
        provider=provider,
        scan_result=scan_result,
        claims_summary={
            "total_claims": 500,
            "total_billed": 125000.00,
            "total_paid": 100000.00,
        },
        peer_comparison={
            "specialty": "Cardiology",
            "peer_count": 200,
            "provider_total_billed": 125000.00,
            "peer_mean_billed": 50000.00,
            "peer_median_billed": 45000.00,
            "provider_percentile": 95.0,
            "zscore": 3.2,
        },
        timeline=[
            {"month": "2024-01-01", "claim_count": 50, "total_billed": 12500.00},
            {"month": "2024-02-01", "claim_count": 45, "total_billed": 11250.00},
        ],
    )

    pdf_path = generate_dossier_pdf(dossier, output_dir=tmp_path)
    assert pdf_path.exists()
    assert pdf_path.suffix == ".pdf"
    assert pdf_path.stat().st_size > 0


def test_generate_pdf_minimal_dossier(tmp_path: Path):
    """PDF generation should work even with minimal data."""
    provider = Provider(npi="0000000000")
    scan_result = ScanResult(npi="0000000000", provider_name="", overall_score=0.0)
    dossier = Dossier(provider=provider, scan_result=scan_result)

    pdf_path = generate_dossier_pdf(dossier, output_dir=tmp_path)
    assert pdf_path.exists()
    assert pdf_path.stat().st_size > 0
