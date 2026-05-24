"""Shared fixtures: synthetic Medicaid claims CSV matching HHS schema."""

import csv
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from data.loader import load_claims


# Provider NPIs
CLEAN_NPI = "1000000001"
VOLUME_NPI = "2000000002"       # impossibly high monthly claims
REVENUE_NPI = "3000000003"      # total paid far above peers
SPIKE_NPI = "4000000004"        # one month is 10x the others
CONSISTENCY_NPI = "6000000006"  # all rows same paid amount

# HHS schema: each row is (provider, procedure, month) aggregation
# Columns: BILLING_PROVIDER_NPI_NUM, SERVICING_PROVIDER_NPI_NUM, HCPCS_CODE,
#           CLAIM_FROM_MONTH, TOTAL_UNIQUE_BENEFICIARIES, TOTAL_CLAIMS, TOTAL_PAID


def _month(year: int, month: int) -> str:
    return date(year, month, 1).isoformat()


def _generate_rows() -> list[dict]:
    """Build synthetic rows matching HHS Medicaid Provider Spending schema."""
    rows = []

    def add(npi, hcpcs, month, beneficiaries, claims, paid):
        rows.append({
            "BILLING_PROVIDER_NPI_NUM": npi,
            "SERVICING_PROVIDER_NPI_NUM": npi,
            "HCPCS_CODE": hcpcs,
            "CLAIM_FROM_MONTH": month,
            "TOTAL_UNIQUE_BENEFICIARIES": beneficiaries,
            "TOTAL_CLAIMS": claims,
            "TOTAL_PAID": f"{paid:.2f}",
        })

    # --- Clean provider: normal billing across 6 months ---
    for m in range(1, 7):
        add(CLEAN_NPI, "99213", _month(2024, m), 20, 30, 3000.00)
        add(CLEAN_NPI, "99214", _month(2024, m), 10, 15, 2000.00)

    # --- Volume abuser: impossibly high claims in one month ---
    for m in range(1, 7):
        add(VOLUME_NPI, "99214", _month(2024, m), 50, 100, 10000.00)
    # One month with 5000 claims (way above MAX_CLAIMS_PER_MONTH=1500)
    add(VOLUME_NPI, "99214", _month(2024, 7), 200, 5000, 500000.00)

    # --- Revenue outlier: total paid far above all peers ---
    for m in range(1, 7):
        add(REVENUE_NPI, "99215", _month(2024, m), 100, 200, 2_000_000.00)

    # --- Spike provider: 6 normal months, 1 extreme spike ---
    for m in range(1, 7):
        add(SPIKE_NPI, "99213", _month(2024, m), 15, 25, 5000.00)
    # Spike month: ~20x normal; total paid = 6*5000 + 100000 = 130000 (above MIN_TOTAL_PAID)
    add(SPIKE_NPI, "99213", _month(2024, 7), 200, 500, 100000.00)

    # --- Consistency provider: one dominant code (99214) at ~81% of billing,
    #     billed at an identical per-claim rate every month (CV = 0%) ---
    # Total: 6*(15000+3500) = 111000 > MIN_TOTAL_PAID=100000; dominance=90000/111000=81%>=70%
    for m in range(1, 7):
        add(CONSISTENCY_NPI, "99214", _month(2024, m), 80, 150, 15000.00)  # $100/claim, CV=0%
        add(CONSISTENCY_NPI, "99213", _month(2024, m), 20, 30, 3500.00)    # filler ~19%

    # --- Filler providers: 20 normal providers so z-score stats are meaningful ---
    # Each filler: 6 months * ~20000-38000 = 120000-228000 (above MIN_TOTAL_PAID)
    for p in range(20):
        filler_npi = f"99000000{p:02d}"
        for m in range(1, 7):
            add(filler_npi, "99213", _month(2024, m), 10, 20, 20000 + (p % 10) * 2000)

    return rows


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """Write synthetic claims CSV and return its path."""
    filepath = tmp_path / "test_claims.csv"
    rows = _generate_rows()
    fieldnames = list(rows[0].keys())
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return filepath


@pytest.fixture
def monthly_df(sample_csv: Path) -> pd.DataFrame:
    """Provider+month aggregation matching the preprocessed monthly file."""
    df = load_claims(sample_csv)
    return (
        df.groupby(["npi", "service_month"], as_index=False)
        .agg(total_claims=("total_claims", "sum"), total_paid=("total_paid", "sum"))
    )


@pytest.fixture
def procedure_df(sample_csv: Path) -> pd.DataFrame:
    """Provider+procedure+month aggregation matching the preprocessed procedure file."""
    df = load_claims(sample_csv)
    return (
        df.groupby(["npi", "procedure_code", "service_month"], as_index=False)
        .agg(total_claims=("total_claims", "sum"), total_paid=("total_paid", "sum"))
    )
