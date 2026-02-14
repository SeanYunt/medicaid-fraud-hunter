"""Shared fixtures: synthetic Medicaid claims CSV with known provider profiles."""

import csv
from datetime import date, timedelta
from pathlib import Path

import pytest


# Provider NPIs
CLEAN_NPI = "1000000001"
VOLUME_NPI = "2000000002"       # >50 procedures in a single day
REVENUE_NPI = "3000000003"      # total billing far above peers
SPIKE_NPI = "4000000004"        # one month is 10x the others
WEEKEND_NPI = "5000000005"      # nearly all claims on weekends
CONSISTENCY_NPI = "6000000006"  # all claims same dollar amount


def _weekday(start: date, offset: int) -> date:
    """Return a weekday date offset from start."""
    d = start + timedelta(days=offset)
    while d.weekday() >= 5:  # skip Sat/Sun
        d += timedelta(days=1)
    return d


def _saturday(start: date, offset: int) -> date:
    """Return a Saturday near start + offset days."""
    d = start + timedelta(days=offset)
    d += timedelta(days=(5 - d.weekday()) % 7)  # next Saturday
    return d


def _generate_rows() -> list[dict]:
    """Build synthetic claims rows for each provider archetype."""
    rows = []
    base = date(2024, 1, 1)
    claim_id = 1

    def add(npi, svc_date, amount, proc="99213", specialty="Internal Medicine"):
        nonlocal claim_id
        rows.append({
            "CLM_ID": str(claim_id),
            "NPI": npi,
            "PROVIDER_NAME": f"Provider {npi}",
            "SPECIALTY": specialty,
            "STATE": "TX",
            "CITY": "Houston",
            "ZIP": "77001",
            "SRVC_DT": svc_date.isoformat(),
            "PROC_CD": proc,
            "DIAG_CD": "Z00.0",
            "BILLED_AMT": f"{amount:.2f}",
            "PAID_AMT": f"{amount * 0.8:.2f}",
            "POS": "11",
            "UNITS": "1",
        })
        claim_id += 1

    # --- Clean provider: 30 normal claims spread over weekdays ---
    for i in range(30):
        add(CLEAN_NPI, _weekday(base, i * 2), 100 + (i % 5) * 20)

    # --- Volume abuser: 60 claims on a single day ---
    single_day = _weekday(base, 10)
    for i in range(60):
        add(VOLUME_NPI, single_day, 150, proc="99214")
    # plus some normal days so the provider exists in the dataset
    for i in range(10):
        add(VOLUME_NPI, _weekday(base, 30 + i * 3), 150)

    # --- Revenue outlier: huge billed amounts (must be >3 std devs above mean) ---
    for i in range(35):
        add(REVENUE_NPI, _weekday(base, i * 2), 200_000)

    # --- Spike provider: several normal months then one extreme spike ---
    # 6 normal months of $200/claim
    for month_offset in range(6):
        month_base = date(2024, 1 + month_offset, 5)
        for i in range(10):
            add(SPIKE_NPI, _weekday(month_base, i * 2), 200)
    # 1 spike month at $6000/claim (~30x a normal month)
    spike_month = date(2024, 7, 5)
    for i in range(10):
        add(SPIKE_NPI, _weekday(spike_month, i * 2), 6000)

    # --- Weekend provider: >90% claims on Saturdays ---
    for i in range(35):
        add(WEEKEND_NPI, _saturday(base, i * 7), 300, proc="99215")
    # a couple weekday claims so ratio isn't exactly 100%
    for i in range(3):
        add(WEEKEND_NPI, _weekday(base, i * 10), 300)

    # --- Consistency provider: all claims identical amount ---
    for i in range(40):
        add(CONSISTENCY_NPI, _weekday(base, i), 99.99, proc="99211")

    # --- Filler providers: 20 normal providers so z-score stats are meaningful ---
    for p in range(20):
        filler_npi = f"99000000{p:02d}"
        for i in range(15):
            add(filler_npi, _weekday(base, i * 3), 80 + (p % 10) * 15)

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
