from pathlib import Path

import click
import polars as pl

from data.loader import load_claims
from data.models import RedFlag, RedFlagType, ScanResult

# Thresholds — tune these based on real data
# Volume: max plausible claims per provider per month
MAX_CLAIMS_PER_MONTH = 1500
REVENUE_ZSCORE_THRESHOLD = 3.0  # Standard deviations above mean
SPIKE_MULTIPLIER = 5.0  # Monthly billing spike vs provider's own average
CONSISTENCY_RATIO_THRESHOLD = 0.9  # Suspicious if >90% of rows share same paid amount
CONSISTENCY_MIN_ROWS = 30  # Minimum rows to evaluate consistency


def scan_all(filepath: Path, threshold: float = 0.3) -> list[ScanResult]:
    """Scan the entire dataset and return providers with anomaly scores above threshold."""
    click.echo("Loading dataset...")
    lf = load_claims(filepath)

    click.echo("Running anomaly detection...")

    # --- Volume impossibility ---
    volume_flags = _detect_volume_impossibility(lf)

    # --- Revenue outliers ---
    revenue_flags = _detect_revenue_outliers(lf)

    # --- Billing spikes ---
    spike_flags = _detect_billing_spikes(lf)

    # --- Suspicious consistency ---
    consistency_flags = _detect_suspicious_consistency(lf)

    # Merge all flags by NPI
    all_npis = (set(volume_flags) | set(revenue_flags) | set(spike_flags)
                | set(consistency_flags))

    results = []
    for npi in all_npis:
        flags = []
        flags.extend(volume_flags.get(npi, []))
        flags.extend(revenue_flags.get(npi, []))
        flags.extend(spike_flags.get(npi, []))
        flags.extend(consistency_flags.get(npi, []))

        if not flags:
            continue

        overall_score = min(1.0, sum(f.severity for f in flags) / len(flags) + 0.1 * len(flags))

        result = ScanResult(
            npi=npi,
            provider_name="",
            overall_score=overall_score,
            red_flags=flags,
        )
        if result.overall_score >= threshold:
            results.append(result)

    results.sort(key=lambda r: r.overall_score, reverse=True)
    click.echo(f"Found {len(results)} suspicious providers above threshold {threshold}")
    return results


def _detect_volume_impossibility(lf: pl.LazyFrame) -> dict[str, list[RedFlag]]:
    """Flag providers with impossibly high claim counts in a single month."""
    monthly_counts = (
        lf.group_by(["npi", "service_month"])
        .agg(pl.col("total_claims").sum().alias("month_claims"))
        .filter(pl.col("month_claims") > MAX_CLAIMS_PER_MONTH)
        .collect()
    )

    flags: dict[str, list[RedFlag]] = {}
    for row in monthly_counts.iter_rows(named=True):
        npi = str(row["npi"])
        count = row["month_claims"]
        severity = min(1.0, count / (MAX_CLAIMS_PER_MONTH * 3))
        flag = RedFlag(
            flag_type=RedFlagType.VOLUME_IMPOSSIBILITY,
            description=f"{count:,} claims in {row['service_month']} (max plausible: {MAX_CLAIMS_PER_MONTH:,})",
            severity=severity,
            evidence={"month": str(row["service_month"]), "claims": count},
        )
        flags.setdefault(npi, []).append(flag)

    return flags


def _detect_revenue_outliers(lf: pl.LazyFrame) -> dict[str, list[RedFlag]]:
    """Flag providers whose total paid amount is far above peers."""
    provider_totals = (
        lf.group_by("npi")
        .agg(pl.col("total_paid").sum().alias("total_paid_sum"))
        .collect()
    )

    if provider_totals.is_empty():
        return {}

    mean_val = provider_totals["total_paid_sum"].mean()
    std_val = provider_totals["total_paid_sum"].std()

    if std_val is None or std_val == 0:
        return {}

    flags: dict[str, list[RedFlag]] = {}
    for row in provider_totals.iter_rows(named=True):
        zscore = (row["total_paid_sum"] - mean_val) / std_val
        if zscore > REVENUE_ZSCORE_THRESHOLD:
            npi = str(row["npi"])
            severity = min(1.0, zscore / 10.0)
            flag = RedFlag(
                flag_type=RedFlagType.REVENUE_OUTLIER,
                description=f"Total paid ${row['total_paid_sum']:,.2f} ({zscore:.1f} std devs above mean ${mean_val:,.2f})",
                severity=severity,
                evidence={"total_paid": row["total_paid_sum"], "zscore": round(zscore, 2)},
            )
            flags.setdefault(npi, []).append(flag)

    return flags


def _detect_billing_spikes(lf: pl.LazyFrame) -> dict[str, list[RedFlag]]:
    """Flag providers with sudden monthly billing spikes vs their own history."""
    monthly = (
        lf.group_by(["npi", "service_month"])
        .agg(pl.col("total_paid").sum().alias("monthly_total"))
        .collect()
    )

    flags: dict[str, list[RedFlag]] = {}

    for npi in monthly["npi"].unique().to_list():
        provider_monthly = monthly.filter(pl.col("npi") == npi).sort("service_month")
        if len(provider_monthly) < 3:
            continue

        totals = provider_monthly["monthly_total"].to_list()
        avg = sum(totals) / len(totals)
        if avg == 0:
            continue

        for row in provider_monthly.iter_rows(named=True):
            ratio = row["monthly_total"] / avg
            if ratio > SPIKE_MULTIPLIER:
                severity = min(1.0, ratio / 10.0)
                flag = RedFlag(
                    flag_type=RedFlagType.BILLING_SPIKE,
                    description=f"Monthly paid ${row['monthly_total']:,.2f} in {row['service_month']} is {ratio:.1f}x their average ${avg:,.2f}",
                    severity=severity,
                    evidence={"month": str(row["service_month"]), "amount": row["monthly_total"], "ratio": round(ratio, 2)},
                )
                flags.setdefault(str(npi), []).append(flag)

    return flags


def _detect_suspicious_consistency(lf: pl.LazyFrame) -> dict[str, list[RedFlag]]:
    """Flag providers where an unusually high fraction of rows share the same paid amount."""
    # For each provider, find total rows and the count of the most common total_paid value
    provider_stats = (
        lf.group_by(["npi", "total_paid"])
        .agg(pl.len().alias("amount_count"))
        .sort("amount_count", descending=True)
        .group_by("npi")
        .agg([
            pl.col("amount_count").sum().alias("total_rows"),
            pl.col("amount_count").first().alias("top_amount_count"),
            pl.col("total_paid").first().alias("top_amount"),
        ])
        .filter(pl.col("total_rows") >= CONSISTENCY_MIN_ROWS)
        .with_columns(
            (pl.col("top_amount_count") / pl.col("total_rows")).alias("consistency_ratio")
        )
        .filter(pl.col("consistency_ratio") > CONSISTENCY_RATIO_THRESHOLD)
        .collect()
    )

    flags: dict[str, list[RedFlag]] = {}
    for row in provider_stats.iter_rows(named=True):
        npi = str(row["npi"])
        ratio = row["consistency_ratio"]
        top_amount = row["top_amount"]
        total = row["total_rows"]
        severity = min(1.0, ratio)
        flag = RedFlag(
            flag_type=RedFlagType.SUSPICIOUS_CONSISTENCY,
            description=(
                f"{ratio:.0%} of {total} line items paid identical amount "
                f"${top_amount:,.2f} — suggests copy-paste billing"
            ),
            severity=severity,
            evidence={
                "consistency_ratio": round(ratio, 3),
                "top_amount": top_amount,
                "total_rows": total,
            },
        )
        flags.setdefault(npi, []).append(flag)

    return flags
