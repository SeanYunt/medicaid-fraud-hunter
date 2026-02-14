from pathlib import Path

import click
import polars as pl

from data.loader import load_claims
from data.models import RedFlag, RedFlagType, ScanResult

# Thresholds — tune these based on real data
MAX_PROCEDURES_PER_DAY = 50  # Physical impossibility threshold
REVENUE_ZSCORE_THRESHOLD = 3.0  # Standard deviations above mean
SPIKE_MULTIPLIER = 5.0  # Monthly billing spike vs provider's own average
WEEKEND_RATIO_THRESHOLD = 0.4  # Suspicious if >40% of claims on weekends
CONSISTENCY_RATIO_THRESHOLD = 0.9  # Suspicious if >90% of claims share same amount
CONSISTENCY_MIN_CLAIMS = 30  # Minimum claims to evaluate consistency


def scan_all(filepath: Path, threshold: float = 0.3) -> list[ScanResult]:
    """Scan the entire dataset and return providers with anomaly scores above threshold."""
    click.echo("Loading dataset...")
    lf = load_claims(filepath)

    click.echo("Running anomaly detection...")
    results = []

    # --- Volume impossibility ---
    volume_flags = _detect_volume_impossibility(lf)

    # --- Revenue outliers ---
    revenue_flags = _detect_revenue_outliers(lf)

    # --- Billing spikes ---
    spike_flags = _detect_billing_spikes(lf)

    # --- Weekend/after-hours ---
    weekend_flags = _detect_weekend_patterns(lf)

    # --- Suspicious consistency ---
    consistency_flags = _detect_suspicious_consistency(lf)

    # Merge all flags by NPI
    all_npis = (set(volume_flags) | set(revenue_flags) | set(spike_flags)
                | set(weekend_flags) | set(consistency_flags))

    for npi in all_npis:
        flags = []
        flags.extend(volume_flags.get(npi, []))
        flags.extend(revenue_flags.get(npi, []))
        flags.extend(spike_flags.get(npi, []))
        flags.extend(weekend_flags.get(npi, []))
        flags.extend(consistency_flags.get(npi, []))

        if not flags:
            continue

        overall_score = min(1.0, sum(f.severity for f in flags) / len(flags) + 0.1 * len(flags))

        result = ScanResult(
            npi=npi,
            provider_name="",  # populated later from data
            overall_score=overall_score,
            red_flags=flags,
        )
        if result.overall_score >= threshold:
            results.append(result)

    results.sort(key=lambda r: r.overall_score, reverse=True)
    click.echo(f"Found {len(results)} suspicious providers above threshold {threshold}")
    return results


def _detect_volume_impossibility(lf: pl.LazyFrame) -> dict[str, list[RedFlag]]:
    """Flag providers billing more procedures per day than physically possible."""
    names = lf.collect_schema().names()
    npi_col = "npi" if "npi" in names else "NPI"
    date_col = "service_date" if "service_date" in names else "SRVC_DT"

    daily_counts = (
        lf.group_by([npi_col, date_col])
        .agg(pl.len().alias("daily_count"))
        .filter(pl.col("daily_count") > MAX_PROCEDURES_PER_DAY)
        .collect()
    )

    flags: dict[str, list[RedFlag]] = {}
    for row in daily_counts.iter_rows(named=True):
        npi = str(row[npi_col])
        count = row["daily_count"]
        severity = min(1.0, count / (MAX_PROCEDURES_PER_DAY * 3))
        flag = RedFlag(
            flag_type=RedFlagType.VOLUME_IMPOSSIBILITY,
            description=f"Billed {count} procedures on {row[date_col]} (max plausible: {MAX_PROCEDURES_PER_DAY})",
            severity=severity,
            evidence={"date": str(row[date_col]), "count": count},
        )
        flags.setdefault(npi, []).append(flag)

    return flags


def _detect_revenue_outliers(lf: pl.LazyFrame) -> dict[str, list[RedFlag]]:
    """Flag providers whose total billing is far above peers."""
    names = lf.collect_schema().names()
    npi_col = "npi" if "npi" in names else "NPI"
    amount_col = "billed_amount" if "billed_amount" in names else "BILLED_AMT"

    provider_totals = (
        lf.group_by(npi_col)
        .agg(pl.col(amount_col).sum().alias("total_billed"))
        .collect()
    )

    if provider_totals.is_empty():
        return {}

    mean_val = provider_totals["total_billed"].mean()
    std_val = provider_totals["total_billed"].std()

    if std_val is None or std_val == 0:
        return {}

    flags: dict[str, list[RedFlag]] = {}
    for row in provider_totals.iter_rows(named=True):
        zscore = (row["total_billed"] - mean_val) / std_val
        if zscore > REVENUE_ZSCORE_THRESHOLD:
            npi = str(row[npi_col])
            severity = min(1.0, zscore / 10.0)
            flag = RedFlag(
                flag_type=RedFlagType.REVENUE_OUTLIER,
                description=f"Total billed ${row['total_billed']:,.2f} ({zscore:.1f} std devs above mean ${mean_val:,.2f})",
                severity=severity,
                evidence={"total_billed": row["total_billed"], "zscore": round(zscore, 2)},
            )
            flags.setdefault(npi, []).append(flag)

    return flags


def _detect_billing_spikes(lf: pl.LazyFrame) -> dict[str, list[RedFlag]]:
    """Flag providers with sudden monthly billing spikes vs their own history."""
    names = lf.collect_schema().names()
    npi_col = "npi" if "npi" in names else "NPI"
    date_col = "service_date" if "service_date" in names else "SRVC_DT"
    amount_col = "billed_amount" if "billed_amount" in names else "BILLED_AMT"

    monthly = (
        lf.with_columns(pl.col(date_col).cast(pl.Date).dt.truncate("1mo").alias("month"))
        .group_by([npi_col, "month"])
        .agg(pl.col(amount_col).sum().alias("monthly_total"))
        .collect()
    )

    flags: dict[str, list[RedFlag]] = {}

    for npi in monthly[npi_col].unique().to_list():
        provider_monthly = monthly.filter(pl.col(npi_col) == npi).sort("month")
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
                    description=f"Monthly billing of ${row['monthly_total']:,.2f} in {row['month']} is {ratio:.1f}x their average ${avg:,.2f}",
                    severity=severity,
                    evidence={"month": str(row["month"]), "amount": row["monthly_total"], "ratio": round(ratio, 2)},
                )
                flags.setdefault(str(npi), []).append(flag)

    return flags


def _detect_weekend_patterns(lf: pl.LazyFrame) -> dict[str, list[RedFlag]]:
    """Flag providers with unusually high weekend billing ratios."""
    names = lf.collect_schema().names()
    npi_col = "npi" if "npi" in names else "NPI"
    date_col = "service_date" if "service_date" in names else "SRVC_DT"

    with_dow = lf.with_columns(
        pl.col(date_col).cast(pl.Date).dt.weekday().alias("dow")
    )

    provider_weekend = (
        with_dow.group_by(npi_col)
        .agg([
            pl.len().alias("total_claims"),
            pl.col("dow").is_in([6, 7]).sum().alias("weekend_claims"),
        ])
        .with_columns(
            (pl.col("weekend_claims") / pl.col("total_claims")).alias("weekend_ratio")
        )
        .filter(pl.col("weekend_ratio") > WEEKEND_RATIO_THRESHOLD)
        .filter(pl.col("total_claims") > 20)  # ignore low-volume providers
        .collect()
    )

    flags: dict[str, list[RedFlag]] = {}
    for row in provider_weekend.iter_rows(named=True):
        npi = str(row[npi_col])
        ratio = row["weekend_ratio"]
        severity = min(1.0, ratio / 0.8)
        flag = RedFlag(
            flag_type=RedFlagType.WEEKEND_AFTERHOURS,
            description=f"{ratio:.0%} of {row['total_claims']} claims on weekends (expected ~28%)",
            severity=severity,
            evidence={"weekend_ratio": round(ratio, 3), "total_claims": row["total_claims"]},
        )
        flags.setdefault(npi, []).append(flag)

    return flags


def _detect_suspicious_consistency(lf: pl.LazyFrame) -> dict[str, list[RedFlag]]:
    """Flag providers where an unusually high fraction of claims share the same billed amount."""
    names = lf.collect_schema().names()
    npi_col = "npi" if "npi" in names else "NPI"
    amount_col = "billed_amount" if "billed_amount" in names else "BILLED_AMT"

    if amount_col not in names:
        return {}

    # For each provider, find total claims and the count of the most common amount
    provider_stats = (
        lf.group_by([npi_col, amount_col])
        .agg(pl.len().alias("amount_count"))
        .sort("amount_count", descending=True)
        .group_by(npi_col)
        .agg([
            pl.col("amount_count").sum().alias("total_claims"),
            pl.col("amount_count").first().alias("top_amount_count"),
            pl.col(amount_col).first().alias("top_amount"),
        ])
        .filter(pl.col("total_claims") >= CONSISTENCY_MIN_CLAIMS)
        .with_columns(
            (pl.col("top_amount_count") / pl.col("total_claims")).alias("consistency_ratio")
        )
        .filter(pl.col("consistency_ratio") > CONSISTENCY_RATIO_THRESHOLD)
        .collect()
    )

    flags: dict[str, list[RedFlag]] = {}
    for row in provider_stats.iter_rows(named=True):
        npi = str(row[npi_col])
        ratio = row["consistency_ratio"]
        top_amount = row["top_amount"]
        total = row["total_claims"]
        severity = min(1.0, ratio)
        flag = RedFlag(
            flag_type=RedFlagType.SUSPICIOUS_CONSISTENCY,
            description=(
                f"{ratio:.0%} of {total} claims billed at identical amount "
                f"${top_amount:,.2f} — suggests copy-paste billing"
            ),
            severity=severity,
            evidence={
                "consistency_ratio": round(ratio, 3),
                "top_amount": top_amount,
                "total_claims": total,
            },
        )
        flags.setdefault(npi, []).append(flag)

    return flags
