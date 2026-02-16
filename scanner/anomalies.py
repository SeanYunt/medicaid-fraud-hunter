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
MIN_TOTAL_PAID = 100_000  # Ignore providers below this total — too small for viable qui tam case


def scan_all(
    filepath: Path,
    threshold: float = 0.3,
    monthly_path: Path | None = None,
    procedure_path: Path | None = None,
) -> list[ScanResult]:
    """Scan the dataset and return providers with anomaly scores above threshold.

    If monthly_path and procedure_path are provided, uses the small preprocessed
    files instead of reading the full raw dataset.
    """
    if monthly_path and procedure_path:
        click.echo("Loading preprocessed summaries...")
        monthly_df = pl.read_parquet(monthly_path)
        procedure_df = pl.read_parquet(procedure_path)
    else:
        click.echo("Loading raw dataset (consider running 'preprocess' first)...")
        lf = load_claims(filepath)
        click.echo("Aggregating monthly data...")
        monthly_df = (
            lf.group_by(["npi", "service_month"])
            .agg([
                pl.col("total_claims").sum().alias("total_claims"),
                pl.col("total_paid").sum().alias("total_paid"),
            ])
            .collect()
        )
        click.echo("Aggregating procedure data...")
        procedure_df = (
            lf.group_by(["npi", "total_paid"])
            .agg(pl.len().alias("row_count"))
            .collect()
        )

    # Filter out providers with total paid below minimum threshold
    provider_totals = (
        monthly_df.group_by("npi")
        .agg(pl.col("total_paid").sum().alias("total_paid_sum"))
    )
    qualifying_npis = (
        provider_totals.filter(pl.col("total_paid_sum") >= MIN_TOTAL_PAID)["npi"].to_list()
    )
    excluded = provider_totals.height - len(qualifying_npis)
    monthly_df = monthly_df.filter(pl.col("npi").is_in(qualifying_npis))
    procedure_df = procedure_df.filter(pl.col("npi").is_in(qualifying_npis))
    click.echo(f"Filtered to {len(qualifying_npis):,} providers with >=${MIN_TOTAL_PAID:,} total paid "
               f"({excluded:,} excluded)")

    click.echo("Running anomaly detection...")

    # --- Volume impossibility ---
    volume_flags = _detect_volume_impossibility(monthly_df)

    # --- Revenue outliers ---
    revenue_flags = _detect_revenue_outliers(monthly_df)

    # --- Billing spikes ---
    spike_flags = _detect_billing_spikes(monthly_df)

    # --- Suspicious consistency ---
    consistency_flags = _detect_suspicious_consistency(procedure_df)

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

        # Score based on corroborating evidence: distinct detector types matter
        # more than repeated flags from the same detector.
        max_severity = max(f.severity for f in flags)
        distinct_types = len({f.flag_type for f in flags})
        overall_score = min(1.0, max_severity * 0.5 + distinct_types * 0.2)

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


def _detect_volume_impossibility(monthly_df: pl.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers with impossibly high claim counts in a single month."""
    flagged = monthly_df.filter(pl.col("total_claims") > MAX_CLAIMS_PER_MONTH)

    flags: dict[str, list[RedFlag]] = {}
    for row in flagged.iter_rows(named=True):
        npi = str(row["npi"])
        count = row["total_claims"]
        severity = min(1.0, count / (MAX_CLAIMS_PER_MONTH * 3))
        flag = RedFlag(
            flag_type=RedFlagType.VOLUME_IMPOSSIBILITY,
            description=f"{count:,} claims in {row['service_month']} (max plausible: {MAX_CLAIMS_PER_MONTH:,})",
            severity=severity,
            evidence={"month": str(row["service_month"]), "claims": count},
        )
        flags.setdefault(npi, []).append(flag)

    return flags


def _detect_revenue_outliers(monthly_df: pl.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers whose revenue per claim is far above peers.

    Uses median and MAD (median absolute deviation) instead of mean/std
    to resist skew from large providers distorting the baseline.
    """
    provider_totals = (
        monthly_df.group_by("npi")
        .agg([
            pl.col("total_paid").sum().alias("total_paid_sum"),
            pl.col("total_claims").sum().alias("total_claims_sum"),
        ])
        .filter(pl.col("total_claims_sum") > 0)
        .with_columns(
            (pl.col("total_paid_sum") / pl.col("total_claims_sum")).alias("paid_per_claim")
        )
    )

    if provider_totals.is_empty():
        return {}

    median_val = provider_totals["paid_per_claim"].median()
    # MAD = median of absolute deviations from the median
    mad_val = (provider_totals["paid_per_claim"] - median_val).abs().median()

    if mad_val is None or mad_val == 0:
        return {}

    # Scale MAD to be comparable to std dev for normal distributions
    # (1.4826 is the consistency constant for normal distributions)
    scaled_mad = mad_val * 1.4826

    flags: dict[str, list[RedFlag]] = {}
    for row in provider_totals.iter_rows(named=True):
        modified_zscore = (row["paid_per_claim"] - median_val) / scaled_mad
        if modified_zscore > REVENUE_ZSCORE_THRESHOLD:
            npi = str(row["npi"])
            severity = min(1.0, modified_zscore / 10.0)
            flag = RedFlag(
                flag_type=RedFlagType.REVENUE_OUTLIER,
                description=(
                    f"Revenue per claim ${row['paid_per_claim']:,.2f} "
                    f"({modified_zscore:.1f} MADs above median ${median_val:,.2f}/claim)"
                ),
                severity=severity,
                evidence={
                    "paid_per_claim": round(row["paid_per_claim"], 2),
                    "total_paid": row["total_paid_sum"],
                    "total_claims": row["total_claims_sum"],
                    "modified_zscore": round(modified_zscore, 2),
                },
            )
            flags.setdefault(npi, []).append(flag)

    return flags


def _detect_billing_spikes(monthly_df: pl.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers with sudden monthly billing spikes vs their own history."""
    flags: dict[str, list[RedFlag]] = {}

    for npi in monthly_df["npi"].unique().to_list():
        provider_monthly = monthly_df.filter(pl.col("npi") == npi).sort("service_month")
        if len(provider_monthly) < 3:
            continue

        totals = provider_monthly["total_paid"].to_list()
        avg = sum(totals) / len(totals)
        if avg == 0:
            continue

        for row in provider_monthly.iter_rows(named=True):
            ratio = row["total_paid"] / avg
            if ratio > SPIKE_MULTIPLIER:
                severity = min(1.0, ratio / 10.0)
                flag = RedFlag(
                    flag_type=RedFlagType.BILLING_SPIKE,
                    description=f"Monthly paid ${row['total_paid']:,.2f} in {row['service_month']} is {ratio:.1f}x their average ${avg:,.2f}",
                    severity=severity,
                    evidence={"month": str(row["service_month"]), "amount": row["total_paid"], "ratio": round(ratio, 2)},
                )
                flags.setdefault(str(npi), []).append(flag)

    return flags


def _detect_suspicious_consistency(procedure_df: pl.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers where an unusually high fraction of rows share the same paid amount."""
    # Exclude $0 rows — uniform zeros are a data artifact, not copy-paste fraud
    procedure_df = procedure_df.filter(pl.col("total_paid") != 0)

    provider_stats = (
        procedure_df
        .sort("row_count", descending=True)
        .group_by("npi")
        .agg([
            pl.col("row_count").sum().alias("total_rows"),
            pl.col("row_count").first().alias("top_amount_count"),
            pl.col("total_paid").first().alias("top_amount"),
        ])
        .filter(pl.col("total_rows") >= CONSISTENCY_MIN_ROWS)
        .with_columns(
            (pl.col("top_amount_count") / pl.col("total_rows")).alias("consistency_ratio")
        )
        .filter(pl.col("consistency_ratio") > CONSISTENCY_RATIO_THRESHOLD)
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
