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

# NOS/miscellaneous HCPCS codes — vague codes that obscure what was actually billed
NOS_CODES = {
    "B9998",  # Enteral supplies, not otherwise classified
    "E1399",  # Durable medical equipment, miscellaneous
    "A9999",  # Miscellaneous DME supply or accessory, NOS
    "A9270",  # Non-covered item or service
    "K0108",  # Wheelchair component or accessory, NOS
    "L9900",  # Orthotic/prosthetic supply, accessory, NOS
    "S9999",  # Services, not otherwise classified
    "T9999",  # Not otherwise classified
}
NOS_CONCENTRATION_THRESHOLD = 0.25  # Flag if >25% of total paid is under NOS codes

# E&M office visit codes by reimbursement level (1=lowest, 5=highest)
EM_CODES = {"99211": 1, "99212": 2, "99213": 3, "99214": 4, "99215": 5}
UPCODING_SHIFT_THRESHOLD = 0.4   # Avg code level shift > 0.4 between early/late periods
UPCODING_MIN_CLAIMS = 50         # Minimum E&M claims to evaluate

# Flag types that map to a specific named scheme (vs general statistical outliers)
SCHEME_FLAG_TYPES = {RedFlagType.NOS_CODE_CONCENTRATION, RedFlagType.UPCODING_TRAJECTORY}


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
    code_df: pl.DataFrame | None = None

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
        click.echo("Aggregating procedure code data...")
        code_df = (
            lf.group_by(["npi", "procedure_code", "service_month"])
            .agg([
                pl.col("total_claims").sum(),
                pl.col("total_paid").sum(),
            ])
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

    # --- Scheme-specific detectors (require raw code-level data) ---
    if code_df is not None:
        nos_flags = _detect_nos_concentration(code_df)
        upcoding_flags = _detect_upcoding_trajectory(code_df)
    else:
        nos_flags = {}
        upcoding_flags = {}

    # Merge all flags by NPI
    all_npis = (set(volume_flags) | set(revenue_flags) | set(spike_flags)
                | set(consistency_flags) | set(nos_flags) | set(upcoding_flags))

    results = []
    for npi in all_npis:
        flags = []
        flags.extend(volume_flags.get(npi, []))
        flags.extend(revenue_flags.get(npi, []))
        flags.extend(spike_flags.get(npi, []))
        flags.extend(consistency_flags.get(npi, []))
        flags.extend(nos_flags.get(npi, []))
        flags.extend(upcoding_flags.get(npi, []))

        if not flags:
            continue

        # Scoring weights scheme-specific flags (legally actionable patterns)
        # more heavily than general statistical outliers.
        max_severity = max(f.severity for f in flags)
        distinct_types = len({f.flag_type for f in flags})
        scheme_types = len({f.flag_type for f in flags if f.flag_type in SCHEME_FLAG_TYPES})
        overall_score = min(1.0, max_severity * 0.4 + distinct_types * 0.15 + scheme_types * 0.2)

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


def _detect_nos_concentration(code_df: pl.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers where NOS/miscellaneous codes make up a large share of billing.

    NOS codes lack specificity and are a classic vehicle for DME and supply fraud —
    they obscure what was actually billed and are harder to audit than named items.
    """
    provider_totals = (
        code_df.group_by("npi")
        .agg(pl.col("total_paid").sum().alias("total_paid_all"))
    )
    nos_totals = (
        code_df.filter(pl.col("procedure_code").is_in(NOS_CODES))
        .group_by("npi")
        .agg(pl.col("total_paid").sum().alias("nos_paid"))
    )
    merged = (
        provider_totals.join(nos_totals, on="npi", how="inner")
        .with_columns(
            (pl.col("nos_paid") / pl.col("total_paid_all")).alias("nos_ratio")
        )
        .filter(pl.col("nos_ratio") >= NOS_CONCENTRATION_THRESHOLD)
    )

    flags: dict[str, list[RedFlag]] = {}
    for row in merged.iter_rows(named=True):
        npi = str(row["npi"])
        ratio = row["nos_ratio"]
        nos_paid = row["nos_paid"]
        severity = min(1.0, ratio / 0.5)  # 50%+ NOS = max severity
        flag = RedFlag(
            flag_type=RedFlagType.NOS_CODE_CONCENTRATION,
            description=(
                f"{ratio:.0%} of billing (${nos_paid:,.0f}) under miscellaneous/unclassified "
                f"codes — vague codes obscure what was actually provided"
            ),
            severity=severity,
            evidence={
                "nos_ratio": round(ratio, 3),
                "nos_paid": round(nos_paid, 2),
                "total_paid": round(row["total_paid_all"], 2),
            },
        )
        flags.setdefault(npi, []).append(flag)

    return flags


def _detect_upcoding_trajectory(code_df: pl.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers with a systematic shift toward higher-reimbursed E&M codes over time.

    Compares the weighted average E&M code level in the first half of the provider's
    billing history vs the second half. A significant upward shift (with no change in
    patient volume) is a signature of deliberate upcoding rather than clinical change.
    """
    em_df = code_df.filter(pl.col("procedure_code").is_in(set(EM_CODES.keys())))

    if em_df.is_empty():
        return {}

    em_df = em_df.with_columns(
        pl.col("procedure_code")
        .map_elements(lambda c: EM_CODES.get(c, 0), return_dtype=pl.Int32)
        .alias("code_level")
    )

    flags: dict[str, list[RedFlag]] = {}
    for npi in em_df["npi"].unique().to_list():
        provider_em = em_df.filter(pl.col("npi") == npi).sort("service_month")

        total_claims = provider_em["total_claims"].sum()
        if total_claims < UPCODING_MIN_CLAIMS:
            continue

        months = provider_em["service_month"].unique().sort().to_list()
        if len(months) < 6:
            continue

        mid = len(months) // 2
        early = provider_em.filter(pl.col("service_month").is_in(months[:mid]))
        late = provider_em.filter(pl.col("service_month").is_in(months[mid:]))

        def weighted_avg(df: pl.DataFrame) -> float:
            total = df["total_claims"].sum()
            if total == 0:
                return 0.0
            return float((df["code_level"] * df["total_claims"]).sum()) / total

        early_avg = weighted_avg(early)
        late_avg = weighted_avg(late)
        shift = late_avg - early_avg

        if shift >= UPCODING_SHIFT_THRESHOLD:
            severity = min(1.0, shift / 1.5)
            total_paid = float(provider_em["total_paid"].sum())
            flag = RedFlag(
                flag_type=RedFlagType.UPCODING_TRAJECTORY,
                description=(
                    f"E&M code level shifted +{shift:.2f} points over time "
                    f"({early_avg:.2f} → {late_avg:.2f}) — systematic upgrade toward "
                    f"higher-reimbursed codes across {int(total_claims):,} claims"
                ),
                severity=severity,
                evidence={
                    "early_avg_level": round(early_avg, 2),
                    "late_avg_level": round(late_avg, 2),
                    "shift": round(shift, 2),
                    "total_em_claims": int(total_claims),
                    "total_em_paid": round(total_paid, 2),
                },
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
