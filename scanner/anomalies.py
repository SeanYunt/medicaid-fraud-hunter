from pathlib import Path

import click
import pandas as pd

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
    code_df: pd.DataFrame | None = None

    if monthly_path and procedure_path:
        click.echo("Loading preprocessed summaries...")
        monthly_df = pd.read_parquet(monthly_path, engine="pyarrow")
        procedure_df = pd.read_parquet(procedure_path, engine="pyarrow")
    else:
        click.echo("Loading raw dataset (consider running 'preprocess' first)...")
        df = load_claims(filepath)
        click.echo("Aggregating monthly data...")
        monthly_df = (
            df.groupby(["npi", "service_month"], as_index=False)
            .agg(total_claims=("total_claims", "sum"), total_paid=("total_paid", "sum"))
        )
        click.echo("Aggregating procedure data...")
        procedure_df = (
            df.groupby(["npi", "total_paid"])
            .size()
            .reset_index(name="row_count")
        )
        click.echo("Aggregating procedure code data...")
        code_df = (
            df.groupby(["npi", "procedure_code", "service_month"], as_index=False)
            .agg(total_claims=("total_claims", "sum"), total_paid=("total_paid", "sum"))
        )

    # Filter out providers with total paid below minimum threshold
    provider_totals = (
        monthly_df.groupby("npi", as_index=False)["total_paid"]
        .sum()
        .rename(columns={"total_paid": "total_paid_sum"})
    )
    qualifying_npis = (
        provider_totals[provider_totals["total_paid_sum"] >= MIN_TOTAL_PAID]["npi"].tolist()
    )
    excluded = len(provider_totals) - len(qualifying_npis)
    monthly_df = monthly_df[monthly_df["npi"].isin(qualifying_npis)].copy()
    procedure_df = procedure_df[procedure_df["npi"].isin(qualifying_npis)].copy()
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


def _detect_volume_impossibility(monthly_df: pd.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers with impossibly high claim counts in a single month."""
    flagged = monthly_df[monthly_df["total_claims"] > MAX_CLAIMS_PER_MONTH]

    flags: dict[str, list[RedFlag]] = {}
    for _, row in flagged.iterrows():
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


def _detect_revenue_outliers(monthly_df: pd.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers whose revenue per claim is far above peers.

    Uses median and MAD (median absolute deviation) instead of mean/std
    to resist skew from large providers distorting the baseline.
    """
    provider_totals = (
        monthly_df.groupby("npi", as_index=False)
        .agg(total_paid_sum=("total_paid", "sum"), total_claims_sum=("total_claims", "sum"))
    )
    provider_totals = provider_totals[provider_totals["total_claims_sum"] > 0].copy()
    provider_totals["paid_per_claim"] = (
        provider_totals["total_paid_sum"] / provider_totals["total_claims_sum"]
    )

    if provider_totals.empty:
        return {}

    median_val = provider_totals["paid_per_claim"].median()
    # MAD = median of absolute deviations from the median
    mad_val = (provider_totals["paid_per_claim"] - median_val).abs().median()

    if pd.isna(mad_val) or mad_val == 0:
        return {}

    # Scale MAD to be comparable to std dev for normal distributions
    # (1.4826 is the consistency constant for normal distributions)
    scaled_mad = mad_val * 1.4826

    flags: dict[str, list[RedFlag]] = {}
    for _, row in provider_totals.iterrows():
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


def _detect_billing_spikes(monthly_df: pd.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers with sudden monthly billing spikes vs their own history."""
    flags: dict[str, list[RedFlag]] = {}

    for npi in monthly_df["npi"].unique():
        provider_monthly = monthly_df[monthly_df["npi"] == npi].sort_values("service_month")
        if len(provider_monthly) < 3:
            continue

        totals = provider_monthly["total_paid"].tolist()
        avg = sum(totals) / len(totals)
        if avg == 0:
            continue

        for _, row in provider_monthly.iterrows():
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


def _detect_nos_concentration(code_df: pd.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers where NOS/miscellaneous codes make up a large share of billing.

    NOS codes lack specificity and are a classic vehicle for DME and supply fraud —
    they obscure what was actually billed and are harder to audit than named items.
    """
    provider_totals = (
        code_df.groupby("npi", as_index=False)["total_paid"]
        .sum()
        .rename(columns={"total_paid": "total_paid_all"})
    )
    nos_totals = (
        code_df[code_df["procedure_code"].isin(NOS_CODES)]
        .groupby("npi", as_index=False)["total_paid"]
        .sum()
        .rename(columns={"total_paid": "nos_paid"})
    )
    merged = provider_totals.merge(nos_totals, on="npi", how="inner")
    merged["nos_ratio"] = merged["nos_paid"] / merged["total_paid_all"]
    merged = merged[merged["nos_ratio"] >= NOS_CONCENTRATION_THRESHOLD]

    flags: dict[str, list[RedFlag]] = {}
    for _, row in merged.iterrows():
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


def _detect_upcoding_trajectory(code_df: pd.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers with a systematic shift toward higher-reimbursed E&M codes over time.

    Compares the weighted average E&M code level in the first half of the provider's
    billing history vs the second half. A significant upward shift (with no change in
    patient volume) is a signature of deliberate upcoding rather than clinical change.
    """
    em_df = code_df[code_df["procedure_code"].isin(set(EM_CODES.keys()))].copy()

    if em_df.empty:
        return {}

    em_df["code_level"] = em_df["procedure_code"].map(lambda c: EM_CODES.get(c, 0)).astype(int)

    flags: dict[str, list[RedFlag]] = {}
    for npi in em_df["npi"].unique():
        provider_em = em_df[em_df["npi"] == npi].sort_values("service_month")

        total_claims = provider_em["total_claims"].sum()
        if total_claims < UPCODING_MIN_CLAIMS:
            continue

        months = sorted(provider_em["service_month"].unique())
        if len(months) < 6:
            continue

        mid = len(months) // 2
        early = provider_em[provider_em["service_month"].isin(months[:mid])]
        late = provider_em[provider_em["service_month"].isin(months[mid:])]

        def weighted_avg(df: pd.DataFrame) -> float:
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


def _detect_suspicious_consistency(procedure_df: pd.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers where an unusually high fraction of rows share the same paid amount."""
    # Exclude $0 rows — uniform zeros are a data artifact, not copy-paste fraud
    procedure_df = procedure_df[procedure_df["total_paid"] != 0].copy().reset_index(drop=True)

    if procedure_df.empty:
        return {}

    # For each NPI: total row count, and the most-frequent paid amount with its count
    totals = procedure_df.groupby("npi")["row_count"].sum().reset_index(name="total_rows")
    idx = procedure_df.groupby("npi")["row_count"].idxmax()
    top_rows = (
        procedure_df.loc[idx.values, ["npi", "total_paid", "row_count"]]
        .copy()
        .rename(columns={"row_count": "top_amount_count", "total_paid": "top_amount"})
    )

    provider_stats = totals.merge(top_rows, on="npi")
    provider_stats = provider_stats[provider_stats["total_rows"] >= CONSISTENCY_MIN_ROWS].copy()
    provider_stats["consistency_ratio"] = (
        provider_stats["top_amount_count"] / provider_stats["total_rows"]
    )
    provider_stats = provider_stats[provider_stats["consistency_ratio"] > CONSISTENCY_RATIO_THRESHOLD]

    flags: dict[str, list[RedFlag]] = {}
    for _, row in provider_stats.iterrows():
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
