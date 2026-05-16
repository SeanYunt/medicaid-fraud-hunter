import time
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
    state_npis: set[str] | None = None,
) -> list[ScanResult]:
    """Scan the dataset and return providers with anomaly scores above threshold.

    If monthly_path and procedure_path are provided, uses the small preprocessed
    files instead of reading the full raw dataset.

    If state_npis is provided, only providers whose NPI appears in that set are
    included in output.  Revenue-outlier detection still uses national statistics
    so a state provider is measured against the full national peer group.
    """
    code_df: pd.DataFrame | None = None

    if monthly_path and procedure_path:
        t0 = time.time()
        click.echo("Loading preprocessed summaries...", nl=False)
        monthly_df = pd.read_parquet(monthly_path, engine="pyarrow")
        procedure_df = pd.read_parquet(procedure_path, engine="pyarrow")
        click.echo(f" done ({time.time() - t0:.1f}s)")
    else:
        t0 = time.time()
        click.echo("Loading raw dataset (consider running 'preprocess' first)...", nl=False)
        df = load_claims(filepath)
        click.echo(f" done ({time.time() - t0:.1f}s)")
        t0 = time.time()
        click.echo("Aggregating monthly data...", nl=False)
        monthly_df = (
            df.groupby(["npi", "service_month"], as_index=False)
            .agg(total_claims=("total_claims", "sum"), total_paid=("total_paid", "sum"))
        )
        click.echo(f" done ({time.time() - t0:.1f}s)")
        t0 = time.time()
        click.echo("Aggregating procedure data...", nl=False)
        procedure_df = (
            df.groupby(["npi", "total_paid"])
            .size()
            .reset_index(name="row_count")
        )
        click.echo(f" done ({time.time() - t0:.1f}s)")
        t0 = time.time()
        click.echo("Aggregating procedure code data...", nl=False)
        code_df = (
            df.groupby(["npi", "procedure_code", "service_month"], as_index=False)
            .agg(total_claims=("total_claims", "sum"), total_paid=("total_paid", "sum"))
        )
        click.echo(f" done ({time.time() - t0:.1f}s)")

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

    # national_monthly_df is used as the baseline for revenue-outlier z-scores so
    # that state providers are compared against all national peers, not just peers
    # within the same state.
    national_monthly_df = monthly_df

    if state_npis is not None:
        act_monthly = monthly_df[monthly_df["npi"].isin(state_npis)].copy()
        act_procedure = procedure_df[procedure_df["npi"].isin(state_npis)].copy()
        if code_df is not None:
            code_df = code_df[code_df["npi"].isin(state_npis)].copy()
        click.echo(f"Filtered to {act_monthly['npi'].nunique():,} providers in state scope")
    else:
        act_monthly = monthly_df
        act_procedure = procedure_df

    num_detectors = 4 if code_df is None else 6
    click.echo("Running anomaly detection...")

    # --- Volume impossibility (fixed threshold — safe on filtered data) ---
    t0 = time.time()
    click.echo(f"  [1/{num_detectors}] Volume impossibility detector...", nl=False)
    volume_flags = _detect_volume_impossibility(act_monthly)
    click.echo(f" done ({time.time() - t0:.1f}s, {len(volume_flags):,} flagged)")

    # --- Revenue outliers (national baseline, state-filtered output) ---
    t0 = time.time()
    click.echo(f"  [2/{num_detectors}] Revenue outlier detector...", nl=False)
    revenue_flags = _detect_revenue_outliers(national_monthly_df, state_npis=state_npis)
    click.echo(f" done ({time.time() - t0:.1f}s, {len(revenue_flags):,} flagged)")

    # --- Billing spikes (provider-relative — safe on filtered data) ---
    t0 = time.time()
    click.echo(f"  [3/{num_detectors}] Billing spike detector...", nl=False)
    spike_flags = _detect_billing_spikes(act_monthly)
    click.echo(f" done ({time.time() - t0:.1f}s, {len(spike_flags):,} flagged)")

    # --- Suspicious consistency (provider-specific — safe on filtered data) ---
    t0 = time.time()
    click.echo(f"  [4/{num_detectors}] Suspicious consistency detector...", nl=False)
    consistency_flags = _detect_suspicious_consistency(act_procedure)
    click.echo(f" done ({time.time() - t0:.1f}s, {len(consistency_flags):,} flagged)")

    # --- Scheme-specific detectors (require raw code-level data) ---
    if code_df is not None:
        t0 = time.time()
        click.echo(f"  [5/{num_detectors}] NOS concentration detector...", nl=False)
        nos_flags = _detect_nos_concentration(code_df)
        click.echo(f" done ({time.time() - t0:.1f}s, {len(nos_flags):,} flagged)")

        t0 = time.time()
        click.echo(f"  [6/{num_detectors}] Upcoding trajectory detector...", nl=False)
        upcoding_flags = _detect_upcoding_trajectory(code_df)
        click.echo(f" done ({time.time() - t0:.1f}s, {len(upcoding_flags):,} flagged)")
    else:
        nos_flags = {}
        upcoding_flags = {}

    # Merge all flags by NPI
    t0 = time.time()
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
    click.echo(f"Scored and ranked {len(all_npis):,} providers ({time.time() - t0:.1f}s)")
    click.echo(f"Found {len(results):,} suspicious providers above threshold {threshold}")
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


def _detect_revenue_outliers(
    monthly_df: pd.DataFrame,
    state_npis: set[str] | None = None,
) -> dict[str, list[RedFlag]]:
    """Flag providers whose revenue per claim is far above peers.

    Uses median and MAD (median absolute deviation) instead of mean/std
    to resist skew from large providers distorting the baseline.

    When state_npis is provided, the national baseline (median/MAD) is still
    computed from the full monthly_df, but flags are only emitted for NPIs that
    appear in state_npis.
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
        npi = str(row["npi"])
        if state_npis is not None and npi not in state_npis:
            continue
        modified_zscore = (row["paid_per_claim"] - median_val) / scaled_mad
        if modified_zscore > REVENUE_ZSCORE_THRESHOLD:
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
    """Flag providers with sudden monthly billing spikes vs their own history.

    Scale fix: replaced O(N²) Python loop (250K providers × full-frame boolean mask over
    20M rows) with a single groupby aggregate + merge. O(N) at national scale.
    """
    # Compute per-provider month count and mean in one pass, then merge back
    stats = (
        monthly_df.groupby("npi", as_index=False)
        .agg(_month_count=("total_paid", "count"), _avg=("total_paid", "mean"))
    )
    stats = stats[(stats["_month_count"] >= 3) & (stats["_avg"] > 0)]
    if stats.empty:
        return {}

    df = monthly_df.merge(stats[["npi", "_avg"]], on="npi", how="inner")
    df["_ratio"] = df["total_paid"] / df["_avg"]
    spikes = df[df["_ratio"] > SPIKE_MULTIPLIER]

    flags: dict[str, list[RedFlag]] = {}
    for _, row in spikes.iterrows():
        npi = str(row["npi"])
        ratio = row["_ratio"]
        avg = row["_avg"]
        severity = min(1.0, ratio / 10.0)
        flag = RedFlag(
            flag_type=RedFlagType.BILLING_SPIKE,
            description=f"Monthly paid ${row['total_paid']:,.2f} in {row['service_month']} is {ratio:.1f}x their average ${avg:,.2f}",
            severity=severity,
            evidence={"month": str(row["service_month"]), "amount": row["total_paid"], "ratio": round(ratio, 2)},
        )
        flags.setdefault(npi, []).append(flag)

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
    """Flag providers where an unusually high fraction of rows share the same paid amount.

    Scale fix: removed unnecessary .copy() of potentially 50M+ row DataFrame (caused
    severe memory pressure at national scale), and replaced groupby().idxmax()+loc with
    sort+drop_duplicates — avoids Python-level group iteration on object-dtype NPI keys.
    """
    # Exclude $0 rows — uniform zeros are a data artifact, not copy-paste fraud
    df = procedure_df[procedure_df["total_paid"] != 0]

    if df.empty:
        return {}

    # For each NPI: total row count across all paid amounts
    totals = (
        df.groupby("npi", as_index=False)["row_count"]
        .sum()
        .rename(columns={"row_count": "total_rows"})
    )

    # Most-frequent paid amount per provider: sort descending so drop_duplicates
    # keeps the highest-count row (first occurrence) for each NPI.
    top_rows = (
        df.sort_values("row_count", ascending=False)
        .drop_duplicates(subset="npi", keep="first")
        [["npi", "total_paid", "row_count"]]
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
