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
DOMINANCE_THRESHOLD = 0.70   # Top procedure >= 70% of total billing
RATE_CV_THRESHOLD = 0.08     # Per-claim rate coefficient of variation < 8%
CONSISTENCY_MIN_MONTHS = 3   # Minimum months of data to evaluate consistency
MIN_TOTAL_PAID = 100_000  # Ignore providers below this total — too small for viable qui tam case

# Per-diem and high-frequency community support codes used almost exclusively by
# multi-staff organisational providers (community behavioral health, supported
# housing, day habilitation programs).  When one of these is the dominant billing
# code, the 1,500 claims/month solo-practitioner threshold produces systematic
# false positives and must not be applied.
ORG_BILLING_CODES = {
    "H0038",  # Community prep services, per 15 min
    "H0039",  # Assertive community treatment, per diem
    "H0040",  # Assertive community treatment, per month
    "H0043",  # Supported housing (ACT per diem)
    "H2015",  # Comprehensive community support, per 15 min
    "H2016",  # Comprehensive community support, per diem
    "H2017",  # Psychosocial rehabilitation services, per 15 min
    "H2018",  # Psychosocial rehabilitation services, per diem
    "H2019",  # Therapeutic behavioral services, per 15 min
    "H2020",  # Therapeutic behavioral services, per diem
    "T1016",  # Case management, per 15 min
    "T1017",  # Targeted case management, per 15 min
    "T1019",  # Personal care services, per 15 min
    "T1020",  # Personal care services, per diem
    "T2020",  # Day habilitation, waiver, per 15 min
    "T2021",  # Day habilitation, waiver, per diem
}

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
        click.echo("Loading preprocessed summaries...")
        monthly_df = pd.read_parquet(monthly_path, engine="pyarrow")
        code_df = pd.read_parquet(procedure_path, engine="pyarrow")
        click.echo(f"done ({time.time() - t0:.1f}s)")
    else:
        t0 = time.time()
        click.echo("Loading raw dataset (consider running 'preprocess' first)...")
        df = load_claims(filepath)
        click.echo(f"done ({time.time() - t0:.1f}s)")
        t0 = time.time()
        click.echo("Aggregating monthly data...")
        monthly_df = (
            df.groupby(["npi", "service_month"], as_index=False)
            .agg(total_claims=("total_claims", "sum"), total_paid=("total_paid", "sum"))
        )
        click.echo(f"done ({time.time() - t0:.1f}s)")
        t0 = time.time()
        click.echo("Aggregating procedure code data...")
        code_df = (
            df.groupby(["npi", "procedure_code", "service_month"], as_index=False)
            .agg(total_claims=("total_claims", "sum"), total_paid=("total_paid", "sum"))
        )
        click.echo(f"done ({time.time() - t0:.1f}s)")

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
    if code_df is not None:
        code_df = code_df[code_df["npi"].isin(qualifying_npis)].copy()
    click.echo(f"Filtered to {len(qualifying_npis):,} providers with >=${MIN_TOTAL_PAID:,} total paid "
               f"({excluded:,} excluded)")

    # national_monthly_df is used as the baseline for revenue-outlier z-scores so
    # that state providers are compared against all national peers, not just peers
    # within the same state.
    national_monthly_df = monthly_df

    if state_npis is not None:
        act_monthly = monthly_df[monthly_df["npi"].isin(state_npis)].copy()
        if code_df is not None:
            code_df = code_df[code_df["npi"].isin(state_npis)].copy()
        click.echo(f"Filtered to {act_monthly['npi'].nunique():,} providers in state scope")
    else:
        act_monthly = monthly_df

    num_detectors = 4 if code_df is None else 6

    # Attempt to load NPPES entity types so the volume detector can skip
    # Type 2 (organisational) providers.  The NPPES zip is optional — if absent
    # the code-based heuristic (ORG_BILLING_CODES) still catches the most common
    # organisational billing patterns.
    org_npis: set[str] | None = None
    try:
        from data.nppes import find_nppes_zip, load_organization_npis
        nppes_zip = find_nppes_zip()
        t0 = time.time()
        click.echo("Loading NPPES entity types for volume detector...")
        org_npis = load_organization_npis(nppes_zip)
        click.echo(f"  {len(org_npis):,} organisational NPIs identified ({time.time() - t0:.1f}s)")
    except FileNotFoundError:
        pass  # NPPES zip absent — code-based heuristic still applies

    click.echo("Running anomaly detection...")

    # --- Volume impossibility (fixed threshold — safe on filtered data) ---
    t0 = time.time()
    click.echo(f"  [1/{num_detectors}] Volume impossibility detector...")
    volume_flags = _detect_volume_impossibility(act_monthly, org_npis=org_npis, code_df=code_df)
    click.echo(f"  done ({time.time() - t0:.1f}s, {len(volume_flags):,} flagged)")

    # --- Revenue outliers (national baseline, state-filtered output) ---
    t0 = time.time()
    click.echo(f"  [2/{num_detectors}] Revenue outlier detector...")
    revenue_flags = _detect_revenue_outliers(national_monthly_df, state_npis=state_npis)
    click.echo(f"  done ({time.time() - t0:.1f}s, {len(revenue_flags):,} flagged)")

    # --- Billing spikes (provider-relative — safe on filtered data) ---
    t0 = time.time()
    click.echo(f"  [3/{num_detectors}] Billing spike detector...")
    spike_flags = _detect_billing_spikes(act_monthly)
    click.echo(f"  done ({time.time() - t0:.1f}s, {len(spike_flags):,} flagged)")

    # --- Suspicious consistency (procedure dominance + rate uniformity) ---
    t0 = time.time()
    click.echo(f"  [4/{num_detectors}] Suspicious consistency detector...")
    consistency_flags = _detect_suspicious_consistency(code_df) if code_df is not None else {}
    click.echo(f"  done ({time.time() - t0:.1f}s, {len(consistency_flags):,} flagged)")

    # --- Scheme-specific detectors (require raw code-level data) ---
    if code_df is not None:
        t0 = time.time()
        click.echo(f"  [5/{num_detectors}] NOS concentration detector...")
        nos_flags = _detect_nos_concentration(code_df)
        click.echo(f"  done ({time.time() - t0:.1f}s, {len(nos_flags):,} flagged)")

        t0 = time.time()
        click.echo(f"  [6/{num_detectors}] Upcoding trajectory detector...")
        upcoding_flags = _detect_upcoding_trajectory(code_df)
        click.echo(f"  done ({time.time() - t0:.1f}s, {len(upcoding_flags):,} flagged)")
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


def _detect_volume_impossibility(
    monthly_df: pd.DataFrame,
    org_npis: set[str] | None = None,
    code_df: pd.DataFrame | None = None,
) -> dict[str, list[RedFlag]]:
    """Flag providers with impossibly high claim counts in a single month.

    Skips organisational providers identified by either:
    - NPPES entity type 2 (when org_npis is provided), or
    - A dominant billing code from ORG_BILLING_CODES (when code_df is provided).

    The 1,500/month threshold is calibrated for solo practitioners; applying it
    to multi-staff organisations produces systematic false positives.
    """
    excluded: set[str] = set(org_npis) if org_npis else set()

    if code_df is not None and not code_df.empty:
        # Find each provider's highest-volume procedure code across all months.
        # If that code is in ORG_BILLING_CODES the provider bills like a multi-staff
        # organisation and the solo-practitioner volume threshold doesn't apply.
        code_totals = (
            code_df.groupby(["npi", "procedure_code"], as_index=False)["total_claims"]
            .sum()
        )
        code_totals["npi"] = code_totals["npi"].astype(str)
        top_codes = (
            code_totals.sort_values("total_claims", ascending=False)
            .drop_duplicates(subset="npi", keep="first")
            .set_index("npi")["procedure_code"]
        )
        excluded |= set(top_codes[top_codes.isin(ORG_BILLING_CODES)].index)

    if excluded:
        scan_df = monthly_df[~monthly_df["npi"].astype(str).isin(excluded)]
    else:
        scan_df = monthly_df

    flagged = scan_df[scan_df["total_claims"] > MAX_CLAIMS_PER_MONTH]

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


def _detect_suspicious_consistency(code_df: pd.DataFrame) -> dict[str, list[RedFlag]]:
    """Flag providers whose billing is dominated by one procedure billed at a
    suspiciously uniform per-claim rate — the signature of copy-paste or phantom billing.

    Two conditions must both hold:
      1. A single procedure code accounts for >= DOMINANCE_THRESHOLD of total paid.
      2. The per-claim rate for that code has a coefficient of variation (std/mean)
         below RATE_CV_THRESHOLD across months — almost the same amount every month.
    """
    if code_df is None or code_df.empty:
        return {}

    # Per-provider total paid
    provider_totals = (
        code_df.groupby("npi", as_index=False)["total_paid"]
        .sum()
        .rename(columns={"total_paid": "grand_total"})
    )

    # Per (npi, procedure_code) totals — to find the dominant code
    proc_totals = (
        code_df.groupby(["npi", "procedure_code"], as_index=False)
        .agg(proc_paid=("total_paid", "sum"), proc_claims=("total_claims", "sum"))
    )
    proc_totals = proc_totals.merge(provider_totals, on="npi")
    proc_totals["dominance"] = proc_totals["proc_paid"] / proc_totals["grand_total"]

    # Keep only the single highest-revenue procedure per provider
    dominant = (
        proc_totals.sort_values("dominance", ascending=False)
        .drop_duplicates(subset="npi", keep="first")
    )
    dominant = dominant[dominant["dominance"] >= DOMINANCE_THRESHOLD].copy()

    if dominant.empty:
        return {}

    # Per-claim rate per (npi, procedure_code, month) — skip zero-claim rows
    monthly_rates = code_df[code_df["total_claims"] > 0].copy()
    monthly_rates["rate"] = monthly_rates["total_paid"] / monthly_rates["total_claims"]

    rate_stats = (
        monthly_rates.groupby(["npi", "procedure_code"], as_index=False)
        .agg(
            mean_rate=("rate", "mean"),
            std_rate=("rate", "std"),
            month_count=("rate", "count"),
        )
    )
    rate_stats["std_rate"] = rate_stats["std_rate"].fillna(0.0)
    rate_stats = rate_stats[rate_stats["month_count"] >= CONSISTENCY_MIN_MONTHS]
    rate_stats["cv"] = rate_stats["std_rate"] / rate_stats["mean_rate"].replace(0.0, float("nan"))
    rate_stats = rate_stats.dropna(subset=["cv"])
    rate_stats = rate_stats[rate_stats["cv"] < RATE_CV_THRESHOLD]

    if rate_stats.empty:
        return {}

    flagged = dominant.merge(rate_stats, on=["npi", "procedure_code"])

    flags: dict[str, list[RedFlag]] = {}
    for _, row in flagged.iterrows():
        npi = str(row["npi"])
        severity = min(1.0, row["dominance"] * (1.0 - row["cv"]))
        flag = RedFlag(
            flag_type=RedFlagType.SUSPICIOUS_CONSISTENCY,
            description=(
                f"{row['dominance']:.0%} of billing is procedure {row['procedure_code']} "
                f"at ${row['mean_rate']:,.2f}/claim with only {row['cv']:.1%} rate variation "
                f"across {int(row['month_count'])} months — consistent with copy-paste billing"
            ),
            severity=severity,
            evidence={
                "procedure_code": row["procedure_code"],
                "dominance_ratio": round(row["dominance"], 3),
                "mean_rate_per_claim": round(row["mean_rate"], 2),
                "rate_cv": round(row["cv"], 4),
                "months_observed": int(row["month_count"]),
            },
        )
        flags.setdefault(npi, []).append(flag)

    return flags
