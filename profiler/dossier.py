from pathlib import Path

import click
import polars as pl

from data.loader import load_claims, load_claims_for_provider
from data.models import Dossier, Provider, RedFlag, ScanResult


def build_dossier(filepath: Path, npi: str, scan_result: ScanResult | None = None) -> Dossier:
    """Build a comprehensive dossier for a specific provider."""
    click.echo(f"Building dossier for provider {npi}...")

    claims = load_claims_for_provider(filepath, npi)
    if claims.is_empty():
        raise click.ClickException(f"No claims found for NPI {npi}")

    provider = _extract_provider_info(claims, npi)
    claims_summary = _summarize_claims(claims)
    peer_comparison = _compare_to_peers(filepath, npi, claims)
    timeline = _build_timeline(claims)

    if scan_result is None:
        scan_result = ScanResult(npi=npi, provider_name=provider.name, overall_score=0.0)

    return Dossier(
        provider=provider,
        scan_result=scan_result,
        claims_summary=claims_summary,
        peer_comparison=peer_comparison,
        timeline=timeline,
    )


def _extract_provider_info(claims: pl.DataFrame, npi: str) -> Provider:
    """Extract provider details from their claims data."""
    names = claims.columns

    def get_first(col_options: list[str], default: str = "") -> str:
        for col in col_options:
            if col in names:
                vals = claims[col].drop_nulls()
                if len(vals) > 0:
                    return str(vals[0])
        return default

    return Provider(
        npi=npi,
        name=get_first(["provider_name", "PROVIDER_NAME"]),
        specialty=get_first(["specialty", "SPECIALTY"]),
        state=get_first(["state", "STATE"]),
        city=get_first(["city", "CITY"]),
        zip_code=get_first(["zip_code", "ZIP"]),
    )


def _summarize_claims(claims: pl.DataFrame) -> dict:
    """Generate a billing summary from the provider's claims."""
    names = claims.columns
    amount_col = "billed_amount" if "billed_amount" in names else "BILLED_AMT"
    paid_col = "paid_amount" if "paid_amount" in names else "PAID_AMT"
    date_col = "service_date" if "service_date" in names else "SRVC_DT"
    proc_col = "procedure_code" if "procedure_code" in names else "PROC_CD"

    summary = {
        "total_claims": len(claims),
    }

    if amount_col in names:
        summary["total_billed"] = claims[amount_col].sum()
        summary["avg_billed_per_claim"] = claims[amount_col].mean()
        summary["max_single_claim"] = claims[amount_col].max()

    if paid_col in names:
        summary["total_paid"] = claims[paid_col].sum()

    if date_col in names:
        dates = claims[date_col].cast(pl.Date)
        summary["date_range_start"] = str(dates.min())
        summary["date_range_end"] = str(dates.max())

        # Claims per day stats
        daily = claims.with_columns(pl.col(date_col).cast(pl.Date)).group_by(date_col).agg(
            pl.len().alias("daily_count")
        )
        summary["avg_claims_per_active_day"] = round(daily["daily_count"].mean(), 1)
        summary["max_claims_in_a_day"] = daily["daily_count"].max()
        summary["active_days"] = len(daily)

    if proc_col in names:
        top_procedures = (
            claims.group_by(proc_col)
            .agg([
                pl.len().alias("count"),
                pl.col(amount_col).sum().alias("total_billed") if amount_col in names else pl.lit(0).alias("total_billed"),
            ])
            .sort("count", descending=True)
            .head(10)
        )
        summary["top_procedures"] = top_procedures.to_dicts()

    return summary


def _compare_to_peers(filepath: Path, npi: str, provider_claims: pl.DataFrame) -> dict:
    """Compare this provider's billing to peers in the same specialty/region."""
    names = provider_claims.columns
    specialty_col = "specialty" if "specialty" in names else "SPECIALTY"
    state_col = "state" if "state" in names else "STATE"
    amount_col = "billed_amount" if "billed_amount" in names else "BILLED_AMT"
    npi_col = "npi" if "npi" in names else "NPI"

    if specialty_col not in names or amount_col not in names:
        return {"note": "Peer comparison unavailable — missing specialty or billing columns"}

    specialty_vals = provider_claims[specialty_col].drop_nulls()
    if len(specialty_vals) == 0:
        return {"note": "No specialty data available for peer comparison"}

    specialty = str(specialty_vals[0])

    lf = load_claims(filepath)
    lf_names = lf.collect_schema().names()
    lf_specialty_col = "specialty" if "specialty" in lf_names else "SPECIALTY"
    lf_amount_col = "billed_amount" if "billed_amount" in lf_names else "BILLED_AMT"
    lf_npi_col = "npi" if "npi" in lf_names else "NPI"

    peers = (
        lf.filter(pl.col(lf_specialty_col) == specialty)
        .group_by(lf_npi_col)
        .agg([
            pl.col(lf_amount_col).sum().alias("total_billed"),
            pl.len().alias("claim_count"),
        ])
        .collect()
    )

    if peers.is_empty():
        return {"note": "No peers found in same specialty"}

    provider_total = provider_claims[amount_col].sum()
    peer_mean = peers["total_billed"].mean()
    peer_median = peers["total_billed"].median()
    peer_std = peers["total_billed"].std()

    percentile_rank = (
        peers.filter(pl.col("total_billed") <= provider_total).height / peers.height * 100
    )

    comparison = {
        "specialty": specialty,
        "peer_count": peers.height,
        "provider_total_billed": provider_total,
        "peer_mean_billed": round(peer_mean, 2),
        "peer_median_billed": round(peer_median, 2),
        "provider_percentile": round(percentile_rank, 1),
    }

    if peer_std and peer_std > 0:
        comparison["zscore"] = round((provider_total - peer_mean) / peer_std, 2)

    return comparison


def _build_timeline(claims: pl.DataFrame) -> list[dict]:
    """Build a monthly billing timeline for the provider."""
    names = claims.columns
    date_col = "service_date" if "service_date" in names else "SRVC_DT"
    amount_col = "billed_amount" if "billed_amount" in names else "BILLED_AMT"

    if date_col not in names:
        return []

    monthly = (
        claims.with_columns(pl.col(date_col).cast(pl.Date).dt.truncate("1mo").alias("month"))
        .group_by("month")
        .agg([
            pl.len().alias("claim_count"),
            pl.col(amount_col).sum().alias("total_billed") if amount_col in names else pl.lit(0).alias("total_billed"),
        ])
        .sort("month")
    )

    return [
        {
            "month": str(row["month"]),
            "claim_count": row["claim_count"],
            "total_billed": row["total_billed"],
        }
        for row in monthly.iter_rows(named=True)
    ]
