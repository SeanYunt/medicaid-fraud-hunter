from pathlib import Path

import click
import polars as pl

from data.loader import load_claims, load_claims_for_provider
from data.models import Dossier, Provider, ScanResult


def build_dossier(filepath: Path, npi: str, scan_result: ScanResult | None = None) -> Dossier:
    """Build a comprehensive dossier for a specific provider."""
    click.echo(f"Building dossier for provider {npi}...")

    claims = load_claims_for_provider(filepath, npi)
    if claims.is_empty():
        raise click.ClickException(f"No claims found for NPI {npi}")

    provider = Provider(npi=npi)
    claims_summary = _summarize_claims(claims)
    peer_comparison = _compare_to_peers(filepath, npi, claims)
    timeline = _build_timeline(claims)

    if scan_result is None:
        scan_result = ScanResult(npi=npi, provider_name="", overall_score=0.0)

    return Dossier(
        provider=provider,
        scan_result=scan_result,
        claims_summary=claims_summary,
        peer_comparison=peer_comparison,
        timeline=timeline,
    )


def _summarize_claims(claims: pl.DataFrame) -> dict:
    """Generate a summary from the provider's aggregated claims data."""
    names = claims.columns

    summary = {
        "total_rows": len(claims),
    }

    if "total_claims" in names:
        summary["total_claims"] = claims["total_claims"].sum()

    if "total_paid" in names:
        summary["total_paid"] = claims["total_paid"].sum()

    if "beneficiaries" in names:
        summary["total_beneficiaries"] = claims["beneficiaries"].sum()

    if "service_month" in names:
        months = claims["service_month"].cast(pl.Date)
        summary["date_range_start"] = str(months.min())
        summary["date_range_end"] = str(months.max())
        summary["active_months"] = claims["service_month"].n_unique()

    if "procedure_code" in names:
        agg_cols = [pl.len().alias("row_count")]
        if "total_claims" in names:
            agg_cols.append(pl.col("total_claims").sum().alias("claims"))
        if "total_paid" in names:
            agg_cols.append(pl.col("total_paid").sum().alias("paid"))

        top_procedures = (
            claims.group_by("procedure_code")
            .agg(agg_cols)
            .sort("row_count", descending=True)
            .head(10)
        )
        summary["top_procedures"] = top_procedures.to_dicts()

    return summary


def _compare_to_peers(filepath: Path, npi: str, provider_claims: pl.DataFrame) -> dict:
    """Compare this provider's total paid amount to all other providers."""
    if "total_paid" not in provider_claims.columns:
        return {"note": "Peer comparison unavailable — missing total_paid column"}

    lf = load_claims(filepath)

    peers = (
        lf.group_by("npi")
        .agg(pl.col("total_paid").sum().alias("total_paid_sum"))
        .collect()
    )

    if peers.is_empty():
        return {"note": "No peers found"}

    provider_total = provider_claims["total_paid"].sum()
    peer_mean = peers["total_paid_sum"].mean()
    peer_median = peers["total_paid_sum"].median()
    peer_std = peers["total_paid_sum"].std()

    percentile_rank = (
        peers.filter(pl.col("total_paid_sum") <= provider_total).height / peers.height * 100
    )

    comparison = {
        "peer_count": peers.height,
        "provider_total_paid": provider_total,
        "peer_mean_paid": round(peer_mean, 2),
        "peer_median_paid": round(peer_median, 2),
        "provider_percentile": round(percentile_rank, 1),
    }

    if peer_std and peer_std > 0:
        comparison["zscore"] = round((provider_total - peer_mean) / peer_std, 2)

    return comparison


def _build_timeline(claims: pl.DataFrame) -> list[dict]:
    """Build a monthly billing timeline for the provider."""
    if "service_month" not in claims.columns:
        return []

    agg_cols = [pl.len().alias("row_count")]
    if "total_claims" in claims.columns:
        agg_cols.append(pl.col("total_claims").sum().alias("total_claims"))
    if "total_paid" in claims.columns:
        agg_cols.append(pl.col("total_paid").sum().alias("total_paid"))

    monthly = (
        claims.group_by("service_month")
        .agg(agg_cols)
        .sort("service_month")
    )

    return [
        {
            "month": str(row["service_month"]),
            "total_claims": row.get("total_claims", row["row_count"]),
            "total_paid": row.get("total_paid", 0),
        }
        for row in monthly.iter_rows(named=True)
    ]
