from pathlib import Path

import click
import pandas as pd

from data.fetch import lookup_npi
from data.loader import load_claims, load_claims_for_provider
from data.models import Dossier, Provider, ScanResult


def build_dossier(
    filepath: Path,
    npi: str,
    scan_result: ScanResult | None = None,
    monthly_path: Path | None = None,
) -> Dossier:
    """Build a comprehensive dossier for a specific provider."""
    click.echo(f"Building dossier for provider {npi}...")

    click.echo("  Loading provider claims from dataset...")
    claims = load_claims_for_provider(filepath, npi)
    if claims.empty:
        raise click.ClickException(f"No claims found for NPI {npi}")
    click.echo(f"  Loaded {len(claims):,} rows for NPI {npi}")

    click.echo("  Looking up NPI in NPPES registry...")
    npi_info = lookup_npi(npi)
    provider = Provider(
        npi=npi,
        name=npi_info.get("name", ""),
        specialty=npi_info.get("specialty", ""),
        address=npi_info.get("address", ""),
        city=npi_info.get("city", ""),
        state=npi_info.get("state", ""),
        zip=npi_info.get("zip", ""),
        enumeration_type=npi_info.get("enumeration_type", ""),
    )
    if provider.name:
        click.echo(f"  Provider: {provider.name}")
    else:
        click.echo("  Warning: NPI not found in NPPES registry")

    click.echo("  Summarizing claims...")
    claims_summary = _summarize_claims(claims)
    click.echo("  Computing peer comparison...")
    peer_comparison = _compare_to_peers(filepath, npi, claims, monthly_path=monthly_path)
    click.echo("  Building billing timeline...")
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


def _summarize_claims(claims: pd.DataFrame) -> dict:
    """Generate a summary from the provider's aggregated claims data."""
    names = claims.columns.tolist()

    summary: dict = {
        "total_rows": len(claims),
    }

    if "total_claims" in names:
        summary["total_claims"] = int(claims["total_claims"].sum())

    if "total_paid" in names:
        summary["total_paid"] = float(claims["total_paid"].sum())

    if "beneficiaries" in names:
        summary["total_beneficiaries"] = int(claims["beneficiaries"].sum())

    if "service_month" in names:
        # Handle both "YYYY-MM" and "YYYY-MM-DD" formats
        service_months = claims["service_month"].astype(str)
        service_months = service_months.apply(lambda s: s + "-01" if len(s) <= 7 else s)
        parsed = pd.to_datetime(service_months, format="%Y-%m-%d")
        summary["date_range_start"] = str(parsed.min().date())
        summary["date_range_end"] = str(parsed.max().date())
        summary["active_months"] = claims["service_month"].nunique()

    if "procedure_code" in names:
        grp = claims.groupby("procedure_code")
        top_agg = pd.DataFrame({"row_count": grp.size()})
        if "total_claims" in names:
            top_agg["claims"] = grp["total_claims"].sum()
        if "total_paid" in names:
            top_agg["paid"] = grp["total_paid"].sum()
        top_procedures = (
            top_agg.reset_index()
            .sort_values("row_count", ascending=False)
            .head(10)
        )
        summary["top_procedures"] = top_procedures.to_dict("records")

    return summary


def _compare_to_peers(
    filepath: Path,
    npi: str,
    provider_claims: pd.DataFrame,
    monthly_path: Path | None = None,
) -> dict:
    """Compare this provider's total paid amount to all other providers."""
    if "total_paid" not in provider_claims.columns:
        return {"note": "Peer comparison unavailable — missing total_paid column"}

    if monthly_path and monthly_path.exists():
        # Fast path: use preprocessed summary (~1MB) instead of raw file (~2.8GB)
        peers = (
            pd.read_parquet(monthly_path, engine="pyarrow")
            .groupby("npi", as_index=False)["total_paid"]
            .sum()
            .rename(columns={"total_paid": "total_paid_sum"})
        )
    else:
        df = load_claims(filepath)
        peers = (
            df.groupby("npi", as_index=False)["total_paid"]
            .sum()
            .rename(columns={"total_paid": "total_paid_sum"})
        )

    if peers.empty:
        return {"note": "No peers found"}

    provider_total = float(provider_claims["total_paid"].sum())
    peer_mean = float(peers["total_paid_sum"].mean())
    peer_median = float(peers["total_paid_sum"].median())
    peer_std = float(peers["total_paid_sum"].std())

    percentile_rank = (
        (peers["total_paid_sum"] <= provider_total).sum() / len(peers) * 100
    )

    comparison = {
        "peer_count": len(peers),
        "provider_total_paid": provider_total,
        "peer_mean_paid": round(peer_mean, 2),
        "peer_median_paid": round(peer_median, 2),
        "provider_percentile": round(float(percentile_rank), 1),
    }

    if pd.notna(peer_std) and peer_std > 0:
        comparison["zscore"] = round((provider_total - peer_mean) / peer_std, 2)

    return comparison


def _build_timeline(claims: pd.DataFrame) -> list[dict]:
    """Build a monthly billing timeline for the provider."""
    if "service_month" not in claims.columns:
        return []

    grp = claims.groupby("service_month")
    monthly = pd.DataFrame({"row_count": grp.size()})
    if "total_claims" in claims.columns:
        monthly["total_claims"] = grp["total_claims"].sum()
    if "total_paid" in claims.columns:
        monthly["total_paid"] = grp["total_paid"].sum()
    monthly = monthly.reset_index().sort_values("service_month")

    return [
        {
            "month": str(row["service_month"]),
            "total_claims": row["total_claims"] if "total_claims" in monthly.columns else row["row_count"],
            "total_paid": row["total_paid"] if "total_paid" in monthly.columns else 0,
        }
        for _, row in monthly.iterrows()
    ]
