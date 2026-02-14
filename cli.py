import csv
from pathlib import Path

import click

from data.fetch import find_dataset
from data.loader import load_claims
from profiler.dossier import build_dossier
from reports.pdf import generate_dossier_pdf
from scanner.anomalies import scan_all

OUTPUT_DIR = Path(__file__).parent / "output"


@click.group()
def cli():
    """Medicaid Fraud Hunter — Scan claims data and build evidence dossiers."""
    pass


@cli.command()
@click.option("--threshold", default=0.3, type=float,
              help="Minimum anomaly score to include (0.0-1.0)")
@click.option("--data-path", default=None, type=click.Path(exists=True),
              help="Path to CSV dataset (auto-detected if not specified)")
@click.option("--top", default=50, type=int,
              help="Number of top results to display")
def scan(threshold: float, data_path: str | None, top: int):
    """Scan the dataset for suspicious providers."""
    filepath = Path(data_path) if data_path else find_dataset()
    click.echo(f"Scanning {filepath}...")

    results = scan_all(filepath, threshold=threshold)

    # Save full results to CSV
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "scan_results.csv"

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "npi", "provider_name", "score", "num_flags", "flag_types"])
        for i, result in enumerate(results, 1):
            flag_types = ", ".join(set(f.flag_type.value for f in result.red_flags))
            writer.writerow([
                i, result.npi, result.provider_name,
                f"{result.overall_score:.3f}", len(result.red_flags), flag_types,
            ])

    click.echo(f"\nFull results saved to {output_path}")
    click.echo(f"\nTop {min(top, len(results))} suspicious providers:")
    click.echo("-" * 80)

    for i, result in enumerate(results[:top], 1):
        flag_summary = ", ".join(set(f.flag_type.value for f in result.red_flags))
        click.echo(f"  {i:3d}. NPI {result.npi} | Score: {result.overall_score:.0%} | "
                    f"Flags: {len(result.red_flags)} ({flag_summary})")

    if results:
        click.echo(f"\nTo investigate a provider, run: python cli.py profile <NPI>")


@cli.command()
@click.argument("npi")
@click.option("--data-path", default=None, type=click.Path(exists=True),
              help="Path to CSV dataset (auto-detected if not specified)")
def profile(npi: str, data_path: str | None):
    """Build an evidence dossier for a specific provider."""
    filepath = Path(data_path) if data_path else find_dataset()

    # Check if we have scan results for this provider
    scan_result = _load_scan_result(npi)

    dossier = build_dossier(filepath, npi, scan_result)

    # Generate PDF
    pdf_path = generate_dossier_pdf(dossier)
    click.echo(f"\nDossier generated: {pdf_path}")

    # Print summary to terminal
    click.echo(f"\n{'=' * 60}")
    click.echo(f"Provider: {dossier.provider.name or 'N/A'} (NPI: {npi})")
    click.echo(f"Specialty: {dossier.provider.specialty or 'N/A'}")
    click.echo(f"Location: {dossier.provider.city}, {dossier.provider.state}")

    s = dossier.claims_summary
    if s:
        click.echo(f"\nClaims: {s.get('total_claims', 'N/A'):,}")
        if "total_billed" in s:
            click.echo(f"Total Billed: ${s['total_billed']:,.2f}")
        if "date_range_start" in s:
            click.echo(f"Date Range: {s['date_range_start']} to {s['date_range_end']}")

    if dossier.scan_result.red_flags:
        click.echo(f"\nRed Flags ({len(dossier.scan_result.red_flags)}):")
        for flag in dossier.scan_result.red_flags:
            click.echo(f"  - [{flag.severity:.0%}] {flag.description}")

    pc = dossier.peer_comparison
    if pc and "provider_percentile" in pc:
        click.echo(f"\nPeer Ranking: {pc['provider_percentile']}th percentile in {pc.get('specialty', 'N/A')}")

    click.echo(f"{'=' * 60}")


def _load_scan_result(npi: str):
    """Try to load a previous scan result for this NPI."""
    scan_csv = OUTPUT_DIR / "scan_results.csv"
    if not scan_csv.exists():
        return None

    from data.models import ScanResult
    with open(scan_csv) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["npi"] == npi:
                return ScanResult(
                    npi=npi,
                    provider_name=row.get("provider_name", ""),
                    overall_score=float(row.get("score", 0)),
                )
    return None


if __name__ == "__main__":
    cli()
