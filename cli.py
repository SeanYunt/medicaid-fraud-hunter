import csv
from pathlib import Path

import click

from data.fetch import find_dataset
from data.loader import find_preprocessed, preprocess
from profiler.dossier import build_dossier
from reports.pdf import generate_dossier_pdf
from scanner.anomalies import scan_all

OUTPUT_DIR = Path(__file__).parent / "output"


@click.group()
def cli():
    """Medicaid Fraud Hunter — Scan claims data and build evidence dossiers."""
    pass


@cli.command()
@click.option("--data-path", default=None, type=click.Path(exists=True),
              help="Path to raw dataset (auto-detected if not specified)")
def preprocess_cmd(data_path: str | None):
    """Pre-aggregate the raw dataset into small summary files for fast scanning."""
    filepath = Path(data_path) if data_path else find_dataset()
    monthly_path, procedure_path = preprocess(filepath)
    click.echo(f"\nPreprocessing complete. Run 'python cli.py scan' to analyze.")


@cli.command()
@click.option("--threshold", default=0.3, type=float,
              help="Minimum anomaly score to include (0.0-1.0)")
@click.option("--data-path", default=None, type=click.Path(exists=True),
              help="Path to dataset (auto-detected if not specified)")
@click.option("--top", default=50, type=int,
              help="Number of top results to display")
@click.option("--state", default=None, metavar="STATE",
              help="Filter to providers in this state (2-letter abbreviation, e.g. WV). "
                   "Requires NPPES zip in data/raw/.")
def scan(threshold: float, data_path: str | None, top: int, state: str | None):
    """Scan the dataset for suspicious providers."""
    filepath = Path(data_path) if data_path else find_dataset()

    preprocessed = find_preprocessed()
    if preprocessed:
        monthly_path, procedure_path = preprocessed
        click.echo(f"Using preprocessed data from {monthly_path.parent}")
        click.echo(f"Scanning: {filepath}")
    else:
        monthly_path = procedure_path = None
        click.echo(f"Scanning: {filepath}")
        click.echo("Tip: Run 'python cli.py preprocess' first for much faster scans.")

    state_npis: set[str] | None = None
    if state:
        state = state.upper().strip()
        from data.nppes import find_nppes_zip, load_npi_state_map
        try:
            nppes_path = find_nppes_zip()
        except FileNotFoundError as e:
            raise click.ClickException(str(e))
        click.echo(f"Loading NPPES state filter for {state}...")
        npi_state_map = load_npi_state_map(nppes_path)
        state_npis = {npi for npi, s in npi_state_map.items() if s == state}
        click.echo(f"Found {len(state_npis):,} providers registered in {state}")

    results = scan_all(filepath, threshold=threshold,
                       monthly_path=monthly_path, procedure_path=procedure_path,
                       state_npis=state_npis)

    # Save full results to CSV
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "scan_results.csv"

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "npi", "score", "num_flags", "flag_types"])
        for i, result in enumerate(results, 1):
            flag_types = ", ".join(set(f.flag_type.value for f in result.red_flags))
            writer.writerow([
                i, result.npi,
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
              help="Path to dataset (auto-detected if not specified)")
def profile(npi: str, data_path: str | None):
    """Build an evidence dossier for a specific provider."""
    filepath = Path(data_path) if data_path else find_dataset()

    preprocessed = find_preprocessed()
    monthly_path = preprocessed[0] if preprocessed else None
    procedure_path = preprocessed[1] if preprocessed else None

    results = scan_all(
        filepath,
        threshold=0.0,
        monthly_path=monthly_path,
        procedure_path=procedure_path,
        state_npis={npi},
    )
    scan_result = results[0] if results else None

    dossier = build_dossier(filepath, npi, scan_result, monthly_path=monthly_path)

    # Generate PDF
    pdf_path = generate_dossier_pdf(dossier)
    click.echo(f"\nDossier generated: {pdf_path}")

    # Print summary to terminal
    click.echo(f"\n{'=' * 60}")
    click.echo(f"Provider NPI: {npi}")
    if dossier.provider.name:
        click.echo(f"Provider Name: {dossier.provider.name}")
    if dossier.provider.specialty:
        click.echo(f"Specialty: {dossier.provider.specialty}")

    s = dossier.claims_summary
    if s:
        if "total_claims" in s:
            click.echo(f"Total Claims: {s['total_claims']:,}")
        if "total_paid" in s:
            click.echo(f"Total Paid: ${s['total_paid']:,.2f}")
        if "date_range_start" in s:
            click.echo(f"Date Range: {s['date_range_start']} to {s['date_range_end']}")

    if dossier.scan_result.red_flags:
        click.echo(f"\nRed Flags ({len(dossier.scan_result.red_flags)}):")
        for flag in dossier.scan_result.red_flags:
            click.echo(f"  - [{flag.severity:.0%}] {flag.description}")

    pc = dossier.peer_comparison
    if pc and "provider_percentile" in pc:
        click.echo(f"\nPeer Ranking: {pc['provider_percentile']}th percentile")

    click.echo(f"{'=' * 60}")


@cli.command()
@click.argument("query")
@click.option("--state", default=None, metavar="STATE",
              help="Narrow results to a specific state (2-letter abbreviation, e.g. WV)")
def lookup(query: str, state: str | None):
    """Search the NPPES provider registry by name or NPI.

    QUERY can be a provider name (case-insensitive substring match) or a
    10-digit NPI for an exact lookup.

    \b
    Examples:
      python cli.py lookup "Acme Health Clinic"
      python cli.py lookup "Acme Health Clinic" --state WV
      python cli.py lookup 1234567890
    """
    from data.nppes import find_nppes_zip, search_providers

    try:
        nppes_path = find_nppes_zip()
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    is_npi = query.strip().isdigit() and len(query.strip()) == 10
    if is_npi:
        click.echo(f"Looking up NPI {query.strip()}...")
    else:
        location = f" in {state.upper()}" if state else ""
        click.echo(f"Searching for '{query}'{location}...")

    results = search_providers(nppes_path, query, state=state)

    if not results:
        click.echo("No providers found.")
        return

    click.echo(f"\nFound {len(results)} provider(s):\n")
    click.echo("-" * 70)
    for r in results:
        click.echo(f"  NPI:      {r['npi']}")
        if r["name"]:
            click.echo(f"  Name:     {r['name']}")
        addr_parts = [p for p in [r["address"], r["city"], r["state"], r["zip"]] if p]
        if addr_parts:
            click.echo(f"  Address:  {', '.join(addr_parts)}")
        if r["taxonomy"]:
            click.echo(f"  Taxonomy: {r['taxonomy']}")
        click.echo(f"  → python cli.py profile {r['npi']}  to generate dossier")
        click.echo()



@cli.command("spark-scan")
@click.option("--threshold", default=0.3, type=float,
              help="Minimum anomaly score to include (0.0-1.0)")
@click.option("--data-path", default=None, type=click.Path(exists=True),
              help="Path to dataset (auto-detected if not specified)")
@click.option("--top", default=50, type=int,
              help="Number of top results to display")
def spark_scan(threshold: float, data_path: str | None, top: int):
    """Scan the dataset using PySpark (distributed-ready version of 'scan')."""
    try:
        from spark.loader import (
            build_monthly_summary,
            build_procedure_summary,
            get_or_create_session,
            load_claims,
        )
        from spark.anomalies import run_all_detectors
    except ImportError:
        click.echo("PySpark is not installed. Run: pip install pyspark", err=True)
        raise SystemExit(1)

    filepath = Path(data_path) if data_path else find_dataset()

    click.echo(f"Starting Spark session...")
    spark = get_or_create_session()
    spark.sparkContext.setLogLevel("WARN")

    click.echo(f"Loading claims: {filepath}")
    df = load_claims(spark, filepath)

    click.echo("Building monthly summary...")
    monthly_df = build_monthly_summary(df)

    click.echo("Building procedure summary...")
    procedure_df = build_procedure_summary(df)

    click.echo("Running anomaly detectors...")
    results_df = run_all_detectors(monthly_df, procedure_df, threshold=threshold)

    results = results_df.collect()
    click.echo(f"\nFound {len(results)} suspicious providers above threshold {threshold}")

    # Save to CSV
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "spark_scan_results.csv"
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "npi", "score", "num_flags", "flag_types"])
        for i, row in enumerate(results[:top], 1):
            writer.writerow([i, row["npi"], f"{row['overall_score']:.3f}",
                             row["num_flags"], row["flag_types"]])
    click.echo(f"Results saved to {output_path}")

    click.echo(f"\nTop {min(top, len(results))} suspicious providers:")
    click.echo("-" * 80)
    for i, row in enumerate(results[:top], 1):
        click.echo(f"  {i:3d}. NPI {row['npi']} | Score: {row['overall_score']:.0%} | "
                   f"Flags: {row['num_flags']} ({row['flag_types']})")

    spark.stop()


if __name__ == "__main__":
    cli()
