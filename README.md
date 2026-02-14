# Medicaid Fraud Hunter

A CLI tool that scans public HHS Medicaid provider spending data to flag potentially fraudulent billing patterns and generate investigation-ready PDF dossiers.

## Quick Start

```bash
pip install -r requirements.txt

# One-time preprocessing (aggregates raw data into small summary files)
python cli.py preprocess --data-path data/raw/medicaid-provider-spending-sample.parquet

# Scan for suspicious providers
python cli.py scan --data-path data/raw/medicaid-provider-spending-sample.parquet --top 50

# Generate a detailed PDF dossier for a specific provider
python cli.py profile <NPI> --data-path data/raw/medicaid-provider-spending-sample.parquet
```

## Data Source

Uses the [HHS Medicaid Provider Spending](https://data.cms.gov/) dataset (public). The raw file contains aggregated Medicaid claims with columns for provider NPIs, procedure codes, service months, beneficiary counts, claim counts, and payment amounts.

A 1,000-provider sample (`medicaid-provider-spending-sample.parquet`, 2.8 MB) is included for development and proof-of-concept use. The full dataset is 2.8 GB.

## Anomaly Detectors

| Detector | What It Catches | Method |
|---|---|---|
| **Volume Impossibility** | >1,500 claims in a single month | Hard threshold on monthly claim count |
| **Revenue Outlier** | Abnormally high revenue per claim | Median/MAD comparison across all providers |
| **Billing Spike** | Sudden surge in a provider's own billing | Monthly paid vs provider's own average (5x+) |
| **Suspicious Consistency** | >90% of billing rows at identical dollar amount | Consistency ratio on non-zero paid amounts |

## Scoring

Providers are scored based on **corroborating evidence** from independent detectors, not raw flag count:

- 1 detector fires: max **70%** score
- 2 detectors fire: max **90%** score
- 3+ detectors fire: **100%** score

This prioritizes providers where multiple independent analytical methods point to the same conclusion — the cases most likely to hold up under investigation.

## Pipeline Commands

### `preprocess`

Reads the raw dataset once and writes two small summary parquet files (~1 MB combined), eliminating repeated disk I/O on the full file.

### `scan`

Runs all four anomaly detectors and outputs a ranked list of suspicious providers. Results are saved to `output/scan_results.csv`.

Options:
- `--threshold` (default 0.3): Minimum anomaly score to include (0.0-1.0)
- `--top` (default 50): Number of top results to display
- `--data-path`: Path to raw dataset (auto-detected if not specified)

### `profile <NPI>`

Generates a comprehensive PDF dossier for a specific provider including:
- Claims summary (totals, date range, top procedures)
- Peer comparison (percentile rank, z-score)
- Monthly billing timeline
- All detected red flags with severity and evidence

Output: `output/dossiers/dossier_<NPI>_<timestamp>.pdf`

## Testing

```bash
python -m pytest tests/ -v
```

27 tests covering all detectors, data loading, dossier generation, and PDF output using synthetic test fixtures.

## Tech Stack

- **Polars** — fast, memory-efficient dataframe processing with lazy evaluation
- **Click** — CLI framework
- **ReportLab** — PDF generation
- **Pytest** — testing
