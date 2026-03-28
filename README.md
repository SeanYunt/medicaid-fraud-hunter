# Medicaid Fraud Hunter

A CLI tool that scans public HHS Medicaid provider spending data to flag potentially fraudulent billing patterns and generate investigation-ready PDF dossiers.

## Quick Start

**With Docker (recommended):**

```bash
docker build -t medicaid-fraud-hunter .

docker run --rm -v "${PWD}/data:/app/data" -v "${PWD}/output:/app/output" medicaid-fraud-hunter preprocess
docker run --rm -v "${PWD}/data:/app/data" -v "${PWD}/output:/app/output" medicaid-fraud-hunter scan --top 50
docker run --rm -v "${PWD}/data:/app/data" -v "${PWD}/output:/app/output" medicaid-fraud-hunter profile <NPI>
```

**Without Docker:**

```bash
pip install -r requirements.txt

python cli.py preprocess
python cli.py scan --top 50
python cli.py profile <NPI>
```

## Data Setup

A 1,000-provider sample (`medicaid-provider-spending-sample.parquet`, 2.8 MB) is included for development and proof-of-concept use. For full analysis, download the complete dataset (~2.8 GB):

1. Go to [data.cms.gov](https://data.cms.gov/) and search for **"Medicaid Provider Spending"** (also listed under Medicare/Medicaid claims data).
2. Download the Parquet or CSV export.
3. Place the file in `data/raw/`:
   ```
   data/raw/medicaid-provider-spending.parquet
   ```
   The tool auto-detects any `.parquet` or `.csv` file in that directory (largest file wins).

4. Run preprocessing to build the fast-scan summaries:
   ```bash
   python cli.py preprocess
   # or
   docker run --rm -v "${PWD}/data:/app/data" -v "${PWD}/output:/app/output" medicaid-fraud-hunter preprocess
   ```

The `data/raw/` and `data/processed/` directories are excluded from git (see `.gitignore`).

## Data Source

Uses the [HHS Medicaid Provider Spending](https://data.cms.gov/) dataset (public). The raw file contains aggregated Medicaid claims with columns for provider NPIs, procedure codes, service months, beneficiary counts, claim counts, and payment amounts.

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

### `spark-scan`

Same as `scan` but executes via PySpark in `local[*]` mode. Produces identical results and writes to `output/spark_scan_results.csv`. Designed to run on a cluster as data volume grows.

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
# Local
python -m pytest tests/ -v

# Docker
docker run --rm --entrypoint python medicaid-fraud-hunter -m pytest tests/ -v
```

27 tests covering all detectors, data loading, dossier generation, and PDF output using synthetic test fixtures.

## Tech Stack

- **Polars** — fast, memory-efficient dataframe processing with lazy evaluation
- **PySpark** — distributed-ready anomaly detection (`spark-scan`); runs locally via `local[*]`
- **Click** — CLI framework
- **ReportLab** — PDF generation
- **Pytest** — testing
- **Docker** — containerized runtime with JDK 17 + Python 3.11 (Eclipse Temurin base image)
