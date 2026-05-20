# Medicaid Fraud Hunter

**Open-source analytics for detecting Medicaid billing fraud using publicly available government data.**

Medicaid fraud costs U.S. taxpayers an estimated **$100 billion per year**. This project uses public CMS datasets and statistical anomaly detection to surface providers whose billing patterns deviate sharply from their peers — generating investigation-ready PDF dossiers that can support enforcement actions, qui tam filings, and regulatory referrals.

---

## Business Value

### Built on Public Data — No FOIA Required

All analysis runs on datasets published by HHS/CMS at [data.cms.gov](https://data.cms.gov/). This means:

- **Zero access barriers.** Any researcher, attorney, or investigator can reproduce results.
- **Legally defensible sourcing.** Government-published data is admissible and credible.
- **Scalable coverage.** Millions of provider records analyzed in minutes on a laptop.

The tool is designed to do the statistical legwork that investigative teams would otherwise do manually — identifying the 0.1% of providers whose patterns warrant a closer look.

### Aligned with the DOGE Medicaid Fraud Initiative

In early 2025, DOGE launched an effort to crowdsource Medicaid fraud detection, explicitly inviting public submissions of suspected fraud leads. This tool is purpose-built for exactly that use case.

> *"DOGE's attempt to crowdsource Medicaid fraud scrutiny raises important questions about the future of healthcare fraud enforcement."*
> — [Health Law Advisor, 2025](https://www.healthlawadvisor.com/doges-attempt-to-crowdsource-medicaid-fraud-scrutiny-is-this-the-future-of-healthcare-fraud-investigations)

The corroborating-evidence scoring model prioritizes providers where multiple independent analytical signals converge — the cases most likely to survive legal scrutiny and support a successful enforcement action.

### Partnering with Investigators and Qui Tam Attorneys

I am actively seeking partnerships with:

- **Healthcare fraud investigators** (OIG, state Medicaid fraud units, private)
- **Qui tam / False Claims Act attorneys** looking for data-driven lead generation
- **Compliance officers** conducting internal audits

If you are working a Medicaid fraud matter and want access to this analysis — or want to discuss applying it to a specific state, specialty, or date range — please reach out:

**Sean Yunt** · [seanyunt@gmail.com](mailto:seanyunt@gmail.com)

Under the False Claims Act, qui tam relators who bring fraud to light can recover 15–30% of government recoveries. Data-driven leads generated from public records are a legitimate and increasingly recognized basis for such actions.

---

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

---

## Data Setup

1. Go to [data.cms.gov](https://data.cms.gov/) and search for **"Medicaid Provider Spending"**.
2. Download the Parquet or CSV export (~2.8 GB).
3. Place the file in `data/raw/`:
   ```
   data/raw/medicaid-provider-spending.parquet
   ```
   The tool auto-detects any `.parquet` or `.csv` file in that directory (largest file wins).

4. Run preprocessing to build fast-scan summaries:
   ```bash
   python cli.py preprocess
   ```

The `data/raw/` and `data/processed/` directories are excluded from git (see `.gitignore`).

---

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

---

## Pipeline Commands

### `preprocess`

Reads the raw dataset once and writes two small summary parquet files (~1 MB combined), eliminating repeated disk I/O on the full file.

### `scan`

Runs all four anomaly detectors and outputs a ranked list of suspicious providers to `output/scan_results.csv`.

Options:
- `--threshold` (default 0.3): Minimum anomaly score to include (0.0–1.0)
- `--top` (default 50): Number of top results to display
- `--data-path`: Path to raw dataset (auto-detected if not specified)

### `spark-scan`

Same as `scan` but executes via PySpark in `local[*]` mode. Produces identical results and writes to `output/spark_scan_results.csv`. Designed to run on a cluster as data volume grows.

### `profile <NPI>`

Generates a comprehensive PDF dossier for a specific provider including:
- Claims summary (totals, date range, top procedures)
- Peer comparison (percentile rank, z-score)
- Monthly billing timeline
- All detected red flags with severity and evidence

Output: `output/dossiers/dossier_<NPI>_<timestamp>.pdf`

---

## Testing

```bash
# Local
python -m pytest tests/ -v

# Docker
docker run --rm --entrypoint python medicaid-fraud-hunter -m pytest tests/ -v
```

27 tests covering all detectors, data loading, dossier generation, and PDF output.

---

## Tech Stack

- **Polars** — fast, memory-efficient dataframe processing with lazy evaluation
- **PySpark** — distributed-ready anomaly detection (`spark-scan`); runs locally via `local[*]`
- **Click** — CLI framework
- **ReportLab** — PDF generation
- **Pytest** — testing
- **Docker** — containerized runtime with JDK 17 + Python 3.11 (Eclipse Temurin base image)
