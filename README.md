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

**Sean Yunt** · [sean@blackdiamondconsulting.ai](mailto:sean@blackdiamondconsulting.ai)

Under the False Claims Act, qui tam relators who bring fraud to light can recover 15–30% of government recoveries. Data-driven leads generated from public records are a legitimate and increasingly recognized basis for such actions.

---

## Quick Start

**Web UI (recommended):**

```bash
# Place your data file in data/raw/ first (see Data Setup below)
docker compose up -d
```

Then open `http://<server>:8084` — the UI provides Scan, Profile, Lookup, and Dossiers tabs.

**CLI with Docker:**

```bash
docker build -t medicaid-fraud-hunter .

docker run --rm -v "${PWD}/data:/app/data" -v "${PWD}/output:/app/output" medicaid-fraud-hunter preprocess
docker run --rm -v "${PWD}/data:/app/data" -v "${PWD}/output:/app/output" medicaid-fraud-hunter scan --top 50
docker run --rm -v "${PWD}/data:/app/data" -v "${PWD}/output:/app/output" medicaid-fraud-hunter profile <NPI>
```

**CLI without Docker:**

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

Six independent detectors run against every qualifying provider (≥$100,000 total paid):

| Detector | What It Catches | Method |
|---|---|---|
| **Volume Impossibility** | >1,500 claims in a single month | Hard threshold on monthly claim count |
| **Revenue Outlier** | Abnormally high revenue per claim | Median/MAD modified z-score vs national peers |
| **Billing Spike** | Sudden surge vs provider's own history | Monthly paid vs provider's own rolling average (5x+) |
| **Suspicious Consistency** | One procedure dominates billing at a robotically uniform per-claim rate | Single code ≥70% of total paid + rate CV <8% across ≥3 months |
| **NOS Code Concentration** | >25% of billing under vague "not otherwise specified" codes | Ratio of NOS/miscellaneous HCPCS codes to total paid |
| **Upcoding Trajectory** | Systematic shift toward higher-reimbursed E&M codes over time | Weighted average E&M level in early vs late billing periods (≥50 claims, ≥6 months) |

## Scoring

Providers are scored on **corroborating evidence** from independent detectors. Scheme-specific detectors (NOS concentration, upcoding trajectory) carry extra weight because they represent a named fraud pattern rather than a statistical outlier:

```
score = min(1.0, max_severity × 0.4 + distinct_types × 0.15 + scheme_types × 0.2)
```

A provider flagged by 2 statistical detectors + 1 scheme detector scores ≈ 90%. Three or more signals of any combination reach 100%. This prioritizes cases where multiple independent methods converge — the leads most likely to survive legal scrutiny.

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
- Claims summary (totals, date range, top procedures with HCPCS descriptions)
- Procedure breakdown by month (last 12 months, top 5 procedures per month with % of month total)
- Peer comparison (percentile rank, z-score vs all providers)
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

64 tests covering all detectors, data loading, dossier generation, PDF output, and the full Spark pipeline.

---

## Tech Stack

- **Pandas + PyArrow** — dataframe processing and Parquet I/O (migrated from Polars for broader CPU compatibility)
- **PySpark** — distributed-ready anomaly detection (`spark-scan`); runs locally via `local[*]`
- **FastAPI + uvicorn** — async API server with Server-Sent Events for streaming scan/profile progress
- **Click** — CLI framework
- **ReportLab** — PDF generation
- **Pytest** — testing
- **Docker** — two images: `Dockerfile` (CLI + Spark, Eclipse Temurin JDK 17 + Python 3.11) and `Dockerfile.api` (web UI + API, Python 3.11-slim, no JDK)
