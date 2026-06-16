# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt          # CLI + Spark + tests
pip install -r api-requirements.txt      # API server only

# CLI pipeline (run in order)
python cli.py preprocess                 # Build fast-scan summaries from raw dataset
python cli.py scan --top 50             # Scan for suspicious providers
python cli.py scan --top 50 --state WV  # Filter to one state (requires NPPES zip)
python cli.py profile <NPI>             # Build PDF dossier for a specific provider
python cli.py lookup "Provider Name"    # Search NPPES registry by name or NPI
python cli.py spark-scan                # PySpark version of scan

# API server
uvicorn api.main:app --port 8084        # Dev server
docker-compose up -d                    # Production (API on :8084; serve web/ separately on :8083)

# Tests
python -m pytest tests/ -v              # All 64 tests
python -m pytest tests/test_anomalies.py -v   # Single file
python -m pytest tests/test_anomalies.py::test_revenue_outlier -v  # Single test
```

## Architecture

### Data Flow

```
data/raw/*.parquet (2.8GB HHS dataset)
    ↓ preprocess
data/processed/provider_monthly.parquet   (~1MB)
data/processed/provider_procedure.parquet (~1MB)
    ↓ scan_all()
ScanResult list (scored & ranked)
    ↓ build_dossier() + NPPES API
Dossier dataclass
    ↓ generate_dossier_pdf()
output/dossiers/dossier_<NPI>_<timestamp>.pdf
```

**Preprocessed summaries are the key optimization.** Without them, scanning reads the full 2.8GB file. With them, `scan_all()` reads only ~2MB. The `profile` command still reads the raw file per-provider using PyArrow pushdown filters.

### Module Map

| Module | Purpose |
|---|---|
| `data/models.py` | Core dataclasses: `Provider`, `RedFlag`, `ScanResult`, `Dossier`, `RedFlagType` enum |
| `data/loader.py` | `load_claims()` (CSV/Parquet → DataFrame), `preprocess()`, column normalization |
| `data/fetch.py` | `find_dataset()` (auto-detect largest file in `data/raw/`), `lookup_npi()` (NPPES API) |
| `data/nppes.py` | Local NPPES zip parsing for state-filter and provider search |
| `scanner/anomalies.py` | 6 detectors + `scan_all()` orchestrator |
| `scanner/hcpcs.py` | HCPCS code description lookup |
| `spark/anomalies.py` | PySpark versions of the same 4 detectors (no NOS/upcoding in Spark path) |
| `spark/loader.py` | Spark session creation and claims loading |
| `profiler/dossier.py` | `build_dossier()` — assembles claims summary, peer comparison, timeline |
| `reports/pdf.py` | ReportLab PDF generation |
| `api/main.py` | FastAPI routes: `/api/scan`, `/api/profile`, `/api/lookup`, `/api/status`, `/api/dossiers` |
| `api/stream.py` | SSE streaming: captures stdout in a background thread, emits as SSE events |
| `cli.py` | Click CLI wiring all the above together |

### Column Normalization

The HHS dataset uses SCREAMING_SNAKE_CASE column names. `data/loader._normalize()` renames them to internal names immediately on load. Always use internal names in all code beyond the loader:

| Internal | HHS Raw |
|---|---|
| `npi` | `BILLING_PROVIDER_NPI_NUM` |
| `procedure_code` | `HCPCS_CODE` |
| `service_month` | `CLAIM_FROM_MONTH` |
| `total_claims` | `TOTAL_CLAIMS` |
| `total_paid` | `TOTAL_PAID` |

**NPI type gotcha:** The HHS Parquet stores NPI as `int64`. `load_claims_for_provider()` handles this by casting the filter value to match the schema type. Everywhere else, NPI is `str`.

### Scoring Formula

```
score = min(1.0, max_severity × 0.4 + distinct_types × 0.15 + scheme_types × 0.2)
```

`scheme_types` = count of flags from `{NOS_CODE_CONCENTRATION, UPCODING_TRAJECTORY}`. These get extra weight because they name a specific fraud pattern rather than being generic statistical outliers.

### SSE Streaming (API)

`api/stream.py` redirects `sys.stdout` to a line-buffered queue in a background thread. The API endpoints yield SSE events: `{"type": "progress", "msg": str}` for each output line, then `{"type": "done", "result": ...}` or `{"type": "error", "msg": str}`. A mutex (`_op_lock`) prevents concurrent scans from stomping each other's stdout capture.

### Detector Parity: Pandas vs Spark

`scanner/anomalies.py` and `spark/anomalies.py` implement the same detectors but the Spark path only covers 4 of 6 (no NOS concentration or upcoding trajectory). All thresholds are declared as module-level constants and must be kept in sync between the two files.

### Test Fixtures

`tests/conftest.py` generates synthetic HHS-format CSV data covering all detector cases:
- `CLEAN_NPI` — normal billing (negative control)
- `VOLUME_NPI` — 5,000 claims in one month
- `REVENUE_NPI` — $2M/month (far above peers)
- `SPIKE_NPI` — 20× billing spike in month 7
- `CONSISTENCY_NPI` — 81% billing concentration at 0% rate variation

20 filler providers ensure z-score statistics are meaningful.

## Data Setup

Place the HHS Medicaid Provider Spending dataset in `data/raw/` as either `.parquet` or `.csv`. The loader auto-selects the largest file. Run `python cli.py preprocess` before scanning.

For state filtering (`--state WV`), download the NPPES Data Dissemination zip from CMS and place it in `data/raw/`.

`data/raw/` and `data/processed/` are gitignored.

## Docker

Two Dockerfiles:
- `Dockerfile` — CLI + Spark (Eclipse Temurin JDK 17 + Python 3.11, needed for PySpark)
- `Dockerfile.api` — API only (Python 3.11-slim, no JDK, much smaller image)

`docker-compose.yml` runs only the API image on port 8084. The web UI (`web/`) and dossier PDFs must be served separately on port 8083.
