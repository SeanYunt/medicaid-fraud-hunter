"""FastAPI layer for Medicaid Fraud Hunter.

Wraps scan, profile, and lookup as HTTP endpoints.
Scan and profile stream progress via Server-Sent Events (SSE).
"""

import json
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Generator

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from data.fetch import find_dataset
from data.loader import find_preprocessed
from data.nppes import find_nppes_zip, load_npi_state_map, search_providers
from profiler.dossier import build_dossier
from reports.pdf import generate_dossier_pdf
from scanner.anomalies import scan_all

from .stream import stream_operation

DOSSIERS_DIR = Path(os.environ.get("DOSSIERS_DIR", "/app/output/dossiers"))

# ---------------------------------------------------------------------------
# Optional IP-based quota (set SCAN_QUOTA=N to allow N ops/IP/24h; 0 = off)
# QUOTA_ALLOWLIST is a comma-separated list of IPs that bypass the limit.
# ---------------------------------------------------------------------------
_QUOTA_LIMIT = int(os.environ.get("SCAN_QUOTA", "0"))
_QUOTA_ALLOWLIST: set[str] = {
    ip.strip() for ip in os.environ.get("QUOTA_ALLOWLIST", "").split(",") if ip.strip()
}
_quota_log: dict[str, list[float]] = defaultdict(list)


def _enforce_quota(request: Request) -> None:
    """Raise 429 if IP has exhausted its daily quota. No-op when SCAN_QUOTA=0."""
    if _QUOTA_LIMIT <= 0:
        return
    ip = (request.client.host if request.client else "unknown")
    if ip in _QUOTA_ALLOWLIST:
        return
    now = time.time()
    _quota_log[ip] = [t for t in _quota_log[ip] if now - t < 86_400]
    if len(_quota_log[ip]) >= _QUOTA_LIMIT:
        raise HTTPException(
            status_code=429,
            detail=(
                f"Free access limit of {_QUOTA_LIMIT} scan(s) per 24 hours reached. "
                "Contact sean@blackdiamondconsulting.ai for continued access."
            ),
        )
    _quota_log[ip].append(now)


app = FastAPI(title="Medicaid Fraud Hunter", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------

class ScanRequest(BaseModel):
    threshold: float = Field(0.3, ge=0.0, le=1.0)
    top: int = Field(50, ge=1, le=500)
    state: str | None = Field(None, min_length=2, max_length=2)


class ProfileRequest(BaseModel):
    npi: str = Field(..., min_length=10, max_length=10, pattern=r"^\d{10}$")
    force: bool = Field(False)


class LookupRequest(BaseModel):
    query: str = Field(..., min_length=1)
    state: str | None = Field(None, min_length=2, max_length=2)


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def _serialize_scan_result(result) -> dict:
    return {
        "npi": result.npi,
        "overall_score": round(result.overall_score, 3),
        "num_flags": len(result.red_flags),
        "flag_types": sorted({f.flag_type.value for f in result.red_flags}),
        "red_flags": [
            {
                "flag_type": f.flag_type.value,
                "description": f.description,
                "severity": round(f.severity, 3),
                "evidence": f.evidence,
            }
            for f in result.red_flags
        ],
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/api/status")
def status():
    """Health check — confirms API is up and preprocessed data is available."""
    preprocessed = find_preprocessed()
    try:
        dataset = find_dataset()
        dataset_name = dataset.name
    except FileNotFoundError:
        dataset_name = None
    return {
        "status": "ok",
        "dataset": dataset_name,
        "preprocessed": preprocessed is not None,
    }


@app.post("/api/scan")
def scan(req: ScanRequest, request: Request):
    """Scan for suspicious providers. Streams SSE progress, then results."""
    _enforce_quota(request)

    def run() -> dict:
        filepath = find_dataset()
        preprocessed = find_preprocessed()
        monthly_path = preprocessed[0] if preprocessed else None
        procedure_path = preprocessed[1] if preprocessed else None

        state_npis: set[str] | None = None
        if req.state:
            state = req.state.upper()
            nppes_path = find_nppes_zip()
            npi_state_map = load_npi_state_map(nppes_path)
            state_npis = {npi for npi, s in npi_state_map.items() if s == state}

        results = scan_all(
            filepath,
            threshold=req.threshold,
            monthly_path=monthly_path,
            procedure_path=procedure_path,
            state_npis=state_npis,
        )

        payload = {
            "total": len(results),
            "providers": [_serialize_scan_result(r) for r in results[: req.top]],
            "params": {"threshold": req.threshold, "top": req.top, "state": req.state},
        }

        DOSSIERS_DIR.mkdir(parents=True, exist_ok=True)
        last_scan_path = DOSSIERS_DIR / "last_scan_result.json"
        last_scan_path.write_text(json.dumps(payload), encoding="utf-8")

        return payload

    def event_stream() -> Generator[str, None, None]:
        yield from stream_operation(run)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/api/last-scan")
def last_scan():
    """Return the most recent scan results, persisted across connections."""
    path = DOSSIERS_DIR / "last_scan_result.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="No scan results available yet.")
    return json.loads(path.read_text(encoding="utf-8"))


@app.post("/api/profile")
def profile(req: ProfileRequest, request: Request):
    """Build a dossier for a specific NPI. Streams SSE progress, then PDF info."""
    _enforce_quota(request)

    def run() -> dict:
        if not req.force:
            existing = sorted(DOSSIERS_DIR.glob(f"dossier_{req.npi}_*.pdf"), reverse=True)
            if existing:
                pdf_path = existing[0]
                print(f"Cached dossier found: {pdf_path.name} (use force=true to regenerate)")
                return {
                    "npi": req.npi,
                    "provider_name": None,
                    "overall_score": None,
                    "num_flags": None,
                    "pdf_filename": pdf_path.name,
                    "scan_result": None,
                    "cached": True,
                }

        filepath = find_dataset()
        preprocessed = find_preprocessed()
        monthly_path = preprocessed[0] if preprocessed else None
        procedure_path = preprocessed[1] if preprocessed else None

        results = scan_all(
            filepath,
            threshold=0.0,
            monthly_path=monthly_path,
            procedure_path=procedure_path,
            state_npis={req.npi},
        )
        scan_result = results[0] if results else None

        dossier = build_dossier(filepath, req.npi, scan_result, monthly_path=monthly_path)
        pdf_path = generate_dossier_pdf(dossier, output_dir=DOSSIERS_DIR)

        return {
            "npi": req.npi,
            "provider_name": dossier.provider.name,
            "overall_score": round(dossier.scan_result.overall_score, 3),
            "num_flags": len(dossier.scan_result.red_flags),
            "pdf_filename": pdf_path.name,
            "scan_result": _serialize_scan_result(dossier.scan_result),
            "cached": False,
        }

    def event_stream() -> Generator[str, None, None]:
        yield from stream_operation(run)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/api/lookup")
def lookup(req: LookupRequest):
    """Search the NPPES registry by provider name or exact NPI. Returns immediately."""
    try:
        nppes_path = find_nppes_zip()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    results = search_providers(nppes_path, req.query, state=req.state)
    return {"results": results}


@app.get("/api/dossiers")
def list_dossiers():
    """List generated dossier PDFs, newest first."""
    DOSSIERS_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(DOSSIERS_DIR.glob("dossier_*.pdf"), reverse=True)
    return {
        "dossiers": [
            {
                "filename": f.name,
                "size_bytes": f.stat().st_size,
                # Extract NPI from filename: dossier_<NPI>_<timestamp>.pdf
                "npi": f.stem.split("_")[1] if len(f.stem.split("_")) >= 2 else "",
            }
            for f in files
        ]
    }
