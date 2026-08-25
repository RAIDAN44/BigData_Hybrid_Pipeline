from __future__ import annotations

import json
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import demo_adapter

VIVA_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = VIVA_ROOT.parent
FRONTEND = VIVA_ROOT / "frontend"
CONTENT = VIVA_ROOT / "content" / "content.json"
REPORTS = PROJECT_ROOT / "reports"
SCREENSHOTS = REPORTS / "screenshots"
UPLOADS = VIVA_ROOT / "runtime" / "uploads"
UPLOADS.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="VIVA_APP — Hybrid Big Data Pipeline", version="2.0")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")

SOURCE_ALLOWLIST = [
    ".gitignore", "README.md", "requirements.txt",
    "config/__init__.py", "config/settings.py",
    "src/__init__.py", "src/main.py", "src/file_router.py", "src/create_small_sample.py",
    "src/batch_loader.py", "src/spark_loader.py", "src/quality_rules.py",
    "src/elt_pipeline.py", "src/mongo_setup.py", "src/classification_dry_run.py",
    "src/upsert_update_proof.py", "src/final_compliance_audit.py", "src/metrics.py",
    "tests/__init__.py", "tests/test_assignment_contract.py", "tests/test_classification.py",
    "tests/test_cleaning_rules.py", "tests/test_elt_scalability_contract.py",
    "tests/test_final_main_contract.py", "tests/test_main_router_contract.py",
    "tests/test_quality_rules.py", "tests/test_rule_coverage.py",
    "tests/test_run_id_contract.py", "tests/test_spark_loader_contract.py",
    "docs/architecture.md", "docs/DESIGN_DECISIONS.md", "docs/EVIDENCE_MATRIX.md",
    "docs/DEMO_SCRIPT.md", "docs/VIVA_NOTES.md",
    "reports/results.json", "reports/results.md", "reports/classification_dry_run.json",
    "reports/elt_write_report_first_run.json", "reports/elt_write_report_final_idempotency.json",
    "reports/elt_write_report_large_30m_final.json", "reports/spark_large_run_final.json",
    "reports/upsert_update_proof.json", "reports/final_verification.json",
    "reports/final_compliance_audit.json", "reports/final_compliance_audit.md",
]
REPORT_ALLOWLIST = [x for x in SOURCE_ALLOWLIST if x.startswith("reports/")]

FALLBACK_RESULTS = {
    "router": {"threshold_mb": 200, "small_engine": "python_batch", "large_engine": "pyspark"},
    "runs": {
        "small_python_batch": {"file_size_mb": 41.77, "rows_read": 100000, "raw_loaded": 100000, "valid_count": 70002, "corrected_count": 21697, "quarantine_count": 8301, "batch_size": 5000, "raw_load_performance": {"elapsed_seconds": 28.873, "throughput_records_per_second": 3463.47, "batches": 20}, "quality_elt_performance": {"elapsed_seconds": 36.011, "throughput_records_per_second": 2776.93}},
        "large_pyspark": {"file_size_mb": 12650.32, "rows_read": 30000000, "raw_loaded": 30000000, "valid_count": 20994411, "corrected_count": 6501781, "quarantine_count": 2503808, "partitions": 99, "csv_corrupt_records": 0, "raw_load_performance": {"elapsed_seconds": 636.5871, "throughput_records_per_second": 47126.31, "input_partitions": 99, "output_partitions": 99}, "quality_elt_performance": {"elapsed_seconds": 28965.1154, "throughput_records_per_second": 1035.73}}
    },
    "consistency": {"small": {"equation": "100000 = 70002 + 21697 + 8301", "pass": True}, "large": {"equation": "30000000 = 20994411 + 6501781 + 2503808", "pass": True}}
}
FALLBACK_LARGE_RULES = {
    "ARABIC_DIGITS_TO_LATIN": 1670912, "ARABIC_DECIMAL_SEPARATOR_NORMALIZED": 1336617,
    "WHITESPACE_TRIMMED": 671538, "PHONE_NORMALIZED_YE": 670062, "DATE_NORMALIZED": 668151,
    "EMAIL_REPEATED_SYMBOLS": 667720, "ORDER_TOTAL_RECALCULATED": 500976,
    "CURRENCY_NORMALIZED_YER": 334967, "CURRENCY_SUFFIX_REMOVED": 333888,
    "KNOWN_PRICE_WORD_TO_NUMBER": 333569, "THOUSANDS_SEPARATOR_NORMALIZED": 333494,
    "NEGATIVE_QTY_DERIVED": 210021, "ITEM_TOTAL_RESIDUAL_DERIVED": 210018,
    "ITEM_PRICE_RESIDUAL_DERIVED": 210018, "STATUS_SYNONYM_NORMALIZED": 111094,
}
FALLBACK_LARGE_ERRORS = {
    "CORRUPTED_ITEMS_JSON": 419906, "MISSING_CUSTOMER_ID": 419474,
    "EMAIL_INVALID_UNRECOVERABLE": 418709, "DUPLICATE_ORDER_ID": 417584,
    "MULTIPLE_CONFLICTING_ERRORS": 221255, "INVALID_IMPOSSIBLE_DATE": 210524,
    "STATUS_UNKNOWN": 210194, "CURRENCY_UNKNOWN": 210190,
    "PHONE_INVALID_UNRECOVERABLE": 210042, "EMPTY_ITEMS": 209934,
    "TOTAL_UNKNOWN_UNRECOVERABLE": 209432, "MISSING_ORDER_ID": 209392,
}


def _safe_path(rel: str) -> Path:
    if rel not in SOURCE_ALLOWLIST:
        raise HTTPException(403, "File is not in the fixed read-only allowlist")
    p = (PROJECT_ROOT / rel).resolve()
    if PROJECT_ROOT.resolve() not in p.parents and p != PROJECT_ROOT.resolve():
        raise HTTPException(403, "Unsafe path")
    return p


def _json(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


@app.get("/api/health")
def health():
    return {"status": "ok", "project_root": str(PROJECT_ROOT), "viva_root": str(VIVA_ROOT)}


@app.get("/api/content")
def content():
    return JSONResponse(_json(CONTENT, {}))


@app.get("/api/results")
def results():
    # Historical official evidence is deliberately frozen for the viva UI.
    # A later LIVE run may update MongoDB and runtime reports, but must never
    # rewrite the official evidence shown on this page.
    snapshot = _json(VIVA_ROOT / "content" / "official_results_snapshot.json", None)
    if snapshot:
        return snapshot
    return {
        "source": "VIVA official snapshot (verified historical evidence)",
        "data": FALLBACK_RESULTS,
        "large_correction_counts": FALLBACK_LARGE_RULES,
        "large_error_counts": FALLBACK_LARGE_ERRORS,
        "large_correction_source": "VIVA official snapshot",
        "raw_historical_total": 30100000,
        "tests_passed": 53,
    }


@app.get("/api/source-list")
def source_list():
    return [{"path": rel, "exists": (PROJECT_ROOT / rel).exists()} for rel in SOURCE_ALLOWLIST]


@app.get("/api/source")
def source(rel: str):
    p = _safe_path(rel)
    if not p.exists() or not p.is_file():
        raise HTTPException(404, "File not found")
    try:
        text = p.read_text(encoding="utf-8-sig", errors="replace")
    except Exception as exc:
        raise HTTPException(500, str(exc))
    return {"path": rel, "text": text, "bytes": p.stat().st_size}


@app.get("/api/evidence")
def evidence():
    items = []
    if SCREENSHOTS.exists():
        for p in sorted(SCREENSHOTS.glob("*.png")):
            items.append({"name": p.name, "url": f"/api/evidence/{p.name}", "bytes": p.stat().st_size})
    return {"items": items}


@app.get("/api/evidence/{name}")
def evidence_file(name: str):
    if Path(name).name != name or not name.lower().endswith(".png"):
        raise HTTPException(403, "Unsafe evidence name")
    p = (SCREENSHOTS / name).resolve()
    if SCREENSHOTS.resolve() not in p.parents or not p.exists():
        raise HTTPException(404, "Evidence not found")
    return FileResponse(p)


@app.get("/api/mongo-status")
def mongo_status():
    result: dict[str, Any] = {"connected": False, "orders_validated": {}, "orders_raw": {}, "counts": {}}
    try:
        if str(PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(PROJECT_ROOT))
        from config.settings import MONGO_URI, MONGO_DATABASE  # type: ignore
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        try:
            client.admin.command("ping")
            db = client[MONGO_DATABASE]
            result["connected"] = True
            for col_name in ("orders_validated", "orders_raw"):
                info = db.command({"listCollections": 1, "filter": {"name": col_name}}).get("cursor", {}).get("firstBatch", [])
                options = (info[0].get("options", {}) if info else {})
                entry = {
                    "validator_exists": bool(options.get("validator")),
                    "validationLevel": options.get("validationLevel"),
                    "validationAction": options.get("validationAction"),
                }
                if col_name == "orders_validated":
                    entry["unique_order_id"] = any(dict(idx.get("key", {})) == {"order_id": 1} and idx.get("unique", False) for idx in db[col_name].list_indexes())
                result[col_name] = entry
            for name in ("orders_raw", "orders_validated", "orders_quarantine", "orders_validated_100k_evidence", "orders_quarantine_100k_evidence"):
                if name in db.list_collection_names():
                    result["counts"][name] = int(db.command("collStats", name).get("count", -1))
        finally:
            client.close()
    except Exception as exc:
        result["error"] = str(exc)
    return result


@app.post("/api/demo/upload")
async def demo_upload(file: UploadFile = File(...)):
    name = Path(file.filename or "upload.csv").name
    if not name.lower().endswith(".csv"):
        raise HTTPException(400, "CSV files only")
    token = uuid.uuid4().hex
    target = (UPLOADS / f"{token}_{name}").resolve()
    if UPLOADS.resolve() not in target.parents:
        raise HTTPException(403, "Unsafe upload path")
    with target.open("wb") as out:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
    demo_adapter.UPLOAD_REGISTRY[token] = target
    preflight = demo_adapter.inspect_csv(target)
    return {"token": token, "preflight": preflight}


@app.get("/api/demo/preflight/{token}")
def demo_preflight(token: str):
    try:
        return demo_adapter.inspect_csv(demo_adapter.get_upload(token))
    except Exception as exc:
        raise HTTPException(400, str(exc))


@app.get("/api/demo/modes")
def demo_modes():
    return demo_adapter.mode_status()


@app.post("/api/demo/run/{token}")
def demo_run(token: str, mode: str = "rehearsal"): # mode is explicit per run
    try:
        return {"job_id": demo_adapter.start_job(token, mode=mode)}
    except Exception as exc:
        raise HTTPException(400, str(exc))


@app.get("/api/demo/job/{job_id}")
def demo_job(job_id: str):
    try:
        return demo_adapter.get_job(job_id)
    except Exception as exc:
        raise HTTPException(404, str(exc))


@app.post("/api/demo/cleanup")
def demo_cleanup():
    return demo_adapter.cleanup_rehearsal_data()
