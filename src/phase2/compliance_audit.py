"""
Final Phase 2 Compliance Audit.

Read-only audit:
- Does not modify MongoDB.
- Does not run the 30M pipeline.
- Does not create indexes.
- Does not refresh Materialized Views.
"""

import json
import subprocess
from pathlib import Path

from api.app import app

from src.phase2.queries import QUERY_CATALOG
from src.phase2.indexes import get_index_catalog
from src.phase2.aggregations import AGGREGATION_CATALOG
from src.phase2.materialized_views import get_materialized_view_catalog
from src.phase2.jobs import JOB_DEFINITIONS


ROOT = Path(__file__).resolve().parents[2]

checks = []


def add(section, name, passed, detail):
    checks.append(
        {
            "section": section,
            "name": name,
            "passed": bool(passed),
            "detail": detail,
        }
    )

    print(
        f"[{'PASS' if passed else 'FAIL'}] "
        f"{section:<20} {name}"
    )

    if detail:
        print(
            f"       {detail}"
        )


def exists(relative_path):
    return (
        ROOT
        / relative_path
    ).exists()


def read_text(relative_path):
    path = (
        ROOT
        / relative_path
    )

    if not path.exists():
        return ""

    return path.read_text(
        encoding="utf-8-sig",
        errors="replace",
    )


print("=" * 88)
print("BIG DATA FINAL PHASE 2 - COMPLIANCE AUDIT")
print("MODE: READ ONLY")
print("=" * 88)
print()


# ------------------------------------------------------------------
# 1. QUERIES + INDEXES + EXPLAIN
# ------------------------------------------------------------------

expected_queries = {
    "orders_by_customer",
    "orders_by_date_range",
    "orders_by_city_and_date",
    "high_value_orders",
    "orders_by_payment_method",
}

add(
    "Queries/Indexes",
    "At least 5 practical queries",
    len(QUERY_CATALOG) >= 5
    and expected_queries.issubset(
        set(
            QUERY_CATALOG
        )
    ),
    f"Detected {len(QUERY_CATALOG)} queries.",
)


index_catalog = (
    get_index_catalog()
)

required_index_names = {
    "ix_phase2_customer_id",
    "ix_phase2_order_date",
    "ix_phase2_city_order_date",
}

actual_index_names = {
    item["name"]
    for item in index_catalog
}

add(
    "Queries/Indexes",
    "At least 3 Phase 2 indexes",
    required_index_names.issubset(
        actual_index_names
    ),
    (
        "Required indexes: "
        + ", ".join(
            sorted(
                required_index_names
            )
        )
    ),
)


compound_indexes = [
    item
    for item in index_catalog
    if (
        item.get(
            "compound"
        )
        is True
        or len(
            item.get(
                "keys",
                [],
            )
        ) >= 2
    )
]

add(
    "Queries/Indexes",
    "At least one Compound Index",
    len(
        compound_indexes
    ) >= 1,
    (
        "Compound indexes detected: "
        + ", ".join(
            item[
                "name"
            ]
            for item
            in compound_indexes
        )
    ),
)


explain_files = [
    "reports/phase2/"
    "explain_before_final_indexes.json",

    "reports/phase2/"
    "explain_after_final_indexes.json",

    "reports/phase2/"
    "explain_before_after_comparison.json",
]

missing_explain = [
    path
    for path
    in explain_files
    if not exists(
        path
    )
]

add(
    "Queries/Indexes",
    "Explain before/after evidence",
    not missing_explain,
    (
        "All 3 Explain evidence files exist."
        if not missing_explain
        else
        "Missing: "
        + ", ".join(
            missing_explain
        )
    ),
)


# ------------------------------------------------------------------
# 2. AGGREGATIONS
# ------------------------------------------------------------------

expected_aggregations = {
    "sales_by_city",
    "orders_by_status",
    "sales_by_payment_method",
    "delivery_type_summary",
    "monthly_sales",
}

add(
    "Aggregations",
    "At least 5 aggregation reports",
    len(
        AGGREGATION_CATALOG
    ) >= 5
    and expected_aggregations.issubset(
        set(
            AGGREGATION_CATALOG
        )
    ),
    (
        f"Detected "
        f"{len(AGGREGATION_CATALOG)} reports."
    ),
)


aggregation_evidence = [
    (
        "reports/phase2/"
        "aggregations/"
        f"{name}.json"
    )
    for name
    in expected_aggregations
]

missing_aggregations = [
    path
    for path
    in aggregation_evidence
    if not exists(
        path
    )
]

add(
    "Aggregations",
    "Actual result evidence for 5 reports",
    not missing_aggregations,
    (
        "All 5 report files exist."
        if not missing_aggregations
        else
        "Missing: "
        + ", ".join(
            missing_aggregations
        )
    ),
)


# ------------------------------------------------------------------
# 3. MATERIALIZED VIEWS
# ------------------------------------------------------------------

mv_catalog = (
    get_materialized_view_catalog()
)

if isinstance(
    mv_catalog,
    dict,
):
    mv_names = set(
        mv_catalog
    )
else:
    mv_names = {
        (
            item.get(
                "name"
            )
            or item.get(
                "collection"
            )
            or item.get(
                "view_name"
            )
        )
        for item
        in mv_catalog
        if isinstance(
            item,
            dict,
        )
    }

expected_mvs = {
    "monthly_sales_summary",
    "city_sales_summary",
}

add(
    "Materialized Views",
    "At least 2 Materialized Views",
    expected_mvs.issubset(
        mv_names
    ),
    (
        "Detected: "
        + ", ".join(
            sorted(
                name
                for name
                in mv_names
                if name
            )
        )
    ),
)


mv_evidence = [
    "reports/phase2/"
    "materialized_views_bootstrap.txt",

    "reports/phase2/"
    "materialized_views_refresh_no_changes.txt",

    "reports/phase2/"
    "mv_incremental_update_proof.json",
]

missing_mv_evidence = [
    path
    for path
    in mv_evidence
    if not exists(
        path
    )
]

add(
    "Materialized Views",
    "Bootstrap + incremental refresh evidence",
    not missing_mv_evidence,
    (
        "Bootstrap and incremental proof exist."
        if not missing_mv_evidence
        else
        "Missing: "
        + ", ".join(
            missing_mv_evidence
        )
    ),
)


proof_path = (
    ROOT
    / "reports/phase2/"
      "mv_incremental_update_proof.json"
)

mv_proof_pass = False

if proof_path.exists():
    try:
        proof = json.loads(
            proof_path.read_text(
                encoding="utf-8-sig"
            )
        )

        mv_proof_pass = (
            proof.get(
                "all_passed"
            )
            is True
        )

    except Exception:
        mv_proof_pass = False

add(
    "Materialized Views",
    "Incremental update proof PASS",
    mv_proof_pass,
    (
        "Isolated incremental proof passed."
        if mv_proof_pass
        else
        "Incremental proof did not report all_passed=true."
    ),
)


# ------------------------------------------------------------------
# 4. SCHEDULED JOBS
# ------------------------------------------------------------------

expected_jobs = {
    "refresh_materialized_views",
    "generate_daily_analytics_report",
}

add(
    "Scheduled Jobs",
    "At least 2 real scheduled jobs",
    expected_jobs.issubset(
        set(
            JOB_DEFINITIONS
        )
    ),
    (
        f"Detected "
        f"{len(JOB_DEFINITIONS)} jobs."
    ),
)


job_schedules_ok = (
    JOB_DEFINITIONS[
        "refresh_materialized_views"
    ][
        "schedule"
    ][
        "expression"
    ]
    == "0 * * * *"
    and
    JOB_DEFINITIONS[
        "generate_daily_analytics_report"
    ][
        "schedule"
    ][
        "expression"
    ]
    == "0 2 * * *"
)

add(
    "Scheduled Jobs",
    "Defined schedules",
    job_schedules_ok,
    (
        "Hourly MV refresh + daily analytics report."
    ),
)


job_evidence = [
    "reports/phase2/"
    "job_refresh_materialized_views_manual.txt",

    "reports/phase2/"
    "job_generate_daily_analytics_report_manual.txt",
]

missing_job_evidence = [
    path
    for path
    in job_evidence
    if not exists(
        path
    )
]

add(
    "Scheduled Jobs",
    "Manual-run execution evidence",
    not missing_job_evidence,
    (
        "Manual execution evidence exists for both jobs."
        if not missing_job_evidence
        else
        "Missing: "
        + ", ".join(
            missing_job_evidence
        )
    ),
)


# ------------------------------------------------------------------
# 5. UNIFIED FASTAPI
# ------------------------------------------------------------------

required_routes = {
    ("GET", "/health"),
    ("POST", "/ingest"),
    ("POST", "/indexes"),
    ("GET", "/queries"),
    ("GET", "/queries/{name}"),
    ("GET", "/aggregations"),
    ("GET", "/aggregations/{name}"),
    ("POST", "/refresh-mv"),
    ("GET", "/jobs"),
    ("POST", "/jobs/{name}/run"),
}

found_routes = set()

for route in app.routes:

    path = getattr(
        route,
        "path",
        None,
    )

    methods = getattr(
        route,
        "methods",
        None,
    )

    if not path or not methods:
        continue

    for method in methods:
        if method in {
            "GET",
            "POST",
        }:
            found_routes.add(
                (
                    method,
                    path,
                )
            )

missing_routes = (
    required_routes
    - found_routes
)

add(
    "Unified API",
    "All 10 required endpoints",
    not missing_routes,
    (
        "10/10 required routes detected."
        if not missing_routes
        else
        "Missing: "
        + str(
            sorted(
                missing_routes
            )
        )
    ),
)


swagger_ok = any(
    getattr(
        route,
        "path",
        None,
    )
    == "/docs"
    for route
    in app.routes
)

add(
    "Unified API",
    "Swagger /docs",
    swagger_ok,
    (
        "Swagger route exists."
        if swagger_ok
        else
        "Swagger route missing."
    ),
)


api_ingest_evidence = (
    "reports/phase2/api/"
    "isolated_ingest_verification.json"
)

api_ingest_pass = False

if exists(
    api_ingest_evidence
):
    try:
        evidence = json.loads(
            read_text(
                api_ingest_evidence
            )
        )

        api_ingest_pass = (
            evidence.get(
                "overall_pass"
            )
            is True
            and evidence.get(
                "gateway"
            )
            == "src.main"
        )

    except Exception:
        api_ingest_pass = False

add(
    "Unified API",
    "POST /ingest runtime proof",
    api_ingest_pass,
    (
        "Isolated runtime proof uses src.main and passed."
        if api_ingest_pass
        else
        "Runtime ingest proof missing or failed."
    ),
)


# ------------------------------------------------------------------
# 6. README + REQUIREMENTS + ENV
# ------------------------------------------------------------------

readme = read_text(
    "README.md"
)

required_readme_sections = [
    "## Final Phase 2",
    "## Unified FastAPI",
    "## Queries",
    "## Query Indexes",
    "### Explain Evidence",
    "## Aggregation Reports",
    "## Materialized Views",
    "## Scheduled Jobs",
    "## Environment Configuration",
    "## Final Testing",
]

missing_readme = [
    item
    for item
    in required_readme_sections
    if item not in readme
]

add(
    "GitHub/README",
    "README documents Phase 2",
    not missing_readme,
    (
        "All required Phase 2 README sections exist."
        if not missing_readme
        else
        "Missing: "
        + ", ".join(
            missing_readme
        )
    ),
)


requirements = (
    read_text(
        "requirements.txt"
    )
    .lower()
)

required_dependencies = [
    "pymongo==",
    "pyspark==",
    "pytest==",
    "apscheduler==",
    "fastapi==",
    "uvicorn==",
]

missing_dependencies = [
    dependency
    for dependency
    in required_dependencies
    if dependency
    not in requirements
]

add(
    "GitHub/README",
    "requirements.txt complete",
    not missing_dependencies,
    (
        "Required runtime/test dependencies are pinned."
        if not missing_dependencies
        else
        "Missing: "
        + ", ".join(
            missing_dependencies
        )
    ),
)


env_text = read_text(
    ".env.example"
)

env_required = [
    "MONGO_URI=",
    "MONGO_DATABASE=",
    "SMALL_FILE_THRESHOLD_MB=",
    "BATCH_SIZE=",
    "SPARK_MASTER=",
]

env_ok = (
    exists(
        ".env.example"
    )
    and all(
        item in env_text
        for item
        in env_required
    )
)

add(
    "GitHub/README",
    ".env.example present",
    env_ok,
    (
        "Safe example environment configuration exists."
        if env_ok
        else
        ".env.example is missing required variables."
    ),
)


# ------------------------------------------------------------------
# 7. AUTOMATED PHASE 2 TEST FILE
# ------------------------------------------------------------------

add(
    "Verification",
    "Phase 2 automated test suite",
    exists(
        "tests/test_phase2_final.py"
    ),
    (
        "tests/test_phase2_final.py exists."
    ),
)


# ------------------------------------------------------------------
# 8. GIT BASELINE SAFETY
# ------------------------------------------------------------------

try:
    result = subprocess.run(
        [
            "git",
            "rev-parse",
            "midterm-submission",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    tag_ok = (
        result.returncode
        == 0
    )

except Exception:
    tag_ok = False

add(
    "Git Safety",
    "midterm-submission baseline tag",
    tag_ok,
    (
        "Protected Midterm baseline tag exists."
        if tag_ok
        else
        "midterm-submission tag not found."
    ),
)


# ------------------------------------------------------------------
# FINAL RESULT
# ------------------------------------------------------------------

passed_count = sum(
    1
    for item
    in checks
    if item[
        "passed"
    ]
)

failed = [
    item
    for item
    in checks
    if not item[
        "passed"
    ]
]

print()
print("=" * 88)
print("FINAL COMPLIANCE SUMMARY")
print("=" * 88)

print(
    f"PASS : {passed_count}"
)

print(
    f"FAIL : {len(failed)}"
)

print()

if failed:

    print(
        "FAILED CHECKS:"
    )

    for item in failed:
        print(
            f"- {item['section']} / "
            f"{item['name']}"
        )

print()

overall = (
    len(
        failed
    )
    == 0
)

print(
    "FINAL PHASE 2 COMPLIANCE:",
    "PASS"
    if overall
    else "FAIL",
)

report = {
    "audit":
        "final_phase2_compliance",

    "mode":
        "read_only",

    "pass_count":
        passed_count,

    "fail_count":
        len(
            failed
        ),

    "overall_pass":
        overall,

    "checks":
        checks,
}

output_path = (
    ROOT
    / "reports/phase2/"
      "final_compliance_audit.json"
)

output_path.parent.mkdir(
    parents=True,
    exist_ok=True,
)

output_path.write_text(
    json.dumps(
        report,
        ensure_ascii=False,
        indent=2,
    ),
    encoding="utf-8",
)

print()
print(
    "Report:",
    output_path,
)
