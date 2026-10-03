"""
Unified FastAPI interface for the Big Data final project.

This API is only a unified execution layer over existing
Midterm and Phase 2 functions. It does not implement a
separate ingestion backend.

Required endpoints:
GET  /health
POST /ingest
POST /indexes
GET  /queries
GET  /queries/{name}
GET  /aggregations
GET  /aggregations/{name}
POST /refresh-mv
GET  /jobs
POST /jobs/{name}/run
"""

import subprocess
import sys

from pathlib import Path
from typing import Optional

from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Request,
)

from pydantic import (
    BaseModel,
    Field,
)

from pymongo import MongoClient


from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
)

from src.phase2.queries import (
    QUERY_CATALOG,
    get_query_catalog,
    run_query,
)

from src.phase2.indexes import (
    create_phase2_indexes,
    get_index_catalog,
)

from src.phase2.aggregations import (
    AGGREGATION_CATALOG,
    get_aggregation_catalog,
    run_aggregation,
)

from src.phase2.materialized_views import (
    get_status as get_mv_status,
    refresh_materialized_views,
)

from src.phase2.jobs import (
    JOB_DEFINITIONS,
    get_jobs,
    run_job,
)


PROJECT_ROOT = (
    Path(
        __file__
    )
    .resolve()
    .parents[1]
)


app = FastAPI(
    title=(
        "Big Data Hybrid Pipeline - "
        "Unified Final API"
    ),
    description=(
        "Unified grading and execution interface "
        "for Midterm Pipeline + Final Phase 2."
    ),
    version="2.0.0",
)


class IngestRequest(BaseModel):
    """
    Input for the existing Midterm ingestion gateway.
    """

    input_path: str = Field(
        ...,
        min_length=1,
        description=(
            "CSV file path passed to the existing "
            "src.main Midterm pipeline."
        ),
    )

    batch_size: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Optional Python Batch size. "
            "If omitted, src.main uses its existing default."
        ),
    )


def _resolve_input_path(
    raw_path: str,
) -> Path:
    """
    Resolve a dynamic CSV path without using
    any fixed project dataset filename.
    """

    path = Path(
        raw_path
    ).expanduser()

    if not path.is_absolute():
        path = (
            PROJECT_ROOT
            / path
        )

    path = path.resolve()

    if not path.exists():
        raise HTTPException(
            status_code=400,
            detail={
                "error":
                    "input_file_not_found",

                "input_path":
                    str(
                        path
                    ),
            },
        )

    if not path.is_file():
        raise HTTPException(
            status_code=400,
            detail={
                "error":
                    "input_path_is_not_a_file",

                "input_path":
                    str(
                        path
                    ),
            },
        )

    if (
        path.suffix.lower()
        != ".csv"
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "error":
                    "input_must_be_csv",

                "input_path":
                    str(
                        path
                    ),
            },
        )

    return path


@app.get(
    "/health",
    tags=["System"],
)
def health():
    """
    Verify API and MongoDB availability.
    """

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    try:
        client.admin.command(
            "ping"
        )

        db = client[
            MONGO_DATABASE
        ]

        collection = db[
            VALIDATED_COLLECTION
        ]

        validated_count = (
            collection
            .estimated_document_count()
        )

        mv_status = (
            get_mv_status()
        )

        return {
            "status":
                "ok",

            "api":
                "available",

            "mongodb":
                "available",

            "database":
                MONGO_DATABASE,

            "validated_collection":
                VALIDATED_COLLECTION,

            "validated_estimated_count":
                validated_count,

            "materialized_views_initialized":
                mv_status.get(
                    "initialized",
                    False,
                ),
        }

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "status":
                    "unavailable",

                "error_type":
                    type(
                        exc
                    ).__name__,

                "message":
                    str(
                        exc
                    ),
            },
        ) from exc

    finally:
        client.close()


@app.post(
    "/ingest",
    tags=["Midterm Pipeline"],
)
def ingest(
    request_body: IngestRequest,
):
    """
    Execute the EXISTING Midterm ingestion gateway.

    Equivalent to:
    python -m src.main --input <CSV_PATH>

    No alternative ingestion implementation exists here.
    """

    input_path = (
        _resolve_input_path(
            request_body.input_path
        )
    )

    command = [
        sys.executable,
        "-m",
        "src.main",
        "--input",
        str(
            input_path
        ),
    ]

    if (
        request_body.batch_size
        is not None
    ):
        command.extend(
            [
                "--batch-size",
                str(
                    request_body.batch_size
                ),
            ]
        )

    try:
        result = subprocess.run(
            command,
            cwd=str(
                PROJECT_ROOT
            ),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error":
                    "pipeline_execution_failed",

                "error_type":
                    type(
                        exc
                    ).__name__,

                "message":
                    str(
                        exc
                    ),
            },
        ) from exc

    response = {
        "gateway":
            "src.main",

        "input_path":
            str(
                input_path
            ),

        "batch_size":
            request_body.batch_size,

        "return_code":
            result.returncode,

        "success":
            result.returncode
            == 0,

        "stdout":
            result.stdout,

        "stderr":
            result.stderr,
    }

    if (
        result.returncode
        != 0
    ):
        raise HTTPException(
            status_code=500,
            detail=response,
        )

    return response


@app.post(
    "/indexes",
    tags=["Phase 2 - Indexes"],
)
def indexes():
    """
    Create or verify the three Phase 2 query indexes.
    """

    try:
        result = (
            create_phase2_indexes()
        )

        return {
            "status":
                "success",

            "required_phase2_indexes":
                get_index_catalog(),

            "result":
                result,
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error_type":
                    type(
                        exc
                    ).__name__,

                "message":
                    str(
                        exc
                    ),
            },
        ) from exc


@app.get(
    "/queries",
    tags=["Phase 2 - Queries"],
)
def queries():
    """
    List the five practical Phase 2 queries.
    """

    catalog = (
        get_query_catalog()
    )

    return {
        "query_count":
            len(
                catalog
            ),

        "queries":
            catalog,
    }


@app.get(
    "/queries/{name}",
    tags=["Phase 2 - Queries"],
)
def query_by_name(
    name: str,
    request: Request,
    limit: int = Query(
        default=50,
        ge=1,
        le=1000,
    ),
):
    """
    Execute one practical Phase 2 query.

    Query-specific parameters are passed as
    normal URL query parameters.
    """

    if name not in QUERY_CATALOG:
        raise HTTPException(
            status_code=404,
            detail={
                "error":
                    "unknown_query",

                "name":
                    name,

                "available_queries":
                    sorted(
                        QUERY_CATALOG
                    ),
            },
        )

    parameters = {
        key:
            value

        for (
            key,
            value,
        )
        in request.query_params.items()

        if key != "limit"
    }

    try:
        return run_query(
            name,
            parameters,
            limit=limit,
        )

    except (
        ValueError,
        KeyError,
    ) as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error":
                    "invalid_query_parameters",

                "message":
                    str(
                        exc
                    ),

                "required_parameters":
                    QUERY_CATALOG[
                        name
                    ][
                        "required_parameters"
                    ],
            },
        ) from exc

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error_type":
                    type(
                        exc
                    ).__name__,

                "message":
                    str(
                        exc
                    ),
            },
        ) from exc


@app.get(
    "/aggregations",
    tags=["Phase 2 - Aggregations"],
)
def aggregations():
    """
    List the five Phase 2 aggregation reports.
    """

    catalog = (
        get_aggregation_catalog()
    )

    return {
        "aggregation_count":
            len(
                catalog
            ),

        "aggregations":
            catalog,
    }


@app.get(
    "/aggregations/{name}",
    tags=["Phase 2 - Aggregations"],
)
def aggregation_by_name(
    name: str,
):
    """
    Execute one aggregation report independently.
    """

    if (
        name
        not in AGGREGATION_CATALOG
    ):
        raise HTTPException(
            status_code=404,
            detail={
                "error":
                    "unknown_aggregation",

                "name":
                    name,

                "available_aggregations":
                    sorted(
                        AGGREGATION_CATALOG
                    ),
            },
        )

    try:
        return (
            run_aggregation(
                name
            )
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error_type":
                    type(
                        exc
                    ).__name__,

                "message":
                    str(
                        exc
                    ),
            },
        ) from exc


@app.post(
    "/refresh-mv",
    tags=["Phase 2 - Materialized Views"],
)
def refresh_mv():
    """
    Run incremental Materialized-View refresh.
    """

    try:
        result = (
            refresh_materialized_views()
        )

        return {
            "status":
                "success",

            "result":
                result,
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error_type":
                    type(
                        exc
                    ).__name__,

                "message":
                    str(
                        exc
                    ),
            },
        ) from exc


@app.get(
    "/jobs",
    tags=["Phase 2 - Jobs"],
)
def jobs():
    """
    List scheduled jobs and latest execution logs.
    """

    try:
        return get_jobs()

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error_type":
                    type(
                        exc
                    ).__name__,

                "message":
                    str(
                        exc
                    ),
            },
        ) from exc


@app.post(
    "/jobs/{name}/run",
    tags=["Phase 2 - Jobs"],
)
def run_named_job(
    name: str,
):
    """
    Manually execute one scheduled job.
    """

    if (
        name
        not in JOB_DEFINITIONS
    ):
        raise HTTPException(
            status_code=404,
            detail={
                "error":
                    "unknown_job",

                "name":
                    name,

                "available_jobs":
                    sorted(
                        JOB_DEFINITIONS
                    ),
            },
        )

    try:
        result = run_job(
            name,
            trigger_source="manual",
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error_type":
                    type(
                        exc
                    ).__name__,

                "message":
                    str(
                        exc
                    ),
            },
        ) from exc

    if not result.get(
        "success",
        False,
    ):
        raise HTTPException(
            status_code=500,
            detail=result,
        )

    return result


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api.app:app",
        host="127.0.0.1",
        port=8000,
        reload=False,
    )
