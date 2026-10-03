from pathlib import Path
from types import SimpleNamespace

import pytest

from api import app as api_app
from src.phase2.queries import (
    QUERY_CATALOG,
    get_query_catalog,
)
from src.phase2.indexes import (
    get_index_catalog,
)
from src.phase2.aggregations import (
    AGGREGATION_CATALOG,
    get_aggregation_catalog,
)
from src.phase2.materialized_views import (
    get_materialized_view_catalog,
)
from src.phase2.jobs import (
    JOB_DEFINITIONS,
)


ROOT = Path(__file__).resolve().parents[1]


def _catalog_names(catalog):
    if isinstance(catalog, dict):
        return set(catalog.keys())

    names = set()

    for item in catalog:
        if isinstance(item, dict):
            name = (
                item.get("name")
                or item.get("collection")
                or item.get("view_name")
            )

            if name:
                names.add(name)

    return names


def test_phase2_has_five_required_queries():
    expected = {
        "orders_by_customer",
        "orders_by_date_range",
        "orders_by_city_and_date",
        "high_value_orders",
        "orders_by_payment_method",
    }

    assert set(QUERY_CATALOG) == expected

    catalog = get_query_catalog()

    assert _catalog_names(catalog) == expected


def test_phase2_has_three_required_query_indexes():
    catalog = get_index_catalog()

    names = {
        item["name"]
        for item in catalog
    }

    assert names == {
        "ix_phase2_customer_id",
        "ix_phase2_order_date",
        "ix_phase2_city_order_date",
    }

    compound = [
        item
        for item in catalog
        if item.get("compound") is True
    ]

    assert len(compound) >= 1

    city_date = next(
        item
        for item in catalog
        if item["name"]
        == "ix_phase2_city_order_date"
    )

    assert city_date["compound"] is True

    assert len(
        city_date["keys"]
    ) == 2


def test_phase2_has_five_aggregations():
    expected = {
        "sales_by_city",
        "orders_by_status",
        "sales_by_payment_method",
        "delivery_type_summary",
        "monthly_sales",
    }

    assert set(
        AGGREGATION_CATALOG
    ) == expected

    catalog = (
        get_aggregation_catalog()
    )

    assert (
        _catalog_names(
            catalog
        )
        == expected
    )


def test_phase2_has_two_materialized_views():
    catalog = (
        get_materialized_view_catalog()
    )

    names = (
        _catalog_names(
            catalog
        )
    )

    assert names == {
        "monthly_sales_summary",
        "city_sales_summary",
    }


def test_phase2_has_two_scheduled_jobs():
    assert set(
        JOB_DEFINITIONS
    ) == {
        "refresh_materialized_views",
        "generate_daily_analytics_report",
    }

    refresh = JOB_DEFINITIONS[
        "refresh_materialized_views"
    ]

    daily = JOB_DEFINITIONS[
        "generate_daily_analytics_report"
    ]

    assert (
        refresh[
            "schedule"
        ][
            "expression"
        ]
        == "0 * * * *"
    )

    assert (
        daily[
            "schedule"
        ][
            "expression"
        ]
        == "0 2 * * *"
    )

    assert (
        refresh[
            "schedule"
        ][
            "timezone"
        ]
        == "UTC"
    )

    assert (
        daily[
            "schedule"
        ][
            "timezone"
        ]
        == "UTC"
    )


def test_unified_api_contains_all_required_routes():
    required = {
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

    found = set()

    for route in api_app.app.routes:
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
                found.add(
                    (
                        method,
                        path,
                    )
                )

    assert required.issubset(
        found
    )

    assert any(
        getattr(
            route,
            "path",
            None,
        )
        == "/docs"
        for route
        in api_app.app.routes
    )


def test_ingest_uses_existing_midterm_gateway(
    monkeypatch,
    tmp_path,
):
    csv_path = (
        tmp_path
        / "different_test_data.csv"
    )

    csv_path.write_text(
        "order_id,customer_id\n"
        "TEST-1,CUSTOMER-X\n",
        encoding="utf-8",
    )

    captured = {}

    def fake_run(
        command,
        **kwargs,
    ):
        captured[
            "command"
        ] = command

        captured[
            "kwargs"
        ] = kwargs

        return SimpleNamespace(
            returncode=0,
            stdout=(
                "HYBRID PIPELINE "
                "ENGINE EXECUTION: PASS"
            ),
            stderr="",
        )

    monkeypatch.setattr(
        api_app.subprocess,
        "run",
        fake_run,
    )

    body = api_app.IngestRequest(
        input_path=str(
            csv_path
        ),
        batch_size=321,
    )

    result = api_app.ingest(
        body
    )

    command = captured[
        "command"
    ]

    assert command[1:3] == [
        "-m",
        "src.main",
    ]

    assert "--input" in command

    assert str(
        csv_path.resolve()
    ) in command

    assert "--batch-size" in command

    assert "321" in command

    assert result[
        "gateway"
    ] == "src.main"

    assert result[
        "success"
    ] is True

    assert result[
        "return_code"
    ] == 0


def test_requirements_include_final_dependencies():
    text = (
        ROOT
        / "requirements.txt"
    ).read_text(
        encoding="utf-8-sig"
    ).lower()

    required = [
        "pymongo==",
        "pyspark==",
        "pytest==",
        "apscheduler==",
        "fastapi==",
        "uvicorn==",
    ]

    for dependency in required:
        assert dependency in text


def test_env_example_exists_and_has_no_real_secret():
    path = (
        ROOT
        / ".env.example"
    )

    assert path.exists()

    text = path.read_text(
        encoding="utf-8-sig"
    )

    required = [
        "MONGO_URI=",
        "MONGO_DATABASE=",
        "SMALL_FILE_THRESHOLD_MB=",
        "BATCH_SIZE=",
        "SPARK_MASTER=",
    ]

    for key in required:
        assert key in text

    lowered = text.lower()

    forbidden = [
        "password=",
        "secret=",
        "api_key=",
        "token=",
    ]

    for value in forbidden:
        assert value not in lowered


def test_readme_documents_all_phase2_sections():
    text = (
        ROOT
        / "README.md"
    ).read_text(
        encoding="utf-8-sig"
    )

    required = [
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
        "POST /ingest",
        "GET  /health",
        "python -m src.main",
        "python -m src.phase2.jobs",
        "python -m src.phase2.materialized_views",
    ]

    for item in required:
        assert item in text
