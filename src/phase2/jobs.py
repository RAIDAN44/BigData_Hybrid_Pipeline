"""
Phase 2 Scheduled Jobs.

Required jobs:
1. refresh_materialized_views
2. generate_daily_analytics_report

Features:
- Defined schedules.
- Manual execution.
- Scheduled execution with APScheduler.
- MongoDB execution logs.
- Start/end timestamps.
- Success/failure status.
- Result/error details.
"""

import argparse
import json
import time

from datetime import (
    datetime,
    timezone,
)

from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import (
    BlockingScheduler,
)

from apscheduler.triggers.cron import (
    CronTrigger,
)

from pymongo import (
    ASCENDING,
    DESCENDING,
    MongoClient,
)

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
)

from src.phase2.materialized_views import (
    MONTHLY_MV,
    CITY_MV,
    METADATA_COLLECTION,
    METADATA_ID,
    refresh_materialized_views,
)


JOB_LOG_COLLECTION = "phase2_job_logs"

REPORT_DIRECTORY = Path(
    "reports/phase2/jobs"
)

UTC = ZoneInfo("UTC")


JOB_DEFINITIONS = {
    "refresh_materialized_views": {
        "description": (
            "Run the incremental refresh mechanism "
            "for Phase 2 Materialized Views."
        ),

        "schedule": {
            "type":
                "cron",

            "expression":
                "0 * * * *",

            "human":
                "Every hour at minute 00 UTC",

            "timezone":
                "UTC",
        },
    },

    "generate_daily_analytics_report": {
        "description": (
            "Generate a daily analytics snapshot "
            "from the persisted Materialized Views."
        ),

        "schedule": {
            "type":
                "cron",

            "expression":
                "0 2 * * *",

            "human":
                "Daily at 02:00 UTC",

            "timezone":
                "UTC",
        },
    },
}


def utc_now():
    return datetime.now(
        timezone.utc
    )


def _mongo_client():
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )


def _ensure_log_indexes(
    collection,
):
    collection.create_index(
        [
            (
                "job_name",
                ASCENDING,
            ),
            (
                "started_at",
                DESCENDING,
            ),
        ],
        name="ix_phase2_job_name_started_at",
    )

    collection.create_index(
        [
            (
                "status",
                ASCENDING,
            ),
            (
                "started_at",
                DESCENDING,
            ),
        ],
        name="ix_phase2_job_status_started_at",
    )


def generate_daily_analytics_report():
    """
    Generate a real project report using the
    already-materialized analytics summaries.

    This does not rescan orders_validated.
    """

    client = _mongo_client()

    try:
        client.admin.command(
            "ping"
        )

        db = client[
            MONGO_DATABASE
        ]

        metadata = db[
            METADATA_COLLECTION
        ].find_one(
            {
                "_id":
                    METADATA_ID
            },
            {
                "_id":
                    0
            },
        )

        if not metadata:
            raise RuntimeError(
                "Materialized Views are not initialized."
            )

        monthly_rows = list(
            db[
                MONTHLY_MV
            ].find(
                {},
                {
                    "_id":
                        0
                },
            ).sort(
                "month",
                ASCENDING,
            )
        )

        city_rows = list(
            db[
                CITY_MV
            ].find(
                {},
                {
                    "_id":
                        0
                },
            ).sort(
                "total_sales",
                DESCENDING,
            )
        )

        if not monthly_rows:
            raise RuntimeError(
                "monthly_sales_summary is empty."
            )

        if not city_rows:
            raise RuntimeError(
                "city_sales_summary is empty."
            )

        total_orders_monthly = sum(
            int(
                row.get(
                    "order_count",
                    0,
                )
            )
            for row in monthly_rows
        )

        total_orders_city = sum(
            int(
                row.get(
                    "order_count",
                    0,
                )
            )
            for row in city_rows
        )

        if (
            total_orders_monthly
            != total_orders_city
        ):
            raise RuntimeError(
                "Daily report consistency check failed: "
                f"monthly={total_orders_monthly}, "
                f"city={total_orders_city}."
            )

        generated_at = utc_now()

        report = {
            "report_name":
                "daily_analytics_report",

            "generated_at":
                generated_at,

            "source":
                "Phase 2 Materialized Views",

            "mv_watermark":
                metadata.get(
                    "watermark"
                ),

            "validated_orders_represented":
                total_orders_monthly,

            "monthly_summary_count":
                len(
                    monthly_rows
                ),

            "city_summary_count":
                len(
                    city_rows
                ),

            "monthly_sales_summary":
                monthly_rows,

            "city_sales_summary":
                city_rows,

            "consistency":
                True,
        }

        REPORT_DIRECTORY.mkdir(
            parents=True,
            exist_ok=True,
        )

        timestamp = (
            generated_at.strftime(
                "%Y%m%dT%H%M%SZ"
            )
        )

        output_path = (
            REPORT_DIRECTORY
            / (
                "daily_analytics_"
                f"{timestamp}.json"
            )
        )

        output_path.write_text(
            json.dumps(
                report,
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )

        return {
            "report_name":
                "daily_analytics_report",

            "output_path":
                str(
                    output_path
                ),

            "validated_orders_represented":
                total_orders_monthly,

            "monthly_summary_count":
                len(
                    monthly_rows
                ),

            "city_summary_count":
                len(
                    city_rows
                ),

            "consistency":
                True,
        }

    finally:
        client.close()


def _execute_job_function(
    job_name,
):
    if (
        job_name
        == "refresh_materialized_views"
    ):
        return (
            refresh_materialized_views()
        )

    if (
        job_name
        == "generate_daily_analytics_report"
    ):
        return (
            generate_daily_analytics_report()
        )

    raise KeyError(
        f"Unknown job: {job_name}"
    )


def run_job(
    job_name,
    trigger_source="manual",
):
    """
    Run one job and always record its execution.

    trigger_source:
    - manual
    - scheduled
    """

    if job_name not in JOB_DEFINITIONS:
        raise KeyError(
            f"Unknown job: {job_name}"
        )

    if trigger_source not in {
        "manual",
        "scheduled",
    }:
        raise ValueError(
            "trigger_source must be "
            "'manual' or 'scheduled'."
        )

    started_at = utc_now()

    monotonic_start = (
        time.perf_counter()
    )

    status = "success"
    result = None
    error = None

    try:
        result = (
            _execute_job_function(
                job_name
            )
        )

    except Exception as exc:
        status = "failure"

        error = {
            "type":
                type(
                    exc
                ).__name__,

            "message":
                str(
                    exc
                ),
        }

    finished_at = utc_now()

    duration_seconds = round(
        time.perf_counter()
        - monotonic_start,
        6,
    )

    log_document = {
        "job_name":
            job_name,

        "description":
            JOB_DEFINITIONS[
                job_name
            ][
                "description"
            ],

        "trigger_source":
            trigger_source,

        "schedule":
            JOB_DEFINITIONS[
                job_name
            ][
                "schedule"
            ],

        "started_at":
            started_at,

        "finished_at":
            finished_at,

        "duration_seconds":
            duration_seconds,

        "status":
            status,

        "result":
            result,

        "error":
            error,
    }

    client = _mongo_client()

    try:
        client.admin.command(
            "ping"
        )

        log_collection = client[
            MONGO_DATABASE
        ][
            JOB_LOG_COLLECTION
        ]

        _ensure_log_indexes(
            log_collection
        )

        # Insert a copy so PyMongo does not add _id
        # to the dictionary returned later to API/CLI callers.
        insert_result = (
            log_collection.insert_one(
                dict(
                    log_document
                )
            )
        )

        log_id = str(
            insert_result.inserted_id
        )

    finally:
        client.close()

    response = dict(
        log_document
    )

    response[
        "log_id"
    ] = log_id

    if status == "failure":
        response[
            "success"
        ] = False

    else:
        response[
            "success"
        ] = True

    return response


def _latest_job_log(
    collection,
    job_name,
):
    document = collection.find_one(
        {
            "job_name":
                job_name
        },
        {
            "_id":
                0
        },
        sort=[
            (
                "started_at",
                DESCENDING,
            )
        ],
    )

    return document


def get_jobs():
    """
    Return both scheduled job definitions
    and their latest execution logs.
    """

    client = _mongo_client()

    try:
        client.admin.command(
            "ping"
        )

        db = client[
            MONGO_DATABASE
        ]

        collection_names = set(
            db.list_collection_names()
        )

        log_collection = (
            db[
                JOB_LOG_COLLECTION
            ]
            if JOB_LOG_COLLECTION
            in collection_names
            else None
        )

        jobs = []

        for (
            job_name,
            definition,
        ) in JOB_DEFINITIONS.items():

            latest_log = None

            if log_collection is not None:
                latest_log = (
                    _latest_job_log(
                        log_collection,
                        job_name,
                    )
                )

            jobs.append(
                {
                    "name":
                        job_name,

                    "description":
                        definition[
                            "description"
                        ],

                    "schedule":
                        definition[
                            "schedule"
                        ],

                    "manual_run_supported":
                        True,

                    "latest_run":
                        latest_log,
                }
            )

        return {
            "job_count":
                len(
                    jobs
                ),

            "jobs":
                jobs,
        }

    finally:
        client.close()


def _scheduled_refresh_materialized_views():
    result = run_job(
        "refresh_materialized_views",
        trigger_source="scheduled",
    )

    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


def _scheduled_generate_daily_report():
    result = run_job(
        "generate_daily_analytics_report",
        trigger_source="scheduled",
    )

    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


def build_scheduler():
    """
    Build the real APScheduler instance.
    """

    scheduler = BlockingScheduler(
        timezone=UTC
    )

    scheduler.add_job(
        _scheduled_refresh_materialized_views,

        trigger=CronTrigger(
            minute=0,
            timezone=UTC,
        ),

        id="refresh_materialized_views",

        name=(
            "Incremental Materialized "
            "Views Refresh"
        ),

        replace_existing=True,

        coalesce=True,

        max_instances=1,

        misfire_grace_time=300,
    )

    scheduler.add_job(
        _scheduled_generate_daily_report,

        trigger=CronTrigger(
            hour=2,
            minute=0,
            timezone=UTC,
        ),

        id="generate_daily_analytics_report",

        name=(
            "Daily Analytics Report"
        ),

        replace_existing=True,

        coalesce=True,

        max_instances=1,

        misfire_grace_time=600,
    )

    return scheduler


def start_scheduler():
    scheduler = build_scheduler()

    print()
    print("=" * 80)
    print("PHASE 2 SCHEDULED JOBS")
    print("=" * 80)

    now = utc_now()

    for job in scheduler.get_jobs():

        next_run = (
            job.trigger.get_next_fire_time(
                None,
                now,
            )
        )

        print(
            f"{job.id}: "
            f"next_run={next_run}"
        )

    print()
    print(
        "Scheduler started. "
        "Press Ctrl+C to stop."
    )

    scheduler.start()


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Manage Phase 2 Scheduled Jobs."
        )
    )

    group = (
        parser.add_mutually_exclusive_group(
            required=True
        )
    )

    group.add_argument(
        "--list",
        action="store_true",
        help=(
            "List job definitions and "
            "latest execution status."
        ),
    )

    group.add_argument(
        "--run",
        choices=sorted(
            JOB_DEFINITIONS
        ),
        help=(
            "Run one job manually."
        ),
    )

    group.add_argument(
        "--start",
        action="store_true",
        help=(
            "Start the APScheduler process."
        ),
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.list:
        result = get_jobs()

        print(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )

        return 0

    if args.run:
        result = run_job(
            args.run,
            trigger_source="manual",
        )

        print(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )

        return (
            0
            if result[
                "success"
            ]
            else 1
        )

    start_scheduler()

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
