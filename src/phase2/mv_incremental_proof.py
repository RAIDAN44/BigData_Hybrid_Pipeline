"""
Isolated integration proof for Phase 2 Materialized Views.

Safety:
- Reads real documents from orders_validated.
- Copies a small sample to temporary collections.
- NEVER modifies orders_validated.
- Runs the real materialized_views.py implementation.
- Changes exactly one temporary document.
- Verifies incremental refresh correctness.
- Drops all temporary collections in finally.
"""

import copy
import json
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
)

import src.phase2.materialized_views as mv


REPORT_PATH = Path(
    "reports/phase2/mv_incremental_update_proof.json"
)

TEMP_SOURCE = "_phase2_mv_proof_source"
TEMP_MONTHLY = "_phase2_mv_proof_monthly_summary"
TEMP_CITY = "_phase2_mv_proof_city_summary"
TEMP_MONTHLY_PARTIAL = "_phase2_mv_proof_monthly_partitions"
TEMP_CITY_PARTIAL = "_phase2_mv_proof_city_partitions"
TEMP_METADATA = "_phase2_mv_proof_metadata"
TEMP_METADATA_ID = "proof_materialized_views"

TEMP_COLLECTIONS = [
    TEMP_SOURCE,
    TEMP_MONTHLY,
    TEMP_CITY,
    TEMP_MONTHLY_PARTIAL,
    TEMP_CITY_PARTIAL,
    TEMP_METADATA,
]


def rows_by(collection, key):
    return {
        row[key]: row
        for row in collection.find(
            {},
            {"_id": 0},
        )
    }


def summary_count(rows):
    return sum(
        int(
            row.get(
                "order_count",
                0,
            )
        )
        for row in rows.values()
    )


def summary_sales(rows):
    return sum(
        int(
            row.get(
                "total_sales",
                0,
            )
        )
        for row in rows.values()
    )


client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000,
)

original_globals = {
    "VALIDATED_COLLECTION":
        mv.VALIDATED_COLLECTION,

    "MONTHLY_MV":
        mv.MONTHLY_MV,

    "CITY_MV":
        mv.CITY_MV,

    "MONTHLY_PARTIAL":
        mv.MONTHLY_PARTIAL,

    "CITY_PARTIAL":
        mv.CITY_PARTIAL,

    "METADATA_COLLECTION":
        mv.METADATA_COLLECTION,

    "METADATA_ID":
        mv.METADATA_ID,

    "LAST_UPDATED_INDEX":
        mv.LAST_UPDATED_INDEX,

    "FIRST_PROCESSED_INDEX":
        mv.FIRST_PROCESSED_INDEX,
}


try:
    client.admin.command("ping")

    db = client[
        MONGO_DATABASE
    ]

    production = db[
        VALIDATED_COLLECTION
    ]

    print()
    print("=" * 80)
    print("PHASE 2 - ISOLATED INCREMENTAL MV PROOF")
    print("=" * 80)

    print(
        "Production collection :",
        VALIDATED_COLLECTION,
    )

    print(
        "Production mode       : READ ONLY"
    )

    # --------------------------------------------------------
    # Remove stale proof collections only.
    # --------------------------------------------------------

    for name in TEMP_COLLECTIONS:
        db[name].drop()

    # --------------------------------------------------------
    # Copy 500 real validated documents.
    # --------------------------------------------------------

    sample = list(
        production.find(
            {
                "first_processed_at": {
                    "$type": "date"
                },

                "last_updated_at": {
                    "$type": "date"
                },

                "city": {
                    "$type": "string"
                },

                "order_date": {
                    "$type": "string"
                },

                "total_amount": {
                    "$type": "int"
                },
            },
            {
                "order_id": 1,
                "city": 1,
                "order_date": 1,
                "total_amount": 1,
                "first_processed_at": 1,
                "last_updated_at": 1,
            },
        ).limit(500)
    )

    if len(sample) != 500:
        raise RuntimeError(
            "Could not obtain 500 suitable proof records."
        )

    db[
        TEMP_SOURCE
    ].insert_many(
        sample
    )

    print(
        "Temporary sample      :",
        len(sample),
        "documents",
    )

    # --------------------------------------------------------
    # Redirect the real MV implementation to temporary names.
    # --------------------------------------------------------

    mv.VALIDATED_COLLECTION = (
        TEMP_SOURCE
    )

    mv.MONTHLY_MV = (
        TEMP_MONTHLY
    )

    mv.CITY_MV = (
        TEMP_CITY
    )

    mv.MONTHLY_PARTIAL = (
        TEMP_MONTHLY_PARTIAL
    )

    mv.CITY_PARTIAL = (
        TEMP_CITY_PARTIAL
    )

    mv.METADATA_COLLECTION = (
        TEMP_METADATA
    )

    mv.METADATA_ID = (
        TEMP_METADATA_ID
    )

    mv.LAST_UPDATED_INDEX = (
        "ix_proof_last_updated_at"
    )

    mv.FIRST_PROCESSED_INDEX = (
        "ix_proof_first_processed_at"
    )

    # --------------------------------------------------------
    # Initial temp bootstrap.
    # --------------------------------------------------------

    bootstrap = (
        mv.bootstrap_materialized_views()
    )

    monthly_before = rows_by(
        db[
            TEMP_MONTHLY
        ],
        "month",
    )

    city_before = rows_by(
        db[
            TEMP_CITY
        ],
        "city",
    )

    # --------------------------------------------------------
    # Select exactly one temporary record.
    # --------------------------------------------------------

    target = db[
        TEMP_SOURCE
    ].find_one(
        {}
    )

    if not target:
        raise RuntimeError(
            "Temporary proof source is empty."
        )

    original_order_id = (
        target[
            "order_id"
        ]
    )

    old_city = (
        target[
            "city"
        ]
    )

    old_month = str(
        target[
            "order_date"
        ]
    )[:7]

    old_total = int(
        target[
            "total_amount"
        ]
    )

    first_processed_at = (
        target[
            "first_processed_at"
        ]
    )

    expected_partition = (
        first_processed_at.strftime(
            mv.PROCESSING_MINUTE_FORMAT
        )
    )

    # Values intentionally unique to the temporary proof.
    new_city = (
        "__PHASE2_INCREMENTAL_PROOF_CITY__"
    )

    new_month = "2099-12"

    new_order_date = (
        "2099-12-15T12:00:00"
    )

    new_total = (
        old_total
        + 12345
    )

    changed = copy.deepcopy(
        target
    )

    # first_processed_at intentionally stays unchanged.
    changed[
        "city"
    ] = new_city

    changed[
        "order_date"
    ] = new_order_date

    changed[
        "total_amount"
    ] = new_total

    changed[
        "last_updated_at"
    ] = datetime.now(
        timezone.utc
    )

    # Full replacement mirrors the project's upsert/update model.
    result = db[
        TEMP_SOURCE
    ].replace_one(
        {
            "_id":
                target[
                    "_id"
                ]
        },
        changed,
    )

    if result.modified_count != 1:
        raise RuntimeError(
            "Proof update did not modify exactly one document."
        )

    # --------------------------------------------------------
    # Real incremental refresh.
    # --------------------------------------------------------

    refresh = (
        mv.refresh_materialized_views()
    )

    monthly_after = rows_by(
        db[
            TEMP_MONTHLY
        ],
        "month",
    )

    city_after = rows_by(
        db[
            TEMP_CITY
        ],
        "city",
    )

    after_document = db[
        TEMP_SOURCE
    ].find_one(
        {
            "_id":
                target[
                    "_id"
                ]
        }
    )

    # --------------------------------------------------------
    # Assertions / evidence.
    # --------------------------------------------------------

    before_old_month = int(
        monthly_before[
            old_month
        ][
            "order_count"
        ]
    )

    after_old_month = int(
        monthly_after[
            old_month
        ][
            "order_count"
        ]
    )

    before_old_city = int(
        city_before[
            old_city
        ][
            "order_count"
        ]
    )

    after_old_city = int(
        city_after[
            old_city
        ][
            "order_count"
        ]
    )

    validations = {
        "bootstrap_materialized_500":
            bootstrap.get(
                "validated_orders_materialized"
            )
            == 500,

        "changed_documents_is_one":
            refresh.get(
                "changed_documents"
            )
            == 1,

        "one_partition_recomputed":
            refresh.get(
                "affected_partition_count"
            )
            == 1,

        "expected_partition_detected":
            expected_partition
            in refresh.get(
                "affected_partitions",
                [],
            ),

        "first_processed_at_preserved":
            after_document.get(
                "first_processed_at"
            )
            == first_processed_at,

        "old_month_decremented":
            after_old_month
            == (
                before_old_month
                - 1
            ),

        "new_month_incremented":
            int(
                monthly_after[
                    new_month
                ][
                    "order_count"
                ]
            )
            == 1,

        "old_city_decremented":
            after_old_city
            == (
                before_old_city
                - 1
            ),

        "new_city_incremented":
            int(
                city_after[
                    new_city
                ][
                    "order_count"
                ]
            )
            == 1,

        "monthly_total_count_preserved":
            summary_count(
                monthly_after
            )
            == 500,

        "city_total_count_preserved":
            summary_count(
                city_after
            )
            == 500,

        "monthly_sales_delta_correct":
            (
                summary_sales(
                    monthly_after
                )
                - summary_sales(
                    monthly_before
                )
            )
            == (
                new_total
                - old_total
            ),

        "city_sales_delta_correct":
            (
                summary_sales(
                    city_after
                )
                - summary_sales(
                    city_before
                )
            )
            == (
                new_total
                - old_total
            ),

        "mv_consistency_true":
            refresh.get(
                "consistency"
            )
            is True,
    }

    all_passed = all(
        validations.values()
    )

    report = {
        "proof":
            "phase2_incremental_materialized_views",

        "production_collection":
            VALIDATED_COLLECTION,

        "production_modified":
            False,

        "temporary_source":
            TEMP_SOURCE,

        "sample_documents":
            500,

        "tested_order_id":
            original_order_id,

        "stable_partition":
            expected_partition,

        "before": {
            "city":
                old_city,

            "month":
                old_month,

            "total_amount":
                old_total,
        },

        "after": {
            "city":
                new_city,

            "month":
                new_month,

            "total_amount":
                new_total,
        },

        "bootstrap_result":
            bootstrap,

        "incremental_refresh_result":
            refresh,

        "validations":
            validations,

        "all_passed":
            all_passed,

        "cleanup":
            "temporary collections dropped in finally",
    }

    REPORT_PATH.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    print()
    print(
        "Changed documents     :",
        refresh.get(
            "changed_documents"
        ),
    )

    print(
        "Affected partitions   :",
        refresh.get(
            "affected_partitions"
        ),
    )

    print(
        "Stable partition      :",
        expected_partition,
    )

    print(
        "Old city              :",
        old_city,
    )

    print(
        "New city              :",
        new_city,
    )

    print(
        "Old month             :",
        old_month,
    )

    print(
        "New month             :",
        new_month,
    )

    print()
    print(
        "VALIDATIONS:"
    )

    for name, passed in (
        validations.items()
    ):
        print(
            f"  {'PASS' if passed else 'FAIL'} "
            f"- {name}"
        )

    print()
    print(
        "OVERALL:",
        "PASS"
        if all_passed
        else "FAIL",
    )

    print(
        "Evidence:",
        REPORT_PATH,
    )

    if not all_passed:
        raise RuntimeError(
            "Incremental MV proof failed."
        )

finally:

    # Restore module globals.
    for name, value in (
        original_globals.items()
    ):
        setattr(
            mv,
            name,
            value,
        )

    # Always remove proof collections.
    try:
        db
    except NameError:
        pass
    else:
        for name in TEMP_COLLECTIONS:
            db[
                name
            ].drop()

    client.close()
