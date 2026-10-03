"""
Phase 2 Materialized Views.

Public materialized summaries:
1. monthly_sales_summary
2. city_sales_summary

Incremental-refresh design:
- last_updated_at detects changed validated records.
- first_processed_at is the stable partition key.
- Source data is partitioned by processing minute.
- Only affected processing-minute partitions are recomputed.
- Final public summaries are rebuilt from the small partial
  collections, not from the full orders_validated collection.
"""

import argparse
import json
from datetime import datetime, timedelta, timezone

from pymongo import (
    ASCENDING,
    DESCENDING,
    MongoClient,
)

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
)


MONTHLY_MV = "monthly_sales_summary"
CITY_MV = "city_sales_summary"

MONTHLY_PARTIAL = "phase2_mv_monthly_partitions"
CITY_PARTIAL = "phase2_mv_city_partitions"

METADATA_COLLECTION = "phase2_mv_metadata"
METADATA_ID = "materialized_views"

LAST_UPDATED_INDEX = "ix_phase2_last_updated_at"
FIRST_PROCESSED_INDEX = "ix_phase2_first_processed_at"

PROCESSING_MINUTE_FORMAT = "%Y-%m-%dT%H:%M"


MATERIALIZED_VIEW_CATALOG = {
    "monthly_sales_summary": {
        "source_aggregation":
            "monthly_sales",

        "grouping_field":
            "order_date month",

        "description": (
            "Persisted monthly order count, total order "
            "value, and average order value."
        ),

        "incremental_strategy": (
            "Detect changes with last_updated_at, map each "
            "changed record to its stable first_processed_at "
            "processing-minute partition, recompute only "
            "affected partitions, then roll up partial results."
        ),
    },

    "city_sales_summary": {
        "source_aggregation":
            "sales_by_city",

        "grouping_field":
            "city",

        "description": (
            "Persisted order count, total order value, "
            "and average order value grouped by city."
        ),

        "incremental_strategy": (
            "Detect changes with last_updated_at, map each "
            "changed record to its stable first_processed_at "
            "processing-minute partition, recompute only "
            "affected partitions, then roll up partial results."
        ),
    },
}


def utc_now():
    return datetime.now(
        timezone.utc
    )


def get_materialized_view_catalog():
    return MATERIALIZED_VIEW_CATALOG


def _client():
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )


def _ensure_supporting_indexes(db):
    """
    Operational indexes used by incremental refresh.

    These are supporting Materialized-View indexes and are
    NOT counted as the three Query/Explain indexes implemented
    earlier in Phase 2.
    """

    source = db[
        VALIDATED_COLLECTION
    ]

    source.create_index(
        [
            (
                "last_updated_at",
                ASCENDING,
            )
        ],
        name=LAST_UPDATED_INDEX,
    )

    source.create_index(
        [
            (
                "first_processed_at",
                ASCENDING,
            )
        ],
        name=FIRST_PROCESSED_INDEX,
    )

    db[
        MONTHLY_PARTIAL
    ].create_index(
        [
            (
                "processing_minute",
                ASCENDING,
            ),
            (
                "month",
                ASCENDING,
            ),
        ],
        unique=True,
        name=(
            "uq_mv_monthly_partition_minute_month"
        ),
    )

    db[
        CITY_PARTIAL
    ].create_index(
        [
            (
                "processing_minute",
                ASCENDING,
            ),
            (
                "city",
                ASCENDING,
            ),
        ],
        unique=True,
        name=(
            "uq_mv_city_partition_minute_city"
        ),
    )

    db[
        MONTHLY_MV
    ].create_index(
        [
            (
                "month",
                ASCENDING,
            )
        ],
        unique=True,
        name="uq_mv_monthly_sales_month",
    )

    db[
        CITY_MV
    ].create_index(
        [
            (
                "city",
                ASCENDING,
            )
        ],
        unique=True,
        name="uq_mv_city_sales_city",
    )


def _latest_watermark(source):
    document = source.find_one(
        {
            "last_updated_at": {
                "$type":
                    "date"
            }
        },
        {
            "_id":
                0,

            "last_updated_at":
                1,
        },
        sort=[
            (
                "last_updated_at",
                DESCENDING,
            )
        ],
    )

    if not document:
        return None

    return document.get(
        "last_updated_at"
    )


def _processing_minute_expression():
    return {
        "$dateToString": {
            "format":
                PROCESSING_MINUTE_FORMAT,

            "date":
                "$first_processed_at",

            "timezone":
                "UTC",
        }
    }


def _monthly_partial_pipeline(
    source_match,
):
    return [
        {
            "$match":
                source_match
        },

        {
            "$project": {
                "processing_minute":
                    _processing_minute_expression(),

                "month": {
                    "$substrBytes": [
                        "$order_date",
                        0,
                        7,
                    ]
                },

                "total_amount":
                    1,
            }
        },

        {
            "$group": {
                "_id": {
                    "processing_minute":
                        "$processing_minute",

                    "month":
                        "$month",
                },

                "order_count": {
                    "$sum":
                        1
                },

                "total_sales": {
                    "$sum":
                        "$total_amount"
                },
            }
        },

        {
            "$project": {
                "_id":
                    0,

                "processing_minute":
                    "$_id.processing_minute",

                "month":
                    "$_id.month",

                "order_count":
                    1,

                "total_sales":
                    1,
            }
        },

        {
            "$merge": {
                "into":
                    MONTHLY_PARTIAL,

                "on": [
                    "processing_minute",
                    "month",
                ],

                "whenMatched":
                    "replace",

                "whenNotMatched":
                    "insert",
            }
        },
    ]


def _city_partial_pipeline(
    source_match,
):
    return [
        {
            "$match":
                source_match
        },

        {
            "$project": {
                "processing_minute":
                    _processing_minute_expression(),

                "city":
                    1,

                "total_amount":
                    1,
            }
        },

        {
            "$group": {
                "_id": {
                    "processing_minute":
                        "$processing_minute",

                    "city":
                        "$city",
                },

                "order_count": {
                    "$sum":
                        1
                },

                "total_sales": {
                    "$sum":
                        "$total_amount"
                },
            }
        },

        {
            "$project": {
                "_id":
                    0,

                "processing_minute":
                    "$_id.processing_minute",

                "city":
                    "$_id.city",

                "order_count":
                    1,

                "total_sales":
                    1,
            }
        },

        {
            "$merge": {
                "into":
                    CITY_PARTIAL,

                "on": [
                    "processing_minute",
                    "city",
                ],

                "whenMatched":
                    "replace",

                "whenNotMatched":
                    "insert",
            }
        },
    ]


def _rebuild_public_summaries(
    db,
    refreshed_at,
):
    """
    Rebuild public summaries from the small partial collections.

    This does NOT rescan orders_validated.
    """

    monthly_partial = db[
        MONTHLY_PARTIAL
    ]

    city_partial = db[
        CITY_PARTIAL
    ]

    monthly_target = db[
        MONTHLY_MV
    ]

    city_target = db[
        CITY_MV
    ]

    monthly_target.delete_many(
        {}
    )

    city_target.delete_many(
        {}
    )

    list(
        monthly_partial.aggregate(
            [
                {
                    "$group": {
                        "_id":
                            "$month",

                        "order_count": {
                            "$sum":
                                "$order_count"
                        },

                        "total_sales": {
                            "$sum":
                                "$total_sales"
                        },
                    }
                },

                {
                    "$project": {
                        "_id":
                            0,

                        "month":
                            "$_id",

                        "order_count":
                            1,

                        "total_sales":
                            1,

                        "average_order_value": {
                            "$round": [
                                {
                                    "$divide": [
                                        "$total_sales",
                                        "$order_count",
                                    ]
                                },
                                2,
                            ]
                        },

                        "refreshed_at": {
                            "$literal":
                                refreshed_at
                        },
                    }
                },

                {
                    "$sort": {
                        "month":
                            1
                    }
                },

                {
                    "$merge": {
                        "into":
                            MONTHLY_MV,

                        "on":
                            "month",

                        "whenMatched":
                            "replace",

                        "whenNotMatched":
                            "insert",
                    }
                },
            ],
            allowDiskUse=True,
        )
    )

    list(
        city_partial.aggregate(
            [
                {
                    "$group": {
                        "_id":
                            "$city",

                        "order_count": {
                            "$sum":
                                "$order_count"
                        },

                        "total_sales": {
                            "$sum":
                                "$total_sales"
                        },
                    }
                },

                {
                    "$project": {
                        "_id":
                            0,

                        "city":
                            "$_id",

                        "order_count":
                            1,

                        "total_sales":
                            1,

                        "average_order_value": {
                            "$round": [
                                {
                                    "$divide": [
                                        "$total_sales",
                                        "$order_count",
                                    ]
                                },
                                2,
                            ]
                        },

                        "refreshed_at": {
                            "$literal":
                                refreshed_at
                        },
                    }
                },

                {
                    "$sort": {
                        "total_sales":
                            -1
                    }
                },

                {
                    "$merge": {
                        "into":
                            CITY_MV,

                        "on":
                            "city",

                        "whenMatched":
                            "replace",

                        "whenNotMatched":
                            "insert",
                    }
                },
            ],
            allowDiskUse=True,
        )
    )


def _summary_order_count(
    collection,
):
    rows = list(
        collection.aggregate(
            [
                {
                    "$group": {
                        "_id":
                            None,

                        "total": {
                            "$sum":
                                "$order_count"
                        },
                    }
                }
            ]
        )
    )

    if not rows:
        return 0

    return int(
        rows[
            0
        ].get(
            "total",
            0,
        )
    )


def bootstrap_materialized_views():
    """
    Initial full Materialized-View build.

    This full source scan happens once.
    Later refreshes are incremental.
    """

    client = _client()

    try:
        client.admin.command(
            "ping"
        )

        db = client[
            MONGO_DATABASE
        ]

        source = db[
            VALIDATED_COLLECTION
        ]

        _ensure_supporting_indexes(
            db
        )

        cutoff = _latest_watermark(
            source
        )

        if cutoff is None:
            raise RuntimeError(
                "No last_updated_at watermark "
                "was found in orders_validated."
            )

        started_at = utc_now()

        monthly_partial = db[
            MONTHLY_PARTIAL
        ]

        city_partial = db[
            CITY_PARTIAL
        ]

        monthly_target = db[
            MONTHLY_MV
        ]

        city_target = db[
            CITY_MV
        ]

        monthly_partial.delete_many(
            {}
        )

        city_partial.delete_many(
            {}
        )

        monthly_target.delete_many(
            {}
        )

        city_target.delete_many(
            {}
        )

        source_match = {
            "last_updated_at": {
                "$lte":
                    cutoff
            },

            "first_processed_at": {
                "$type":
                    "date"
            },
        }

        list(
            source.aggregate(
                _monthly_partial_pipeline(
                    source_match
                ),
                allowDiskUse=True,
            )
        )

        list(
            source.aggregate(
                _city_partial_pipeline(
                    source_match
                ),
                allowDiskUse=True,
            )
        )

        refreshed_at = utc_now()

        _rebuild_public_summaries(
            db,
            refreshed_at,
        )

        monthly_total = (
            _summary_order_count(
                monthly_target
            )
        )

        city_total = (
            _summary_order_count(
                city_target
            )
        )

        if monthly_total != city_total:
            raise RuntimeError(
                "Materialized View consistency "
                "check failed: monthly total "
                f"{monthly_total} != city total "
                f"{city_total}."
            )

        finished_at = utc_now()

        metadata = db[
            METADATA_COLLECTION
        ]

        metadata.update_one(
            {
                "_id":
                    METADATA_ID
            },
            {
                "$set": {
                    "watermark":
                        cutoff,

                    "partition_strategy":
                        "first_processed_at_minute",

                    "change_detection_field":
                        "last_updated_at",

                    "last_refresh_type":
                        "bootstrap",

                    "last_refresh_started_at":
                        started_at,

                    "last_refresh_finished_at":
                        finished_at,

                    "changed_documents":
                        monthly_total,

                    "affected_partitions":
                        "all",

                    "monthly_partial_rows":
                        monthly_partial.count_documents(
                            {}
                        ),

                    "city_partial_rows":
                        city_partial.count_documents(
                            {}
                        ),

                    "monthly_rows":
                        monthly_target.count_documents(
                            {}
                        ),

                    "city_rows":
                        city_target.count_documents(
                            {}
                        ),

                    "validated_orders_materialized":
                        monthly_total,
                }
            },
            upsert=True,
        )

        return {
            "refresh_type":
                "bootstrap",

            "watermark":
                cutoff,

            "partition_strategy":
                "first_processed_at_minute",

            "monthly_collection":
                MONTHLY_MV,

            "monthly_rows":
                monthly_target.count_documents(
                    {}
                ),

            "city_collection":
                CITY_MV,

            "city_rows":
                city_target.count_documents(
                    {}
                ),

            "monthly_partial_rows":
                monthly_partial.count_documents(
                    {}
                ),

            "city_partial_rows":
                city_partial.count_documents(
                    {}
                ),

            "validated_orders_materialized":
                monthly_total,

            "consistency":
                monthly_total == city_total,
        }

    finally:
        client.close()


def _processing_minute_bounds(
    processing_minute,
):
    start = datetime.strptime(
        processing_minute,
        PROCESSING_MINUTE_FORMAT,
    )

    end = (
        start
        + timedelta(
            minutes=1
        )
    )

    return start, end


def _recompute_partition(
    db,
    processing_minute,
):
    """
    Recompute one stable processing-minute partition.

    Existing partial rows for the minute are removed first.
    Then current source truth for that minute is aggregated.
    """

    source = db[
        VALIDATED_COLLECTION
    ]

    monthly_partial = db[
        MONTHLY_PARTIAL
    ]

    city_partial = db[
        CITY_PARTIAL
    ]

    start, end = (
        _processing_minute_bounds(
            processing_minute
        )
    )

    monthly_partial.delete_many(
        {
            "processing_minute":
                processing_minute
        }
    )

    city_partial.delete_many(
        {
            "processing_minute":
                processing_minute
        }
    )

    source_match = {
        "first_processed_at": {
            "$gte":
                start,

            "$lt":
                end,
        }
    }

    list(
        source.aggregate(
            _monthly_partial_pipeline(
                source_match
            ),
            allowDiskUse=True,
        )
    )

    list(
        source.aggregate(
            _city_partial_pipeline(
                source_match
            ),
            allowDiskUse=True,
        )
    )


def refresh_materialized_views():
    """
    Incremental Materialized-View refresh.

    Only processing-minute partitions containing records
    changed after the stored watermark are recomputed.
    """

    client = _client()

    try:
        client.admin.command(
            "ping"
        )

        db = client[
            MONGO_DATABASE
        ]

        source = db[
            VALIDATED_COLLECTION
        ]

        metadata_collection = db[
            METADATA_COLLECTION
        ]

        _ensure_supporting_indexes(
            db
        )

        metadata = (
            metadata_collection.find_one(
                {
                    "_id":
                        METADATA_ID
                }
            )
        )

        if not metadata:
            raise RuntimeError(
                "Materialized Views are not initialized. "
                "Run --bootstrap first."
            )

        watermark = metadata.get(
            "watermark"
        )

        if watermark is None:
            raise RuntimeError(
                "Materialized View watermark is missing."
            )

        newest_changed = (
            source.find_one(
                {
                    "last_updated_at": {
                        "$gt":
                            watermark
                    }
                },
                {
                    "_id":
                        0,

                    "last_updated_at":
                        1,
                },
                sort=[
                    (
                        "last_updated_at",
                        DESCENDING,
                    )
                ],
            )
        )

        started_at = utc_now()

        if not newest_changed:

            finished_at = utc_now()

            metadata_collection.update_one(
                {
                    "_id":
                        METADATA_ID
                },
                {
                    "$set": {
                        "last_refresh_type":
                            "incremental_no_changes",

                        "last_refresh_started_at":
                            started_at,

                        "last_refresh_finished_at":
                            finished_at,

                        "changed_documents":
                            0,

                        "affected_partitions":
                            [],
                    }
                },
            )

            return {
                "refresh_type":
                    "incremental",

                "result":
                    "no_changes",

                "changed_documents":
                    0,

                "affected_partitions":
                    [],

                "watermark":
                    watermark,
            }

        new_watermark = (
            newest_changed[
                "last_updated_at"
            ]
        )

        change_window = {
            "last_updated_at": {
                "$gt":
                    watermark,

                "$lte":
                    new_watermark,
            }
        }

        partition_rows = list(
            source.aggregate(
                [
                    {
                        "$match":
                            change_window
                    },

                    {
                        "$match": {
                            "first_processed_at": {
                                "$type":
                                    "date"
                            }
                        }
                    },

                    {
                        "$group": {
                            "_id":
                                _processing_minute_expression()
                        }
                    },

                    {
                        "$sort": {
                            "_id":
                                1
                        }
                    },
                ]
            )
        )

        affected_partitions = [
            row[
                "_id"
            ]
            for row
            in partition_rows
            if row.get(
                "_id"
            )
        ]

        changed_documents = (
            source.count_documents(
                change_window
            )
        )

        for processing_minute in (
            affected_partitions
        ):
            _recompute_partition(
                db,
                processing_minute,
            )

        refreshed_at = utc_now()

        _rebuild_public_summaries(
            db,
            refreshed_at,
        )

        monthly_target = db[
            MONTHLY_MV
        ]

        city_target = db[
            CITY_MV
        ]

        monthly_total = (
            _summary_order_count(
                monthly_target
            )
        )

        city_total = (
            _summary_order_count(
                city_target
            )
        )

        if monthly_total != city_total:
            raise RuntimeError(
                "Incremental Materialized View "
                "consistency check failed: "
                f"{monthly_total} != {city_total}."
            )

        finished_at = utc_now()

        metadata_collection.update_one(
            {
                "_id":
                    METADATA_ID
            },
            {
                "$set": {
                    "watermark":
                        new_watermark,

                    "last_refresh_type":
                        "incremental",

                    "last_refresh_started_at":
                        started_at,

                    "last_refresh_finished_at":
                        finished_at,

                    "changed_documents":
                        changed_documents,

                    "affected_partitions":
                        affected_partitions,

                    "monthly_partial_rows":
                        db[
                            MONTHLY_PARTIAL
                        ].count_documents(
                            {}
                        ),

                    "city_partial_rows":
                        db[
                            CITY_PARTIAL
                        ].count_documents(
                            {}
                        ),

                    "monthly_rows":
                        monthly_target.count_documents(
                            {}
                        ),

                    "city_rows":
                        city_target.count_documents(
                            {}
                        ),

                    "validated_orders_materialized":
                        monthly_total,
                }
            },
        )

        return {
            "refresh_type":
                "incremental",

            "result":
                "refreshed",

            "previous_watermark":
                watermark,

            "new_watermark":
                new_watermark,

            "changed_documents":
                changed_documents,

            "affected_partition_count":
                len(
                    affected_partitions
                ),

            "affected_partitions":
                affected_partitions,

            "monthly_rows":
                monthly_target.count_documents(
                    {}
                ),

            "city_rows":
                city_target.count_documents(
                    {}
                ),

            "validated_orders_materialized":
                monthly_total,

            "consistency":
                monthly_total == city_total,
        }

    finally:
        client.close()


def get_status():
    client = _client()

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

        metadata = (
            db[
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
            if METADATA_COLLECTION
            in collection_names
            else None
        )

        source_indexes = {
            item[
                "name"
            ]:
                list(
                    item[
                        "key"
                    ].items()
                )

            for item
            in db[
                VALIDATED_COLLECTION
            ].list_indexes()
        }

        return {
            "initialized":
                metadata is not None,

            "metadata":
                metadata,

            "monthly_collection":
                MONTHLY_MV,

            "monthly_rows":
                db[
                    MONTHLY_MV
                ].count_documents(
                    {}
                )
                if MONTHLY_MV
                in collection_names
                else 0,

            "city_collection":
                CITY_MV,

            "city_rows":
                db[
                    CITY_MV
                ].count_documents(
                    {}
                )
                if CITY_MV
                in collection_names
                else 0,

            "monthly_partial_rows":
                db[
                    MONTHLY_PARTIAL
                ].count_documents(
                    {}
                )
                if MONTHLY_PARTIAL
                in collection_names
                else 0,

            "city_partial_rows":
                db[
                    CITY_PARTIAL
                ].count_documents(
                    {}
                )
                if CITY_PARTIAL
                in collection_names
                else 0,

            "supporting_indexes": {
                LAST_UPDATED_INDEX: {
                    "present":
                        LAST_UPDATED_INDEX
                        in source_indexes,

                    "keys":
                        source_indexes.get(
                            LAST_UPDATED_INDEX
                        ),
                },

                FIRST_PROCESSED_INDEX: {
                    "present":
                        FIRST_PROCESSED_INDEX
                        in source_indexes,

                    "keys":
                        source_indexes.get(
                            FIRST_PROCESSED_INDEX
                        ),
                },
            },
        }

    finally:
        client.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Manage Phase 2 Materialized Views."
        )
    )

    group = (
        parser.add_mutually_exclusive_group(
            required=True
        )
    )

    group.add_argument(
        "--catalog",
        action="store_true",
        help="Show Materialized View definitions.",
    )

    group.add_argument(
        "--bootstrap",
        action="store_true",
        help="Build initial Materialized Views once.",
    )

    group.add_argument(
        "--refresh",
        action="store_true",
        help="Run incremental Materialized-View refresh.",
    )

    group.add_argument(
        "--status",
        action="store_true",
        help="Show current Materialized-View status.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.catalog:
        result = (
            get_materialized_view_catalog()
        )

    elif args.bootstrap:
        result = (
            bootstrap_materialized_views()
        )

    elif args.refresh:
        result = (
            refresh_materialized_views()
        )

    else:
        result = get_status()

    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
