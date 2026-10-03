"""
Phase 2 indexes.

All indexes in this module are NEW final-project indexes.
Existing Midterm indexes are intentionally not counted here.
"""

import argparse
import json

from pymongo import (
    ASCENDING,
    MongoClient,
)

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
)


PHASE2_INDEXES = [
    {
        "name":
            "ix_phase2_customer_id",

        "keys": [
            (
                "customer_id",
                ASCENDING,
            ),
        ],

        "serves_query":
            "orders_by_customer",

        "reason": (
            "Avoid scanning the full validated collection "
            "when searching by customer_id."
        ),

        "compound":
            False,
    },

    {
        "name":
            "ix_phase2_order_date",

        "keys": [
            (
                "order_date",
                ASCENDING,
            ),
        ],

        "serves_query":
            "orders_by_date_range",

        "reason": (
            "Support efficient range filtering "
            "on normalized order_date values."
        ),

        "compound":
            False,
    },

    {
        "name":
            "ix_phase2_city_order_date",

        "keys": [
            (
                "city",
                ASCENDING,
            ),
            (
                "order_date",
                ASCENDING,
            ),
        ],

        "serves_query":
            "orders_by_city_and_date",

        "reason": (
            "Use equality on city followed by "
            "a range condition on order_date."
        ),

        "compound":
            True,
    },
]


def get_index_catalog():
    """Return Phase 2 index specifications."""

    result = []

    for specification in PHASE2_INDEXES:
        result.append(
            {
                "name":
                    specification[
                        "name"
                    ],

                "keys": [
                    [
                        key,
                        direction,
                    ]
                    for (
                        key,
                        direction,
                    )
                    in specification[
                        "keys"
                    ]
                ],

                "serves_query":
                    specification[
                        "serves_query"
                    ],

                "reason":
                    specification[
                        "reason"
                    ],

                "compound":
                    specification[
                        "compound"
                    ],
            }
        )

    return result


def list_collection_indexes():
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    try:
        client.admin.command("ping")

        collection = client[
            MONGO_DATABASE
        ][
            VALIDATED_COLLECTION
        ]

        return [
            {
                "name":
                    item.get(
                        "name"
                    ),

                "keys":
                    [
                        [
                            key,
                            direction,
                        ]
                        for (
                            key,
                            direction,
                        )
                        in item.get(
                            "key",
                            {}
                        ).items()
                    ],

                "unique":
                    bool(
                        item.get(
                            "unique",
                            False,
                        )
                    ),
            }

            for item
            in collection.list_indexes()
        ]

    finally:
        client.close()


def create_phase2_indexes():
    """
    Create the three required NEW Phase 2 indexes.

    MongoDB create_index is idempotent when the same
    index name and specification already exist.
    """

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    try:
        client.admin.command("ping")

        collection = client[
            MONGO_DATABASE
        ][
            VALIDATED_COLLECTION
        ]

        created = []

        for specification in PHASE2_INDEXES:

            name = collection.create_index(
                specification[
                    "keys"
                ],
                name=specification[
                    "name"
                ],
            )

            created.append(
                {
                    "name":
                        name,

                    "serves_query":
                        specification[
                            "serves_query"
                        ],

                    "reason":
                        specification[
                            "reason"
                        ],

                    "compound":
                        specification[
                            "compound"
                        ],
                }
            )

        return {
            "created_or_verified":
                created,

            "phase2_index_count":
                len(created),
        }

    finally:
        client.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Manage Phase 2 indexes."
        )
    )

    group = parser.add_mutually_exclusive_group(
        required=True
    )

    group.add_argument(
        "--catalog",
        action="store_true",
        help="Show required Phase 2 indexes.",
    )

    group.add_argument(
        "--list",
        action="store_true",
        help="Show indexes currently in MongoDB.",
    )

    group.add_argument(
        "--create",
        action="store_true",
        help="Create the three Phase 2 indexes.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.catalog:
        result = get_index_catalog()

    elif args.list:
        result = list_collection_indexes()

    else:
        result = create_phase2_indexes()

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
