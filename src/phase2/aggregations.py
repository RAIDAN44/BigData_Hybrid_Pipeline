"""
Phase 2 aggregation reports.

Each aggregation is independently executable and
operates on the existing orders_validated collection.
"""

import argparse
import json
from pathlib import Path

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
)


AGGREGATION_CATALOG = {
    "sales_by_city": {
        "description": (
            "Total sales, order count, and average "
            "order value grouped by city."
        ),
    },

    "orders_by_status": {
        "description": (
            "Order count and total order value "
            "grouped by order status."
        ),
    },

    "sales_by_payment_method": {
        "description": (
            "Order count, total sales, and average "
            "order value grouped by payment method."
        ),
    },

    "delivery_type_summary": {
        "description": (
            "Order count, total sales, and delivery "
            "cost statistics grouped by delivery type."
        ),
    },

    "monthly_sales": {
        "description": (
            "Monthly order count, total sales, and "
            "average order value."
        ),
    },
}


def get_aggregation_catalog():
    return AGGREGATION_CATALOG


def build_pipeline(name):
    if name not in AGGREGATION_CATALOG:
        raise KeyError(
            f"Unknown aggregation: {name}"
        )

    if name == "sales_by_city":
        return [
            {
                "$group": {
                    "_id": "$city",
                    "order_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$total_amount"
                    },
                    "average_order_value": {
                        "$avg": "$total_amount"
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "city": "$_id",
                    "order_count": 1,
                    "total_sales": 1,
                    "average_order_value": {
                        "$round": [
                            "$average_order_value",
                            2,
                        ]
                    },
                }
            },
            {
                "$sort": {
                    "total_sales": -1
                }
            },
        ]

    if name == "orders_by_status":
        return [
            {
                "$group": {
                    "_id": "$status",
                    "order_count": {
                        "$sum": 1
                    },
                    "total_order_value": {
                        "$sum": "$total_amount"
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "status": "$_id",
                    "order_count": 1,
                    "total_order_value": 1,
                }
            },
            {
                "$sort": {
                    "order_count": -1
                }
            },
        ]

    if name == "sales_by_payment_method":
        return [
            {
                "$group": {
                    "_id": "$payment_method",
                    "order_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$total_amount"
                    },
                    "average_order_value": {
                        "$avg": "$total_amount"
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "payment_method": "$_id",
                    "order_count": 1,
                    "total_sales": 1,
                    "average_order_value": {
                        "$round": [
                            "$average_order_value",
                            2,
                        ]
                    },
                }
            },
            {
                "$sort": {
                    "total_sales": -1
                }
            },
        ]

    if name == "delivery_type_summary":
        return [
            {
                "$group": {
                    "_id": "$delivery_type",
                    "order_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$total_amount"
                    },
                    "total_delivery_cost": {
                        "$sum": "$delivery_cost"
                    },
                    "average_delivery_cost": {
                        "$avg": "$delivery_cost"
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "delivery_type": "$_id",
                    "order_count": 1,
                    "total_sales": 1,
                    "total_delivery_cost": 1,
                    "average_delivery_cost": {
                        "$round": [
                            "$average_delivery_cost",
                            2,
                        ]
                    },
                }
            },
            {
                "$sort": {
                    "order_count": -1
                }
            },
        ]

    if name == "monthly_sales":
        return [
            {
                "$project": {
                    "month": {
                        "$substrBytes": [
                            "$order_date",
                            0,
                            7,
                        ]
                    },
                    "total_amount": 1,
                }
            },
            {
                "$group": {
                    "_id": "$month",
                    "order_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$total_amount"
                    },
                    "average_order_value": {
                        "$avg": "$total_amount"
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "month": "$_id",
                    "order_count": 1,
                    "total_sales": 1,
                    "average_order_value": {
                        "$round": [
                            "$average_order_value",
                            2,
                        ]
                    },
                }
            },
            {
                "$sort": {
                    "month": 1
                }
            },
        ]

    raise KeyError(
        f"Unknown aggregation: {name}"
    )


def run_aggregation(name):
    pipeline = build_pipeline(name)

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

        results = list(
            collection.aggregate(
                pipeline,
                allowDiskUse=True,
            )
        )

        return {
            "aggregation_name": name,
            "description":
                AGGREGATION_CATALOG[
                    name
                ][
                    "description"
                ],
            "result_count": len(results),
            "results": results,
        }

    finally:
        client.close()


def save_report(name, result):
    output_dir = Path(
        "reports/phase2/aggregations"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_dir
        / f"{name}.json"
    )

    output_path.write_text(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    return output_path


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run Phase 2 aggregation reports."
        )
    )

    group = parser.add_mutually_exclusive_group(
        required=True
    )

    group.add_argument(
        "--list",
        action="store_true",
        help="List available aggregation reports.",
    )

    group.add_argument(
        "--name",
        choices=sorted(
            AGGREGATION_CATALOG
        ),
        help="Run one aggregation report.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.list:
        print(
            json.dumps(
                AGGREGATION_CATALOG,
                ensure_ascii=False,
                indent=2,
            )
        )

        return 0

    result = run_aggregation(
        args.name
    )

    output_path = save_report(
        args.name,
        result,
    )

    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )

    print(
        f"\nSaved report: {output_path}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
