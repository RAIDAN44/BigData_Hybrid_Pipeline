"""
Phase 2 practical queries.

These queries are additions for the final project.
They operate on the existing orders_validated collection
without changing the Midterm ingestion or ELT pipeline.
"""

import argparse
import json

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
)


QUERY_CATALOG = {
    "orders_by_customer": {
        "description": "Return orders for a specific customer.",
        "required_parameters": [
            "customer_id",
        ],
    },

    "orders_by_date_range": {
        "description": "Return orders inside a date range.",
        "required_parameters": [
            "start_date",
            "end_date",
        ],
    },

    "orders_by_city_and_date": {
        "description": (
            "Return orders for a city "
            "inside a date range."
        ),
        "required_parameters": [
            "city",
            "start_date",
            "end_date",
        ],
    },

    "high_value_orders": {
        "description": (
            "Return orders whose total amount "
            "is greater than or equal to a threshold."
        ),
        "required_parameters": [
            "min_amount",
        ],
    },

    "orders_by_payment_method": {
        "description": (
            "Return orders using a specific "
            "payment method."
        ),
        "required_parameters": [
            "payment_method",
        ],
    },
}


RESULT_PROJECTION = {
    "_id": 0,
    "order_id": 1,
    "order_date": 1,
    "status": 1,
    "customer_id": 1,
    "customer_name": 1,
    "city": 1,
    "district": 1,
    "delivery_type": 1,
    "payment_method": 1,
    "payment_status": 1,
    "total_amount": 1,
    "currency": 1,
    "quality_status": 1,
}


def get_query_catalog():
    """Return the five Phase 2 query definitions."""

    return QUERY_CATALOG


def _required_text(
    parameters,
    name,
):
    value = parameters.get(name)

    if value is None:
        raise ValueError(
            f"Missing required parameter: {name}"
        )

    value = str(value).strip()

    if not value:
        raise ValueError(
            f"Parameter cannot be empty: {name}"
        )

    return value


def _required_number(
    parameters,
    name,
):
    value = parameters.get(name)

    if value is None:
        raise ValueError(
            f"Missing required parameter: {name}"
        )

    if isinstance(value, bool):
        raise ValueError(
            f"Parameter must be numeric: {name}"
        )

    try:
        return float(value)

    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Parameter must be numeric: {name}"
        ) from exc


def _normalize_limit(limit):
    try:
        value = int(limit)

    except (TypeError, ValueError) as exc:
        raise ValueError(
            "limit must be an integer."
        ) from exc

    if value < 1:
        raise ValueError(
            "limit must be greater than zero."
        )

    return min(
        value,
        1000,
    )


def build_query(
    name,
    parameters,
):
    """
    Build the MongoDB filter for one Phase 2 query.
    """

    if name not in QUERY_CATALOG:
        raise KeyError(
            f"Unknown query: {name}"
        )

    if name == "orders_by_customer":

        customer_id = _required_text(
            parameters,
            "customer_id",
        )

        return {
            "customer_id":
                customer_id,
        }

    if name == "orders_by_date_range":

        start_date = _required_text(
            parameters,
            "start_date",
        )

        end_date = _required_text(
            parameters,
            "end_date",
        )

        if start_date > end_date:
            raise ValueError(
                "start_date must not be "
                "after end_date."
            )

        return {
            "order_date": {
                "$gte":
                    start_date,

                "$lte":
                    end_date,
            }
        }

    if name == "orders_by_city_and_date":

        city = _required_text(
            parameters,
            "city",
        )

        start_date = _required_text(
            parameters,
            "start_date",
        )

        end_date = _required_text(
            parameters,
            "end_date",
        )

        if start_date > end_date:
            raise ValueError(
                "start_date must not be "
                "after end_date."
            )

        return {
            "city":
                city,

            "order_date": {
                "$gte":
                    start_date,

                "$lte":
                    end_date,
            },
        }

    if name == "high_value_orders":

        min_amount = _required_number(
            parameters,
            "min_amount",
        )

        return {
            "total_amount": {
                "$gte":
                    min_amount,
            }
        }

    if name == "orders_by_payment_method":

        payment_method = _required_text(
            parameters,
            "payment_method",
        )

        return {
            "payment_method":
                payment_method,
        }

    raise KeyError(
        f"Unknown query: {name}"
    )


def run_query(
    name,
    parameters,
    limit=50,
):
    """
    Execute one named Phase 2 query.

    This function is reusable later by FastAPI.
    """

    query_filter = build_query(
        name,
        parameters,
    )

    normalized_limit = _normalize_limit(
        limit
    )

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    try:
        client.admin.command("ping")

        db = client[
            MONGO_DATABASE
        ]

        collection = db[
            VALIDATED_COLLECTION
        ]

        documents = list(
            collection.find(
                query_filter,
                RESULT_PROJECTION,
            ).limit(
                normalized_limit
            )
        )

        return {
            "query_name":
                name,

            "description":
                QUERY_CATALOG[
                    name
                ][
                    "description"
                ],

            "filter":
                query_filter,

            "limit":
                normalized_limit,

            "returned_count":
                len(documents),

            "results":
                documents,
        }

    finally:
        client.close()


def _parse_parameters(values):
    parameters = {}

    for value in values:
        if "=" not in value:
            raise ValueError(
                "Parameters must use key=value format."
            )

        key, raw_value = value.split(
            "=",
            1,
        )

        key = key.strip()

        if not key:
            raise ValueError(
                "Parameter name cannot be empty."
            )

        parameters[
            key
        ] = raw_value.strip()

    return parameters


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run Phase 2 practical queries."
        )
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List available Phase 2 queries.",
    )

    parser.add_argument(
        "--name",
        choices=sorted(
            QUERY_CATALOG
        ),
        help="Query name to execute.",
    )

    parser.add_argument(
        "--param",
        action="append",
        default=[],
        help=(
            "Query parameter in key=value format. "
            "May be repeated."
        ),
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum records returned. Default: 50.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.list:
        print(
            json.dumps(
                QUERY_CATALOG,
                ensure_ascii=False,
                indent=2,
            )
        )

        return 0

    if not args.name:
        raise SystemExit(
            "Use --list or provide --name."
        )

    parameters = _parse_parameters(
        args.param
    )

    result = run_query(
        args.name,
        parameters,
        limit=args.limit,
    )

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
