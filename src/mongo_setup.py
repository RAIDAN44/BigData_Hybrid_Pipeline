from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
)


# ============================================================
# VALIDATED COLLECTION SCHEMA
# ============================================================

VALIDATED_SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",

        "required": [
            "order_id",
            "order_date",
            "status",
            "customer_id",
            "customer_name",
            "customer_phone",
            "customer_email",
            "city",
            "district",
            "delivery_type",
            "delivery_cost",
            "payment_method",
            "payment_status",
            "payment_amount",
            "currency",
            "total_amount",
            "items_json",
            "quality_status",
            "corrections",
            "record_fingerprint",
        ],

        "properties": {

            "order_id": {
                "bsonType": "string",
            },

            "order_date": {
                "bsonType": "string",
            },

            "status": {
                "bsonType": "string",
            },

            "customer_id": {
                "bsonType": "string",
            },

            "customer_name": {
                "bsonType": "string",
            },

            "customer_phone": {
                "bsonType": "string",
            },

            "customer_email": {
                "bsonType": "string",
            },

            "city": {
                "bsonType": "string",
            },

            "district": {
                "bsonType": "string",
            },

            "delivery_type": {
                "bsonType": "string",
            },

            "delivery_cost": {
                "bsonType": [
                    "int",
                    "long",
                    "double",
                    "decimal",
                ],
            },

            "payment_method": {
                "bsonType": "string",
            },

            "payment_status": {
                "bsonType": "string",
            },

            "payment_amount": {
                "bsonType": [
                    "int",
                    "long",
                    "double",
                    "decimal",
                ],
            },

            "currency": {
                "bsonType": "string",
            },

            "total_amount": {
                "bsonType": [
                    "int",
                    "long",
                    "double",
                    "decimal",
                ],
            },

            "items_json": {
                "bsonType": "string",
            },

            "quality_status": {
                "enum": [
                    "valid",
                    "corrected",
                ],
            },

            "corrections": {
                "bsonType": "array",
            },

            "record_fingerprint": {
                "bsonType": "string",
            },
        },
    },
}


def create_mongo_client():
    """
    Create and verify the MongoDB connection.
    """

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    client.admin.command("ping")

    return client


def get_database(client):
    """
    Return the project's MongoDB database.
    """

    return client[MONGO_DATABASE]


def ensure_validated_schema(db):
    """
    Ensure orders_validated has the MongoDB Schema Validation
    required by the assignment.

    This changes collection metadata only.
    It does not rewrite existing business documents.
    """

    existing = set(
        db.list_collection_names()
    )

    if VALIDATED_COLLECTION not in existing:

        db.create_collection(
            VALIDATED_COLLECTION,
            validator=VALIDATED_SCHEMA,
            validationLevel="strict",
            validationAction="error",
        )

    else:

        db.command({
            "collMod": VALIDATED_COLLECTION,
            "validator": VALIDATED_SCHEMA,
            "validationLevel": "strict",
            "validationAction": "error",
        })

    return db[VALIDATED_COLLECTION]
