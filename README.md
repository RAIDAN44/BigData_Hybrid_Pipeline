# Hybrid Big Data Data-Quality Pipeline

University Big Data Project - Midterm + Final Phase 2

## Project Objective

Hybrid Raw-first ELT pipeline for dirty e-commerce orders using Python Batch, Apache PySpark, MongoDB, and MongoDB Spark Connector.

## Architecture

Dirty CSV -> File Router -> Python Batch (small) / PySpark (large) -> orders_raw -> Cleaning & Validation -> Valid / Corrected / Quarantined -> orders_validated / orders_quarantine -> Metrics

## Routing

Default threshold: 200 MB

- File <= 200 MB: Python Batch
- File > 200 MB: PySpark

The Router prints file size, threshold, selected engine, and selection reason.

## Main Command

python -m src.main --input "<CSV_PATH>"

Safe modes:

python -m src.main --input "<CSV_PATH>" --dry-route
python -m src.main --input "<CSV_PATH>" --raw-only

## Python Batch Path

- Python csv module
- Streaming CSV
- No list(reader)
- No full-file Pandas loading
- Configurable batch size
- MongoDB insert_many
- Tested batch size: 5,000

## PySpark Path

- SparkSession
- DataFrame API
- Explicit String schema
- MongoDB Spark Connector
- Parallel partitions

Official large run:
- File size: 12,650.32 MB
- Rows: 30,000,000
- Input partitions: 99
- Output partitions: 99
- CSV corrupt rows: 0
- Raw ingestion elapsed: 636.59 sec
- Raw throughput: 47,126.31 records/sec
- run_id: pipeline-20260816T235754Z-866233e7

## Raw-first ELT

Every record is inserted into orders_raw before cleaning. Raw metadata includes run_id, source file/path, ingestion timestamp, engine, and raw record. Dirty source values are preserved as strings.

## Quality Classification

Every Raw record becomes exactly one logical result:
- Valid
- Corrected
- Quarantined

Corrections are deterministic only. Unsafe or ambiguous records are quarantined instead of guessed.

## Cleaning Rules

Implemented rules include Arabic-digit normalization, decimal/thousand separator normalization, known price words, currency normalization, Yemen phone normalization, repeated email-symbol repair, date normalization, status synonym normalization, numeric-string quantity correction, missing-item-SKU quarantine, negative-quantity quarantine, item price/total derivation, and order-total recalculation.

Corrected records preserve an audit trail with field, original, corrected, and rule_code.

## Core Quarantine Codes

- MISSING_ORDER_ID
- MISSING_CUSTOMER_ID
- INVALID_IMPOSSIBLE_DATE
- CORRUPTED_ITEMS_JSON
- EMPTY_ITEMS
- UNKNOWN_PRICE
- AMBIGUOUS_NEGATIVE_VALUE
- DUPLICATE_ORDER_ID
- MULTIPLE_CONFLICTING_ERRORS

## Business Key and Idempotency

Stable business key: order_id

orders_validated has a unique index on order_id. Upsert is used so repeated processing does not create duplicate validated records.

## Official 100K Evidence

run_id: run-20260816T195634Z-2294f5ec

- Raw: 100,000
- Valid: 70,002
- Corrected: 21,697
- Quarantined: 8,301
- Consistency: 100,000 = 70,002 + 21,697 + 8,301

Preserved evidence collections:
- orders_validated_100k_evidence: 91,699
- orders_quarantine_100k_evidence: 8,301

## Official 30M Result

- Raw: 30,000,000
- Valid: 20,994,411
- Corrected: 6,501,781
- Quarantined: 2,503,808
- orders_validated: 27,496,192
- orders_quarantine: 2,503,808
- Consistency: PASS
- Quality ELT elapsed: 28,965.12 sec
- Quality ELT throughput: 1,035.73 records/sec

## Bounded-Memory Processing

The 30M ELT does not materialize the full MongoDB final state in Python RAM. Existing-state lookup is bounded to 2,000 keys per Raw batch using MongoDB queries.

## Final Reports

- reports/results.json
- reports/results.md
- reports/final_verification.json
- reports/spark_large_run_final.json
- reports/elt_write_report_large_30m_final.json
- reports/classification_dry_run.json
- reports/elt_write_report_final_idempotency.json
- reports/upsert_update_proof.json

## Testing

Run:

python -m pytest tests -q

Final verified result: 53 tests passed.

## Final Core Status

- File Router: PASS
- Python Batch: PASS
- PySpark PASS
- MongoDB Spark Connector: PASS
- Raw-first: PASS
- Cleaning rules: PASS
- Valid / Corrected / Quarantine: PASS
- Correction audit trail: PASS
- Upsert: PASS
- Idempotency: PASS
- 100K evidence preserved: PASS
- 12.35 GB execution: PASS
- 30M processing: PASS
- Final consistency: PASS
- Final metrics: PASS
- Automated tests: PASS

## Design Decisions and Rationale

The engineering rationale behind routing, the 200 MB threshold, Raw-first ELT,
fixed String schemas, deterministic corrections, quarantine, Upsert/idempotency,
bounded-memory processing, and the Spark design is documented in:

`docs/DESIGN_DECISIONS.md`

This document is also intended as a technical reference for the project viva.


---

---

## Final Phase 2

Phase 2 extends the existing Midterm project inside the same repository.
The original ingestion and ELT architecture remains the authoritative data pipeline.
Final features are added on top of `orders_validated`.

### Phase 2 Features

- 5 practical MongoDB queries.
- 3 query indexes, including one Compound Index.
- `explain("executionStats")` evidence before and after indexes.
- 5 aggregation reports.
- 2 Materialized Views with incremental refresh.
- 2 scheduled jobs using APScheduler.
- Unified FastAPI interface with Swagger `/docs`.

## Installation

Install the Python dependencies:

```bash
python -m pip install -r requirements.txt
```

MongoDB must be available before running database operations.

Default local connection:

```text
mongodb://127.0.0.1:27017
```

Configuration can be overridden with environment variables documented in `.env.example`.

## Unified FastAPI

Start the API from the project root:

```bash
python -m uvicorn api.app:app --host 127.0.0.1 --port 8000
```

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

Required endpoints:

```text
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
```

`POST /ingest` does not implement a new loader. It invokes the existing Midterm gateway:

```bash
python -m src.main --input "<CSV_PATH>"
```

Example request body:

```json
{
  "input_path": "data/samples/orders_small_sample.csv"
}
```

An optional `batch_size` may also be supplied for the Python Batch path.

## Queries

List all Phase 2 queries:

```bash
python -m src.phase2.queries --list
```

Implemented queries:

- `orders_by_customer`
- `orders_by_date_range`
- `orders_by_city_and_date`
- `high_value_orders`
- `orders_by_payment_method`

Each query accepts dynamic parameters and does not depend on a fixed dataset size or fixed result values.

## Query Indexes

Create or verify the required Phase 2 query indexes:

```bash
python -m src.phase2.indexes --create
```

Required query indexes:

- `ix_phase2_customer_id` ? `customer_id ASC`
- `ix_phase2_order_date` ? `order_date ASC`
- `ix_phase2_city_order_date` ? `city ASC, order_date ASC` ? Compound Index

Materialized View support indexes are operational indexes and are not counted as the three required query indexes.

### Explain Evidence

Three queries were measured with MongoDB execution statistics before and after creating the Phase 2 indexes.

- `reports/phase2/explain_before_final_indexes.json`
- `reports/phase2/explain_after_final_indexes.json`
- `reports/phase2/explain_before_after_comparison.json`

Execution time may vary with cache and I/O state, so evaluation also considers execution plan, `docsExamined`, and `keysExamined`.

## Aggregation Reports

List reports:

```bash
python -m src.phase2.aggregations --list
```

Run one report independently:

```bash
python -m src.phase2.aggregations --name orders_by_status
```

Implemented reports:

- `sales_by_city`
- `orders_by_status`
- `sales_by_payment_method`
- `delivery_type_summary`
- `monthly_sales`

Evidence is stored under `reports/phase2/aggregations/`.

`total_sales` represents aggregated validated order value and should not automatically be interpreted as realized accounting revenue.

## Materialized Views

Public Materialized View collections:

- `monthly_sales_summary`
- `city_sales_summary`

Initial bootstrap:

```bash
python -m src.phase2.materialized_views --bootstrap
```

Incremental refresh:

```bash
python -m src.phase2.materialized_views --refresh
```

Status:

```bash
python -m src.phase2.materialized_views --status
```

Incremental refresh uses `last_updated_at` as the change-detection watermark and `first_processed_at` to map changes to stable processing-minute partitions.

Only affected partitions are recomputed from `orders_validated`; public summaries are rolled up from smaller partial collections rather than rescanning the full validated dataset on every refresh.

Evidence:

- `reports/phase2/materialized_views_bootstrap.txt`
- `reports/phase2/materialized_views_refresh_no_changes.txt`
- `reports/phase2/mv_incremental_update_proof.json`

## Scheduled Jobs

List jobs:

```bash
python -m src.phase2.jobs --list
```

Implemented jobs:

- `refresh_materialized_views` ? every hour at minute 00 UTC.
- `generate_daily_analytics_report` ? daily at 02:00 UTC.

Manual execution:

```bash
python -m src.phase2.jobs --run refresh_materialized_views
python -m src.phase2.jobs --run generate_daily_analytics_report
```

Start APScheduler:

```bash
python -m src.phase2.jobs --start
```

Execution logs are stored in MongoDB collection `phase2_job_logs`.

Each log records `job_name`, `trigger_source`, `schedule`, `started_at`, `finished_at`, `duration_seconds`, `status`, `result`, and `error`.

Generated daily analytics files are stored under `reports/phase2/jobs/`.

## Phase 2 Runtime Evidence

Phase 2 evidence is isolated under `reports/phase2/`.

The API ingestion endpoint was verified with an isolated temporary database so production data was not modified.

- `reports/phase2/api/isolated_ingest_verification.json`

## Environment Configuration

No real credentials are committed.

Supported environment variables:

- `MONGO_URI`
- `MONGO_DATABASE`
- `SMALL_FILE_THRESHOLD_MB`
- `BATCH_SIZE`
- `SPARK_MASTER`

See `.env.example` for safe example values.

## Final Testing

Run the existing automated tests:

```bash
python -m pytest tests -q
```

Phase 2 must also work with different filenames, row counts, and business values. The implementation must not depend on fixed development data.
