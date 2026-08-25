"""
spark_loader.py

المهمة:
هذا الملف مسؤول عن إدخال الملفات الكبيرة إلى MongoDB باستخدام PySpark.

الفكرة:
- قراءة CSV الكبير باستخدام Spark DataFrame.
- تقسيم العمل على Partitions لمعالجة متوازية.
- الاحتفاظ بالبيانات Raw بدون Cleaning.
- إضافة Metadata لكل سجل.
- الكتابة إلى MongoDB باستخدام MongoDB Spark Connector.
- التحقق بعد الإدخال أن عدد السجلات صحيح ولا توجد CSV Corrupt Records.
"""

import argparse
import json
import os
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

# أدوات PySpark
from pyspark import StorageLevel
from pyspark.sql import SparkSession

# دوال تستخدم في بناء وتحويل DataFrame.
from pyspark.sql.functions import (
    col,
    current_timestamp,
    length,
    lit,
    spark_partition_id,
    struct,
    trim,
)

# أنواع Schema الخاصة بـ Spark.
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# إعدادات المشروع.
from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    REPORTS_DIR,
    SMALL_FILE_THRESHOLD_MB,
    SPARK_APP_NAME,
    SPARK_MASTER,
)


# ============================================================
# SOURCE CONTRACT
# الأعمدة التي نتوقع وجودها في CSV
# ============================================================

RAW_COLUMNS = [
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
]

# Spark يضع هنا الصف الذي فشل Parsing الخاص به.
CORRUPT_COLUMN = "_corrupt_record"

# Prefix خاص بالـ Collections المؤقتة للاختبارات.
TEMP_COLLECTION_PREFIX = "_spark_"


# ============================================================
# SCHEMA
# تعريف شكل CSV بشكل صريح
# ============================================================

def build_csv_schema():

    # نحفظ كل قيم المصدر كـ String حتى لا يغير Spark
    # البيانات الخام قبل مرحلة Cleaning.
    fields = [
        StructField(
            name,
            StringType(),
            True,
        )
        for name in RAW_COLUMNS
    ]

    # عمود خاص بالصفوف التالفة أثناء CSV Parsing.
    fields.append(
        StructField(
            CORRUPT_COLUMN,
            StringType(),
            True,
        )
    )

    return StructType(fields)


# ============================================================
# CONNECTOR JAR DISCOVERY
# البحث عن ملفات MongoDB Spark Connector
# ============================================================

def _jar_candidates():

    home = Path.home()

    # أماكن شائعة تخزن فيها Java/Spark Dependencies.
    roots = [
        home / ".ivy2" / "jars",
        home / ".ivy2" / "cache",
        home / ".m2" / "repository",
        home / ".cache" / "coursier",
    ]

    return [
        root
        for root in roots
        if root.exists()
    ]


def discover_mongo_jars():
    """
    العثور على MongoDB Spark Connector والـ Java Drivers المطلوبة.
    """

    # يمكن تحديد JARs يدويًا باستخدام Environment Variable.
    override = os.getenv(
        "MONGO_SPARK_JARS",
        "",
    ).strip()

    if override:

        separators = [";", ","]
        values = [override]

        for separator in separators:

            split_values = []

            for value in values:
                split_values.extend(
                    value.split(separator)
                )

            values = split_values

        jars = [
            str(Path(value.strip()).resolve())
            for value in values
            if value.strip()
        ]

        missing = [
            value
            for value in jars
            if not Path(value).exists()
        ]

        if missing:
            raise RuntimeError(
                "MONGO_SPARK_JARS contains missing files:\n"
                + "\n".join(missing)
            )

        return jars


    # المكتبات الأساسية المطلوبة للربط مع MongoDB.
    required_patterns = {
        "connector": "mongo-spark-connector_2.13",
        "driver_sync": "mongodb-driver-sync",
        "driver_core": "mongodb-driver-core",
        "bson": "bson-",
    }

    found = {}

    # البحث داخل Cache الجهاز.
    for root in _jar_candidates():

        try:

            iterator = root.rglob("*.jar")

            for jar in iterator:

                name = jar.name.lower()

                for key, pattern in required_patterns.items():

                    if (
                        key not in found
                        and pattern.lower() in name
                    ):
                        found[key] = str(
                            jar.resolve()
                        )

                # Dependency إضافية اختيارية.
                if (
                    "bson_record_codec" not in found
                    and "bson-record-codec" in name
                ):
                    found["bson_record_codec"] = str(
                        jar.resolve()
                    )

        except PermissionError:
            continue


    # التأكد أن كل Dependencies الأساسية موجودة.
    missing = [
        key
        for key in required_patterns
        if key not in found
    ]

    if missing:

        searched = "\n".join(
            str(path)
            for path in _jar_candidates()
        )

        raise RuntimeError(
            "MongoDB Spark Connector JARs were not "
            "found in the local cache.\n"
            f"Missing: {missing}\n"
            f"Searched:\n{searched}\n"
            "Do not continue to the large file."
        )


    order = [
        "connector",
        "driver_sync",
        "driver_core",
        "bson",
        "bson_record_codec",
    ]

    return [
        found[key]
        for key in order
        if key in found
    ]


# ============================================================
# SPARK SESSION
# إنشاء بيئة Spark
# ============================================================

def create_spark_session(jars):

    classpath = os.pathsep.join(
        jars
    )

    # إنشاء SparkSession باستخدام الإعدادات الموجودة في settings.py.
    builder = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(
            f"{SPARK_APP_NAME}-RawLoader"
        )

        # تحميل MongoDB Connector JARs.
        .config(
            "spark.jars",
            ",".join(jars),
        )
        .config(
            "spark.driver.extraClassPath",
            classpath,
        )
        .config(
            "spark.executor.extraClassPath",
            classpath,
        )

        # اتصال MongoDB.
        .config(
            "spark.mongodb.write.connection.uri",
            MONGO_URI,
        )

        # توحيد التوقيت إلى UTC.
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )

        # ضمان كشف Corrupt Records بشكل صحيح.
        .config(
            "spark.sql.csv.parser."
            "columnPruning.enabled",
            "false",
        )
    )

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    return spark


# ============================================================
# CSV READ
# قراءة CSV باستخدام Spark
# ============================================================

def read_raw_csv(
    spark,
    input_path,
):

    schema = build_csv_schema()

    dataframe = (
        spark.read
        .format("csv")

        # أول صف هو Header.
        .option(
            "header",
            "true",
        )

        .option(
            "encoding",
            "UTF-8",
        )

        # إعدادات التعامل مع علامات الاقتباس داخل CSV.
        .option(
            "quote",
            '"',
        )
        .option(
            "escape",
            '"',
        )

        # PERMISSIVE يسمح بحفظ الصف التالف بدل إسقاطه مباشرة.
        .option(
            "mode",
            "PERMISSIVE",
        )

        .option(
            "columnNameOfCorruptRecord",
            CORRUPT_COLUMN,
        )

        .option(
            "multiLine",
            "false",
        )

        # السماح بحقول طويلة مثل items_json.
        .option(
            "maxCharsPerColumn",
            "-1",
        )

        # استخدام Schema ثابتة بدل Auto Inference.
        .schema(schema)

        # Spark يقرأ الملف ويوزعه على Partitions.
        .load(str(input_path))
    )

    return dataframe


# ============================================================
# RAW DOCUMENT SHAPE
# بناء شكل Document الذي سيذهب إلى MongoDB
# ============================================================

def build_raw_output(
    dataframe,
    input_path,
    run_id,
):

    path = Path(
        input_path
    ).resolve()

    # تجميع أعمدة CSV داخل raw_record.
    raw_struct = struct(
        *[
            col(name).alias(name)
            for name in RAW_COLUMNS
        ]
    )

    return dataframe.select(

        # معرف التشغيل.
        lit(run_id).alias(
            "run_id"
        ),

        # اسم ومسار المصدر.
        lit(path.name).alias(
            "source_file"
        ),

        lit(str(path)).alias(
            "source_path"
        ),

        # Spark لا يعطينا رقم سطر CSV متسلسل موثوق
        # بسبب القراءة الموزعة على Partitions.
        lit(None)
        .cast("long")
        .alias(
            "source_row_number"
        ),

        lit(False).alias(
            "source_row_number_available"
        ),

        # وقت الإدخال.
        current_timestamp().alias(
            "ingested_at"
        ),

        # المحرك المستخدم.
        lit("pyspark").alias(
            "engine_used"
        ),

        # رقم Partition التي عالجت السجل.
        spark_partition_id().alias(
            "spark_partition_id"
        ),

        # إذا فشل Parsing للصف يتم حفظه هنا.
        col(
            CORRUPT_COLUMN
        ).alias(
            "csv_corrupt_record"
        ),

        # البيانات الأصلية.
        raw_struct.alias(
            "raw_record"
        ),
    )


# ============================================================
# MONGO HELPERS
# دوال مساعدة للتعامل مع MongoDB
# ============================================================

def mongo_client():

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    # اختبار الاتصال.
    client.admin.command(
        "ping"
    )

    return client


def production_counts(db):

    # حساب عدد Documents في Collections الأساسية.
    return {
        RAW_COLLECTION:
            db[RAW_COLLECTION].count_documents({}),

        VALIDATED_COLLECTION:
            db[VALIDATED_COLLECTION].count_documents({}),

        QUARANTINE_COLLECTION:
            db[QUARANTINE_COLLECTION].count_documents({}),
    }


def safe_drop_test_collection(
    db,
    collection_name,
):

    # حماية: يسمح بالحذف التلقائي فقط للـ Collections المؤقتة.
    if not collection_name.startswith(
        TEMP_COLLECTION_PREFIX
    ):
        raise RuntimeError(
            "Safety stop: automatic drop is "
            "allowed only for collections beginning "
            f"with {TEMP_COLLECTION_PREFIX!r}."
        )

    db[
        collection_name
    ].drop()


# ============================================================
# ARGUMENTS
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "PySpark Raw-first CSV loader "
            "using MongoDB Spark Connector."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
    )

    parser.add_argument(
        "--collection",
        required=True,
    )

    parser.add_argument(
        "--expected-rows",
        type=int,
        default=None,
    )

    parser.add_argument(
        "--drop-target",
        action="store_true",
    )

    parser.add_argument(
        "--cleanup-after",
        action="store_true",
    )

    # تصريح يسمح بالكتابة في orders_raw الرسمية.
    parser.add_argument(
        "--production-raw",
        action="store_true",
        help=(
            "Authorize append to the official orders_raw "
            "collection. This mode never drops or cleans "
            "the production Raw collection."
        ),
    )

    # Run ID يمكن أن يأتي من main.py.
    parser.add_argument(
        "--run-id",
        default=None,
        help=(
            "Optional externally supplied pipeline run_id. "
            "Used by the unified main entry point."
        ),
    )

    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================

def main():

    args = parse_args()

    input_path = Path(
        args.input
    ).resolve()

    # التأكد أن الملف موجود.
    if not input_path.exists():
        raise FileNotFoundError(
            input_path
        )


    # ========================================================
    # PRODUCTION RAW SAFETY
    # حماية Collection الرسمية من الحذف أو التنظيف
    # ========================================================

    if args.production_raw:

        if args.collection != RAW_COLLECTION:
            raise RuntimeError(
                "--production-raw may only target "
                f"{RAW_COLLECTION!r}."
            )

        if args.drop_target:
            raise RuntimeError(
                "--drop-target is forbidden in "
                "production Raw mode."
            )

        if args.cleanup_after:
            raise RuntimeError(
                "--cleanup-after is forbidden in "
                "production Raw mode."
            )


    # حساب حجم الملف.
    file_size_bytes = input_path.stat().st_size

    file_size_mb = (
        file_size_bytes
        / (1024 ** 2)
    )


    # الملفات الصغيرة يمكن Cache لها أثناء الاختبارات.
    # الملفات الكبيرة لا نعمل لها Cache حتى لا نضغط RAM/Disk.
    small_validation_mode = (
        file_size_mb
        <= SMALL_FILE_THRESHOLD_MB
    )


    # استخدام Run ID من main.py إذا تم تمريره.
    if args.run_id is not None:

        run_id = args.run_id.strip()

        if not run_id:
            raise RuntimeError(
                "--run-id cannot be empty."
            )

        if len(run_id) > 128:
            raise RuntimeError(
                "--run-id is too long."
            )

        if any(
            character.isspace()
            for character in run_id
        ):
            raise RuntimeError(
                "--run-id cannot contain whitespace."
            )

        run_id_source = "external"

    else:

        # إذا لم يصل Run ID ننشئ واحدًا جديدًا.
        run_id = (
            "spark-"
            + datetime.now(
                timezone.utc
            ).strftime(
                "%Y%m%dT%H%M%SZ"
            )
            + "-"
            + uuid.uuid4().hex[:8]
        )

        run_id_source = "generated"


    spark = None
    client = None
    dataframe = None
    cleanup_done = False

    started = time.perf_counter()


    print("=" * 92)
    print("PHASE 14A - PYSPARK RAW LOADER TEST")
    print("=" * 92)

    print(f"Input                   : {input_path}")
    print(f"File size               : {file_size_mb:,.2f} MB")
    print(f"Target database         : {MONGO_DATABASE}")
    print(f"Target collection       : {args.collection}")
    print(f"run_id                  : {run_id}")
    print("Engine                  : PySpark")

    print(
        "Execution mode          : "
        + (
            "small_validation_cache"
            if small_validation_mode
            else "large_streaming_no_cache"
        )
    )

    print(
        "Cache enabled           : "
        + (
            "YES"
            if small_validation_mode
            else "NO"
        )
    )

    print("=" * 92)


    try:

        # ====================================================
        # 1. MONGODB PRE-CHECK
        # ====================================================

        client = mongo_client()

        db = client[
            MONGO_DATABASE
        ]

        # حفظ أعداد Collections قبل التشغيل.
        production_before = production_counts(
            db
        )


        if args.production_raw:

            existing_target = (
                db[
                    RAW_COLLECTION
                ].count_documents({})
            )

            # منع تشغيل نفس run_id مرتين.
            existing_same_run = (
                db[
                    RAW_COLLECTION
                ].count_documents(
                    {
                        "run_id": run_id
                    }
                )
            )

            if existing_same_run != 0:
                raise RuntimeError(
                    "Production run_id already exists "
                    f"in orders_raw: {run_id} "
                    f"({existing_same_run:,} documents). "
                    "Refusing accidental duplicate append."
                )

            print(
                "\n[1/7] Production Raw append authorization: PASS"
            )

            print(
                f"Existing Raw documents  : "
                f"{existing_target:,}"
            )

            print(
                "Existing same run_id    : 0 - PASS"
            )


        elif args.drop_target:

            safe_drop_test_collection(
                db,
                args.collection,
            )

            print(
                "\n[1/7] Test target reset: PASS"
            )


        else:

            existing_target = (
                db[
                    args.collection
                ].count_documents({})
            )

            if existing_target != 0:
                raise RuntimeError(
                    "Target collection is not empty. "
                    "Use an isolated empty collection "
                    "or explicitly authorize "
                    "--production-raw."
                )

            print(
                "\n[1/7] Empty target check: PASS"
            )


        # ====================================================
        # 2. CONNECTOR JARS
        # ====================================================

        print(
            "\n[2/7] Discovering MongoDB "
            "Spark Connector JARs..."
        )

        jars = discover_mongo_jars()

        for jar in jars:
            print(
                f"  {jar}"
            )

        print(
            "Connector dependencies  : PASS"
        )


        # ====================================================
        # 3. SPARK SESSION
        # ====================================================

        print(
            "\n[3/7] Starting SparkSession..."
        )

        spark = create_spark_session(
            jars
        )

        spark_version = spark.version

        scala_version = (
            spark.sparkContext
            ._jvm
            .scala.util.Properties
            .versionNumberString()
        )

        ui_url = (
            spark.sparkContext.uiWebUrl
        )

        print(
            f"Spark version           : "
            f"{spark_version}"
        )

        print(
            f"Scala version           : "
            f"{scala_version}"
        )

        print(
            f"Spark master            : "
            f"{spark.sparkContext.master}"
        )

        print(
            f"Spark UI                : "
            f"{ui_url}"
        )


        # ====================================================
        # 4. READ CSV
        # Spark يقسم الملف إلى Partitions ويقرأها بالتوازي
        # ====================================================

        print(
            "\n[4/7] Reading CSV with "
            "explicit String schema..."
        )

        dataframe = read_raw_csv(
            spark,
            input_path,
        )

        # عدد Partitions التي قسم Spark البيانات إليها.
        partitions = (
            dataframe.rdd
            .getNumPartitions()
        )

        print(
            f"Partitions              : "
            f"{partitions}"
        )


        rows_read = None
        corrupt_rows = None


        if small_validation_mode:

            # في الاختبارات الصغيرة نعمل Cache
            # لأننا سننفذ أكثر من Action على نفس DataFrame.
            dataframe.persist(
                StorageLevel.MEMORY_AND_DISK
            )

            rows_read = (
                dataframe.count()
            )

            # حساب صفوف CSV التالفة.
            corrupt_rows = (
                dataframe
                .filter(
                    col(
                        CORRUPT_COLUMN
                    ).isNotNull()
                    & (
                        length(
                            trim(
                                col(
                                    CORRUPT_COLUMN
                                )
                            )
                        ) > 0
                    )
                )
                .count()
            )

            print(
                f"Rows read               : "
                f"{rows_read:,}"
            )

            print(
                f"CSV corrupt records     : "
                f"{corrupt_rows:,}"
            )

            if corrupt_rows != 0:
                raise RuntimeError(
                    "CSV parsing integrity failure: "
                    f"{corrupt_rows:,} structurally corrupt "
                    "CSV rows detected."
                )

            print(
                "CSV parsing integrity    : PASS"
            )


            if (
                args.expected_rows
                is not None
                and rows_read
                != args.expected_rows
            ):
                raise RuntimeError(
                    "Spark row-count mismatch: "
                    f"expected={args.expected_rows:,}, "
                    f"actual={rows_read:,}"
                )


        else:

            # في الملفات الكبيرة نتجنب count() قبل الكتابة،
            # لأن count نفسها Spark Action وستقرأ الملف بالكامل.
            print(
                "Pre-write full count    : SKIPPED"
            )

            print(
                "Pre-write corrupt count : SKIPPED"
            )

            print(
                "Large-file cache        : DISABLED"
            )


        # التأكد أن Schema الفعلية مطابقة للعقد المتوقع.
        actual_columns = [
            field.name
            for field
            in dataframe.schema.fields
        ]

        expected_columns = (
            RAW_COLUMNS
            + [CORRUPT_COLUMN]
        )

        if actual_columns != expected_columns:
            raise RuntimeError(
                "Spark source schema "
                "does not match contract."
            )


        # كل حقول Raw يجب أن تبقى Strings.
        for field in dataframe.schema.fields:

            if not isinstance(
                field.dataType,
                StringType,
            ):
                raise RuntimeError(
                    "Non-string Raw source field "
                    f"detected: {field.name}"
                )

        print(
            "Fixed String schema      : PASS"
        )


        # ====================================================
        # 5. BUILD RAW DOCUMENTS
        # ====================================================

        print(
            "\n[5/7] Building Raw-first "
            "MongoDB documents..."
        )

        raw_output = build_raw_output(
            dataframe,
            input_path,
            run_id,
        )

        # عدد Partitions بعد تجهيز Output.
        output_partitions = (
            raw_output.rdd
            .getNumPartitions()
        )

        print(
            f"Output partitions       : "
            f"{output_partitions}"
        )

        print(
            "Raw metadata             : PASS"
        )


        # ====================================================
        # 6. WRITE TO MONGODB
        # الكتابة المتوازية عبر Spark Connector
        # ====================================================

        print(
            "\n[6/7] Writing with MongoDB "
            "Spark Connector..."
        )

        write_started = (
            time.perf_counter()
        )

        (
            raw_output.write

            # استخدام MongoDB Spark Connector.
            .format("mongodb")

            # إضافة البيانات الجديدة بدون حذف القديمة.
            .mode("append")

            .option(
                "connection.uri",
                MONGO_URI,
            )

            .option(
                "database",
                MONGO_DATABASE,
            )

            .option(
                "collection",
                args.collection,
            )

            # Raw layer تستخدم Insert.
            .option(
                "operationType",
                "insert",
            )

            # السماح بإدخال Parallel/Unordered.
            .option(
                "ordered",
                "false",
            )

            # هذه Spark Action الفعلية التي تبدأ التنفيذ.
            .save()
        )


        write_elapsed = (
            time.perf_counter()
            - write_started
        )

        print(
            f"Connector write time    : "
            f"{write_elapsed:.2f} sec"
        )


        # ====================================================
        # 7. VERIFY MONGODB
        # ====================================================

        print(
            "\n[7/7] Verifying MongoDB..."
        )

        target = db[
            args.collection
        ]

        # عدد السجلات الخاصة بهذا Run فقط.
        mongo_run_count = (
            target.count_documents(
                {
                    "run_id": run_id
                }
            )
        )

        # العدد الكلي في Collection.
        mongo_total_count = (
            target.count_documents({})
        )

        # حساب Corrupt Records بعد الكتابة.
        mongo_corrupt_rows = (
            target.count_documents(
                {
                    "run_id": run_id,
                    "csv_corrupt_record": {
                        "$nin": [
                            None,
                            "",
                        ]
                    },
                }
            )
        )


        # في الملف الكبير لم نعمل Spark count قبل الكتابة،
        # لذلك نستخدم عدد Documents المكتوبة لهذا Run.
        if rows_read is None:
            rows_read = mongo_run_count

        if corrupt_rows is None:
            corrupt_rows = mongo_corrupt_rows


        # التأكد أن Spark وMongoDB متوافقان.
        if mongo_run_count != rows_read:
            raise RuntimeError(
                "Mongo/Spark count mismatch: "
                f"Spark={rows_read:,}, "
                f"Mongo={mongo_run_count:,}"
            )


        if mongo_corrupt_rows != corrupt_rows:
            raise RuntimeError(
                "Corrupt-row verification mismatch: "
                f"Spark={corrupt_rows:,}, "
                f"Mongo={mongo_corrupt_rows:,}"
            )


        if corrupt_rows != 0:
            raise RuntimeError(
                "CSV parsing integrity failure after Raw load: "
                f"{corrupt_rows:,} structurally corrupt rows."
            )


        print(
            f"Mongo corrupt rows      : "
            f"{mongo_corrupt_rows:,}"
        )

        print(
            "CSV parsing integrity    : PASS"
        )


        # أخذ سجل Sample للتحقق من شكل Document.
        sample_document = (
            target.find_one(
                {
                    "run_id": run_id
                },
                {
                    "_id": 0,
                },
            )
        )

        if not sample_document:
            raise RuntimeError(
                "No MongoDB sample document "
                "found after Spark write."
            )


        # Metadata التي يجب أن تكون موجودة.
        required_metadata = {
            "run_id",
            "source_file",
            "source_path",
            "source_row_number",
            "source_row_number_available",
            "ingested_at",
            "engine_used",
            "spark_partition_id",
            "csv_corrupt_record",
            "raw_record",
        }


        missing_metadata = (
            required_metadata
            - set(
                sample_document.keys()
            )
        )

        if missing_metadata:
            raise RuntimeError(
                "Missing Raw metadata fields: "
                f"{sorted(missing_metadata)}"
            )


        # التأكد أن السجل كتب بواسطة PySpark.
        if (
            sample_document.get(
                "engine_used"
            )
            != "pyspark"
        ):
            raise RuntimeError(
                "engine_used is not pyspark."
            )


        sample_raw = (
            sample_document.get(
                "raw_record"
            )
        )

        if not isinstance(
            sample_raw,
            dict,
        ):
            raise RuntimeError(
                "raw_record was not stored "
                "as a nested document."
            )


        # التأكد أن جميع أعمدة المصدر محفوظة.
        missing_raw_fields = (
            set(RAW_COLUMNS)
            - set(sample_raw.keys())
        )

        if missing_raw_fields:
            raise RuntimeError(
                "Missing source fields in "
                f"raw_record: "
                f"{sorted(missing_raw_fields)}"
            )


        print(
            f"Spark rows              : "
            f"{rows_read:,}"
        )

        print(
            f"Mongo run rows          : "
            f"{mongo_run_count:,}"
        )

        print(
            f"Mongo total target rows : "
            f"{mongo_total_count:,}"
        )

        print(
            "Spark = Mongo            : PASS"
        )


        # ====================================================
        # PRODUCTION SAFETY CHECK
        # التأكد أن Collections الأخرى لم تتغير
        # ====================================================

        production_after = production_counts(
            db
        )


        if args.production_raw:

            # العدد الجديد المتوقع في orders_raw.
            expected_raw_after = (
                production_before[
                    RAW_COLLECTION
                ]
                + mongo_run_count
            )

            raw_append_ok = (
                production_after[
                    RAW_COLLECTION
                ]
                == expected_raw_after
            )

            # Raw Loader يجب ألا يغير validated أو quarantine.
            validated_unchanged = (
                production_after[
                    VALIDATED_COLLECTION
                ]
                == production_before[
                    VALIDATED_COLLECTION
                ]
            )

            quarantine_unchanged = (
                production_after[
                    QUARANTINE_COLLECTION
                ]
                == production_before[
                    QUARANTINE_COLLECTION
                ]
            )

            production_safety_pass = (
                raw_append_ok
                and validated_unchanged
                and quarantine_unchanged
            )

            if not production_safety_pass:
                raise RuntimeError(
                    "Production Raw safety gate failed. "
                    "Raw append count and/or downstream "
                    "collection counts are inconsistent."
                )


        else:

            production_safety_pass = (
                production_before
                == production_after
            )

            if not production_safety_pass:
                raise RuntimeError(
                    "Production collections changed "
                    "during isolated Spark test."
                )


        # ====================================================
        # PERFORMANCE
        # ====================================================

        elapsed = (
            time.perf_counter()
            - started
        )

        # Overall Throughput.
        throughput = (
            rows_read / elapsed
            if elapsed > 0
            else 0
        )


        # ====================================================
        # REPORT
        # حفظ تقرير التنفيذ
        # ====================================================

        report = {

            "phase": (
                "phase_14c_large_pyspark_raw"
                if args.production_raw
                else "phase_14a_pyspark_raw_test"
            ),

            "mode": (
                "production_raw_append"
                if args.production_raw
                else "isolated_temp_collection"
            ),

            "run_id": run_id,

            "run_id_source": run_id_source,

            "input": {
                "path": str(input_path),
                "file_name": input_path.name,
                "file_size_bytes": file_size_bytes,
                "file_size_mb": round(
                    file_size_mb,
                    2,
                ),
            },

            "engine": {
                "engine_used": "pyspark",
                "spark_version": spark_version,
                "scala_version": scala_version,
                "master": spark.sparkContext.master,
                "spark_ui": ui_url,
                "input_partitions": partitions,
                "output_partitions": output_partitions,

                "execution_mode": (
                    "small_validation_cache"
                    if small_validation_mode
                    else "large_streaming_no_cache"
                ),

                "cache_enabled": (
                    small_validation_mode
                ),
            },

            "schema": {
                "explicit": True,
                "raw_fields_are_strings": True,
                "source_columns": RAW_COLUMNS,
                "corrupt_record_field": CORRUPT_COLUMN,
            },

            "counts": {
                "rows_read": rows_read,
                "csv_corrupt_records": corrupt_rows,
                "mongo_run_count": mongo_run_count,
                "mongo_total_count": mongo_total_count,
            },

            "performance": {
                "write_elapsed_seconds": round(
                    write_elapsed,
                    4,
                ),

                "total_elapsed_seconds": round(
                    elapsed,
                    4,
                ),

                "throughput_records_per_second": round(
                    throughput,
                    2,
                ),
            },

            "connector_jars": jars,

            "production_counts_before": production_before,

            "production_counts_after": production_after,

            "production_unchanged": (
                production_before
                == production_after
            ),

            "production_safety_pass": (
                production_safety_pass
            ),

            "production_raw_mode": (
                args.production_raw
            ),

            "sample_document": sample_document,
        }


        # تحديد اسم التقرير.
        report_path = (
            REPORTS_DIR
            / (
                "spark_large_run.json"
                if args.production_raw
                else "spark_loader_test.json"
            )
        )


        # حفظ Report بصيغة JSON.
        with report_path.open(
            "w",
            encoding="utf-8",
        ) as file:

            json.dump(
                report,
                file,
                ensure_ascii=False,
                indent=2,
                default=str,
            )


        # ====================================================
        # CLEAN TEMP COLLECTION
        # تنظيف Collection الاختبارية إذا طلب المستخدم
        # ====================================================

        if args.cleanup_after:

            safe_drop_test_collection(
                db,
                args.collection,
            )

            cleanup_done = True

            if (
                args.collection
                in db.list_collection_names()
            ):
                raise RuntimeError(
                    "Temporary Spark collection "
                    "was not removed."
                )


        # ====================================================
        # FINAL SUMMARY
        # ====================================================

        print("\n" + "=" * 92)
        print(
            "PHASE 14A PYSPARK TEST SUMMARY"
        )
        print("=" * 92)

        print(
            f"Input size              : "
            f"{file_size_mb:,.2f} MB"
        )

        print(
            f"Rows read               : "
            f"{rows_read:,}"
        )

        print(
            f"Mongo rows              : "
            f"{mongo_run_count:,}"
        )

        print(
            f"CSV corrupt rows        : "
            f"{corrupt_rows:,}"
        )

        print(
            f"Input partitions        : "
            f"{partitions}"
        )

        print(
            f"Output partitions       : "
            f"{output_partitions}"
        )

        print(
            f"Spark UI                : "
            f"{ui_url}"
        )

        print(
            f"Elapsed                 : "
            f"{elapsed:.2f} sec"
        )

        print(
            f"Throughput              : "
            f"{throughput:,.2f} "
            f"records/sec"
        )


        print(
            "\nSAFETY:"
        )

        if args.production_raw:

            print(
                "  Raw append count                  : PASS"
            )

            print(
                "  orders_validated unchanged        : PASS"
            )

            print(
                "  orders_quarantine unchanged       : PASS"
            )

            print(
                "  Production Raw safety gate        : PASS"
            )

        else:

            print(
                "  Production collections unchanged : PASS"
            )

            print(
                "  Temporary target cleaned         : "
                + (
                    "PASS"
                    if (
                        not args.cleanup_after
                        or cleanup_done
                    )
                    else "FAIL"
                )
            )


        print(
            "\nReport:"
        )

        print(report_path)

        print("\n" + "=" * 92)

        print(
            "PHASE 14A PYSPARK RAW LOADER TEST: PASS"
        )

        print("=" * 92)


    # ========================================================
    # CLEANUP
    # يتم التنفيذ حتى لو حدث Exception
    # ========================================================

    finally:

        # إزالة Cache إذا تم استخدامها.
        if (
            dataframe is not None
            and small_validation_mode
        ):
            try:
                dataframe.unpersist()
            except Exception:
                pass


        # إيقاف SparkSession.
        if spark is not None:

            try:
                spark.stop()
            except Exception:
                pass


        # إغلاق MongoDB Client.
        if client is not None:

            # محاولة تنظيف Collection المؤقتة عند الفشل.
            if (
                args.cleanup_after
                and not cleanup_done
            ):

                try:

                    db = client[
                        MONGO_DATABASE
                    ]

                    safe_drop_test_collection(
                        db,
                        args.collection,
                    )

                except Exception:
                    pass

            client.close()


# تشغيل main فقط عند تشغيل الملف مباشرة.
if __name__ == "__main__":
    main()