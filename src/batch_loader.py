"""
batch_loader.py
---------------

وظيفة الملف:
هذا الملف مسؤول عن إدخال ملفات CSV الصغيرة إلى MongoDB
باستخدام Python Batch Processing.

يتم استخدامه عندما يقرر File Router أن حجم الملف
مناسب لمسار Python Batch.

آلية العمل:
1. فتح ملف CSV وقراءته تدريجياً صفاً بعد صف.
2. الاحتفاظ بقيم CSV كما جاءت بدون Cleaning أو Transformation.
3. إضافة Metadata لكل سجل مثل:
   - Run ID
   - اسم ومسار الملف
   - رقم الصف
   - وقت الإدخال
   - محرك المعالجة المستخدم
4. تجميع السجلات في Batches.
5. إدخال كل Batch إلى MongoDB orders_raw.
6. قياس زمن الإدخال والـ Throughput.
7. التحقق في النهاية أن:
   rows_read = loaded_raw = MongoDB run count.

مهم:
هذا الملف مسؤول عن Raw Ingestion فقط.
التنظيف والتحقق من الجودة يحدث لاحقاً في ELT Pipeline.
"""

import argparse

# مكتبة Python القياسية لقراءة ملفات CSV.
import csv

# تستخدم للحصول على قيم خاصة ببيئة Python مثل sys.maxsize.
import sys

# لقياس زمن التنفيذ وسرعة الإدخال.
import time

# لإنشاء معرف فريد لكل عملية Ingestion.
import uuid

# لإنشاء Timestamp بتوقيت UTC.
from datetime import datetime, timezone

# للتعامل مع مسارات الملفات.
from pathlib import Path


# إعدادات المشروع.
from config.settings import (
    BATCH_SIZE,
    RAW_COLLECTION,
    ENGINE_PYTHON_BATCH,
)


# دوال الاتصال بـ MongoDB.
from src.mongo_setup import (
    create_mongo_client,
    get_database,
)


# ============================================================
# CSV FIELD LIMIT
# السماح بقراءة حقول CSV كبيرة
# ============================================================

def configure_csv_field_limit():
    """
    رفع الحد الأقصى لحجم حقل CSV.

    بعض الحقول مثل items_json قد تحتوي نصاً كبيراً،
    لذلك نقوم برفع الحد قبل قراءة الملف.
    """

    # أكبر قيمة Integer تدعمها بيئة Python الحالية.
    limit = sys.maxsize

    while True:
        try:
            # محاولة تعيين الحد.
            csv.field_size_limit(limit)
            return

        # بعض الأنظمة قد لا تقبل القيمة الكبيرة جداً،
        # لذلك نقللها تدريجياً حتى تصبح مقبولة.
        except OverflowError:
            limit //= 10


# ============================================================
# RUN ID
# إنشاء معرف خاص بكل عملية تحميل
# ============================================================

def generate_run_id():
    """
    إنشاء معرف فريد نسبياً لكل عملية Raw Ingestion.

    يتكون من:
    Timestamp UTC + جزء من UUID.
    """

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    suffix = uuid.uuid4().hex[:8]

    return f"run-{timestamp}-{suffix}"


# ============================================================
# RAW DOCUMENT
# بناء MongoDB Document من صف CSV
# ============================================================

def build_raw_document(
    header,
    row,
    run_id,
    source_file,
    source_row_number,
):
    """
    تحويل صف CSV إلى Raw MongoDB Document.

    لا يتم هنا تنظيف أو تحويل أنواع البيانات.
    القيم تحفظ كما جاءت من CSV حتى تبقى Raw Data الأصلية.
    """

    raw_record = {}

    # ربط كل اسم Column بالقيمة المقابلة له في الصف.
    for index, column_name in enumerate(header):

        if index < len(row):
            raw_record[column_name] = row[index]

        # إذا كان هناك Column في Header بدون قيمة في الصف،
        # نحفظ None بدلاً من تجاهله.
        else:
            raw_record[column_name] = None


    # إذا كان الصف يحتوي قيماً إضافية أكثر من عدد Header،
    # نحتفظ بها بدلاً من حذفها بصمت.
    if len(row) > len(header):
        raw_record["_extra_values"] = row[len(header):]


    # Document النهائي الذي سيتم تخزينه في orders_raw.
    return {

        # معرف عملية الإدخال الحالية.
        "run_id": run_id,

        # اسم ملف المصدر.
        "source_file": source_file.name,

        # المسار الكامل لملف المصدر.
        "source_path": str(source_file.resolve()),

        # رقم الصف الأصلي داخل CSV.
        "source_row_number": source_row_number,

        # وقت إدخال السجل.
        "ingested_at": datetime.now(timezone.utc),

        # المحرك الذي استخدم لإدخال السجل.
        "engine_used": ENGINE_PYTHON_BATCH,

        # البيانات الأصلية للصف.
        "raw_record": raw_record,
    }


# ============================================================
# BATCH INSERT
# إدخال Batch واحدة إلى MongoDB
# ============================================================

def insert_batch(collection, batch, batch_number):
    """
    إدخال Batch واحدة إلى MongoDB
    وإرجاع عدد السجلات التي تم إدخالها.
    """

    # بداية قياس زمن الـ Batch.
    batch_start = time.perf_counter()

    try:

        # إدخال جميع Documents الموجودة في Batch مرة واحدة.
        result = collection.insert_many(
            batch,
            ordered=True
        )

    except Exception as exc:

        # إظهار الخطأ مع رقم Batch ثم إعادة رفع Exception.
        print(
            f"BATCH {batch_number} FAILED: {exc}",
            file=sys.stderr
        )

        raise


    # حساب زمن تنفيذ هذه Batch.
    batch_elapsed = time.perf_counter() - batch_start

    # عدد Documents التي تم إدخالها فعلياً.
    inserted = len(result.inserted_ids)

    # حساب سرعة الإدخال Rows per Second.
    rate = (
        inserted / batch_elapsed
        if batch_elapsed > 0
        else 0
    )

    # عرض إحصائيات Batch.
    print(
        f"BATCH {batch_number:03d} | "
        f"rows={len(batch):,} | "
        f"inserted={inserted:,} | "
        f"seconds={batch_elapsed:.3f} | "
        f"rate={rate:,.2f} rows/sec"
    )

    return inserted


# ============================================================
# CSV -> MONGODB RAW
# عملية Raw Ingestion الرئيسية
# ============================================================

def load_csv_to_raw(
    input_path: Path,
    batch_size: int = BATCH_SIZE,
):
    """
    قراءة CSV بشكل تدريجي وإدخاله إلى orders_raw.

    لا يتم تنفيذ:
    - Cleaning
    - Validation
    - Transformation

    في هذه المرحلة.
    """

    # التأكد أن ملف الإدخال موجود.
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    # Batch Size يجب أن تكون أكبر من صفر.
    if batch_size <= 0:
        raise ValueError(
            "batch_size must be greater than zero."
        )


    # السماح بقراءة حقول CSV كبيرة.
    configure_csv_field_limit()

    # إنشاء Run ID لهذا التشغيل.
    run_id = generate_run_id()


    # عدادات التنفيذ.
    rows_read = 0
    loaded_raw = 0
    batch_number = 0

    # بداية حساب الزمن الكلي.
    total_start = time.perf_counter()

    client = None


    try:

        # إنشاء اتصال مع MongoDB.
        client = create_mongo_client()

        # الحصول على قاعدة بيانات المشروع.
        database = get_database(client)

        # الحصول على Raw Collection.
        collection = database[RAW_COLLECTION]


        # معلومات بداية التنفيذ.
        print("=" * 70)
        print("PYTHON BATCH RAW LOADER")
        print("=" * 70)
        print(f"run_id       : {run_id}")
        print(f"input_file   : {input_path}")
        print(f"collection   : {RAW_COLLECTION}")
        print(f"batch_size   : {batch_size}")
        print(f"engine       : {ENGINE_PYTHON_BATCH}")
        print("=" * 70)


        # القائمة التي ستجمع Documents حتى تصل إلى Batch Size.
        batch = []


        # فتح CSV.
        #
        # utf-8-sig يدعم UTF-8 ويتعامل مع BOM إذا كان موجوداً.
        with input_path.open(
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as source:

            # إنشاء CSV Reader.
            reader = csv.reader(source)

            # أول صف في CSV يعتبر Header.
            header = next(reader, None)

            # رفض الملف الفارغ أو الذي لا يحتوي Header.
            if not header:
                raise ValueError(
                    "Input CSV is empty or has no header."
                )


            # قراءة CSV صفاً بعد صف.
            # الملف لا يتم تحميله كاملاً إلى الذاكرة.
            for row in reader:

                rows_read += 1

                # Header هو الصف المنطقي رقم 1،
                # لذلك أول Data Row يكون رقم 2.
                source_row_number = rows_read + 1


                # تحويل صف CSV إلى Raw MongoDB Document.
                document = build_raw_document(
                    header=header,
                    row=row,
                    run_id=run_id,
                    source_file=input_path,
                    source_row_number=source_row_number,
                )


                # إضافة Document إلى Batch الحالية.
                batch.append(document)


                # إذا وصلت Batch إلى الحجم المطلوب،
                # ندخلها إلى MongoDB.
                if len(batch) >= batch_size:

                    batch_number += 1

                    loaded_raw += insert_batch(
                        collection,
                        batch,
                        batch_number,
                    )

                    # تفريغ Batch وبدء Batch جديدة.
                    batch = []


            # بعد انتهاء CSV قد تبقى Batch جزئية
            # أقل من Batch Size، ويجب إدخالها أيضاً.
            if batch:

                batch_number += 1

                loaded_raw += insert_batch(
                    collection,
                    batch,
                    batch_number,
                )


        # ====================================================
        # PERFORMANCE METRICS
        # ====================================================

        # حساب الزمن الكلي.
        elapsed = time.perf_counter() - total_start

        # حساب Throughput الكلي.
        throughput = (
            loaded_raw / elapsed
            if elapsed > 0
            else 0
        )


        # ====================================================
        # MONGODB VERIFICATION
        # ====================================================

        # حساب Documents الموجودة في MongoDB
        # التي تنتمي إلى هذا Run فقط.
        mongo_run_count = collection.count_documents(
            {"run_id": run_id}
        )


        # عرض ملخص التنفيذ.
        print("\n" + "=" * 70)
        print("RAW LOAD SUMMARY")
        print("=" * 70)
        print(f"run_id             : {run_id}")
        print(f"rows_read          : {rows_read:,}")
        print(f"loaded_raw         : {loaded_raw:,}")
        print(f"mongo_run_count    : {mongo_run_count:,}")
        print(f"batches            : {batch_number}")
        print(f"batch_size         : {batch_size:,}")
        print(f"elapsed_seconds    : {elapsed:.3f}")
        print(f"throughput_rows_s  : {throughput:,.2f}")
        print("=" * 70)


        # عدد الصفوف المقروءة يجب أن يساوي عدد الصفوف المدخلة.
        if rows_read != loaded_raw:
            raise RuntimeError(
                "Raw load consistency failed: "
                f"rows_read={rows_read}, "
                f"loaded_raw={loaded_raw}"
            )


        # عدد الصفوف المدخلة يجب أن يساوي عدد Documents
        # الموجودة فعلياً في MongoDB لهذا Run.
        if loaded_raw != mongo_run_count:
            raise RuntimeError(
                "MongoDB verification failed: "
                f"loaded_raw={loaded_raw}, "
                f"mongo_run_count={mongo_run_count}"
            )


        # إذا وصلنا هنا فهذا يعني نجاح Consistency Checks.
        print("RAW LOAD CONSISTENCY: PASS")


        # إرجاع ملخص التنفيذ إلى main.py.
        return {
            "run_id": run_id,
            "rows_read": rows_read,
            "loaded_raw": loaded_raw,
            "batches": batch_number,
            "batch_size": batch_size,
            "elapsed_seconds": elapsed,
            "throughput": throughput,
        }


    # يتم تنفيذ finally سواء نجح الكود أو حدث Exception.
    # الهدف إغلاق اتصال MongoDB وعدم تركه مفتوحاً.
    finally:

        if client is not None:
            client.close()


# ============================================================
# CLI
# تشغيل Batch Loader بشكل مستقل من Terminal
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Stream a small CSV file into MongoDB orders_raw "
            "using configurable Python batches."
        )
    )


    # مسار CSV المطلوب إدخاله.
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the CSV input file."
    )


    # حجم Batch.
    # إذا لم يحدده المستخدم نستخدم BATCH_SIZE من settings.py.
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=(
            "Number of documents per MongoDB batch. "
            f"Default: {BATCH_SIZE}"
        )
    )

    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================

def main():

    # قراءة Command Line Arguments.
    args = parse_args()

    # تشغيل Raw Loader.
    load_csv_to_raw(
        input_path=Path(args.input),
        batch_size=args.batch_size,
    )


# عند تشغيل الملف مباشرة يتم استدعاء main().
# أما عند استيراد load_csv_to_raw من main.py
# فلن يتم تشغيل main() تلقائياً.
if __name__ == "__main__":
    main()