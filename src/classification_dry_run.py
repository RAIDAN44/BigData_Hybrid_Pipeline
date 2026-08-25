"""
classification_dry_run.py

المهمة:
اختبار قواعد الجودة والتصنيف قبل تنفيذ الكتابة الفعلية.

هذا الملف:
1. يقرأ البيانات من orders_raw.
2. يكتشف order_id المكرر.
3. يطبق نفس classify_record() المستخدمة في المعالجة الحقيقية.
4. يحسب Valid / Corrected / Quarantined.
5. يحسب الأخطاء والتصحيحات وإحصائياتها.
6. يأخذ أمثلة صغيرة من النتائج للمراجعة.
7. يتحقق من صحة الأعداد والـDuplicates.
8. يتأكد أن orders_validated و orders_quarantine لم تتغير.
9. يحفظ النتائج في classification_dry_run.json.

مهم:
Dry Run = Read Only
أي أنه يختبر المعالجة بدون كتابة النتائج النهائية.
"""

import json
import time
from collections import Counter
from datetime import datetime, timezone

from pymongo import DESCENDING

from config.settings import (
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    REPORTS_DIR,
)

from src.mongo_setup import (
    create_mongo_client,
    get_database,
)

# نفس قواعد الجودة المستخدمة لاحقًا في ELT الحقيقي.
from src.quality_rules import (
    classify_record,
    QUALITY_VALID,
    QUALITY_CORRECTED,
    QUALITY_QUARANTINED,
)


# الحالات الثلاث الوحيدة المسموح بها.
VALID_STATUSES = {
    QUALITY_VALID,
    QUALITY_CORRECTED,
    QUALITY_QUARANTINED,
}


# ============================================================
# SAMPLE HELPERS
# تجهيز أمثلة صغيرة لوضعها داخل التقرير
# ============================================================

def compact_valid_sample(row_number, raw, result):
    """تجهيز مثال مختصر لسجل Valid."""

    cleaned = result["cleaned_record"]

    return {
        "source_row_number": row_number,
        "order_id": cleaned.get("order_id"),
        "customer_id": cleaned.get("customer_id"),
        "status": cleaned.get("status"),
        "quality_status": result["quality_status"],
    }


def compact_corrected_sample(row_number, raw, result):
    """تجهيز مثال مختصر لسجل تم تصحيحه."""

    cleaned = result["cleaned_record"]

    return {
        "source_row_number": row_number,
        "order_id": cleaned.get("order_id"),
        "quality_status": result["quality_status"],
        "correction_count": len(
            result["corrections"]
        ),
        "corrections": result["corrections"][:8],
    }


def compact_quarantine_sample(row_number, raw, result):
    """تجهيز مثال مختصر لسجل تم عزله."""

    return {
        "source_row_number": row_number,
        "order_id": raw.get("order_id"),
        "quality_status": result["quality_status"],
        "codes_error": result["codes_error"],
        "details_error": result["details_error"][:8],
    }


# ============================================================
# DUPLICATE DETECTION
# اكتشاف order_id المتكرر داخل نفس Raw Run
# ============================================================

def find_conflicting_duplicate_ids(
    collection,
    run_id,
):
    """
    البحث عن order_id التي تظهر أكثر من مرة.

    يتم Trim للـorder_id أولًا لأن قواعد الجودة
    نفسها تزيل المسافات الزائدة.

    هذه العملية READ ONLY.
    """

    pipeline = [
        # نعمل فقط على الـRun المطلوب.
        {
            "$match": {
                "run_id": run_id
            }
        },

        # استخراج order_id وإزالة المسافات.
        {
            "$project": {
                "order_id": {
                    "$trim": {
                        "input": {
                            "$ifNull": [
                                "$raw_record.order_id",
                                "",
                            ]
                        }
                    }
                }
            }
        },

        # تجاهل order_id الفارغ.
        {
            "$match": {
                "order_id": {
                    "$ne": ""
                }
            }
        },

        # تجميع السجلات حسب order_id وحساب التكرار.
        {
            "$group": {
                "_id": "$order_id",
                "record_count": {
                    "$sum": 1
                },
            }
        },

        # الاحتفاظ فقط بما تكرر أكثر من مرة.
        {
            "$match": {
                "record_count": {
                    "$gt": 1
                }
            }
        },
    ]

    duplicate_ids = set()
    duplicate_record_count = 0

    # لمعرفة توزيع أحجام مجموعات التكرار.
    # مثال: كم مجموعة فيها سجلان؟ كم مجموعة فيها 3 سجلات؟
    group_sizes = Counter()

    cursor = collection.aggregate(
        pipeline,
        allowDiskUse=True,
    )

    for item in cursor:

        duplicate_ids.add(
            item["_id"]
        )

        count = int(
            item["record_count"]
        )

        duplicate_record_count += count

        group_sizes[count] += 1

    return (
        duplicate_ids,
        duplicate_record_count,
        group_sizes,
    )


# ============================================================
# MAIN
# ============================================================

def main():

    client = None

    try:

        # ====================================================
        # MONGODB CONNECTION
        # ====================================================

        client = create_mongo_client()

        db = get_database(client)

        raw_collection = db[
            RAW_COLLECTION
        ]

        validated_collection = db[
            VALIDATED_COLLECTION
        ]

        quarantine_collection = db[
            QUARANTINE_COLLECTION
        ]


        # ====================================================
        # LATEST RAW RUN
        # اختيار آخر عملية Raw Ingestion
        # ====================================================

        latest = raw_collection.find_one(
            {},
            sort=[
                (
                    "ingested_at",
                    DESCENDING,
                )
            ],
            projection={
                "run_id": 1,
            },
        )

        if not latest:
            raise RuntimeError(
                "orders_raw is empty."
            )

        run_id = latest["run_id"]

        raw_query = {
            "run_id": run_id
        }

        # عدد السجلات الموجودة في الـRaw Run المختار.
        expected_raw_count = (
            raw_collection.count_documents(
                raw_query
            )
        )


        # ====================================================
        # READ-ONLY PROOF - BEFORE
        # تسجيل أعداد Final Collections قبل الاختبار
        # ====================================================

        validated_before = (
            validated_collection.count_documents(
                {}
            )
        )

        quarantine_before = (
            quarantine_collection.count_documents(
                {}
            )
        )


        print("=" * 88)
        print("PHASE 11 - CLASSIFICATION DRY RUN")
        print("=" * 88)

        print(
            f"run_id                  : {run_id}"
        )

        print(
            f"raw documents           : "
            f"{expected_raw_count:,}"
        )

        print(
            f"validated before        : "
            f"{validated_before:,}"
        )

        print(
            f"quarantine before       : "
            f"{quarantine_before:,}"
        )

        # أهم نقطة: لا توجد كتابة أثناء Dry Run.
        print(
            "MongoDB write mode       : DISABLED"
        )

        print("=" * 88)


        # ====================================================
        # [1/3] DUPLICATE DETECTION
        # ====================================================

        print(
            "\n[1/3] Detecting duplicate order_id groups..."
        )

        (
            duplicate_ids,
            duplicate_record_count,
            duplicate_group_sizes,
        ) = find_conflicting_duplicate_ids(
            raw_collection,
            run_id,
        )

        duplicate_group_count = len(
            duplicate_ids
        )

        print(
            f"Duplicate groups         : "
            f"{duplicate_group_count:,}"
        )

        print(
            f"Records inside groups    : "
            f"{duplicate_record_count:,}"
        )

        if duplicate_group_sizes:

            print(
                "Duplicate group sizes     : "
                + ", ".join(
                    f"{size} records × "
                    f"{groups} groups"
                    for size, groups
                    in sorted(
                        duplicate_group_sizes.items()
                    )
                )
            )


        # ====================================================
        # [2/3] CLASSIFICATION
        # تطبيق قواعد الجودة على السجلات
        # ====================================================

        print(
            "\n[2/3] Classifying Raw records..."
        )

        # عداد Valid / Corrected / Quarantined.
        status_counts = Counter()

        # عداد كل Error Code.
        error_code_counts = Counter()

        # عداد كل Correction Rule.
        correction_rule_counts = Counter()

        # عداد تركيبات الأخطاء التي تظهر معًا.
        error_combination_counts = Counter()

        # كم تصحيح يوجد في كل سجل؟
        corrections_per_record = Counter()

        # كم خطأ يوجد في كل سجل؟
        errors_per_record = Counter()


        # نحتفظ بـ5 أمثلة فقط من كل نوع للتقرير.
        samples = {
            "valid": [],
            "corrected": [],
            "quarantined": [],
        }


        scanned = 0

        # عدد Duplicate Records التي مرت فعليًا على التصنيف.
        duplicate_records_seen = 0

        started = time.perf_counter()


        # قراءة Raw Records من MongoDB.
        # batch_size(2000) خاص بطريقة جلب البيانات من MongoDB.
        cursor = raw_collection.find(
            raw_query,
            projection={
                "_id": 0,
                "source_row_number": 1,
                "raw_record": 1,
            },
        ).batch_size(2000)


        # المعالجة تتم سجلًا بعد سجل.
        for document in cursor:

            scanned += 1

            row_number = document.get(
                "source_row_number"
            )

            raw = document.get(
                "raw_record"
            )


            # raw_record يجب أن يكون Dictionary.
            if not isinstance(raw, dict):

                raise RuntimeError(
                    "Invalid raw_record structure "
                    f"at source row {row_number}."
                )


            # تجهيز order_id بعد إزالة المسافات.
            order_id_value = raw.get(
                "order_id"
            )

            normalized_order_id = (
                str(order_id_value).strip()
                if order_id_value is not None
                else ""
            )


            # هل هذا order_id ضمن IDs المكررة؟
            duplicate_conflict = (
                normalized_order_id
                in duplicate_ids
            )

            if duplicate_conflict:
                duplicate_records_seen += 1


            # =================================================
            # أهم خطوة:
            # تطبيق نفس quality_rules.py المستخدمة في ELT الحقيقي
            # =================================================

            try:

                result = classify_record(
                    raw,
                    duplicate_conflict=(
                        duplicate_conflict
                    ),
                )

            except Exception as exc:

                raise RuntimeError(
                    "Classification crashed at "
                    f"source_row_number="
                    f"{row_number}, "
                    f"order_id="
                    f"{order_id_value!r}"
                ) from exc


            quality_status = result.get(
                "quality_status"
            )


            # يجب أن تكون النتيجة واحدة فقط من الحالات الثلاث.
            if quality_status not in VALID_STATUSES:

                raise RuntimeError(
                    "Unexpected quality_status "
                    f"{quality_status!r} "
                    f"at row {row_number}."
                )


            # كل سجل يحصل على Classification واحدة.
            status_counts[
                quality_status
            ] += 1


            # =================================================
            # CORRECTION STATISTICS
            # =================================================

            corrections = result.get(
                "corrections",
                [],
            )

            corrections_per_record[
                len(corrections)
            ] += 1


            # حساب عدد مرات استخدام كل Correction Rule.
            for correction in corrections:

                rule_code = correction.get(
                    "rule_code",
                    "UNKNOWN_RULE",
                )

                correction_rule_counts[
                    rule_code
                ] += 1


            # =================================================
            # ERROR STATISTICS
            # =================================================

            codes_error = result.get(
                "codes_error",
                [],
            )

            # إزالة تكرار نفس Error Code داخل نفس السجل.
            unique_error_codes = sorted(
                set(codes_error)
            )

            errors_per_record[
                len(unique_error_codes)
            ] += 1


            for code in unique_error_codes:

                error_code_counts[
                    code
                ] += 1


            # معرفة الأخطاء التي تظهر معًا.
            if unique_error_codes:

                combination_key = " + ".join(
                    unique_error_codes
                )

                error_combination_counts[
                    combination_key
                ] += 1


            # =================================================
            # SAMPLES
            # حفظ 5 أمثلة فقط من كل Classification
            # =================================================

            if (
                quality_status
                == QUALITY_VALID
                and len(
                    samples["valid"]
                ) < 5
            ):

                samples[
                    "valid"
                ].append(
                    compact_valid_sample(
                        row_number,
                        raw,
                        result,
                    )
                )


            elif (
                quality_status
                == QUALITY_CORRECTED
                and len(
                    samples["corrected"]
                ) < 5
            ):

                samples[
                    "corrected"
                ].append(
                    compact_corrected_sample(
                        row_number,
                        raw,
                        result,
                    )
                )


            elif (
                quality_status
                == QUALITY_QUARANTINED
                and len(
                    samples["quarantined"]
                ) < 5
            ):

                samples[
                    "quarantined"
                ].append(
                    compact_quarantine_sample(
                        row_number,
                        raw,
                        result,
                    )
                )


            # =================================================
            # PROGRESS
            # يطبع كل 10000 سجل فقط.
            # هذا ليس Batch Processing Size.
            # =================================================

            if (
                scanned % 10000 == 0
                or scanned == expected_raw_count
            ):

                elapsed_now = (
                    time.perf_counter()
                    - started
                )

                throughput_now = (
                    scanned / elapsed_now
                    if elapsed_now > 0
                    else 0
                )

                print(
                    f"  Progress: "
                    f"{scanned:,}/"
                    f"{expected_raw_count:,} "
                    f"| "
                    f"{throughput_now:,.2f} "
                    f"records/sec"
                )


        # ====================================================
        # PERFORMANCE
        # ====================================================

        elapsed_seconds = (
            time.perf_counter()
            - started
        )

        throughput = (
            scanned / elapsed_seconds
            if elapsed_seconds > 0
            else 0
        )


        # ====================================================
        # [3/3] CONSISTENCY GATES
        # التأكد من صحة نتائج الاختبار
        # ====================================================

        print(
            "\n[3/3] Running consistency gates..."
        )


        valid_count = status_counts[
            QUALITY_VALID
        ]

        corrected_count = status_counts[
            QUALITY_CORRECTED
        ]

        quarantine_count = status_counts[
            QUALITY_QUARANTINED
        ]


        # مجموع التصنيفات الثلاثة.
        classified_total = (
            valid_count
            + corrected_count
            + quarantine_count
        )


        # يجب أن نكون قد قرأنا كل Raw Records.
        if scanned != expected_raw_count:

            raise RuntimeError(
                "RAW SCAN CONSISTENCY FAILED: "
                f"expected={expected_raw_count}, "
                f"scanned={scanned}"
            )


        # أهم معادلة:
        # Raw = Valid + Corrected + Quarantined
        if classified_total != scanned:

            raise RuntimeError(
                "CLASSIFICATION CONSISTENCY FAILED: "
                f"{scanned} != "
                f"{valid_count} + "
                f"{corrected_count} + "
                f"{quarantine_count}"
            )


        # عدد الـDuplicates المكتشفة أولًا
        # يجب أن يساوي ما مر أثناء التصنيف.
        if (
            duplicate_records_seen
            != duplicate_record_count
        ):

            raise RuntimeError(
                "DUPLICATE CONSISTENCY FAILED: "
                f"aggregation="
                f"{duplicate_record_count}, "
                f"classification="
                f"{duplicate_records_seen}"
            )


        # ====================================================
        # READ-ONLY PROOF - AFTER
        # إثبات أن Dry Run لم يكتب في Final Collections
        # ====================================================

        validated_after = (
            validated_collection.count_documents(
                {}
            )
        )

        quarantine_after = (
            quarantine_collection.count_documents(
                {}
            )
        )


        # يجب أن يبقى orders_validated كما كان.
        if (
            validated_after
            != validated_before
        ):

            raise RuntimeError(
                "DRY RUN VIOLATION: "
                "orders_validated changed."
            )


        # ويجب أن يبقى orders_quarantine كما كان.
        if (
            quarantine_after
            != quarantine_before
        ):

            raise RuntimeError(
                "DRY RUN VIOLATION: "
                "orders_quarantine changed."
            )


        # ====================================================
        # REPORT
        # بناء تقرير Dry Run
        # ====================================================

        report = {

            "phase": (
                "classification_dry_run"
            ),

            # دليل أن هذه المرحلة Read Only.
            "mode": "read_only",

            "generated_at": (
                datetime.now(
                    timezone.utc
                ).isoformat()
            ),

            "run_id": run_id,

            "raw_count": (
                expected_raw_count
            ),


            # نتائج التصنيف.
            "classification": {

                "valid_count": (
                    valid_count
                ),

                "corrected_count": (
                    corrected_count
                ),

                "quarantine_count": (
                    quarantine_count
                ),

                "classified_total": (
                    classified_total
                ),
            },


            # نتائج اختبارات الاتساق.
            "consistency": {

                "raw_equals_classified": (
                    expected_raw_count
                    == classified_total
                ),

                "duplicate_records_expected": (
                    duplicate_record_count
                ),

                "duplicate_records_seen": (
                    duplicate_records_seen
                ),

                "validated_unchanged": (
                    validated_before
                    == validated_after
                ),

                "quarantine_unchanged": (
                    quarantine_before
                    == quarantine_after
                ),
            },


            # معلومات Duplicate IDs.
            "duplicates": {

                "group_count": (
                    duplicate_group_count
                ),

                "record_count": (
                    duplicate_record_count
                ),

                "group_size_distribution": {
                    str(key): value
                    for key, value
                    in sorted(
                        duplicate_group_sizes.items()
                    )
                },
            },


            # إحصائيات الأخطاء.
            "error_code_counts": dict(
                error_code_counts.most_common()
            ),


            # إحصائيات التصحيحات.
            "correction_rule_counts": dict(
                correction_rule_counts.most_common()
            ),


            # الأخطاء التي ظهرت مع بعضها.
            "error_combination_counts": dict(
                error_combination_counts.most_common()
            ),


            "corrections_per_record": {
                str(key): value
                for key, value
                in sorted(
                    corrections_per_record.items()
                )
            },


            "errors_per_record": {
                str(key): value
                for key, value
                in sorted(
                    errors_per_record.items()
                )
            },


            # أداء الاختبار.
            "performance": {

                "elapsed_seconds": round(
                    elapsed_seconds,
                    4,
                ),

                "throughput_records_per_second": (
                    round(
                        throughput,
                        2,
                    )
                ),
            },


            # الأمثلة المختصرة.
            "samples": samples,


            # إثبات عدم تغير Final Collections.
            "mongo_collection_counts": {

                "validated_before": (
                    validated_before
                ),

                "validated_after": (
                    validated_after
                ),

                "quarantine_before": (
                    quarantine_before
                ),

                "quarantine_after": (
                    quarantine_after
                ),
            },
        }


        # ====================================================
        # SAVE REPORT
        # ====================================================

        output_path = (
            REPORTS_DIR
            / "classification_dry_run.json"
        )

        with output_path.open(
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
        # TERMINAL SUMMARY
        # عرض ملخص النتائج
        # ====================================================

        print("\n" + "=" * 88)
        print("CLASSIFICATION DRY RUN SUMMARY")
        print("=" * 88)


        print(
            f"Raw records             : "
            f"{expected_raw_count:>10,}"
        )

        print(
            f"Valid                   : "
            f"{valid_count:>10,}"
        )

        print(
            f"Corrected               : "
            f"{corrected_count:>10,}"
        )

        print(
            f"Quarantined             : "
            f"{quarantine_count:>10,}"
        )

        print(
            f"Classified total        : "
            f"{classified_total:>10,}"
        )


        print(
            "\nCONSISTENCY:"
        )

        print(
            f"  {expected_raw_count:,} "
            f"= "
            f"{valid_count:,} "
            f"+ "
            f"{corrected_count:,} "
            f"+ "
            f"{quarantine_count:,}"
        )

        print(
            "  RAW = VALID + CORRECTED "
            "+ QUARANTINED : PASS"
        )


        print(
            f"\nDuplicate groups        : "
            f"{duplicate_group_count:,}"
        )

        print(
            f"Duplicate records       : "
            f"{duplicate_record_count:,}"
        )


        print(
            "\nTOP QUARANTINE ERROR CODES:"
        )

        if error_code_counts:

            for code, count in (
                error_code_counts.most_common(
                    15
                )
            ):

                print(
                    f"  {code:42} "
                    f"{count:>10,}"
                )

        else:

            print("  None")


        print(
            "\nTOP CORRECTION RULES:"
        )

        if correction_rule_counts:

            for code, count in (
                correction_rule_counts.most_common(
                    20
                )
            ):

                print(
                    f"  {code:42} "
                    f"{count:>10,}"
                )

        else:

            print("  None")


        print(
            "\nTOP ERROR COMBINATIONS:"
        )

        if error_combination_counts:

            for combination, count in (
                error_combination_counts.most_common(
                    10
                )
            ):

                print(
                    f"  {count:>8,}  "
                    f"{combination}"
                )

        else:

            print("  None")


        print(
            "\nPERFORMANCE:"
        )

        print(
            f"  Elapsed                : "
            f"{elapsed_seconds:.2f} sec"
        )

        print(
            f"  Throughput             : "
            f"{throughput:,.2f} "
            f"records/sec"
        )


        # إثبات أن Dry Run لم يغير Collections النهائية.
        print(
            "\nREAD-ONLY PROOF:"
        )

        print(
            f"  validated: "
            f"{validated_before:,} "
            f"→ "
            f"{validated_after:,}"
        )

        print(
            f"  quarantine: "
            f"{quarantine_before:,} "
            f"→ "
            f"{quarantine_after:,}"
        )


        print(
            "\nReport:"
        )

        print(output_path)


        print("\n" + "=" * 88)

        print(
            "PHASE 11 CLASSIFICATION DRY RUN: PASS"
        )

        print("=" * 88)


    # إغلاق الاتصال بـMongoDB سواء نجح التنفيذ أو حدث خطأ.
    finally:

        if client is not None:
            client.close()


# تشغيل البرنامج فقط عند تشغيل الملف مباشرة.
if __name__ == "__main__":
    main()