"""
upsert_update_proof.py

المهمة:
إثبات أن عملية Upsert تعمل بشكل صحيح.

الفكرة:
1- نأخذ سجلًا حقيقيًا من orders_validated.
2- ننشئ Collection مؤقتة للاختبار.
3- أول Upsert يجب أن يعمل INSERT.
4- نغير بيانات نفس السجل مع إبقاء order_id نفسه.
5- ثاني Upsert يجب أن يعمل UPDATE وليس INSERT جديد.
6- نتأكد أن عدد السجلات بقي 1 ولا يوجد Duplicate.
7- نحفظ تقرير إثبات.
8- نحذف Collection المؤقتة.

مهم:
الاختبار لا يعدل بيانات Production الأصلية.
"""

import copy
import json
from datetime import datetime, timezone
from pathlib import Path

from pymongo import ASCENDING, ReplaceOne

from config.settings import (
    VALIDATED_COLLECTION,
    REPORTS_DIR,
)

from src.mongo_setup import (
    create_mongo_client,
    get_database,
)


# Collection مؤقتة خاصة باختبار الـ Upsert.
TEST_COLLECTION = "_upsert_update_proof_temp"


def now_utc():
    """إرجاع الوقت الحالي بتوقيت UTC."""

    return datetime.now(
        timezone.utc
    )


def main():

    client = None

    try:
        # ====================================================
        # الاتصال بـ MongoDB.
        # ====================================================

        client = create_mongo_client()
        db = get_database(client)

        # Collection التي تحتوي البيانات المعالجة الحقيقية.
        validated = db[
            VALIDATED_COLLECTION
        ]

        # Collection مؤقتة سنجري فيها الاختبار.
        test_collection = db[
            TEST_COLLECTION
        ]

        print("=" * 88)
        print("PHASE 13B - UPSERT UPDATE PROOF")
        print("=" * 88)


        # ====================================================
        # 1. أخذ سجل حقيقي من orders_validated
        # ====================================================

        # نبحث عن سجل يحتوي على order_id صالح.
        source = validated.find_one(
            {
                "order_id": {
                    "$exists": True,
                    "$ne": None,
                }
            }
        )

        # إذا لم نجد سجلًا مناسبًا نوقف الاختبار.
        if not source:
            raise RuntimeError(
                "orders_validated has no usable record."
            )

        # حفظ order_id لأنه Business Key المستخدم في الاختبار.
        original_order_id = source[
            "order_id"
        ]

        print(
            f"Source order_id          : "
            f"{original_order_id}"
        )


        # ====================================================
        # 2. تجهيز Collection مؤقتة ومعزولة للاختبار
        # ====================================================

        # حذف أي نسخة قديمة من Collection الاختبار.
        test_collection.drop()

        # إعادة الحصول عليها بعد الحذف.
        test_collection = db[
            TEST_COLLECTION
        ]

        # إنشاء Unique Index على order_id.
        # يمنع وجود سجلين بنفس order_id.
        test_collection.create_index(
            [
                (
                    "order_id",
                    ASCENDING,
                )
            ],
            unique=True,
            name="uq_test_order_id",
        )

        print(
            "Temporary collection     : "
            f"{TEST_COLLECTION}"
        )

        print(
            "Unique order_id index    : PASS"
        )


        # ====================================================
        # 3. FIRST UPSERT
        # ====================================================

        # نسخ السجل الحقيقي حتى لا نعدل السجل الأصلي.
        baseline = copy.deepcopy(
            source
        )

        # إزالة MongoDB _id حتى يتم إنشاء ID جديد في Collection المؤقتة.
        baseline.pop(
            "_id",
            None,
        )

        # Metadata خاصة بالاختبار فقط.
        baseline[
            "_proof_test"
        ] = {
            "version": 1,
            "note": "baseline",
            "timestamp": now_utc(),
        }

        # أول Upsert:
        #
        # بما أن Collection المؤقتة فارغة،
        # لن يجد order_id.
        #
        # لذلك upsert=True يؤدي إلى INSERT.
        first_result = (
            test_collection.replace_one(
                {
                    "order_id": (
                        original_order_id
                    )
                },
                baseline,
                upsert=True,
            )
        )

        # عدد السجلات بعد أول Upsert يجب أن يكون 1.
        count_after_first = (
            test_collection.count_documents(
                {}
            )
        )

        print(
            "\nFIRST UPSERT:"
        )

        print(
            f"  Upserted              : "
            f"{(1 if first_result.upserted_id is not None else 0)}"
        )

        print(
            f"  Modified              : "
            f"{first_result.modified_count}"
        )

        print(
            f"  Collection count      : "
            f"{count_after_first}"
        )


        # ====================================================
        # 4. تغيير بيانات نفس السجل
        # ====================================================

        # إنشاء نسخة من السجل الذي أدخلناه.
        changed = copy.deepcopy(
            baseline
        )

        # أخذ اسم العميل الأصلي.
        original_customer_name = (
            changed.get(
                "customer_name"
            )
        )

        # تغيير اسم العميل فقط لأغراض الاختبار.
        #
        # مهم:
        # order_id لا يتغير.
        changed[
            "customer_name"
        ] = (
            f"{original_customer_name} "
            f"[UPSERT-PROOF]"
        )

        # تحديث Metadata الخاصة بالاختبار.
        changed[
            "_proof_test"
        ] = {
            "version": 2,
            "note": (
                "same business key, "
                "changed payload"
            ),
            "timestamp": now_utc(),
        }


        # ====================================================
        # 5. SECOND UPSERT
        # ====================================================

        # الآن نفس order_id موجود بالفعل.
        #
        # لذلك يجب أن يحدث MongoDB السجل الموجود
        # بدل إنشاء سجل جديد.
        second_result = (
            test_collection.replace_one(
                {
                    "order_id": (
                        original_order_id
                    )
                },
                changed,
                upsert=True,
            )
        )

        # العدد يجب أن يظل 1.
        count_after_second = (
            test_collection.count_documents(
                {}
            )
        )

        # قراءة السجل بعد التحديث للتأكد من النتيجة.
        final_document = (
            test_collection.find_one(
                {
                    "order_id": (
                        original_order_id
                    )
                }
            )
        )

        print(
            "\nSECOND UPSERT - SAME order_id, "
            "CHANGED DATA:"
        )

        print(
            f"  Upserted              : "
            f"{(1 if second_result.upserted_id is not None else 0)}"
        )

        print(
            f"  Matched               : "
            f"{second_result.matched_count}"
        )

        print(
            f"  Modified              : "
            f"{second_result.modified_count}"
        )

        print(
            f"  Collection count      : "
            f"{count_after_second}"
        )


        # ====================================================
        # 6. التحقق من نجاح الاختبار
        # ====================================================

        # أول Upsert يجب أن يعمل INSERT.
        first_inserted = (
            (1 if first_result.upserted_id is not None else 0)
            == 1
        )

        # ثاني Upsert يجب ألا يعمل INSERT جديد.
        second_not_inserted = (
            (1 if second_result.upserted_id is not None else 0)
            == 0
        )

        # يجب أن يجد السجل الموجود.
        second_matched = (
            second_result.matched_count
            == 1
        )

        # يجب أن يحدث السجل.
        second_updated = (
            second_result.modified_count
            == 1
        )

        # العدد يجب أن يبقى 1 قبل وبعد التحديث.
        # وهذا يثبت عدم إنشاء Duplicate.
        count_stable = (
            count_after_first == 1
            and count_after_second == 1
        )

        # التأكد أن Business Key لم يتغير.
        same_business_key = (
            final_document[
                "order_id"
            ]
            == original_order_id
        )

        # التأكد أن البيانات الجديدة تم حفظها فعلًا.
        payload_changed = (
            final_document.get(
                "customer_name"
            )
            == changed[
                "customer_name"
            ]
        )

        # جميع الشروط السابقة يجب أن تنجح.
        passed = all(
            [
                first_inserted,
                second_not_inserted,
                second_matched,
                second_updated,
                count_stable,
                same_business_key,
                payload_changed,
            ]
        )

        if not passed:
            raise RuntimeError(
                "Upsert update proof failed."
            )


        # ====================================================
        # 7. حفظ Evidence / Report
        # ====================================================

        # إنشاء تقرير يحتوي نتائج الاختبار.
        report = {
            "phase": (
                "phase_13b_upsert_update_proof"
            ),

            "mode": (
                "isolated_temporary_collection"
            ),

            "source_collection": (
                VALIDATED_COLLECTION
            ),

            "temporary_collection": (
                TEST_COLLECTION
            ),

            # Business Key المستخدم لتحديد السجل.
            "business_key": (
                "order_id"
            ),

            "order_id": (
                original_order_id
            ),

            # نتيجة أول Upsert.
            "first_upsert": {
                "upserted_count": (
                    (1 if first_result.upserted_id is not None else 0)
                ),

                "matched_count": (
                    first_result.matched_count
                ),

                "modified_count": (
                    first_result.modified_count
                ),

                "collection_count": (
                    count_after_first
                ),
            },

            # نتيجة ثاني Upsert بعد تغيير البيانات.
            "second_upsert_changed_payload": {
                "upserted_count": (
                    (1 if second_result.upserted_id is not None else 0)
                ),

                "matched_count": (
                    second_result.matched_count
                ),

                "modified_count": (
                    second_result.modified_count
                ),

                "collection_count": (
                    count_after_second
                ),
            },

            # خلاصة الإثبات.
            "proof": {
                "same_order_id": (
                    same_business_key
                ),

                "payload_updated": (
                    payload_changed
                ),

                "duplicate_created": False,

                "collection_count_stable": (
                    count_stable
                ),

                "result": "PASS",
            },

            # تأكيد أن البيانات الأصلية لم يتم تعديلها.
            "production_data_modified": False,
        }


        # مكان حفظ تقرير الإثبات.
        report_path = (
            REPORTS_DIR
            / "upsert_update_proof.json"
        )

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
        # 8. تنظيف بيانات الاختبار
        # ====================================================

        # حذف Collection المؤقتة بعد انتهاء الاختبار.
        test_collection.drop()

        # التأكد أنها حُذفت فعلًا.
        temp_exists_after_cleanup = (
            TEST_COLLECTION
            in db.list_collection_names()
        )

        if temp_exists_after_cleanup:
            raise RuntimeError(
                "Temporary proof collection "
                "was not removed."
            )


        # ====================================================
        # FINAL SUMMARY
        # ====================================================

        print("\n" + "=" * 88)
        print("PHASE 13B UPSERT UPDATE SUMMARY")
        print("=" * 88)

        print(
            f"Business key            : "
            f"order_id"
        )

        print(
            f"Test order_id           : "
            f"{original_order_id}"
        )

        print(
            "\nBASELINE:"
        )

        print(
            "  First upsert inserted : 1"
        )

        print(
            "  Collection count      : 1"
        )

        print(
            "\nCHANGED SAME BUSINESS KEY:"
        )

        print(
            "  Inserted              : 0"
        )

        print(
            "  Matched               : 1"
        )

        print(
            "  Updated               : 1"
        )

        print(
            "  Collection count      : 1"
        )

        print(
            "\nPROOF:"
        )

        print(
            "  Same order_id         : PASS"
        )

        print(
            "  Existing row updated  : PASS"
        )

        print(
            "  Duplicate created     : NO"
        )

        print(
            "  Production modified   : NO"
        )

        print(
            "  Temporary data cleaned: PASS"
        )

        print(
            "\nReport:"
        )

        print(report_path)

        print("\n" + "=" * 88)

        print(
            "PHASE 13B UPSERT UPDATE PROOF: PASS"
        )

        print("=" * 88)


    finally:

        # ====================================================
        # تنظيف احتياطي وإغلاق الاتصال
        # ====================================================

        if client is not None:

            # إذا حدث Exception أثناء الاختبار،
            # نحاول أيضًا حذف Collection المؤقتة.
            try:
                db[
                    TEST_COLLECTION
                ].drop()

            except Exception:
                pass

            # إغلاق اتصال MongoDB.
            client.close()


if __name__ == "__main__":
    main()