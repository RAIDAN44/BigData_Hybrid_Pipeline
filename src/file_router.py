"""
file_router.py
--------------

وظيفة الملف:
هذا الملف مسؤول عن تحديد محرك معالجة البيانات المناسب
حسب حجم ملف الإدخال.

آلية العمل:
1. التأكد أن مسار الإدخال موجود وأنه ملف فعلي.
2. حساب حجم الملف بالـ MiB بدون قراءة محتواه.
3. مقارنة الحجم مع SMALL_FILE_THRESHOLD_MB الموجود في settings.py.
4. اختيار:
   - Python Batch إذا كان الملف صغيراً.
   - PySpark إذا كان الملف كبيراً.
5. يمكن تشغيل الملف بشكل مستقل من Terminal لاختبار قرار الـ Router.

مهم:
هذا الملف يقرر المحرك فقط، لكنه لا يشغل عملية المعالجة نفسها.
عملية التنفيذ الفعلية يديرها main.py.
"""

# argparse يسمح بتمرير مسار الملف من Command Line.
import argparse

# Path للتعامل مع مسارات الملفات بطريقة منظمة.
from pathlib import Path

# استيراد حد حجم الملف وأسماء محركات المعالجة
# من ملف الإعدادات المركزي.
from config.settings import (
    SMALL_FILE_THRESHOLD_MB,
    ENGINE_PYTHON_BATCH,
    ENGINE_PYSPARK,
)


# ============================================================
# FILE SIZE
# حساب حجم الملف
# ============================================================

def get_file_size_mb(file_path: Path) -> float:
    """
    إرجاع حجم الملف بالـ MiB بدون قراءة محتوى الملف.

    الدالة تعتمد على Metadata الخاصة بالملف،
    ولذلك يمكن استخدامها حتى مع ملفات كبيرة جداً
    بدون تحميل الملف إلى الذاكرة.
    """

    # التأكد أن المسار موجود.
    if not file_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {file_path}"
        )

    # التأكد أن المسار يشير إلى ملف وليس مجلداً.
    if not file_path.is_file():
        raise ValueError(
            f"Input path is not a file: {file_path}"
        )

    # st_size يعطينا حجم الملف بالـ Bytes.
    # نقسم على 1024 × 1024 لتحويل الحجم إلى MiB.
    return file_path.stat().st_size / (1024 * 1024)


# ============================================================
# ENGINE SELECTION
# اختيار محرك المعالجة
# ============================================================

def choose_engine(file_path: Path) -> tuple[str, float]:
    """
    اختيار محرك المعالجة اعتماداً على حجم الملف.

    ترجع الدالة قيمتين:
    1. اسم المحرك المختار.
    2. حجم الملف بالـ MiB.
    """

    # حساب حجم الملف.
    size_mb = get_file_size_mb(file_path)

    # إذا كان حجم الملف أقل من أو يساوي Threshold
    # نستخدم Python Batch.
    if size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = ENGINE_PYTHON_BATCH

    # إذا تجاوز الحجم Threshold نستخدم PySpark.
    else:
        engine = ENGINE_PYSPARK

    # إرجاع اسم المحرك وحجم الملف معاً.
    return engine, size_mb


# ============================================================
# CLI
# استقبال Argument من Terminal
# ============================================================

def parse_args():
    """
    تجهيز Command Line Interface الخاص بالـ File Router.
    """

    parser = argparse.ArgumentParser(
        description="Choose Python Batch or PySpark by file size."
    )

    # مسار ملف الإدخال.
    # required=True يعني أن المستخدم يجب أن يمرر المسار.
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input CSV file."
    )

    return parser.parse_args()


# ============================================================
# MAIN
# تشغيل File Router بشكل مستقل
# ============================================================

def main():

    # قراءة Arguments المرسلة من Terminal.
    args = parse_args()

    # تحويل مسار الإدخال من String إلى Path.
    file_path = Path(args.input)

    # اختيار المحرك والحصول على حجم الملف.
    engine, size_mb = choose_engine(file_path)

    # عرض نتيجة File Router.
    print("=" * 60)
    print("FILE ROUTER")
    print("=" * 60)

    print(f"Input file   : {file_path}")
    print(f"File size MB : {size_mb:.2f}")
    print(f"Threshold MB : {SMALL_FILE_THRESHOLD_MB}")
    print(f"Engine       : {engine}")

    print("=" * 60)


# يتم تشغيل main() فقط إذا تم تشغيل هذا الملف مباشرة.
# عند استيراد دواله من ملف آخر مثل main.py لن يتم تنفيذ main().
if __name__ == "__main__":
    main()