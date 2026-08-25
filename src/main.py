"""
main.py
-------

وظيفة الملف:
هذا هو الملف الرئيسي المسؤول عن تنسيق تشغيل الـ Hybrid Big Data Pipeline.

المهام الأساسية:
1. استقبال مسار ملف البيانات من المستخدم عبر Command Line.
2. معرفة حجم الملف.
3. استخدام File Router لاختيار محرك المعالجة المناسب:
   - Python Batch للملفات الصغيرة.
   - PySpark للملفات الكبيرة.
4. التأكد أن قرار File Router متوافق مع حجم الملف والـ Threshold.
5. تشغيل محرك المعالجة المناسب.
6. إدخال البيانات أولاً إلى Raw Collection في MongoDB.
7. تشغيل مرحلة ELT الخاصة بالتنظيف وفحص جودة البيانات.
8. دعم أوضاع اختبار مثل:
   --dry-route  لاختبار قرار التوجيه فقط.
   --raw-only   لإيقاف التنفيذ بعد Raw Ingestion.

هذا الملف لا يحتوي على كل تفاصيل المعالجة بنفسه،
بل يعمل كـ Orchestrator يربط ملفات المشروع المختلفة مع بعضها.
"""

# لمعالجة Arguments التي يرسلها المستخدم من Terminal.
import argparse

# للتعامل مع Environment Variables.
import os

# لتشغيل عمليات Python أخرى مثل spark_loader و elt_pipeline.
import subprocess

# للوصول إلى Python executable والـ environment الحالية.
import sys

# لإنشاء معرف فريد لكل تشغيل Pipeline.
import uuid

# لإنشاء تاريخ ووقت UTC يستخدم داخل Run ID.
from datetime import datetime, timezone

# للتعامل مع مسارات الملفات والمجلدات.
from pathlib import Path


# استيراد الإعدادات والثوابت المركزية من settings.py.
from config.settings import (
    BATCH_SIZE,
    ENGINE_PYSPARK,
    ENGINE_PYTHON_BATCH,
    RAW_COLLECTION,
    SMALL_FILE_THRESHOLD_MB,
)

# الدالة المسؤولة عن تحميل الملف الصغير باستخدام Python Batch.
from src.batch_loader import (
    load_csv_to_raw,
)

# دوال File Router:
# - choose_engine تحدد محرك المعالجة.
# - get_file_size_mb تحسب حجم الملف بالـ MB.
from src.file_router import (
    choose_engine,
    get_file_size_mb,
)


# ============================================================
# ROUTER NORMALIZATION
# توحيد شكل نتيجة File Router
# ============================================================

def normalize_engine(
    decision,
):
    """
    توحيد النتيجة التي يعيدها File Router.

    الهدف:
    التأكد أن النتيجة النهائية هي أحد المحركين فقط:
    python_batch أو pyspark.

    تدعم الدالة أكثر من شكل محتمل للنتيجة:
    String أو Dictionary أو Tuple/List.
    """

    # المحركات المقبولة فقط.
    valid = {
        ENGINE_PYTHON_BATCH,
        ENGINE_PYSPARK,
    }

    # إذا أعاد Router قيمة نصية مباشرة.
    if isinstance(
        decision,
        str,
    ):

        if decision in valid:
            return decision

    # إذا أعاد Router Dictionary.
    if isinstance(
        decision,
        dict,
    ):

        # البحث عن اسم المحرك تحت أحد المفاتيح المحتملة.
        for key in (
            "engine",
            "selected_engine",
        ):

            value = decision.get(
                key
            )

            if value in valid:
                return value

    # إذا كانت النتيجة Tuple أو List.
    if isinstance(
        decision,
        (
            tuple,
            list,
        ),
    ):

        for value in decision:

            if value in valid:
                return value

    # إذا لم نستطع فهم النتيجة، نوقف التنفيذ.
    raise RuntimeError(
        "Unsupported result returned by "
        f"file_router.choose_engine(): {decision!r}"
    )


# ============================================================
# ROUTE DECISION
# اتخاذ قرار مسار المعالجة
# ============================================================

def resolve_route(
    input_path,
):
    # تحويل مسار الملف إلى Absolute Path.
    path = Path(
        input_path
    ).resolve()

    # التأكد أن الملف موجود فعلاً.
    if not path.exists():
        raise FileNotFoundError(
            path
        )

    # حساب حجم الملف بالميجابايت.
    size_mb = get_file_size_mb(
        path
    )

    # طلب قرار محرك المعالجة من File Router.
    raw_decision = choose_engine(
        path
    )

    # توحيد شكل النتيجة.
    engine = normalize_engine(
        raw_decision
    )

    # حساب المحرك المتوقع بشكل مستقل:
    # الملف <= Threshold  -> Python Batch
    # الملف > Threshold   -> PySpark
    expected_engine = (
        ENGINE_PYTHON_BATCH
        if size_mb
        <= SMALL_FILE_THRESHOLD_MB
        else ENGINE_PYSPARK
    )

    # Consistency Gate:
    # نتأكد أن قرار Router متوافق مع الحجم والـ Threshold.
    if engine != expected_engine:
        raise RuntimeError(
            "Router consistency failure: "
            f"router={engine}, "
            f"expected={expected_engine}, "
            f"size={size_mb:,.2f} MB, "
            f"threshold="
            f"{SMALL_FILE_THRESHOLD_MB} MB"
        )

    # إنشاء سبب واضح لاختيار المحرك.
    reason = (
        "file size <= configured threshold"
        if engine
        == ENGINE_PYTHON_BATCH
        else
        "file size > configured threshold"
    )

    # إرجاع جميع معلومات قرار التوجيه.
    return {
        "path": path,
        "file_size_mb": size_mb,
        "engine": engine,
        "reason": reason,
    }


# ============================================================
# PYSPARK RUNTIME
# تجهيز بيئة تشغيل PySpark
# ============================================================

def build_spark_environment():
    """
    تجهيز Environment خاصة بـ PySpark.

    الهدف:
    استخدام Java الخاصة بمشروع Big Data بدون تغيير
    إعدادات Java العامة في Windows أو Java الخاصة بـ Android Studio.
    """

    # أخذ نسخة من Environment Variables الحالية.
    env = os.environ.copy()

    # مسار Python Environment المستخدمة حالياً.
    env_prefix = Path(
        sys.prefix
    )

    # تحديد مكان Java داخل Environment المشروع.
    java_home = (
        env_prefix
        / "Library"
    )

    # تحديد ملف java.exe.
    java_exe = (
        java_home
        / "bin"
        / "java.exe"
    )

    # التأكد من وجود Java المطلوبة.
    if not java_exe.exists():
        raise RuntimeError(
            "Project Java 17 was not found at "
            f"{java_exe}"
        )

    # استيراد PySpark لتحديد مكان تثبيت Spark.
    import pyspark

    spark_home = Path(
        pyspark.__file__
    ).resolve().parent

    # تحديد JAVA_HOME لهذه العملية فقط.
    env[
        "JAVA_HOME"
    ] = str(
        java_home
    )

    # تحديد مكان Spark.
    env[
        "SPARK_HOME"
    ] = str(
        spark_home
    )

    # جعل PySpark يستخدم نفس Python الذي يشغل المشروع.
    env[
        "PYSPARK_PYTHON"
    ] = sys.executable

    # نفس Python يستخدم كـ Driver.
    env[
        "PYSPARK_DRIVER_PYTHON"
    ] = sys.executable

    # تشغيل Spark محلياً.
    env[
        "SPARK_LOCAL_IP"
    ] = "127.0.0.1"

    # استخدام UTF-8 داخل Python.
    env[
        "PYTHONUTF8"
    ] = "1"

    # إضافة Java وSpark إلى PATH الخاص بهذه العملية.
    env[
        "PATH"
    ] = (
        str(
            java_home
            / "bin"
        )
        + os.pathsep
        + str(
            spark_home
            / "bin"
        )
        + os.pathsep
        + env.get(
            "PATH",
            "",
        )
    )

    # إرجاع البيئة بعد تجهيزها.
    return env


# ============================================================
# PIPELINE RUN-ID
# إنشاء معرف لكل تشغيل
# ============================================================

def generate_pipeline_run_id():

    # Run ID يتكون من:
    # pipeline + UTC timestamp + جزء عشوائي من UUID.
    return (
        "pipeline-"
        + datetime.now(
            timezone.utc
        ).strftime(
            "%Y%m%dT%H%M%SZ"
        )
        + "-"
        + uuid.uuid4().hex[:8]
    )


# ============================================================
# ENGINE EXECUTION
# تشغيل محركات المعالجة
# ============================================================

def run_batch_engine(
    input_path,
    batch_size,
):
    """
    تشغيل Python Batch Loader للملفات الصغيرة.
    """

    print()
    print(
        "Dispatching             : "
        "Python Batch Loader"
    )

    # تسليم الملف إلى batch_loader.py.
    return load_csv_to_raw(
        input_path,
        batch_size,
    )


def run_spark_engine(
    input_path,
    run_id,
):
    """
    تشغيل PySpark Loader للملفات الكبيرة.
    """

    print()
    print(
        "Dispatching             : "
        "PySpark Loader"
    )

    # تجهيز الأمر الذي سيشغل src.spark_loader
    # كعملية Python مستقلة.
    command = [
        sys.executable,
        "-m",
        "src.spark_loader",

        "--input",
        str(
            input_path
        ),

        "--collection",
        RAW_COLLECTION,

        "--production-raw",

        "--run-id",
        run_id,
    ]

    print(
        "Spark target            : "
        f"{RAW_COLLECTION}"
    )

    print(
        "Spark Raw mode          : "
        "append"
    )

    # تشغيل Spark Loader باستخدام Environment المجهزة.
    result = subprocess.run(
        command,
        env=build_spark_environment(),
        check=False,
    )

    # Return Code غير صفر يعني فشل العملية.
    if result.returncode != 0:

        raise RuntimeError(
            "PySpark loader failed with "
            f"exit code {result.returncode}."
        )

    return result.returncode


def run_large_elt(
    raw_run_id,
):
    """
    تشغيل مرحلة ELT بعد انتهاء Raw Ingestion.

    تقوم مرحلة ELT بالتعامل مع البيانات الخام
    وتنفيذ عمليات الجودة والتنظيف والتصنيف.
    """

    print()
    print(
        "Dispatching             : "
        "Quality / Cleaning ELT"
    )

    print(
        f"ELT raw_run_id          : "
        f"{raw_run_id}"
    )

    # تجهيز أمر تشغيل elt_pipeline.py.
    command = [
        sys.executable,
        "-m",
        "src.elt_pipeline",

        "--raw-run-id",
        raw_run_id,

        "--skip-dry-run-contract",
    ]

    # تشغيل ELT كعملية منفصلة.
    result = subprocess.run(
        command,
        check=False,
    )

    # إيقاف المشروع إذا فشلت مرحلة ELT.
    if result.returncode != 0:

        raise RuntimeError(
            "ELT pipeline failed with "
            f"exit code {result.returncode}."
        )

    return result.returncode


# ============================================================
# CLI
# استقبال Arguments من Terminal
# ============================================================

def parse_args():
    # إنشاء Command Line Parser.
    parser = argparse.ArgumentParser(
        description=(
            "Hybrid Big Data Pipeline: "
            "File Router -> Python Batch / PySpark"
        )
    )

    # مسار ملف البيانات.
    # Required يعني أنه Argument إلزامي.
    parser.add_argument(
        "--input",
        required=True,
        help="Path to dirty CSV input file.",
    )

    # حجم الدفعة في Python Batch.
    # إذا لم يحدده المستخدم نستخدم BATCH_SIZE من settings.py.
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=(
            "Python Batch insert size. "
            "Used only for the small-file path."
        ),
    )

    # اختبار File Router فقط بدون معالجة أو كتابة إلى MongoDB.
    parser.add_argument(
        "--dry-route",
        action="store_true",
        help=(
            "Show Router decision without "
            "loading or modifying any data."
        ),
    )

    # تنفيذ Raw Ingestion فقط ثم التوقف قبل ELT.
    parser.add_argument(
        "--raw-only",
        action="store_true",
        help=(
            "Stop after Raw ingestion. "
            "Useful for controlled Spark evidence capture."
        ),
    )

    # إرجاع Arguments التي أدخلها المستخدم.
    return parser.parse_args()


# ============================================================
# MAIN ENTRY POINT
# نقطة التشغيل الرئيسية للمشروع
# ============================================================

def main():

    # قراءة Arguments من Terminal.
    args = parse_args()

    # تحديد مسار المعالجة المناسب للملف.
    route = resolve_route(
        args.input
    )

    # عرض معلومات File Router.
    print("=" * 88)
    print(
        "HYBRID BIG DATA PIPELINE - FILE ROUTER"
    )
    print("=" * 88)

    print(
        f"Input file              : "
        f"{route['path']}"
    )

    print(
        f"File size               : "
        f"{route['file_size_mb']:,.2f} MB"
    )

    print(
        f"Configured threshold    : "
        f"{SMALL_FILE_THRESHOLD_MB} MB"
    )

    print(
        f"Selected engine         : "
        f"{route['engine']}"
    )

    print(
        f"Reason                  : "
        f"{route['reason']}"
    )

    print(
        f"Dry-route mode          : "
        f"{'YES' if args.dry_route else 'NO'}"
    )

    print("=" * 88)

    # إذا تم استخدام --dry-route:
    # نعرض القرار فقط ثم ننهي البرنامج.
    if args.dry_route:

        print()
        print(
            "ROUTER DECISION VERIFIED"
        )

        print(
            "No file processing executed."
        )

        print(
            "No MongoDB write executed."
        )

        return 0


    # ========================================================
    # SMALL FILE -> PYTHON BATCH
    # ========================================================

    if (
        route[
            "engine"
        ]
        == ENGINE_PYTHON_BATCH
    ):

        # تشغيل Python Batch Loader.
        batch_result = (
            run_batch_engine(
                route[
                    "path"
                ],
                args.batch_size,
            )
        )

        # الحصول على Run ID الذي أعاده Batch Loader.
        batch_run_id = (
            batch_result.get(
                "run_id"
            )
        )

        # وجود Run ID ضروري لاستكمال ELT.
        if not batch_run_id:
            raise RuntimeError(
                "Python Batch loader did not "
                "return a run_id."
            )

        print(
            f"Pipeline run_id         : "
            f"{batch_run_id}"
        )

        # إذا طلب المستخدم Raw Only، نتوقف هنا.
        if args.raw_only:

            print()
            print(
                "RAW-ONLY MODE: COMPLETE"
            )

            print(
                "ELT was intentionally not started."
            )

        # وإلا نبدأ مرحلة ELT.
        else:

            run_large_elt(
                batch_run_id
            )


    # ========================================================
    # LARGE FILE -> PYSPARK
    # ========================================================

    elif (
        route[
            "engine"
        ]
        == ENGINE_PYSPARK
    ):

        # إنشاء Run ID خاص بهذا التشغيل.
        pipeline_run_id = (
            generate_pipeline_run_id()
        )

        print(
            f"Pipeline run_id         : "
            f"{pipeline_run_id}"
        )

        # تشغيل PySpark Loader.
        run_spark_engine(
            route[
                "path"
            ],
            pipeline_run_id,
        )

        # في Raw Only نتوقف بعد Raw Ingestion.
        if args.raw_only:

            print()
            print(
                "RAW-ONLY MODE: COMPLETE"
            )

            print(
                "ELT was intentionally not started."
            )

        # في التشغيل الطبيعي نبدأ ELT.
        else:

            run_large_elt(
                pipeline_run_id
            )

    # حماية إضافية في حال ظهر محرك غير معروف.
    else:

        raise RuntimeError(
            "Unsupported engine: "
            f"{route['engine']}"
        )

    # الوصول هنا يعني أن تنفيذ المحرك والـ Pipeline نجح.
    print()
    print("=" * 88)
    print(
        "HYBRID PIPELINE ENGINE EXECUTION: PASS"
    )
    print("=" * 88)

    return 0


# يتم تنفيذ main() فقط عندما نشغل هذا الملف كنقطة تشغيل مباشرة.
# SystemExit يستخدم قيمة return من main كـ Exit Code للبرنامج.
if __name__ == "__main__":
    raise SystemExit(
        main()
    )