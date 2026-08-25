"""
create_small_sample.py

المهمة:
إنشاء عينة CSV صغيرة من البيانات الأصلية الكبيرة.

مثلاً:
Dataset الأصلية → نأخذ أول 100,000 سجل → Sample CSV

مهم:
- لا يتم تنظيف البيانات.
- لا يتم تعديل البيانات.
- الملف الأصلي يُفتح Read Only.
- الهدف إنشاء Dataset أصغر للاختبار.
"""

import argparse
import csv
import sys
import time
from pathlib import Path


def configure_csv_field_limit():
    """
    السماح بقراءة حقول CSV كبيرة مثل items_json.
    """

    limit = sys.maxsize

    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


def create_sample(
    input_path: Path,
    output_path: Path,
    rows: int
) -> int:
    """
    نسخ أول N سجل من Dataset الأصلية
    لإنشاء عينة قابلة لإعادة الاختبار.

    لا يتم تنظيف أو تحويل البيانات.
    """

    # عدد السجلات يجب أن يكون أكبر من صفر.
    if rows <= 0:
        raise ValueError(
            "--rows must be greater than zero."
        )

    # التأكد أن Dataset الأصلية موجودة.
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    # إنشاء مجلد Output إذا لم يكن موجودًا.
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    # السماح بحقول CSV الكبيرة.
    configure_csv_field_limit()

    copied_rows = 0

    # بدء حساب وقت التنفيذ.
    start_time = time.perf_counter()

    # فتح:
    # Source للقراءة فقط.
    # Destination لكتابة العينة.
    with (
        input_path.open(
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as source,

        output_path.open(
            "w",
            encoding="utf-8-sig",
            newline=""
        ) as destination
    ):

        reader = csv.reader(source)
        writer = csv.writer(destination)

        # قراءة Header من الملف الأصلي.
        header = next(reader, None)

        if header is None:
            raise ValueError(
                "Input CSV is empty."
            )

        # نسخ Header إلى ملف العينة.
        writer.writerow(header)

        # نسخ أول N سجل فقط.
        for row in reader:

            # نتوقف عندما نصل للعدد المطلوب.
            if copied_rows >= rows:
                break

            writer.writerow(row)

            copied_rows += 1


    # حساب زمن إنشاء العينة.
    elapsed = (
        time.perf_counter()
        - start_time
    )

    # حساب حجم ملف العينة بالـMB.
    output_size_mb = (
        output_path.stat().st_size
        / (1024 * 1024)
    )


    # عرض ملخص التنفيذ.
    print("=" * 60)
    print("SMALL SAMPLE CREATION")
    print("=" * 60)

    print(f"Input file     : {input_path}")
    print(f"Output file    : {output_path}")
    print(f"Requested rows : {rows:,}")
    print(f"Copied rows    : {copied_rows:,}")
    print(f"Output size MB : {output_size_mb:.2f}")
    print(f"Elapsed seconds: {elapsed:.2f}")

    print("=" * 60)


    # إذا كانت Dataset الأصلية أصغر من العدد المطلوب.
    if copied_rows != rows:

        print(
            f"WARNING: Requested {rows:,} rows, "
            f"but source contained only {copied_rows:,}."
        )

    return copied_rows


def parse_args():
    """
    استقبال Parameters من Terminal.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Create a reproducible small CSV sample "
            "from the original dirty dataset."
        )
    )

    # مسار Dataset الأصلية.
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the original CSV file."
    )

    # مكان حفظ العينة.
    parser.add_argument(
        "--output",
        required=True,
        help="Path for the generated sample CSV."
    )

    # عدد السجلات.
    # الافتراضي = 100,000.
    parser.add_argument(
        "--rows",
        type=int,
        default=100000,
        help=(
            "Number of data rows to copy. "
            "Default: 100000."
        )
    )

    return parser.parse_args()


def main():

    args = parse_args()

    create_sample(
        input_path=Path(args.input),
        output_path=Path(args.output),
        rows=args.rows
    )


if __name__ == "__main__":
    main()