"""
quality_rules.py

المهمة:
هذا الملف يحتوي قواعد جودة وتنظيف البيانات في المشروع.

الفكرة الأساسية:
1. يستقبل سجل Raw.
2. ينظف القيم التي يمكن تصحيحها بشكل مؤكد Deterministic.
3. يسجل كل تعديل في corrections كـ Audit Trail.
4. يسجل الأخطاء التي لا يمكن إصلاحها بأمان في errors.
5. يصنف السجل في النهاية إلى:
   - valid       : لا أخطاء ولا تصحيحات.
   - corrected   : تم تصحيحه ولا توجد أخطاء متبقية.
   - quarantined : توجد أخطاء لا يمكن إصلاحها بأمان.

مهم:
النظام لا يخمن القيم المفقودة أو الخاطئة.
التصحيح يتم فقط عندما توجد قاعدة واضحة أو اشتقاق رياضي موثوق.
"""

import copy
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation


# ============================================================
# QUALITY STATES
# الحالات النهائية الممكنة للسجل
# ============================================================

QUALITY_VALID = "valid"
QUALITY_CORRECTED = "corrected"
QUALITY_QUARANTINED = "quarantined"


# ============================================================
# OFFICIAL ASSIGNMENT QUARANTINE CODES
# أكواد الأخطاء الأساسية التي تؤدي إلى عزل السجل
# ============================================================

ERR_ID_ORDER_MISSING = "MISSING_ORDER_ID"
ERR_ID_CUSTOMER_MISSING = "MISSING_CUSTOMER_ID"
ERR_DATE_IMPOSSIBLE_INVALID = "INVALID_IMPOSSIBLE_DATE"
ERR_JSON_ITEMS_CORRUPTED = "CORRUPTED_ITEMS_JSON"
ERR_ITEMS_EMPTY = "EMPTY_ITEMS"
ERR_PRICE_UNKNOWN = "UNKNOWN_PRICE"
ERR_VALUE_NEGATIVE_AMBIGUOUS = "AMBIGUOUS_NEGATIVE_VALUE"
ERR_ID_ORDER_DUPLICATE = "DUPLICATE_ORDER_ID"
ERR_ERRORS_CONFLICTING_MULTIPLE = "MULTIPLE_CONFLICTING_ERRORS"


# أكواد إضافية ظهرت الحاجة لها من حالات البيانات الحقيقية.
ERR_EMAIL_INVALID = "EMAIL_INVALID_UNRECOVERABLE"
ERR_PHONE_INVALID = "PHONE_INVALID_UNRECOVERABLE"
ERR_CURRENCY_UNKNOWN = "CURRENCY_UNKNOWN"
ERR_STATUS_UNKNOWN = "STATUS_UNKNOWN"
ERR_ITEM_QUANTITY_INVALID = "ITEM_QUANTITY_INVALID"
ERR_ITEM_SKU_MISSING = "MISSING_ITEM_SKU"
ERR_NEGATIVE_QUANTITY = "NEGATIVE_QUANTITY"
ERR_ITEM_COMPONENTS_CONFLICT = "ITEM_COMPONENTS_CONFLICT"
ERR_TOTAL_UNKNOWN = "TOTAL_UNKNOWN_UNRECOVERABLE"


# ============================================================
# RULE CODES
# أكواد توضح نوع التصحيح الذي تم على البيانات
# ============================================================

RULE_WHITESPACE = "WHITESPACE_TRIMMED"
RULE_ARABIC_DIGITS = "ARABIC_DIGITS_TO_LATIN"
RULE_ARABIC_DECIMAL = "ARABIC_DECIMAL_SEPARATOR_NORMALIZED"
RULE_THOUSANDS = "THOUSANDS_SEPARATOR_NORMALIZED"
RULE_KNOWN_PRICE_WORD = "KNOWN_PRICE_WORD_TO_NUMBER"
RULE_CURRENCY_SUFFIX = "CURRENCY_SUFFIX_REMOVED"
RULE_CURRENCY_YER = "CURRENCY_NORMALIZED_YER"
RULE_PHONE = "PHONE_NORMALIZED_YE"
RULE_EMAIL = "EMAIL_REPEATED_SYMBOLS"
RULE_DATE = "DATE_NORMALIZED"
RULE_STATUS_SYNONYM = "STATUS_SYNONYM_NORMALIZED"
RULE_QTY_STRING = "QTY_STRING_TO_NUMBER"
RULE_ITEM_TOTAL = "ITEM_TOTAL_DERIVED"
RULE_ITEM_PRICE_DIRECT = "ITEM_PRICE_DIRECT_DERIVED"
RULE_ITEM_TOTAL_RESIDUAL = "ITEM_TOTAL_RESIDUAL_DERIVED"
RULE_ITEM_PRICE_RESIDUAL = "ITEM_PRICE_RESIDUAL_DERIVED"
RULE_ORDER_TOTAL_DERIVED = "ORDER_TOTAL_DERIVED"
RULE_ORDER_TOTAL_RECALCULATED = "ORDER_TOTAL_RECALCULATED"


# ============================================================
# CONSTANTS
# ثوابت وقواميس تستخدم أثناء التنظيف
# ============================================================

# اكتشاف وجود أرقام عربية/فارسية داخل النص.
ARABIC_DIGITS_RE = re.compile(r"[٠-٩۰-۹]")

# تحويل الأرقام العربية والفارسية إلى 0-9.
ARABIC_DIGITS_MAP = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹",
    "01234567890123456789"
)

# قيم نصية ظهرت ضمن الحالات التي يدعمها المشروع.
# لا نحول أي كلمة رقمية عشوائيًا حتى لا نخمن.
KNOWN_PRICE_WORDS = {
    "ألفان": Decimal("2000"),
    "خمسة آلاف": Decimal("5000"),
}

# الحالات الصحيحة المعروفة للطلب.
VALID_ORDER_STATUSES = {
    "مؤكد",
    "قيد الانتظار",
    "مرتجع",
    "قيد الشحن",
    "تم التسليم",
    "ملغي",
}

# توحيد مرادفات حالة الدفع إلى قيمة Canonical واحدة.
PAYMENT_STATUS_MAP = {
    "تم الدفع": "تم الدفع",
    "بانتظار الدفع": "بانتظار الدفع",
    "مدفوع": "تم الدفع",
}

# تستخدم لإصلاح @@ أو .. المتكررة في البريد.
REPEATED_AT = re.compile(r"@{2,}")
REPEATED_DOT = re.compile(r"\.{2,}")


# ============================================================
# BASIC HELPERS
# دوال مساعدة عامة
# ============================================================

def is_blank(value):
    """فحص هل القيمة مفقودة أو فارغة."""
    return value is None or str(value).strip() == ""


def normalize_digits(value):
    """تحويل الأرقام العربية/الفارسية إلى أرقام إنجليزية."""
    return str(value).translate(ARABIC_DIGITS_MAP)


def to_number(value):
    """
    تحويل Decimal إلى int إذا كان عددًا صحيحًا،
    وإلا إلى float.
    """
    if value is None:
        return None

    if value == value.to_integral_value():
        return int(value)

    return float(value)


def positive_integer(value):
    """التحقق أن القيمة عدد صحيح موجب."""
    return (
        value is not None
        and value > 0
        and value == value.to_integral_value()
    )


def parse_decimal(value):
    """
    تحويل القيمة النصية إلى Decimal بطريقة حتمية.

    يدعم فقط التحويلات المعروفة والمبررة في المشروع،
    مثل الأرقام العربية والفواصل والعملات والقيم النصية المعروفة.
    """

    if is_blank(value):
        return None

    text = str(value).strip()

    # قيم الكلمات الرقمية المعروفة.
    if text in KNOWN_PRICE_WORDS:
        return KNOWN_PRICE_WORDS[text]

    # ٥٠٠٠ → 5000
    text = normalize_digits(text)

    # توحيد الفواصل العربية.
    text = text.replace("٫", ".")
    text = text.replace("٬", ",")

    # إزالة لاحقة العملة المعروفة فقط.
    # مثال: 5000 ريال → 5000
    text = re.sub(
        r"\s*(YER|ريال يمني|ريال)\s*$",
        "",
        text,
        flags=re.IGNORECASE,
    )

    # إزالة Thousands Separator.
    # مثال: 5,000 → 5000
    text = text.strip().replace(",", "")

    try:
        return Decimal(text)

    except InvalidOperation:
        # إذا لم نستطع فهم القيمة بأمان لا نخمن.
        return None


# ============================================================
# AUDIT HELPERS
# تسجيل التصحيحات والأخطاء
# ============================================================

def add_correction(
    corrections,
    field,
    original,
    corrected,
    rule_code,
    details=None,
):
    """
    تسجيل ماذا تم تصحيحه:
    الحقل + القيمة القديمة + الجديدة + القاعدة المستخدمة.
    """

    correction = {
        "field": field,
        "original_value": original,
        "corrected_value": corrected,
        "rule_code": rule_code,
    }

    if details:
        correction["details"] = details

    corrections.append(correction)


def add_error(
    errors,
    code,
    field,
    value,
    message,
):
    """
    تسجيل الخطأ الذي لم نستطع إصلاحه بأمان.
    """

    errors.append(
        {
            "code": code,
            "field": field,
            "value": value,
            "message": message,
        }
    )


# ============================================================
# NUMERIC FORMAT AUDIT
# معرفة أي قواعد رقمية تم استخدامها
# ============================================================

def numeric_rule_codes(original):
    if is_blank(original):
        return []

    text = str(original).strip()

    codes = []

    if text in KNOWN_PRICE_WORDS:
        codes.append(RULE_KNOWN_PRICE_WORD)

    if ARABIC_DIGITS_RE.search(text):
        codes.append(RULE_ARABIC_DIGITS)

    if "٫" in text:
        codes.append(RULE_ARABIC_DECIMAL)

    if "," in text or "٬" in text:
        codes.append(RULE_THOUSANDS)

    if re.search(
        r"(ريال يمني|ريال)\s*$",
        text
    ):
        codes.append(RULE_CURRENCY_SUFFIX)

    return codes


def normalize_numeric_field(
    cleaned,
    field,
    corrections,
):
    """
    تنظيف حقل رقمي وتسجيل قواعد التصحيح التي استخدمت عليه.
    """

    original = cleaned.get(field)

    parsed = parse_decimal(original)

    if parsed is None:
        return None

    corrected = to_number(parsed)

    cleaned[field] = corrected

    # تسجيل نوع التحويل الذي حدث.
    for rule_code in numeric_rule_codes(original):

        add_correction(
            corrections,
            field,
            original,
            corrected,
            rule_code,
        )

    return parsed


# ============================================================
# WHITESPACE
# إزالة المسافات الزائدة
# ============================================================

def normalize_top_level_whitespace(
    cleaned,
    corrections,
):
    for field, value in list(cleaned.items()):

        # نعالج الحقول النصية فقط.
        if not isinstance(value, str):
            continue

        trimmed = value.strip()

        if trimmed != value:

            add_correction(
                corrections,
                field,
                value,
                trimmed,
                RULE_WHITESPACE,
            )

            cleaned[field] = trimmed


# ============================================================
# DATE
# توحيد التاريخ إلى صيغة واحدة
# ============================================================

def normalize_date(
    cleaned,
    corrections,
    errors,
):
    original = cleaned.get("order_date")

    # التاريخ مطلوب.
    if is_blank(original):

        add_error(
            errors,
            ERR_DATE_IMPOSSIBLE_INVALID,
            "order_date",
            original,
            "Order date is missing.",
        )

        return

    # تحويل الأرقام العربية داخل التاريخ أولًا.
    text = normalize_digits(
        str(original).strip()
    )

    # محاولة قراءة ISO مباشرة.
    try:
        parsed = datetime.fromisoformat(
            text.replace("Z", "+00:00")
        )

        canonical = parsed.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        cleaned["order_date"] = canonical

        return

    except ValueError:
        pass

    # صيغ التاريخ المعروفة التي يسمح المشروع بتحويلها.
    known_formats = [
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
    ]

    for fmt in known_formats:

        try:
            parsed = datetime.strptime(
                text,
                fmt
            )

            # جميع التواريخ تصبح بنفس الشكل النهائي.
            canonical = parsed.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

            cleaned["order_date"] = canonical

            add_correction(
                corrections,
                "order_date",
                original,
                canonical,
                RULE_DATE,
            )

            return

        except ValueError:
            continue

    # تاريخ مستحيل أو صيغة غير معروفة → Error.
    add_error(
        errors,
        ERR_DATE_IMPOSSIBLE_INVALID,
        "order_date",
        original,
        "Date is impossible or cannot be converted by a known safe format.",
    )


# ============================================================
# EMAIL
# فحص وإصلاح البريد الإلكتروني
# ============================================================

def email_is_valid(value):
    """فحص بسيط لبنية البريد الإلكتروني."""

    if is_blank(value):
        return False

    text = str(value).strip()

    if " " in text:
        return False

    if text.count("@") != 1:
        return False

    if ".." in text:
        return False

    local, domain = text.split("@", 1)

    if not local or not domain:
        return False

    if (
        local.startswith(".")
        or local.endswith(".")
        or domain.startswith(".")
        or domain.endswith(".")
    ):
        return False

    if "." not in domain:
        return False

    if any(
        not label
        for label in domain.split(".")
    ):
        return False

    return True


def normalize_email(
    cleaned,
    corrections,
    errors,
):
    original = cleaned.get(
        "customer_email"
    )

    if is_blank(original):

        add_error(
            errors,
            ERR_EMAIL_INVALID,
            "customer_email",
            original,
            "Email is missing.",
        )

        return

    text = str(original).strip()

    # إذا كان صحيحًا نتركه كما هو.
    if email_is_valid(text):
        cleaned["customer_email"] = text
        return

    # إصلاح فقط الرموز المتكررة مثل @@ و ..
    candidate = REPEATED_AT.sub(
        "@",
        text
    )

    candidate = REPEATED_DOT.sub(
        ".",
        candidate
    )

    # نقبل التصحيح فقط إذا أصبح البريد صالحًا فعلًا.
    if (
        candidate != text
        and email_is_valid(candidate)
    ):

        cleaned["customer_email"] = (
            candidate
        )

        add_correction(
            corrections,
            "customer_email",
            original,
            candidate,
            RULE_EMAIL,
        )

        return

    # إذا لم يوجد إصلاح مؤكد → Quarantine لاحقًا.
    add_error(
        errors,
        ERR_EMAIL_INVALID,
        "customer_email",
        original,
        "Email cannot be repaired deterministically.",
    )


# ============================================================
# PHONE
# توحيد رقم الهاتف اليمني
# ============================================================

def normalize_phone(
    cleaned,
    corrections,
    errors,
):
    original = cleaned.get(
        "customer_phone"
    )

    if is_blank(original):

        add_error(
            errors,
            ERR_PHONE_INVALID,
            "customer_phone",
            original,
            "Phone number is missing.",
        )

        return

    # تحويل الأرقام العربية ثم إزالة الرموز الشكلية.
    text = normalize_digits(
        str(original).strip()
    )

    compact = re.sub(
        r"[\s\-()]",
        "",
        text
    )

    corrected = None

    # +967 + تسعة أرقام محلية.
    if compact.startswith("+967"):

        national = compact[4:]

        if (
            national.isdigit()
            and len(national) == 9
        ):
            corrected = national

    # أو الرقم موجود أصلًا بالشكل المحلي الصحيح.
    elif (
        compact.isdigit()
        and len(compact) == 9
    ):
        corrected = compact

    if corrected is None:

        add_error(
            errors,
            ERR_PHONE_INVALID,
            "customer_phone",
            original,
            "Phone cannot be safely normalized to the 9-digit Yemeni format.",
        )

        return

    cleaned["customer_phone"] = (
        corrected
    )

    # تسجيل التصحيح فقط إذا تغيرت القيمة.
    if str(original).strip() != corrected:

        add_correction(
            corrections,
            "customer_phone",
            original,
            corrected,
            RULE_PHONE,
        )


# ============================================================
# CURRENCY
# توحيد العملة إلى YER
# ============================================================

def normalize_currency(
    cleaned,
    corrections,
    errors,
):
    original = cleaned.get("currency")

    if is_blank(original):

        add_error(
            errors,
            ERR_CURRENCY_UNKNOWN,
            "currency",
            original,
            "Currency is missing.",
        )

        return

    text = str(original).strip()

    # yer / Yer / YER → YER
    if text.upper() == "YER":

        cleaned["currency"] = "YER"

        if text != "YER":

            add_correction(
                corrections,
                "currency",
                original,
                "YER",
                RULE_CURRENCY_YER,
            )

        return

    # المرادفات العربية المعروفة.
    if text in {
        "ريال يمني",
        "ريال",
    }:

        cleaned["currency"] = "YER"

        add_correction(
            corrections,
            "currency",
            original,
            "YER",
            RULE_CURRENCY_YER,
        )

        return

    # لا نفترض أن أي عملة مجهولة هي YER.
    add_error(
        errors,
        ERR_CURRENCY_UNKNOWN,
        "currency",
        original,
        "Currency is unknown; assigning YER would be guessing.",
    )


# ============================================================
# STATUS
# التحقق من حالات الطلب والدفع
# ============================================================

def normalize_statuses(
    cleaned,
    corrections,
    errors,
):
    status = cleaned.get("status")

    # حالة الطلب يجب أن تكون ضمن الحالات المعروفة.
    if (
        is_blank(status)
        or str(status).strip()
        not in VALID_ORDER_STATUSES
    ):

        add_error(
            errors,
            ERR_STATUS_UNKNOWN,
            "status",
            status,
            "Order status is not in the known canonical status set.",
        )

    else:

        cleaned["status"] = (
            str(status).strip()
        )

    payment_status = cleaned.get(
        "payment_status"
    )

    if not is_blank(payment_status):

        text = str(payment_status).strip()

        canonical = PAYMENT_STATUS_MAP.get(
            text
        )

        if canonical is not None:

            cleaned["payment_status"] = (
                canonical
            )

            # مثال: مدفوع → تم الدفع
            if canonical != text:

                add_correction(
                    corrections,
                    "payment_status",
                    payment_status,
                    canonical,
                    RULE_STATUS_SYNONYM,
                )


# ============================================================
# ITEMS JSON
# قراءة وتنظيف منتجات الطلب
# ============================================================

def parse_items(
    cleaned,
    errors,
):
    value = cleaned.get("items_json")

    if is_blank(value):

        add_error(
            errors,
            ERR_ITEMS_EMPTY,
            "items_json",
            value,
            "Order has no items.",
        )

        return None

    try:
        # تحويل JSON النصي إلى List Python.
        items = json.loads(value)

    except (
        json.JSONDecodeError,
        TypeError,
    ):

        add_error(
            errors,
            ERR_JSON_ITEMS_CORRUPTED,
            "items_json",
            value,
            "items_json cannot be parsed.",
        )

        return None

    # يجب أن يكون items_json عبارة عن List.
    if not isinstance(items, list):

        add_error(
            errors,
            ERR_JSON_ITEMS_CORRUPTED,
            "items_json",
            value,
            "items_json must be a list.",
        )

        return None

    # القائمة لا يجوز أن تكون فارغة.
    if len(items) == 0:

        add_error(
            errors,
            ERR_ITEMS_EMPTY,
            "items_json",
            value,
            "Items list is empty.",
        )

        return None

    # كل Item يجب أن يكون Object/Dictionary.
    if any(
        not isinstance(item, dict)
        for item in items
    ):

        add_error(
            errors,
            ERR_JSON_ITEMS_CORRUPTED,
            "items_json",
            value,
            "One or more items are not objects.",
        )

        return None

    # نرجع نسخة مستقلة من العناصر.
    return [
        dict(item)
        for item in items
    ]


def normalize_item_whitespace(
    items,
    corrections,
):
    """إزالة المسافات الزائدة من الحقول النصية داخل Items."""

    for index, item in enumerate(items):

        for key, value in list(
            item.items()
        ):

            if not isinstance(value, str):
                continue

            trimmed = value.strip()

            if trimmed != value:

                item[key] = trimmed

                add_correction(
                    corrections,
                    f"items_json[{index}].{key}",
                    value,
                    trimmed,
                    RULE_WHITESPACE,
                )


def item_number(
    item,
    field,
    index,
    corrections,
):
    """
    تنظيف حقل رقمي داخل Item مثل:
    qty / unit_price / total
    """

    original = item.get(field)

    parsed = parse_decimal(original)

    if parsed is None:
        return None

    corrected = to_number(parsed)

    item[field] = corrected

    if (
        field == "qty"
        and isinstance(original, str)
    ):
        add_correction(
            corrections,
            f"items_json[{index}].{field}",
            original,
            corrected,
            RULE_QTY_STRING,
            details=(
                "Numeric quantity stored as text "
                "normalized to number."
            ),
        )

    for rule_code in numeric_rule_codes(
        original
    ):

        add_correction(
            corrections,
            f"items_json[{index}].{field}",
            original,
            corrected,
            rule_code,
        )

    return parsed


def clean_items(
    cleaned,
    corrections,
    errors,
    delivery,
    order_total,
    payment_amount,
):
    """
    أهم جزء في معالجة Items.

    يقوم بـ:
    - قراءة items_json.
    - تنظيف القيم.
    - فحص qty / unit_price / total.
    - تنفيذ الاشتقاقات الرياضية الآمنة.
    - محاولة Residual Recovery.
    - رفض الحالات المتعارضة التي تحتاج تخمين.
    """

    items = parse_items(
        cleaned,
        errors
    )

    if items is None:
        return None

    normalize_item_whitespace(
        items,
        corrections
    )

    states = []

    # --------------------------------------------------------
    # FIRST PASS
    # قراءة وفحص المكونات الأساسية لكل Item
    # --------------------------------------------------------

    for index, item in enumerate(items):

        if is_blank(item.get("sku")):
            add_error(
                errors,
                ERR_ITEM_SKU_MISSING,
                f"items_json[{index}].sku",
                item.get("sku"),
                "Item SKU is missing and cannot be inferred.",
            )

        qty = item_number(
            item,
            "qty",
            index,
            corrections,
        )

        unit_price = item_number(
            item,
            "unit_price",
            index,
            corrections,
        )

        item_total = item_number(
            item,
            "total",
            index,
            corrections,
        )

        # ----------------------------------------------------
        # NEGATIVE QUANTITY
        # لا نحول السالب إلى موجب مباشرة.
        # نحاول اشتقاق الكمية من total / price مع دليل إضافي.
        if (
            qty is not None
            and qty < 0
        ):
            add_error(
                errors,
                ERR_NEGATIVE_QUANTITY,
                f"items_json[{index}].qty",
                item.get("qty"),
                "Negative quantity is not accepted.",
            )

        # Quantity مفقودة أو صفر → خطأ.
        if (
            qty is None
            or qty == 0
        ):

            add_error(
                errors,
                ERR_ITEM_QUANTITY_INVALID,
                f"items_json[{index}].qty",
                item.get("qty"),
                "Quantity is missing, non-numeric, or zero.",
            )

        # السعر السالب لا يمكن إصلاحه بأمان.
        if (
            unit_price is not None
            and unit_price < 0
        ):

            add_error(
                errors,
                ERR_VALUE_NEGATIVE_AMBIGUOUS,
                f"items_json[{index}].unit_price",
                item.get("unit_price"),
                "Negative unit price is ambiguous.",
            )

        # Item Total السالب كذلك.
        if (
            item_total is not None
            and item_total < 0
        ):

            add_error(
                errors,
                ERR_VALUE_NEGATIVE_AMBIGUOUS,
                f"items_json[{index}].total",
                item.get("total"),
                "Negative item total is ambiguous.",
            )

        # حفظ حالة كل Item لاستخدامها في المراحل التالية.
        states.append(
            {
                "index": index,
                "item": item,
                "qty": qty,
                "unit_price": unit_price,
                "item_total": item_total,
            }
        )

    # --------------------------------------------------------
    # DIRECT SAFE DERIVATIONS
    # اشتقاقات رياضية مباشرة وآمنة
    # --------------------------------------------------------

    for state in states:

        index = state["index"]
        item = state["item"]

        qty = state["qty"]
        price = state["unit_price"]
        total = state["item_total"]

        # إذا Total مفقود:
        # total = qty × unit_price
        if (
            total is None
            and positive_integer(qty)
            and price is not None
            and price >= 0
        ):

            derived = qty * price

            original = item.get("total")

            item["total"] = to_number(
                derived
            )

            state["item_total"] = (
                derived
            )

            add_correction(
                corrections,
                f"items_json[{index}].total",
                original,
                item["total"],
                RULE_ITEM_TOTAL,
                details="Derived as qty * unit_price.",
            )

        # إذا Unit Price مفقود:
        # price = total / qty
        if (
            state["unit_price"] is None
            and positive_integer(qty)
            and state["item_total"]
            is not None
            and state["item_total"] >= 0
        ):

            derived = (
                state["item_total"]
                / qty
            )

            original = item.get(
                "unit_price"
            )

            item["unit_price"] = (
                to_number(derived)
            )

            state["unit_price"] = (
                derived
            )

            add_correction(
                corrections,
                f"items_json[{index}].unit_price",
                original,
                item["unit_price"],
                RULE_ITEM_PRICE_DIRECT,
                details="Derived as item_total / qty.",
            )

    # --------------------------------------------------------
    # RESIDUAL RECOVERY
    # استرجاع قيمة Item من إجمالي الطلب
    # --------------------------------------------------------

    # نستخدم هذه الطريقة فقط عندما يوجد Item واحد
    # مجهول السعر والإجمالي، حتى لا يصبح الحل تخمينًا.
    targets = [
        state
        for state in states
        if (
            state["unit_price"] is None
            and state["item_total"] is None
            and positive_integer(
                state["qty"]
            )
        )
    ]

    if len(targets) == 1:

        target = targets[0]

        others = [
            state
            for state in states
            if state is not target
        ]

        # شروط قوية قبل استخدام Residual:
        # - Order Total معروف.
        # - Delivery معروف.
        # - Payment Amount يؤكد Order Total.
        # - جميع Items الأخرى معروفة.
        if (
            order_total is not None
            and order_total >= 0
            and delivery is not None
            and delivery >= 0
            and payment_amount is not None
            and payment_amount == order_total
            and all(
                state["item_total"] is not None
                and state["item_total"] >= 0
                for state in others
            )
        ):

            # residual =
            # order_total - delivery - totals of other items
            residual = (
                order_total
                - delivery
                - sum(
                    (
                        state["item_total"]
                        for state in others
                    ),
                    Decimal("0"),
                )
            )

            if residual >= 0:

                item = target["item"]
                index = target["index"]
                qty = target["qty"]

                original_total = (
                    item.get("total")
                )

                original_price = (
                    item.get("unit_price")
                )

                # إجمالي العنصر = المتبقي.
                target["item_total"] = (
                    residual
                )

                # السعر = المتبقي / الكمية.
                target["unit_price"] = (
                    residual / qty
                )

                item["total"] = to_number(
                    target["item_total"]
                )

                item["unit_price"] = (
                    to_number(
                        target["unit_price"]
                    )
                )

                add_correction(
                    corrections,
                    f"items_json[{index}].total",
                    original_total,
                    item["total"],
                    RULE_ITEM_TOTAL_RESIDUAL,
                    details=(
                        "Residual = order_total - delivery "
                        "- other item totals."
                    ),
                )

                add_correction(
                    corrections,
                    f"items_json[{index}].unit_price",
                    original_price,
                    item["unit_price"],
                    RULE_ITEM_PRICE_RESIDUAL,
                    details=(
                        "Derived from residual item total / qty; "
                        "payment_amount corroborates order_total."
                    ),
                )

    # --------------------------------------------------------
    # FINAL ITEM VALIDATION
    # الفحص النهائي بعد جميع التصحيحات الآمنة
    # --------------------------------------------------------

    for state in states:

        index = state["index"]

        qty = state["qty"]
        price = state["unit_price"]
        total = state["item_total"]

        # إذا بقي السعر أو الإجمالي مجهولًا → لا يوجد حل آمن.
        if (
            price is None
            or total is None
        ):

            add_error(
                errors,
                ERR_PRICE_UNKNOWN,
                f"items_json[{index}]",
                state["item"],
                (
                    "Price or item total remains unknown after "
                    "all deterministic derivations."
                ),
            )

            continue

        # العلاقة الأساسية:
        # qty × unit_price يجب أن تساوي total.
        #
        # إذا بقي تعارض، لا نخمن أي قيمة هي الخاطئة.
        if (
            qty is not None
            and qty > 0
            and price >= 0
            and total >= 0
            and qty * price != total
        ):

            add_error(
                errors,
                ERR_ITEM_COMPONENTS_CONFLICT,
                f"items_json[{index}]",
                state["item"],
                (
                    "qty, unit_price and total remain inconsistent "
                    "after safe corrections."
                ),
            )

    # إعادة Items إلى JSON String بعد التنظيف.
    cleaned["items_json"] = json.dumps(
        items,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return states


# ============================================================
# MAIN RECORD CLASSIFIER
# الدالة الرئيسية التي تطبق جميع قواعد الجودة على سجل واحد
# ============================================================

def classify_record(
    raw_record,
    duplicate_conflict=False,
):
    """
    المدخل:
        Raw Record واحد.

    المخرج:
        quality_status
        cleaned_record
        corrections
        codes_error
        details_error

    هذه هي الدالة التي يستدعيها الـ ELT Pipeline لكل سجل.
    """

    # نعمل نسخة حتى لا نعدل Raw Record الأصلي.
    cleaned = copy.deepcopy(
        raw_record
    )

    # Audit Trail للتصحيحات والأخطاء.
    corrections = []
    errors = []

    # --------------------------------------------------------
    # 1. GENERAL TRIM
    # --------------------------------------------------------

    normalize_top_level_whitespace(
        cleaned,
        corrections
    )

    # --------------------------------------------------------
    # 2. REQUIRED IDS
    # --------------------------------------------------------

    # لا يمكن اختراع order_id.
    if is_blank(
        cleaned.get("order_id")
    ):

        add_error(
            errors,
            ERR_ID_ORDER_MISSING,
            "order_id",
            cleaned.get("order_id"),
            "Order ID is missing and cannot be inferred.",
        )

    # لا يمكن اختراع customer_id.
    if is_blank(
        cleaned.get("customer_id")
    ):

        add_error(
            errors,
            ERR_ID_CUSTOMER_MISSING,
            "customer_id",
            cleaned.get("customer_id"),
            "Customer ID is missing and cannot be inferred.",
        )

    # قيمة duplicate_conflict يتم تحديدها خارج هذا الملف
    # ثم تمريرها إلى المصنف.
    if duplicate_conflict:

        add_error(
            errors,
            ERR_ID_ORDER_DUPLICATE,
            "order_id",
            cleaned.get("order_id"),
            (
                "order_id belongs to a genuinely conflicting "
                "duplicate group."
            ),
        )

    # --------------------------------------------------------
    # 3. TEXT / FORMAT RULES
    # --------------------------------------------------------

    normalize_date(
        cleaned,
        corrections,
        errors
    )

    normalize_email(
        cleaned,
        corrections,
        errors
    )

    normalize_phone(
        cleaned,
        corrections,
        errors
    )

    normalize_currency(
        cleaned,
        corrections,
        errors
    )

    normalize_statuses(
        cleaned,
        corrections,
        errors
    )

    # --------------------------------------------------------
    # 4. TOP-LEVEL NUMBERS
    # --------------------------------------------------------

    delivery = normalize_numeric_field(
        cleaned,
        "delivery_cost",
        corrections,
    )

    payment_amount = normalize_numeric_field(
        cleaned,
        "payment_amount",
        corrections,
    )

    order_total = normalize_numeric_field(
        cleaned,
        "total_amount",
        corrections,
    )

    # --------------------------------------------------------
    # 5. ITEMS
    # --------------------------------------------------------

    states = clean_items(
        cleaned,
        corrections,
        errors,
        delivery,
        order_total,
        payment_amount,
    )

    # --------------------------------------------------------
    # 6. ORDER TOTAL
    # التحقق من:
    # Order Total = Sum(Item Totals) + Delivery
    # --------------------------------------------------------

    if (
        states is not None
        and delivery is not None
        and delivery >= 0
    ):

        # هل جميع إجماليات المنتجات معروفة وصحيحة؟
        all_totals_known = all(
            state["item_total"]
            is not None
            and state["item_total"] >= 0
            for state in states
        )

        if all_totals_known:

            expected_total = (
                sum(
                    (
                        state["item_total"]
                        for state in states
                    ),
                    Decimal("0"),
                )
                + delivery
            )

            # إذا Order Total مفقود، يمكن اشتقاقه بأمان.
            if order_total is None:

                original = raw_record.get(
                    "total_amount"
                )

                cleaned["total_amount"] = (
                    to_number(
                        expected_total
                    )
                )

                order_total = expected_total

                add_correction(
                    corrections,
                    "total_amount",
                    original,
                    cleaned["total_amount"],
                    RULE_ORDER_TOTAL_DERIVED,
                    details=(
                        "Derived as sum(item totals) "
                        "+ delivery_cost."
                    ),
                )

            # إذا موجود لكنه لا يساوي المجموع الصحيح،
            # نعيد حسابه من Items + Delivery.
            elif order_total != expected_total:

                original = cleaned.get(
                    "total_amount"
                )

                cleaned["total_amount"] = (
                    to_number(
                        expected_total
                    )
                )

                order_total = expected_total

                add_correction(
                    corrections,
                    "total_amount",
                    original,
                    cleaned["total_amount"],
                    RULE_ORDER_TOTAL_RECALCULATED,
                    details=(
                        "Recalculated as sum(item totals) "
                        "+ delivery_cost."
                    ),
                )

        # إذا Total مفقود ومكونات الحساب نفسها غير مكتملة،
        # لا يمكن اشتقاقه بأمان.
        elif order_total is None:

            add_error(
                errors,
                ERR_TOTAL_UNKNOWN,
                "total_amount",
                raw_record.get(
                    "total_amount"
                ),
                (
                    "Order total is unknown and cannot "
                    "be safely recomputed."
                ),
            )

    elif order_total is None:

        add_error(
            errors,
            ERR_TOTAL_UNKNOWN,
            "total_amount",
            raw_record.get(
                "total_amount"
            ),
            (
                "Order total is unknown and required "
                "components are unusable."
            ),
        )

    # --------------------------------------------------------
    # 7. MULTIPLE CONFLICTING ERRORS
    # تجميع أكواد الأخطاء بدون تكرار
    # --------------------------------------------------------

    codes_error = []

    for error in errors:

        code = error["code"]

        if code not in codes_error:
            codes_error.append(code)

    # إذا وجد أكثر من نوع خطأ جوهري،
    # نسجل أن السجل يحتوي Multiple Conflicting Errors.
    if (
        len(codes_error) > 1
        and ERR_ERRORS_CONFLICTING_MULTIPLE
        not in codes_error
    ):

        add_error(
            errors,
            ERR_ERRORS_CONFLICTING_MULTIPLE,
            "_record",
            None,
            (
                "Multiple substantial errors prevent "
                "a single safe correction path."
            ),
        )

        codes_error.append(
            ERR_ERRORS_CONFLICTING_MULTIPLE
        )

    # --------------------------------------------------------
    # 8. FINAL CLASSIFICATION
    # أهم قرار في الملف
    # --------------------------------------------------------

    # وجود أي Error يعني أن السجل يحتاج Quarantine.
    if errors:

        quality_status = (
            QUALITY_QUARANTINED
        )

    # لا Errors لكن حدثت تعديلات آمنة.
    elif corrections:

        quality_status = (
            QUALITY_CORRECTED
        )

    # لا Errors ولا Corrections = السجل صحيح من البداية.
    else:

        quality_status = (
            QUALITY_VALID
        )

    # --------------------------------------------------------
    # OUTPUT
    # إرجاع النتيجة مع Audit Trail كامل
    # --------------------------------------------------------

    return {
        "quality_status": quality_status,

        # النسخة النهائية بعد التنظيف.
        "cleaned_record": cleaned,

        # ماذا تم تصحيحه وكيف؟
        "corrections": corrections,

        # أكواد الأخطاء المختصرة.
        "codes_error": codes_error,

        # تفاصيل الأخطاء.
        "details_error": errors,
    }