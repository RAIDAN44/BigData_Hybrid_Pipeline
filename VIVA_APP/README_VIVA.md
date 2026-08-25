# VIVA_APP — Hybrid Big Data Pipeline

واجهة عربية RTL مستقلة لعرض ومناقشة مشروع `BigData_Hybrid_Pipeline`، مصممة لتعرض المشروع بصريًا وتسمح بتجربة عملية معزولة بدون تغيير النتائج الرسمية.

## التشغيل

من PowerShell داخل جذر المشروع:

```powershell
.\VIVA_APP\run.ps1
```

إذا كانت سياسة PowerShell تمنع تشغيل السكربت:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\VIVA_APP\run.ps1"
```

## الصفحات النهائية

1. **الرئيسية** — ملخص المشروع، Pipeline بصري، المؤشرات الأساسية، وقرار 200MB.
2. **رحلة المشروع** — Timeline بصري + المراحل الهندسية الكاملة من التكليف حتى التشغيل الآمن >200MB.
3. **النتائج الرسمية** — نتائج 100K و30M، رسوم بيانية، تفسير Valid، Correction Rules وError Codes.
4. **الـSchema وتفسير Valid** — الحقول الـ17، Raw String Schema، Final MongoDB Shape، وفحص Schema Validator Read-only.
5. **الملفات والأكواد** — مستكشف Read-only للملفات الحقيقية مع شرح ملفات Big Data الأساسية وفتح الكود.
6. **حالات البيانات** — 29 حالة فعلية من المشروع مع Rule/Error Codes، ويتضمن الفرق بين Source Duplicate وIdempotent Rerun.
7. **متطلبات التكليف** — البنود 6.1 إلى 6.12 وربطها بالتنفيذ والدليل.
8. **التجربة العملية** — اختيار CSV، Router تلقائي، مراحل التنفيذ، نتائج ورسوم حية، وعزل كامل عن النتائج الرسمية.

## النقاط الأساسية

- `Valid` ليس نسبة ثابتة. لنفس العينة ونفس Base Validation Contract، السجلات التي لا تحتوي Errors ولا تحتاج Corrections يجب أن تتطابق.
- `Corrected` يحتوي سجلات تم إصلاحها بقواعد حتمية آمنة، و`Quarantine` يحتوي سجلات لا يمكن إصلاحها بثقة.
- `orders_validated = Valid + Corrected`، وليس حالة رابعة.
- `BATCH_SIZE=5000` يعني grouping لكتابة MongoDB وليس Parallelism.
- Spark Partitions وحدات بيانات/عمل؛ 99 Partitions لا تعني 99 OS Processes.
- Source Duplicate يختلف عن Idempotent Rerun.

## أمان Live Demo

- الملف المختار يُنسخ إلى `VIVA_APP/runtime/uploads/` فقط.
- Router الحقيقي يحدد المحرك من الحجم، وليس من اسم الملف.
- Collections الرسمية محمية من الكتابة والحذف.
- Collections التجريبية تبدأ فقط بـ `viva_...` أو `_spark_viva_...`.
- PySpark يستخدم `src.main.build_spark_environment()` ثم `src.spark_loader`.
- تنظيف التجربة يحذف فقط Collections ذات prefixes الخاصة بالواجهة.

## الهيكل

```text
VIVA_APP/
├── backend/
│   ├── app.py
│   ├── demo_adapter.py
│   └── requirements.txt
├── content/
│   └── content.json
├── frontend/
│   ├── index.html
│   ├── styles.css
│   └── app.js
├── runtime/
│   ├── uploads/
│   ├── runs/
│   └── logs/
├── tests/
│   └── test_app_smoke.py
├── README_VIVA.md
└── run.ps1
```


## Rehearsal and LIVE modes

- **Rehearsal** is the default. It is intended for pre-viva testing and does not write to the official business collections.
- **LIVE** is deliberately explicit and invokes the real project entry point `python -m src.main --input <CSV>`. This mode can change the official MongoDB state.
- The official historical results shown by VIVA_APP come from `content/official_results_snapshot.json`, so they remain visible even after a LIVE run.
- The UI shows MongoDB counts before and after a LIVE run when available.
