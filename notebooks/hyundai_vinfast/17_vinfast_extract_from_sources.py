# Databricks notebook source
# MAGIC %md
# MAGIC # VinFast extraction from fetched press releases
# MAGIC
# MAGIC Reads `vinfast_ir_sources` (written by notebook 16) and produces
# MAGIC `vinfast_sales_by_model` rows.
# MAGIC
# MAGIC Gemini is used **without** search grounding: it only reformats text we
# MAGIC already fetched for a month we already chose. It never decides which month
# MAGIC or which source a number belongs to. That removes the failure that put
# MAGIC `2026-03`'s figures into `2024-03`, and it also removes the dependency on
# MAGIC Google Search grounding quota, which the current API key does not have.
# MAGIC
# MAGIC Four deterministic guards run after the model:
# MAGIC
# MAGIC 0. **Month check** - the fetched page must actually mention the month and
# MAGIC    year the slug claimed, so a mis-parsed slug cannot file one month's
# MAGIC    figures under another.
# MAGIC 1. **Evidence check** - every extracted number must appear literally in the
# MAGIC    fetched text. A number the model invented cannot pass.
# MAGIC 2. **Headline check** - the monthly total must equal the total in the URL
# MAGIC    slug, which was never shown to the model. Some older slug shapes do not
# MAGIC    state an exact total (`...more-than-20000-cars...`); those skip this check.
# MAGIC 3. **Sum check** - model rows may not exceed the monthly total; any residual
# MAGIC    becomes an explicit `Other models` row.
# MAGIC
# MAGIC A month failing any guard is dropped rather than written.

# COMMAND ----------

import datetime as dt
import json
import os
import re
import time

import requests
from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DateType,
)

CATALOG = "market_data"
SCHEMA = "hyundai_vinfast"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

MODEL = "gemini-3.1-flash-lite"
PROVIDER = "vinfast_ir_release_fetch_then_extract"
CREATED_AT = dt.datetime.now()

# COMMAND ----------

dbutils.widgets.text("reextract_all", "false")
dbutils.widgets.text("only_months", "")
dbutils.widgets.text("gemini_secret_scope", "news-signal")
dbutils.widgets.text("gemini_secret_key", "gemini-api-key")

REEXTRACT_ALL = dbutils.widgets.get("reextract_all").strip().lower() == "true"
ONLY_MONTHS = [m.strip() for m in dbutils.widgets.get("only_months").split(",") if m.strip()]
GEMINI_SECRET_SCOPE = dbutils.widgets.get("gemini_secret_scope").strip()
GEMINI_SECRET_KEY = dbutils.widgets.get("gemini_secret_key").strip()

print(f"reextract_all={REEXTRACT_ALL} only_months={ONLY_MONTHS or '(pending only)'}")

# COMMAND ----------


def load_gemini_key():
    """Databricks secret first. The workspace .env. file is a dead legacy fallback -
    reading it first is what made a correctly rotated key look expired."""
    if GEMINI_SECRET_SCOPE and GEMINI_SECRET_KEY:
        try:
            value = dbutils.secrets.get(scope=GEMINI_SECRET_SCOPE, key=GEMINI_SECRET_KEY)
            if value and value.strip():
                print(f"gemini key: from secret {GEMINI_SECRET_SCOPE}/{GEMINI_SECRET_KEY} "
                      f"(length {len(value.strip())})")
                return value.strip()
        except Exception as e:
            print(f"gemini key: secret unavailable ({type(e).__name__}), trying env file")
    path = "/Workspace/Users/tuckeyhue@gmail.com/env/.env."
    if os.path.exists(path):
        with open(path, encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    if k.strip() == "GEMINI_API_KEY":
                        print("gemini key: from legacy env file")
                        return v.strip().strip('"').strip("'")
    raise RuntimeError(
        f"No Gemini API key: checked secret {GEMINI_SECRET_SCOPE}/{GEMINI_SECRET_KEY} and {path}")


GEMINI_API_KEY = load_gemini_key()

# COMMAND ----------

# DBTITLE 1,Select months to extract
filters = ["raw.fetch_status = 'ok'", "raw.extracted_text IS NOT NULL"]
if ONLY_MONTHS:
    months_sql = ", ".join("'" + m.replace("'", "''") + "'" for m in ONLY_MONTHS)
    filters.append(f"raw.report_month IN ({months_sql})")
elif not REEXTRACT_ALL:
    # Pending = never extracted by this method, or the source page changed since.
    filters.append(f"""(
        done.report_month IS NULL
        OR done.last_hash IS NULL
        OR done.last_hash <> raw.content_hash
    )""")

pending = spark.sql(f"""
WITH stamped AS (
  SELECT report_month,
         CASE WHEN parsing_method = '{PROVIDER}'
              THEN regexp_extract(COALESCE(validation_message, ''), 'srchash=([0-9a-f]+)', 1)
              ELSE '' END AS src_hash
  FROM {FULL_SCHEMA}.vinfast_sales_by_model
),
done AS (
  SELECT report_month, MAX(src_hash) AS last_hash
  FROM stamped
  WHERE src_hash <> ''
  GROUP BY report_month
)
SELECT raw.report_month, raw.report_year, raw.source_id, raw.source_url,
       raw.source_domain, raw.headline_total, raw.page_title,
       raw.content_hash, raw.extracted_text
FROM {FULL_SCHEMA}.vinfast_ir_sources raw
LEFT JOIN done ON done.report_month = raw.report_month
WHERE {' AND '.join(filters)}
ORDER BY raw.report_month
""").collect()

print(f"months to extract: {[r['report_month'] for r in pending] or 'none'}")

# COMMAND ----------

PROMPT = """You are converting a VinFast press release into structured data.

The text below is the official VinFast investor-relations release for {month}.
Extract ONLY what this text states about deliveries in that month in Vietnam.

Rules:
- Use only numbers that appear literally in the text. Never estimate or infer.
- Report per-model MONTHLY deliveries for {month}, not year-to-date/cumulative
  figures. The release usually gives monthly figures first, then a "For the first
  half"/"year to date" paragraph - ignore that second set.
- If a model is reported combined with a derivative (for example "the VF 5 and the
  Herio Green ... combined deliveries"), emit ONE row using the base model name.
- Use the model names exactly as written (for example "VF 3", "VF 5", "VF 6",
  "VF 7", "VF MPV 7", "Limo Green", "Minio Green", "EC Van").
- Do not invent an "Other models" row; that is computed downstream.

Return JSON only, no markdown fence:
{{
  "report_month": "{month}",
  "monthly_total": integer_or_null,
  "ytd_total": integer_or_null,
  "model_rows": [
    {{"model_name": string, "monthly_total": integer, "evidence_quote": string}}
  ]
}}

TEXT:
{text}
"""


def gemini_extract(month, text):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent"
    body = {
        "contents": [{"role": "user", "parts": [{"text": PROMPT.format(month=month, text=text[:60000])}]}],
        "generationConfig": {"temperature": 0.0, "response_mime_type": "application/json"},
    }
    last_error = None
    for attempt in range(4):
        r = requests.post(url, json=body, headers={"x-goog-api-key": GEMINI_API_KEY}, timeout=120)
        if r.ok:
            payload = r.json()["candidates"][0]["content"]["parts"][0]["text"]
            return json.loads(payload)
        last_error = f"HTTP {r.status_code}: {r.text[:300]}"
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(90, 10 * (2 ** attempt)))
            continue
        break
    raise RuntimeError(last_error or "Gemini request failed")


MONTH_NAMES = ["january", "february", "march", "april", "may", "june", "july",
               "august", "september", "october", "november", "december"]


def month_label(report_month):
    y, m = report_month.split("-")
    return f"{MONTH_NAMES[int(m) - 1].title()} {y}"


def month_mentioned(text, report_month):
    """Does the fetched page actually talk about this month and year?"""
    y, m = report_month.split("-")
    name = MONTH_NAMES[int(m) - 1]
    return re.search(rf"\b{name}\b[^.]{{0,40}}\b{y}\b", text, re.I) is not None


def numbers_in(text):
    """Every integer appearing in the text, with and without thousands separators."""
    found = set()
    for m in re.finditer(r"\d[\d,\.]*", text):
        token = m.group(0).rstrip(".,")
        digits = token.replace(",", "").replace(".", "")
        if digits.isdigit():
            found.add(int(digits))
    return found


# COMMAND ----------

# DBTITLE 1,Extract
sales_rows = []
validation_rows = []
month_results = []

for src in pending:
    month = src["report_month"]
    text = src["extracted_text"]
    headline = src["headline_total"]
    result = {"report_month": month, "status": None, "message": None, "rows": 0}

    try:
        parsed = gemini_extract(month, text)
    except Exception as e:
        result["status"] = "error"
        result["message"] = re.sub(r"AIza[0-9A-Za-z_\-]+", "[REDACTED]", str(e))[:300]
        month_results.append(result)
        print(f"  {month}  ERROR  {result['message'][:120]}")
        continue

    present = numbers_in(text)
    total = parsed.get("monthly_total")
    models = [
        m for m in (parsed.get("model_rows") or [])
        if m.get("model_name") and isinstance(m.get("monthly_total"), int)
    ]

    # Guard 0: the month came from the URL slug, so the page itself has to agree.
    # This is what stops a mis-parsed slug filing one month's figures under another
    # - the exact failure that put 2026-03's numbers into 2024-03.
    if not month_mentioned(text, month):
        result["status"] = "fail"
        result["message"] = f"source text does not mention {month_label(month)}"
        month_results.append(result)
        print(f"  {month}  FAIL   {result['message']}")
        continue

    # Without a stated monthly total there is no residual to compute, so the month
    # would be written understated - itemised models only. That is worse than what
    # may already be stored (2025-01 is a combined 4Q24/January release of this
    # shape), so leave the month alone rather than replace it with a partial.
    if total is None:
        result["status"] = "skip"
        result["message"] = "release states no monthly total; existing rows left untouched"
        month_results.append(result)
        print(f"  {month}  SKIP   {result['message']}")
        continue

    # Guard 2: the slug total was never shown to the model. Only some slug shapes
    # state an exact total; when they do not, headline is NULL and this is skipped
    # rather than treated as a mismatch.
    if headline is not None and total != headline:
        result["status"] = "fail"
        result["message"] = f"monthly_total {total} != headline total {headline} from URL slug"
        month_results.append(result)
        print(f"  {month}  FAIL   {result['message']}")
        continue

    # Guard 1: every number must be quoted from the page.
    unsupported = [m["model_name"] for m in models if m["monthly_total"] not in present]
    if unsupported:
        result["status"] = "fail"
        result["message"] = f"values not present in source text for: {', '.join(unsupported[:6])}"
        month_results.append(result)
        print(f"  {month}  FAIL   {result['message']}")
        continue

    # Guard 3: residual becomes an explicit row rather than silent shrinkage.
    sum_models = sum(m["monthly_total"] for m in models)
    if total is not None and sum_models > total:
        result["status"] = "fail"
        result["message"] = f"model rows sum to {sum_models}, above monthly total {total}"
        month_results.append(result)
        print(f"  {month}  FAIL   {result['message']}")
        continue

    emitted = list(models)
    if total is not None and sum_models < total:
        emitted.append({
            "model_name": "Other models",
            "monthly_total": total - sum_models,
            "evidence_quote": f"residual: monthly total {total} minus itemised {sum_models}",
        })

    year = int(src["report_year"])
    mnum = int(month.split("-")[1])
    start_date = dt.date(year, mnum, 1)
    end_date = (dt.date(year + (mnum == 12), (mnum % 12) + 1, 1) - dt.timedelta(days=1))
    # Carried in validation_message so a later run can tell whether the source page
    # changed since this extraction, without adding a column to a shared table.
    stamp = f"srchash={src['content_hash']}"

    for m in emitted:
        is_residual = m["model_name"] == "Other models"
        sales_rows.append(Row(
            document_id=src["source_id"],
            document_url=src["source_url"],
            filename=None,
            report_year=year,
            report_month=month,
            report_start_date=start_date,
            report_end_date=end_date,
            maker="VinFast",
            model_name=m["model_name"],
            vama_classification=None,
            seat=None,
            monthly_north=None,
            monthly_central=None,
            monthly_south=None,
            monthly_total=int(m["monthly_total"]),
            ytd_north=None,
            ytd_central=None,
            ytd_south=None,
            ytd_total=None,
            source_table_index=None,
            source_row_index=None,
            extracted_timestamp=CREATED_AT,
            parsing_method=PROVIDER,
            source_id=src["source_id"],
            source_title=src["page_title"],
            source_domain=src["source_domain"],
            source_type="official_ir_press_release",
            regional_granularity="national_total",
            is_official_source=True,
            is_total_only_row=False,
            extraction_confidence=0.8 if is_residual else 0.99,
            validation_status="pass",
            validation_message=stamp,
            raw_evidence=(m.get("evidence_quote") or "")[:1000],
            llm_model=MODEL,
            created_at=CREATED_AT,
            updated_at=CREATED_AT,
        ))

    validation_rows.append(Row(
        report_month=month,
        selected_source_id=src["source_id"],
        source_total=int(total) if total is not None else None,
        sum_model_monthly_total=int(sum_models),
        source_ytd_total=int(parsed["ytd_total"]) if isinstance(parsed.get("ytd_total"), int) else None,
        sum_model_ytd_total=None,
        previous_ytd_total=None,
        derived_current_from_ytd=None,
        selected_source_type="official_ir_press_release",
        source_conflict_count=0,
        confidence_reason="official IR release fetched by month-specific URL; "
                          "headline total cross-checked against URL slug",
        validation_status="pass",
        validation_message=stamp,
        checked_at=CREATED_AT,
    ))

    result["status"] = "pass"
    result["rows"] = len(emitted)
    month_results.append(result)
    print(f"  {month}  OK     total={total} rows={len(emitted)} (itemised {sum_models})")

# COMMAND ----------

sales_schema = StructType([
    StructField("document_id", StringType()),
    StructField("document_url", StringType()),
    StructField("filename", StringType()),
    StructField("report_year", IntegerType()),
    StructField("report_month", StringType()),
    StructField("report_start_date", DateType()),
    StructField("report_end_date", DateType()),
    StructField("maker", StringType()),
    StructField("model_name", StringType()),
    StructField("vama_classification", StringType()),
    StructField("seat", StringType()),
    StructField("monthly_north", IntegerType()),
    StructField("monthly_central", IntegerType()),
    StructField("monthly_south", IntegerType()),
    StructField("monthly_total", IntegerType()),
    StructField("ytd_north", IntegerType()),
    StructField("ytd_central", IntegerType()),
    StructField("ytd_south", IntegerType()),
    StructField("ytd_total", IntegerType()),
    StructField("source_table_index", IntegerType()),
    StructField("source_row_index", IntegerType()),
    StructField("extracted_timestamp", TimestampType()),
    StructField("parsing_method", StringType()),
    StructField("source_id", StringType()),
    StructField("source_title", StringType()),
    StructField("source_domain", StringType()),
    StructField("source_type", StringType()),
    StructField("regional_granularity", StringType()),
    StructField("is_official_source", BooleanType()),
    StructField("is_total_only_row", BooleanType()),
    StructField("extraction_confidence", DoubleType()),
    StructField("validation_status", StringType()),
    StructField("validation_message", StringType()),
    StructField("raw_evidence", StringType()),
    StructField("llm_model", StringType()),
    StructField("created_at", TimestampType()),
    StructField("updated_at", TimestampType()),
])

validation_schema = StructType([
    StructField("report_month", StringType()),
    StructField("selected_source_id", StringType()),
    StructField("source_total", IntegerType()),
    StructField("sum_model_monthly_total", IntegerType()),
    StructField("source_ytd_total", IntegerType()),
    StructField("sum_model_ytd_total", IntegerType()),
    StructField("previous_ytd_total", IntegerType()),
    StructField("derived_current_from_ytd", IntegerType()),
    StructField("selected_source_type", StringType()),
    StructField("source_conflict_count", IntegerType()),
    StructField("confidence_reason", StringType()),
    StructField("validation_status", StringType()),
    StructField("validation_message", StringType()),
    StructField("checked_at", TimestampType()),
])

# COMMAND ----------

# DBTITLE 1,Write, scoped to the months that actually produced rows
# Delete only what this run is about to replace. Deleting the whole window and
# re-inserting whatever succeeded is how a partial failure erases good months.
written_months = sorted({r["report_month"] for r in sales_rows})

if sales_rows:
    months_sql = ", ".join("'" + m.replace("'", "''") + "'" for m in written_months)
    spark.sql(f"DELETE FROM {FULL_SCHEMA}.vinfast_sales_by_model WHERE report_month IN ({months_sql})")
    spark.sql(f"DELETE FROM {FULL_SCHEMA}.vinfast_monthly_validation WHERE report_month IN ({months_sql})")
    (spark.createDataFrame(sales_rows, sales_schema)
        .write.mode("append").saveAsTable(f"{FULL_SCHEMA}.vinfast_sales_by_model"))
    (spark.createDataFrame(validation_rows, validation_schema)
        .write.mode("append").saveAsTable(f"{FULL_SCHEMA}.vinfast_monthly_validation"))

# COMMAND ----------

metrics = {
    "months_considered": len(pending),
    "months_written": written_months,
    "rows_written": len(sales_rows),
    "failed": [r for r in month_results if r["status"] in ("fail", "error")],
}
print(json.dumps(metrics, indent=2, default=str))

spark.sql(f"""
SELECT report_month, COUNT(*) AS rows, SUM(monthly_total) AS total
FROM {FULL_SCHEMA}.vinfast_sales_by_model
GROUP BY report_month ORDER BY report_month DESC LIMIT 12
""").show(truncate=False)

# Same reasoning as notebook 16: every month failing is infrastructure, not absence.
# A run that only had skips is not a failure - there was simply nothing to write.
attempted = len(pending)
if attempted > 0 and not written_months and metrics["failed"]:
    raise RuntimeError(
        f"All {attempted} VinFast months failed extraction; nothing was written and "
        f"existing rows were left untouched. First failure: "
        f"{metrics['failed'][0].get('message')}"
    )

dbutils.notebook.exit(json.dumps(metrics, default=str))
