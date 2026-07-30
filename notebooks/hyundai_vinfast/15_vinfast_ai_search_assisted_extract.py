# Databricks notebook source
# MAGIC %md
# MAGIC # Sprint 010 — Live VinFast AI-assisted Google Search extraction
# MAGIC
# MAGIC Production replacement for the old VinFast workflow.
# MAGIC
# MAGIC Mechanism:
# MAGIC - Runs inside Databricks.
# MAGIC - Reads `GEMINI_API_KEY` from Giang's Databricks env folder without logging it.
# MAGIC - Calls Gemini `gemini-3.1-flash-lite` with Google Search grounding.
# MAGIC - Audits every month/query and every extracted claim.
# MAGIC - Resolves Google/Vertex/search redirects to canonical publisher URLs.
# MAGIC - Rejects wrapper URLs as final evidence.
# MAGIC - Writes explicit model rows plus `Other models` residual where a reliable monthly total exists.
# MAGIC - Leaves unresolved months as validation `fail` rather than inventing data.

# COMMAND ----------

import datetime as dt
import hashlib
import html
import json
import os
import re
import time
from urllib.parse import urlparse

import requests
from pyspark.sql import Row
from pyspark.sql.types import *

FULL_SCHEMA = "market_data.hyundai_vinfast"
MODEL = "gemini-2.5-flash"  # Switched from 3.1-flash-lite due to quota
PROVIDER = "gemini_google_search_grounding_live_databricks"
RUN_ID = "sprint010_live_gemini_google_search_2024_now"
CREATED_AT = dt.datetime.utcnow()
TARGET_START = "2024-01"
# Run through the last complete calendar month.
_today = dt.date.today()
_prev_month = (_today.replace(day=1) - dt.timedelta(days=1))
# TARGET_END = f"{_prev_month.year:04d}-{_prev_month.month:02d}"
TARGET_END = "2024-12"

try:
    dbutils.widgets.text("target_start", TARGET_START)
    dbutils.widgets.text("target_end", TARGET_END)
    dbutils.widgets.text("max_months", "0")
    dbutils.widgets.text("replace_existing", "true")
except Exception:
    pass
try:
    TARGET_START = dbutils.widgets.get("target_start") or TARGET_START
    TARGET_END = dbutils.widgets.get("target_end") or TARGET_END
    MAX_MONTHS = int(dbutils.widgets.get("max_months") or "0")
    REPLACE_EXISTING = (dbutils.widgets.get("replace_existing") or "true").lower() == "true"
except Exception:
    MAX_MONTHS = 0
    REPLACE_EXISTING = True

# COMMAND ----------

def load_env_value(key):
    candidates = [
        "/Workspace/Users/tuckeyhue@gmail.com/env/.env."
    ]
    for path in candidates:
        try:
            if os.path.exists(path):
                with open(path, encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        k, v = line.split("=", 1)
                        if k.strip() == key:
                            return v.strip().strip('"').strip("'")
        except Exception:
            pass
    # Optional Databricks secret/env fallback; do not log value.
    if os.environ.get(key):
        return os.environ[key]
    raise RuntimeError(f"Missing {key}; checked Databricks env folder candidates and process env")

GEMINI_API_KEY = load_env_value("GEMINI_API_KEY")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.vinfast_ai_search_queries (
  query_id STRING,
  report_month STRING,
  query_text STRING,
  provider STRING,
  model STRING,
  requested_at TIMESTAMP,
  response_text STRING,
  response_json STRING,
  status STRING,
  error_message STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.vinfast_ai_search_claims (
  claim_id STRING,
  query_id STRING,
  report_month STRING,
  maker STRING,
  model_name STRING,
  monthly_total INT,
  ytd_total INT,
  claim_type STRING,
  source_url STRING,
  source_domain STRING,
  source_title STRING,
  source_snippet STRING,
  is_official_source BOOLEAN,
  evidence_quote STRING,
  ai_confidence DOUBLE,
  validation_status STRING,
  validation_message STRING,
  created_at TIMESTAMP
) USING DELTA
""");



# COMMAND ----------

# MAGIC %sql
# MAGIC delete from market_data.hyundai_vinfast.vinfast_ai_search_queries;
# MAGIC delete from market_data.hyundai_vinfast.vinfast_ai_search_claims;

# COMMAND ----------

month_names = {
    1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
    7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December",
}

def months_between(start, end):
    y, m = [int(x) for x in start.split("-")]
    ey, em = [int(x) for x in end.split("-")]
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y += 1
            m = 1
    return out

def target_queries(month):
    yyyy, mm = month.split("-")
    m = str(int(mm))
    return [
        f"doanh số chi tiết bán xe vinfast tháng {m} năm {yyyy}",
        f"VinFast doanh số tháng {m} {yyyy} VF 3 VF 5 VF 6 VF 7 VF 8 VF 9",
        f"VinFast bàn giao xe tháng {m} {yyyy}",
        f"VinFast sales Vietnam {month_names[int(mm)]} {yyyy}",
    ]

def sha(parts):
    return hashlib.sha256("|".join(str(x) for x in parts).encode("utf-8")).hexdigest()

def month_dates(report_month):
    y, m = [int(x) for x in report_month.split("-")]
    start = dt.date(y, m, 1)
    end = dt.date(y, 12, 31) if m == 12 else dt.date(y, m + 1, 1) - dt.timedelta(days=1)
    return y, start, end

def host(url):
    try:
        return urlparse(url or "").netloc.lower()
    except Exception:
        return ""

def is_wrapper_url(url):
    h = host(url)
    return any(x in h for x in ["google.com", "news.google.com", "vertexaisearch.cloud.google.com", "bing.com"])

def is_official(url):
    h = host(url)
    return h.endswith("vinfast.vn") or h.endswith("vinfastauto.us") or h.endswith("vingroup.net")

def normalize_int(v):
    if v is None:
        return None
    if isinstance(v, int):
        return int(v)
    if isinstance(v, float):
        return int(v)
    s = str(v)
    s = re.sub(r"[^0-9]", "", s)
    return int(s) if s else None

def strip_code_fence(text):
    t = (text or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.I)
        t = re.sub(r"\s*```$", "", t)
    return t.strip()

def extract_json(text):
    t = strip_code_fence(text)
    try:
        return json.loads(t)
    except Exception:
        m = re.search(r"\{.*\}", t, flags=re.S)
        if m:
            return json.loads(m.group(0))
        raise

def resolve_url(url):
    if not url:
        return None
    try:
        r = requests.get(url, allow_redirects=True, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        final = r.url
        if final and not is_wrapper_url(final):
            return final
    except Exception:
        pass
    return None if is_wrapper_url(url) else url

def fetch_text(url):
    try:
        r = requests.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
        if not r.ok:
            return ""
        txt = re.sub(r"<script[\s\S]*?</script>", " ", r.text, flags=re.I)
        txt = re.sub(r"<style[\s\S]*?</style>", " ", txt, flags=re.I)
        txt = re.sub(r"<[^>]+>", " ", txt)
        txt = html.unescape(txt)
        return re.sub(r"\s+", " ", txt).strip()[:20000]
    except Exception:
        return ""

# COMMAND ----------

def gemini_search_month(report_month):
    queries = target_queries(report_month)
    prompt = f"""
Bạn là hệ thống trích xuất dữ liệu doanh số VinFast cho warehouse.

Target month: {report_month}.
Search queries to use/consider:
{json.dumps(queries, ensure_ascii=False, indent=2)}

Task:
- Find VinFast Vietnam domestic auto/electric vehicle deliveries/sales for this month.
- Prefer official VinFast/VinFastAuto/Vingroup sources. Direct reputable press is acceptable only if official is unavailable.
- Return JSON only, no markdown.
- Do NOT invent model rows.
- If source states only a total, return total and empty model_rows.
- If source states some models and a monthly total, return stated models only; residual will be computed downstream as Other models.
- If wording says "hơn"/"gần"/"over"/"nearly", set total_is_approx or row_is_approx true.

Required JSON schema:
{{
  "report_month": "YYYY-MM",
  "monthly_total": integer_or_null,
  "monthly_total_is_approx": boolean,
  "ytd_total": integer_or_null,
  "source_title": string_or_null,
  "source_url": string_or_null,
  "source_domain": string_or_null,
  "evidence_quote": string_or_null,
  "model_rows": [
    {{"model_name": string, "monthly_total": integer, "row_is_approx": boolean, "evidence_quote": string, "source_url": string_or_null}}
  ],
  "confidence": number_between_0_and_1,
  "notes": string
}}
""".strip()
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent?key={GEMINI_API_KEY}"
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"googleSearch": {}}],  # Fixed: camelCase for REST API
        "generationConfig": {"temperature": 0.05},
    }
    last_error = None
    for attempt in range(4):
        r = requests.post(url, json=body, timeout=90)
        if r.ok:
            break
        last_error = f"Gemini status {r.status_code}: {r.text[:500]}"
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(120, 15 * (2 ** attempt)))
            continue
        raise RuntimeError(last_error)
    else:
        raise RuntimeError(last_error or "Gemini request failed")
    resp = r.json()
    parts = resp.get("candidates", [{}])[0].get("content", {}).get("parts", [])
    text = "".join(p.get("text", "") for p in parts)
    grounding = resp.get("candidates", [{}])[0].get("groundingMetadata", {})
    chunks = []
    for ch in grounding.get("groundingChunks", []) or []:
        web = ch.get("web") or {}
        if web.get("uri"):
            chunks.append({"title": web.get("title"), "uri": web.get("uri")})
    data = extract_json(text)
    return prompt, text, data, chunks, resp

# COMMAND ----------

target_months = months_between(TARGET_START, TARGET_END)
if MAX_MONTHS and MAX_MONTHS > 0:
    target_months = target_months[:MAX_MONTHS]
months_sql = ",".join([f"'{m}'" for m in target_months])

query_rows = []
claim_rows = []
sales_rows = []
validation_rows = []

for idx, report_month in enumerate(target_months, 1):
    queries = target_queries(report_month)
    qid0 = sha([RUN_ID, report_month, "query", 1])
    status = "ok"
    error_message = None
    response_text = ""
    response_json = {}
    final = None
    resolved_url = None
    page_text = ""
    try:
        prompt, response_text, final, chunks, raw_resp = gemini_search_month(report_month)
        candidate_urls = []
        if final.get("source_url"):
            candidate_urls.append(final.get("source_url"))
        for row in final.get("model_rows") or []:
            if row.get("source_url"):
                candidate_urls.append(row.get("source_url"))
        candidate_urls.extend([c.get("uri") for c in chunks if c.get("uri")])
        for u in candidate_urls:
            resolved = resolve_url(u)
            if resolved and not is_wrapper_url(resolved):
                resolved_url = resolved
                break
        if resolved_url:
            page_text = fetch_text(resolved_url)
        response_json = {
            "parsed": final,
            "grounding_chunks": chunks,
            "resolved_url": resolved_url,
            "run_id": RUN_ID,
            "provider": PROVIDER,
            "model": MODEL,
        }
    except Exception as e:
        status = "error"
        error_message = str(e)[:1000]
        response_json = {"run_id": RUN_ID, "error": error_message}

    for rank, q in enumerate(queries, 1):
        query_rows.append(Row(
            query_id=sha([RUN_ID, report_month, "query", rank]), report_month=report_month,
            query_text=q, provider=PROVIDER, model=MODEL, requested_at=CREATED_AT,
            response_text=response_text if rank == 1 else "see rank_1_query_response_for_month",
            response_json=json.dumps(response_json, ensure_ascii=False), status=status, error_message=error_message,
        ))

    year, start_date, end_date = month_dates(report_month)
    selected_source_id = f"UNRESOLVED_{report_month}"
    source_total = None
    ytd_total = None
    sum_model = None
    validation_status = "fail"
    validation_message = "No reliable Gemini-grounded canonical direct publisher evidence resolved; no final sales rows written."
    confidence_reason = error_message or "unresolved"
    source_type = "unresolved"

    if status == "ok" and final and resolved_url and not is_wrapper_url(resolved_url):
        source_total = normalize_int(final.get("monthly_total"))
        ytd_total = normalize_int(final.get("ytd_total"))
        confidence = float(final.get("confidence") or 0.0)
        models = []
        seen = set()
        for row in final.get("model_rows") or []:
            name = (row.get("model_name") or "").strip()
            val = normalize_int(row.get("monthly_total"))
            if not name or name in seen or not val or val <= 0:
                continue
            if name.upper() in {"TOTAL", "TOTAL_UNSPECIFIED", "ALL", "VINFAST"}:
                continue
            seen.add(name)
            models.append({
                "model_name": name,
                "monthly_total": val,
                "quote": row.get("evidence_quote") or final.get("evidence_quote"),
                "approx": bool(row.get("row_is_approx")),
            })
        sum_model = sum(x["monthly_total"] for x in models) if models else None
        source_id = "vf_live_" + sha([RUN_ID, report_month, resolved_url])[:16]
        selected_source_id = source_id
        source_domain = host(resolved_url)
        title = final.get("source_title") or f"Gemini-grounded canonical source for {report_month}"
        source_type = "official" if is_official(resolved_url) else "press"
        total_approx = bool(final.get("monthly_total_is_approx"))

        # Audit total claim.
        if source_total:
            claim_rows.append(Row(
                claim_id=sha([RUN_ID, report_month, "TOTAL", resolved_url]), query_id=qid0, report_month=report_month,
                maker="VinFast", model_name="TOTAL_UNSPECIFIED", monthly_total=int(source_total), ytd_total=(int(ytd_total) if ytd_total else None),
                claim_type="monthly_total", source_url=resolved_url, source_domain=source_domain, source_title=title,
                source_snippet=final.get("evidence_quote"), is_official_source=is_official(resolved_url), evidence_quote=final.get("evidence_quote"),
                ai_confidence=confidence, validation_status=("warning" if total_approx else "pass"),
                validation_message="Gemini-grounded canonical source states monthly total; approximate wording retained as warning." if total_approx else "Gemini-grounded canonical source states exact monthly total.",
                created_at=CREATED_AT,
            ))
        for m in models:
            claim_rows.append(Row(
                claim_id=sha([RUN_ID, report_month, m["model_name"], resolved_url]), query_id=qid0, report_month=report_month,
                maker="VinFast", model_name=m["model_name"], monthly_total=int(m["monthly_total"]), ytd_total=None,
                claim_type="model_level_approx" if m["approx"] else "model_level", source_url=resolved_url, source_domain=source_domain,
                source_title=title, source_snippet=m["quote"], is_official_source=is_official(resolved_url), evidence_quote=m["quote"],
                ai_confidence=confidence, validation_status=("warning" if m["approx"] or total_approx else "pass"),
                validation_message="Gemini-grounded model claim from canonical source.", created_at=CREATED_AT,
            ))

        def add_sales_row(model_name, monthly_total, raw_evidence, is_total_only, row_conf, row_status, row_msg):
            sales_rows.append(Row(
                document_id=source_id,
                document_url=resolved_url,
                filename=None,
                report_year=int(year),
                report_month=report_month,
                report_start_date=start_date,
                report_end_date=end_date,
                maker="VinFast",
                model_name=model_name,
                vama_classification=None,
                seat=None,
                monthly_north=None,
                monthly_central=None,
                monthly_south=None,
                monthly_total=int(monthly_total),
                ytd_north=None,
                ytd_central=None,
                ytd_south=None,
                ytd_total=None,
                source_table_index=None,
                source_row_index=None,
                extracted_timestamp=CREATED_AT,
                parsing_method=PROVIDER,
                source_id=source_id,
                source_title=title,
                source_domain=source_domain,
                source_type=source_type,
                regional_granularity="national_total",
                is_official_source=is_official(resolved_url),
                is_total_only_row=is_total_only,
                extraction_confidence=float(row_conf),
                validation_status=row_status,
                validation_message=row_msg,
                raw_evidence=raw_evidence,
                llm_model=MODEL,
                created_at=CREATED_AT,
                updated_at=CREATED_AT,
            ))

        if source_total and models:
            explicit_sum = sum(x["monthly_total"] for x in models)
            if explicit_sum <= source_total:
                row_status = "warning" if total_approx or any(x["approx"] for x in models) else "pass"
                row_msg = "Live Gemini-grounded canonical source; explicit model rows reconcile with monthly total using Other models residual."
                for m in models:
                    add_sales_row(m["model_name"], m["monthly_total"], m["quote"], False, confidence, row_status, row_msg)
                residual = source_total - explicit_sum
                if residual > 0:
                    residual_quote = f"Other models is a computed residual from monthly total {source_total} minus explicitly stated model deliveries {explicit_sum}; residual = {residual}."
                    add_sales_row("Other models", residual, residual_quote, False, min(confidence, 0.85), "warning", "Residual allocation computed from canonical total minus explicit model rows; model names not invented.")
                validation_status = row_status
                validation_message = row_msg
                confidence_reason = f"source_total={source_total}; explicit_sum={explicit_sum}; residual={source_total-explicit_sum}; url={resolved_url}"
            else:
                validation_status = "fail"
                validation_message = f"Rejected month: explicit model sum {explicit_sum} exceeds source total {source_total}."
                confidence_reason = validation_message
        elif source_total:
            add_sales_row("TOTAL_UNSPECIFIED", source_total, final.get("evidence_quote"), True, confidence, "warning", "Live Gemini-grounded canonical source gives monthly total only; no explicit model rows accepted.")
            validation_status = "warning"
            validation_message = "Monthly total only; no explicit model rows accepted."
            confidence_reason = f"source_total={source_total}; total_only; url={resolved_url}"
        else:
            validation_status = "fail"
            validation_message = "Resolved canonical source but Gemini did not return reliable monthly total."
            confidence_reason = validation_message

    if not claim_rows or claim_rows[-1].report_month != report_month:
        claim_rows.append(Row(
            claim_id=sha([RUN_ID, report_month, "UNRESOLVED"]), query_id=qid0, report_month=report_month,
            maker="VinFast", model_name="UNRESOLVED", monthly_total=None, ytd_total=None,
            claim_type="no_reliable_canonical_source", source_url=resolved_url, source_domain=host(resolved_url), source_title=None,
            source_snippet=None, is_official_source=False, evidence_quote=None, ai_confidence=0.0,
            validation_status="fail", validation_message=validation_message, created_at=CREATED_AT,
        ))

    validation_rows.append(Row(
        report_month=report_month,
        selected_source_id=selected_source_id,
        source_total=(int(source_total) if source_total else None),
        sum_model_monthly_total=(int(sum_model) if sum_model is not None else None),
        source_ytd_total=(int(ytd_total) if ytd_total else None),
        sum_model_ytd_total=None,
        previous_ytd_total=None,
        derived_current_from_ytd=None,
        selected_source_type=source_type,
        source_conflict_count=0,
        confidence_reason=confidence_reason,
        validation_status=validation_status,
        validation_message=validation_message,
        checked_at=CREATED_AT,
    ))
    time.sleep(1.2)

# COMMAND ----------

# DBTITLE 1,Cell 9
# Replace target-period data produced by previous VinFast workflows.
# Guardrail: if Gemini/search is unavailable (for example 429 quota exhaustion), keep existing
# production sales/validation rows and append only audit rows. This prevents a failed AI run
# from emptying the curated VinFast layer.
spark.sql(f"DELETE FROM {FULL_SCHEMA}.vinfast_ai_search_queries WHERE report_month IN ({months_sql})")
spark.sql(f"DELETE FROM {FULL_SCHEMA}.vinfast_ai_search_claims WHERE report_month IN ({months_sql})")
should_replace_final_tables = REPLACE_EXISTING and len(sales_rows) > 0
if should_replace_final_tables:
    spark.sql(f"DELETE FROM {FULL_SCHEMA}.vinfast_sales_by_model WHERE report_month IN ({months_sql})")
    spark.sql(f"DELETE FROM {FULL_SCHEMA}.vinfast_monthly_validation WHERE report_month IN ({months_sql})")
else:
    print(json.dumps({
        "guardrail": "final_tables_not_replaced",
        "reason": "no accepted live Gemini sales rows" if len(sales_rows) == 0 else "replace_existing=false",
        "audit_rows_will_be_written": True,
        "target_months": target_months,
    }, ensure_ascii=False, default=str))

query_schema = StructType([
    StructField("query_id", StringType()), StructField("report_month", StringType()), StructField("query_text", StringType()),
    StructField("provider", StringType()), StructField("model", StringType()), StructField("requested_at", TimestampType()),
    StructField("response_text", StringType()), StructField("response_json", StringType()), StructField("status", StringType()), StructField("error_message", StringType()),
])
claim_schema = StructType([
    StructField("claim_id", StringType()), StructField("query_id", StringType()), StructField("report_month", StringType()),
    StructField("maker", StringType()), StructField("model_name", StringType()), StructField("monthly_total", IntegerType()),
    StructField("ytd_total", IntegerType()), StructField("claim_type", StringType()), StructField("source_url", StringType()),
    StructField("source_domain", StringType()), StructField("source_title", StringType()), StructField("source_snippet", StringType()),
    StructField("is_official_source", BooleanType()), StructField("evidence_quote", StringType()), StructField("ai_confidence", DoubleType()),
    StructField("validation_status", StringType()), StructField("validation_message", StringType()), StructField("created_at", TimestampType()),
])
sales_schema = StructType([
    StructField("document_id", StringType()), StructField("document_url", StringType()), StructField("filename", StringType()),
    StructField("report_year", IntegerType()), StructField("report_month", StringType()), StructField("report_start_date", DateType()), StructField("report_end_date", DateType()),
    StructField("maker", StringType()), StructField("model_name", StringType()), StructField("vama_classification", StringType()), StructField("seat", StringType()),
    StructField("monthly_north", IntegerType()), StructField("monthly_central", IntegerType()), StructField("monthly_south", IntegerType()), StructField("monthly_total", IntegerType()),
    StructField("ytd_north", IntegerType()), StructField("ytd_central", IntegerType()), StructField("ytd_south", IntegerType()), StructField("ytd_total", IntegerType()),
    StructField("source_table_index", IntegerType()), StructField("source_row_index", IntegerType()), StructField("extracted_timestamp", TimestampType()),
    StructField("parsing_method", StringType()), StructField("source_id", StringType()), StructField("source_title", StringType()), StructField("source_domain", StringType()),
    StructField("source_type", StringType()), StructField("regional_granularity", StringType()), StructField("is_official_source", BooleanType()), StructField("is_total_only_row", BooleanType()),
    StructField("extraction_confidence", DoubleType()), StructField("validation_status", StringType()), StructField("validation_message", StringType()), StructField("raw_evidence", StringType()),
    StructField("llm_model", StringType()), StructField("created_at", TimestampType()), StructField("updated_at", TimestampType()),
])
validation_schema = StructType([
    StructField("report_month", StringType()), StructField("selected_source_id", StringType()), StructField("source_total", IntegerType()), StructField("sum_model_monthly_total", IntegerType()),
    StructField("source_ytd_total", IntegerType()), StructField("sum_model_ytd_total", IntegerType()), StructField("previous_ytd_total", IntegerType()), StructField("derived_current_from_ytd", IntegerType()),
    StructField("selected_source_type", StringType()), StructField("source_conflict_count", IntegerType()), StructField("confidence_reason", StringType()), StructField("validation_status", StringType()),
    StructField("validation_message", StringType()), StructField("checked_at", TimestampType()),
])

spark.createDataFrame(query_rows, query_schema).write.mode("append").saveAsTable(f"{FULL_SCHEMA}.vinfast_ai_search_queries")
spark.createDataFrame(claim_rows, claim_schema).write.mode("append").saveAsTable(f"{FULL_SCHEMA}.vinfast_ai_search_claims")
if sales_rows and should_replace_final_tables:
    spark.createDataFrame(sales_rows, sales_schema).write.mode("append").saveAsTable(f"{FULL_SCHEMA}.vinfast_sales_by_model")
if should_replace_final_tables:
    spark.createDataFrame(validation_rows, validation_schema).write.mode("append").saveAsTable(f"{FULL_SCHEMA}.vinfast_monthly_validation")

# COMMAND ----------

metrics = spark.sql(f"""
WITH target_sales AS (
  SELECT * FROM {FULL_SCHEMA}.vinfast_sales_by_model WHERE report_month IN ({months_sql})
), dupes AS (
  SELECT report_month, model_name, source_id, COUNT(*) c
  FROM {FULL_SCHEMA}.vinfast_sales_by_model
  GROUP BY report_month, model_name, source_id
  HAVING COUNT(*) > 1
), validation_dupes AS (
  SELECT report_month, COUNT(*) c
  FROM {FULL_SCHEMA}.vinfast_monthly_validation
  WHERE report_month IN ({months_sql})
  GROUP BY report_month
  HAVING COUNT(*) > 1
), wrappers AS (
  SELECT COUNT(*) c
  FROM target_sales
  WHERE lower(coalesce(document_url,'')) RLIKE '(vertexaisearch|news.google|google.com|bing.com)'
     OR lower(coalesce(source_domain,'')) RLIKE '(vertexaisearch|news.google|google.com|bing.com)'
), residual_bad AS (
  SELECT report_month
  FROM target_sales
  GROUP BY report_month
  HAVING SUM(monthly_total) <> MAX(CASE WHEN model_name='Other models' THEN NULL ELSE NULL END)
)
SELECT
  '{RUN_ID}' AS run_id,
  '{PROVIDER}' AS provider,
  '{MODEL}' AS model,
  '{TARGET_START}' AS target_start,
  '{TARGET_END}' AS target_end,
  {len(target_months)} AS target_months_attempted,
  (SELECT COUNT(*) FROM {FULL_SCHEMA}.vinfast_ai_search_queries WHERE report_month IN ({months_sql})) AS audit_query_count,
  (SELECT COUNT(*) FROM {FULL_SCHEMA}.vinfast_ai_search_claims WHERE report_month IN ({months_sql})) AS audit_claim_count,
  (SELECT COUNT(*) FROM target_sales) AS final_rows_written,
  (SELECT COUNT(DISTINCT report_month) FROM target_sales) AS final_months_written,
  (SELECT COUNT(*) FROM target_sales WHERE model_name='TOTAL_UNSPECIFIED') AS total_only_rows,
  (SELECT COUNT(*) FROM target_sales WHERE model_name='Other models') AS other_models_rows,
  (SELECT COUNT(*) FROM target_sales WHERE model_name NOT IN ('Other models','TOTAL_UNSPECIFIED')) AS explicit_model_rows,
  (SELECT COUNT(*) FROM {FULL_SCHEMA}.vinfast_monthly_validation WHERE report_month IN ({months_sql}) AND validation_status='pass') AS validation_pass,
  (SELECT COUNT(*) FROM {FULL_SCHEMA}.vinfast_monthly_validation WHERE report_month IN ({months_sql}) AND validation_status='warning') AS validation_warning,
  (SELECT COUNT(*) FROM {FULL_SCHEMA}.vinfast_monthly_validation WHERE report_month IN ({months_sql}) AND validation_status='fail') AS validation_fail,
  (SELECT COUNT(*) FROM dupes) AS duplicate_key_count_all_vinfast,
  (SELECT COUNT(*) FROM validation_dupes) AS duplicate_validation_months,
  (SELECT c FROM wrappers) AS wrapper_final_evidence_count
""").collect()[0].asDict()

print(json.dumps(metrics, ensure_ascii=False, default=str))
dbutils.notebook.exit(json.dumps(metrics, ensure_ascii=False, default=str))