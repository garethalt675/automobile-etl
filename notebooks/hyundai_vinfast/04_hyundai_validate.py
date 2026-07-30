# Databricks notebook source
# MAGIC %md
# MAGIC # Hyundai monthly validation
# MAGIC One validation row per selected Hyundai report month.

# COMMAND ----------

import datetime as dt
import html
import re
from pyspark.sql import Row

FULL_SCHEMA = "market_data.hyundai_vinfast"
NUM_RE = re.compile(r"(?<![\w/])\d{1,3}(?:[\.,]\d{1,3})*(?![\w/])|(?<![\w/])\d+(?![\w/])")
AMT = r"([0-9]{1,3}(?:[\.,][0-9]{3})+|[0-9]+)"


def parse_int_token(token):
    cleaned = re.sub(r"[^0-9]", "", token or "")
    return int(cleaned) if cleaned else None


def compact_text(raw_html, fallback_text):
    text = raw_html or fallback_text or ""
    text = re.sub(r"<script[\s\S]*?</script>|<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def source_totals(text):
    source_total = None
    source_ytd = None
    prose_total = None
    for pat in [
        rf"tổng\s+số\s+xe\s+Hyundai[^.]{{0,120}}?đạt\s+{AMT}\s+xe",
        rf"tổng\s+doanh\s+số\s+xe\s+Hyundai[^.]{{0,120}}?đạt\s+{AMT}\s+xe",
        rf"tổng\s+doanh\s+số\s+xe\s+Hyundai[^.]{{0,120}}?{AMT}\s+xe",
    ]:
        m = re.search(pat, text, flags=re.I)
        if m:
            prose_total = parse_int_token(m.group(1))
            break
    has_header = False
    table_start = text.find("Mẫu xe CBU/CKD")
    has_header = table_start >= 0
    if table_start < 0:
        m_header = re.search(r"Mẫu\s+xe\s+CBU/CKD", text, flags=re.I)
        table_start = m_header.start() if m_header else -1
        has_header = bool(m_header)
    if table_start < 0:
        total_pos = text.find("TỔNG")
        model_positions = [m.start() for m in re.finditer(r"Hyundai\s+(?:Grand\s+i10|Accent|Elantra|Venue|Tucson|Santa\s*Fe|Creta|Palisade|Custin|Stargazer|Kona|Avante|Solati)", text, flags=re.I)]
        candidates = [p for p in model_positions if total_pos < 0 or p < total_pos]
        table_start = min(candidates) if candidates and total_pos > min(candidates) else -1
    segment = text[table_start:] if table_start >= 0 else text
    mt = re.search(r"TỔNG\s+((?:\d[\d\.,]*\s+){1,4})", segment, flags=re.I)
    if mt:
        nums = [parse_int_token(x.group(0)) for x in NUM_RE.finditer(mt.group(1))]
        nums = [n for n in nums if n is not None]
        if len(nums) >= 2:
            table_total = nums[-1] if len(nums) == 2 else nums[-2]
            table_ytd = nums[-1]
            if has_header or prose_total is None or table_total == prose_total:
                source_total = table_total
                source_ytd = table_ytd
    if source_total is None:
        source_total = prose_total
    if source_ytd is None:
        for pat in [
            rf"lũy\s+kế[^.]{{0,120}}?đạt\s+{AMT}\s+xe",
            rf"\d{{1,2}}\s+tháng\s+đầu\s+năm[^.]{{0,120}}?đạt\s+{AMT}\s+xe",
            rf"trong\s+năm\s+20\d{{2}}[^.]{{0,120}}?đạt\s+{AMT}\s+xe",
            rf"\d{{1,2}}\s+th\S+ng\s+.{{0,40}}?n\S*m\s+20\d{{2}}.{{0,100}}?{AMT}\s+xe",
        ]:
            m = re.search(pat, text, flags=re.I)
            if m:
                source_ytd = parse_int_token(m.group(1))
                break
    return source_total, source_ytd


def found_month(text):
    candidates = []
    for m in re.finditer(r"tháng\s*(\d{1,2})\s*/\s*(20\d{2})", text, flags=re.I):
        month, year = int(m.group(1)), int(m.group(2))
        if 1 <= month <= 12 and 2020 <= year <= 2027:
            candidates.append((m.start(), year, month))
    for m in re.finditer(r"tháng\s*(\d{1,2})\s*(?:và|&|,)\s*n[aă]m\s*(20\d{2})", text, flags=re.I):
        month, year = int(m.group(1)), int(m.group(2))
        if 1 <= month <= 12 and 2020 <= year <= 2027:
            candidates.append((m.start(), year, month))
    if not candidates:
        return None
    _, year, month = sorted(candidates, key=lambda x: x[0])[0]
    return f"{year:04d}-{month:02d}"

# COMMAND ----------

selected = spark.sql(f"""
SELECT c.source_id, c.report_month, r.fetch_status, r.http_status, r.raw_html, r.extracted_text
FROM {FULL_SCHEMA}.hyundai_source_candidates c
LEFT JOIN {FULL_SCHEMA}.hyundai_raw_sources r ON c.source_id = r.source_id
WHERE c.is_selected = true
ORDER BY c.report_month
""").collect()

sales = spark.sql(f"""
SELECT report_month, source_id,
       sum(CASE WHEN model_name <> 'TOTAL_UNSPECIFIED' THEN coalesce(monthly_total, 0) ELSE 0 END) AS sum_monthly,
       sum(CASE WHEN model_name <> 'TOTAL_UNSPECIFIED' THEN coalesce(ytd_total, 0) ELSE 0 END) AS sum_ytd,
       count(*) AS row_count,
       sum(CASE WHEN monthly_total IS NULL THEN 1 ELSE 0 END) AS null_monthly_rows,
       sum(CASE WHEN validation_status = 'warning' THEN 1 ELSE 0 END) AS row_warnings
FROM {FULL_SCHEMA}.hyundai_sales_by_model
GROUP BY report_month, source_id
""").collect()
sales_by_key = {(r.report_month, r.source_id): r for r in sales}

prev_ytd_by_month = {}
rows = []
for c in selected:
    text = compact_text(c.raw_html, c.extracted_text)
    source_total, source_ytd = source_totals(text)
    s = sales_by_key.get((c.report_month, c.source_id))
    sum_monthly = int(s.sum_monthly) if s and s.sum_monthly is not None else None
    sum_ytd = int(s.sum_ytd) if s and s.sum_ytd is not None else None
    row_count = int(s.row_count) if s and s.row_count is not None else 0
    null_monthly_rows = int(s.null_monthly_rows) if s and s.null_monthly_rows is not None else 0
    row_warnings = int(s.row_warnings) if s and s.row_warnings is not None else 0
    prev_ytd = prev_ytd_by_month.get(c.report_month[:4])
    derived = (source_ytd - prev_ytd) if source_ytd is not None and prev_ytd is not None else None
    fm = found_month(text) if text else None

    messages = []
    status = "pass"
    if c.fetch_status != "ok" or not (c.http_status and 200 <= int(c.http_status) <= 299):
        status = "fail"
        messages.append(f"fetch not ok: {c.fetch_status}/{c.http_status}")
    if fm and fm != c.report_month:
        status = "fail"
        messages.append(f"source month mismatch: text={fm}, candidate={c.report_month}")
    if row_count == 0:
        status = "fail"
        messages.append("no extracted model rows")
    if null_monthly_rows:
        status = "fail"
        messages.append(f"{null_monthly_rows} rows missing monthly_total")
    if source_total is not None and sum_monthly is not None and abs(source_total - sum_monthly) > 0:
        status = "fail"
        messages.append(f"monthly sum {sum_monthly} differs from source total {source_total}")
    if source_total is None:
        if status == "pass":
            status = "warning"
        messages.append("source monthly total unavailable")
    if source_ytd is None:
        if status == "pass":
            status = "warning"
        messages.append("source YTD total unavailable")
    elif sum_ytd is not None and abs(source_ytd - sum_ytd) > 0:
        status = "fail"
        messages.append(f"YTD sum {sum_ytd} differs from source YTD {source_ytd}")
    if row_warnings and status == "pass":
        status = "warning"
        messages.append(f"{row_warnings} extracted rows carried warnings")
    if not messages:
        messages.append("model rows match source total and YTD total")

    rows.append(Row(
        report_month=c.report_month,
        selected_source_id=c.source_id,
        source_total=source_total,
        sum_model_monthly_total=sum_monthly,
        source_ytd_total=source_ytd,
        sum_model_ytd_total=sum_ytd,
        previous_ytd_total=prev_ytd,
        derived_current_from_ytd=derived,
        validation_status=status,
        validation_message="; ".join(messages)[:2000],
        checked_at=dt.datetime.utcnow(),
    ))
    if source_ytd is not None:
        prev_ytd_by_month[c.report_month[:4]] = source_ytd

if rows:
    target_schema = spark.table(f"{FULL_SCHEMA}.hyundai_monthly_validation").schema
    df = spark.createDataFrame(rows, schema=target_schema)
    df.createOrReplaceTempView("new_hyundai_monthly_validation")
    spark.sql(f"""
      MERGE INTO {FULL_SCHEMA}.hyundai_monthly_validation t
      USING new_hyundai_monthly_validation s
      ON t.report_month = s.report_month AND t.selected_source_id = s.selected_source_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Validated selected months: {len(rows)}")
display(spark.sql(f"""
SELECT validation_status, count(*) AS months
FROM {FULL_SCHEMA}.hyundai_monthly_validation
GROUP BY validation_status
ORDER BY validation_status
"""))
display(spark.sql(f"SELECT * FROM {FULL_SCHEMA}.hyundai_monthly_validation ORDER BY report_month"))
