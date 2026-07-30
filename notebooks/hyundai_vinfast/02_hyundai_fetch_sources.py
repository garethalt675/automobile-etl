# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch selected raw Hyundai sources

# COMMAND ----------

import datetime as dt
import hashlib
import html
import re
import urllib.error
import urllib.request
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

FULL_SCHEMA = "market_data.hyundai_vinfast"
FETCH_TIMEOUT_SECONDS = 15
USER_AGENT = "Mozilla/5.0 OpenClaw data QA; Hyundai VinFast sales ETL"

RAW_SCHEMA = StructType([
    StructField("source_id", StringType(), False),
    StructField("report_month", StringType(), True),
    StructField("url", StringType(), True),
    StructField("fetch_status", StringType(), True),
    StructField("http_status", IntegerType(), True),
    StructField("fetched_at", TimestampType(), True),
    StructField("content_type", StringType(), True),
    StructField("raw_html", StringType(), True),
    StructField("extracted_text", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("error_message", StringType(), True),
])


def html_to_text(raw):
    text = re.sub(r"<script[\s\S]*?</script>|<style[\s\S]*?</style>", " ", raw or "", flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def fetch(url):
    now = dt.datetime.utcnow()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT_SECONDS) as r:
            raw_bytes = r.read()
            raw = raw_bytes.decode("utf-8", "ignore")
            text = html_to_text(raw)
            return {
                "fetch_status": "ok" if 200 <= int(r.status) <= 299 else "http_error",
                "http_status": int(r.status),
                "fetched_at": now,
                "content_type": r.headers.get("content-type"),
                "raw_html": raw,
                "extracted_text": text,
                "content_hash": hashlib.sha256(raw_bytes).hexdigest(),
                "error_message": None,
            }
    except urllib.error.HTTPError as e:
        return {"fetch_status": "http_error", "http_status": int(e.code), "fetched_at": now, "content_type": None, "raw_html": None, "extracted_text": None, "content_hash": None, "error_message": str(e)[:1000]}
    except Exception as e:
        return {"fetch_status": "error", "http_status": None, "fetched_at": now, "content_type": None, "raw_html": None, "extracted_text": None, "content_hash": None, "error_message": (type(e).__name__ + ": " + str(e))[:1000]}

# COMMAND ----------

cands = spark.sql(f"""
SELECT source_id, report_month, url
FROM {FULL_SCHEMA}.hyundai_source_candidates
WHERE is_selected = true
ORDER BY report_month
""").collect()

rows = []
for c in cands:
    f = fetch(c.url)
    rows.append(Row(source_id=c.source_id, report_month=c.report_month, url=c.url, **f))

if rows:
    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    df.createOrReplaceTempView("new_hyundai_raw_sources")
    spark.sql(f"""
      MERGE INTO {FULL_SCHEMA}.hyundai_raw_sources t
      USING new_hyundai_raw_sources s
      ON t.source_id = s.source_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Selected candidates attempted: {len(rows)}")
display(spark.sql(f"""
SELECT report_month, fetch_status, http_status, length(extracted_text) AS text_len, url
FROM {FULL_SCHEMA}.hyundai_raw_sources
ORDER BY report_month
"""))
