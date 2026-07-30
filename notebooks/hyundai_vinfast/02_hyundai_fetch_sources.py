# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch selected raw Hyundai sources

# COMMAND ----------

import datetime as dt
import hashlib
import html
import re
import ssl
import urllib.error
import urllib.request
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

FULL_SCHEMA = "market_data.hyundai_vinfast"
FETCH_TIMEOUT_SECONDS = 15
USER_AGENT = "Mozilla/5.0 OpenClaw data QA; Hyundai VinFast sales ETL"


def _ssl_context():
    """hyundai.thanhcong.vn needs TLS renegotiation that OpenSSL 3.x refuses.

    Some of its pages fail with
        SSL: UNSAFE_LEGACY_RENEGOTIATION_DISABLED
    while others on the same host succeed, so this cannot be spotted from a single
    probe. It cost the 2024-12 and 2025-10 source pages on 2026-07-30. Certificate
    and hostname verification are left fully on; only the legacy-renegotiation
    handshake is permitted.
    """
    ctx = ssl.create_default_context()
    ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)
    return ctx


SSL_CONTEXT = _ssl_context()

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
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT_SECONDS, context=SSL_CONTEXT) as r:
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
    # A failed fetch must NOT clobber content we already have. The previous
    # `WHEN MATCHED THEN UPDATE SET *` overwrote raw_html/extracted_text with NULL
    # whenever a page that once fetched fine transiently failed, which destroyed
    # the only copy of the 2024-12 and 2025-10 prose sources on 2026-07-30 and made
    # those months unrecoverable downstream. Failures now update only the attempt
    # metadata and leave the last good payload in place.
    spark.sql(f"""
      MERGE INTO {FULL_SCHEMA}.hyundai_raw_sources t
      USING new_hyundai_raw_sources s
      ON t.source_id = s.source_id
      WHEN MATCHED AND s.fetch_status = 'ok' THEN UPDATE SET *
      WHEN MATCHED AND s.fetch_status <> 'ok' THEN UPDATE SET
        t.fetch_status = s.fetch_status,
        t.http_status = s.http_status,
        t.fetched_at = s.fetched_at,
        t.error_message = s.error_message
      WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Selected candidates attempted: {len(rows)}")
display(spark.sql(f"""
SELECT report_month, fetch_status, http_status, length(extracted_text) AS text_len, url
FROM {FULL_SCHEMA}.hyundai_raw_sources
ORDER BY report_month
"""))
