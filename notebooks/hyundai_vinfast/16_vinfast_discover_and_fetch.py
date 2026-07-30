# Databricks notebook source
# MAGIC %md
# MAGIC # VinFast source discovery and fetch
# MAGIC
# MAGIC Replaces the discovery half of `15_vinfast_ai_search_assisted_extract`.
# MAGIC
# MAGIC VinFast publishes one investor-relations press release per month with the
# MAGIC Vietnam delivery figures, at a URL that names the month **and** the headline
# MAGIC total:
# MAGIC
# MAGIC ```
# MAGIC /investor-relations/news/vinfast-reports-deliveries-of-17955-electric-vehicles-in-june-2026-in
# MAGIC ```
# MAGIC
# MAGIC So the month is something we *request*, never something a model infers. That
# MAGIC is the whole point of this rewrite: notebook 15 asked Gemini to search for a
# MAGIC month, and when the search returned an article about a different year it filed
# MAGIC those numbers under the requested month anyway. `2024-03` ended up holding
# MAGIC `2026-03`'s figures, complete with a model that did not exist in 2024.
# MAGIC
# MAGIC This notebook only discovers and fetches. Extraction is notebook 17.
# MAGIC
# MAGIC Writes `vinfast_ir_sources`, which is the **only** copy of each release's
# MAGIC text, so a failed fetch must never overwrite a good one.

# COMMAND ----------

import datetime as dt
import hashlib
import html
import json
import re
import ssl
import urllib.error
import urllib.request

from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CATALOG = "market_data"
SCHEMA = "hyundai_vinfast"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

BASE = "https://vinfastauto.us"
DEFAULT_INDEX_URLS = [
    "https://vinfastauto.us/investor-relations/news",
    "https://vinfastauto.us/newsroom",
]

FETCH_TIMEOUT_SECONDS = 45
USER_AGENT = "Mozilla/5.0 (compatible; market-data-etl/1.0)"
CREATED_AT = dt.datetime.now()

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

# VinFast has used at least five slug shapes for the same monthly release:
#
#   vinfast-reports-deliveries-of-17955-electric-vehicles-in-june-2026-in
#   vinfast-delivered-10922-electric-vehicles-in-vietnam-in-august-2025
#   vinfast-delivers-23186-electric-vehicles-in-november-2025-in-vietnam
#   vinfast-sets-another-record-delivering-more-than-20000-cars-in-october-2025
#   vinfast-announces-4q24-global-deliveries-january-2025-domestic-deliveries
#
# Rather than enumerate them, match on "a delivery release naming a month and a
# year" and pull the exact total out separately when the slug states one. The last
# two shapes do not carry an exact monthly total, so headline_total is NULL there
# and notebook 17 skips the slug cross-check for that month instead of failing it.
MONTH_NAMES = "|".join(MONTHS)
MONTH_YEAR_RE = re.compile(rf"(?:^|-)({MONTH_NAMES})-(\d{{4}})(?:-|$)", re.I)
EXACT_TOTAL_RE = re.compile(r"-(\d[\d,]*)-electric-vehicles", re.I)
HREF_RE = re.compile(r"""["'\s](/[^"'\s]*deliver[^"'\s]*)""", re.I)


def parse_slug(slug):
    """Return (report_month, headline_total or None) or None if not a monthly release."""
    m = MONTH_YEAR_RE.search(slug)
    if not m:
        return None
    month = MONTHS[m.group(1).lower()]
    year = int(m.group(2))
    if not (2020 <= year <= 2100):
        return None
    total = None
    # "more than 20,000" is a rounded claim, not the figure - do not treat it as exact.
    if "more-than" not in slug.lower():
        t = EXACT_TOTAL_RE.search(slug)
        if t:
            total = int(t.group(1).replace(",", ""))
    return f"{year:04d}-{month:02d}", total

# COMMAND ----------

dbutils.widgets.text("refetch_all", "false")
dbutils.widgets.text("only_months", "")
dbutils.widgets.text("index_urls", ",".join(DEFAULT_INDEX_URLS))

REFETCH_ALL = dbutils.widgets.get("refetch_all").strip().lower() == "true"
ONLY_MONTHS = [m.strip() for m in dbutils.widgets.get("only_months").split(",") if m.strip()]
INDEX_URLS = [u.strip() for u in dbutils.widgets.get("index_urls").split(",") if u.strip()]

print(f"refetch_all={REFETCH_ALL} only_months={ONLY_MONTHS or '(all discovered)'}")

# COMMAND ----------


def _ssl_context():
    """Some hosts in this pipeline need legacy TLS renegotiation, which OpenSSL 3
    refuses by default. Certificate and hostname verification stay on."""
    ctx = ssl.create_default_context()
    ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)
    return ctx


SSL_CONTEXT = _ssl_context()


def http_get(url):
    """Return (status, text). Decodes using the charset the server declares."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT_SECONDS, context=SSL_CONTEXT) as resp:
        raw = resp.read()
        charset = resp.headers.get_content_charset()
        if not charset:
            m = re.search(rb'charset=["\']?([\w\-]+)', raw[:4000], re.I)
            charset = m.group(1).decode("ascii", "ignore") if m else "utf-8"
        return resp.status, raw.decode(charset, "replace")


def strip_html(page):
    text = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", page)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


# COMMAND ----------

# DBTITLE 1,Discover
discovered = {}
index_errors = []

for index_url in INDEX_URLS:
    try:
        status, page = http_get(index_url)
    except Exception as e:
        index_errors.append(f"{index_url}: {type(e).__name__} {e}")
        continue
    if status != 200:
        index_errors.append(f"{index_url}: HTTP {status}")
        continue
    for href in set(HREF_RE.findall(page)):
        url = href if href.startswith("http") else BASE + href
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        parsed = parse_slug(slug)
        if not parsed:
            continue
        report_month, total = parsed
        prior = discovered.get(report_month)
        # The same release appears on several index pages, sometimes under
        # /newsroom/ as well as /investor-relations/. Prefer whichever variant
        # states an exact total, so the notebook 17 cross-check stays available.
        if prior and prior["headline_total"] is not None and total is None:
            continue
        discovered[report_month] = {
            "source_id": slug,
            "report_month": report_month,
            "report_year": int(report_month[:4]),
            "source_url": url,
            "headline_total": total,
        }

if ONLY_MONTHS:
    discovered = {k: v for k, v in discovered.items() if k in ONLY_MONTHS}

print(json.dumps({
    "index_urls": INDEX_URLS,
    "index_errors": index_errors,
    "discovered_months": sorted(discovered),
}, indent=2))

# A month with no release is normal (it may not be published yet). Discovering
# *nothing at all* means the site changed or the crawl broke - that is not a
# data-absence signal and must not be reported as a clean run.
if not discovered:
    raise RuntimeError(
        "Discovered zero VinFast delivery releases from "
        f"{INDEX_URLS}. Errors: {index_errors or 'none'}. "
        "The index layout or slug pattern has probably changed."
    )

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.vinfast_ir_sources (
  source_id STRING,
  report_month STRING,
  report_year INT,
  source_url STRING,
  source_domain STRING,
  headline_total INT,
  page_title STRING,
  http_status INT,
  fetch_status STRING,
  extracted_text STRING,
  content_hash STRING,
  content_length INT,
  fetched_at TIMESTAMP,
  discovered_at TIMESTAMP,
  error_message STRING
) USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,Decide what to fetch
existing = {
    r["report_month"]: r
    for r in spark.sql(f"""
        SELECT report_month, fetch_status, content_hash
        FROM {FULL_SCHEMA}.vinfast_ir_sources
    """).collect()
}

to_fetch = []
skipped = []
for month, rec in sorted(discovered.items()):
    prior = existing.get(month)
    if REFETCH_ALL or prior is None or prior["fetch_status"] != "ok":
        to_fetch.append(rec)
    else:
        skipped.append(month)

print(f"to fetch : {[r['report_month'] for r in to_fetch] or 'none'}")
print(f"already ok: {skipped or 'none'}")

# COMMAND ----------

# DBTITLE 1,Fetch
rows = []
for rec in to_fetch:
    status, text, err, title = None, None, None, None
    try:
        status, page = http_get(rec["source_url"])
        if status == 200:
            text = strip_html(page)
            m = re.search(r"(?is)<title[^>]*>(.*?)</title>", page)
            title = html.unescape(m.group(1)).strip()[:500] if m else None
            fetch_status = "ok" if len(text) > 500 else "too_short"
            if fetch_status == "too_short":
                err = f"stripped text only {len(text)} chars"
        else:
            fetch_status = "http_error"
            err = f"HTTP {status}"
    except urllib.error.HTTPError as e:
        status, fetch_status, err = e.code, "http_error", f"HTTP {e.code}"
    except Exception as e:
        fetch_status, err = "error", f"{type(e).__name__}: {e}"[:500]

    rows.append(Row(
        source_id=rec["source_id"],
        report_month=rec["report_month"],
        report_year=rec["report_year"],
        source_url=rec["source_url"],
        source_domain="vinfastauto.us",
        headline_total=rec["headline_total"],
        page_title=title,
        http_status=status,
        fetch_status=fetch_status,
        extracted_text=text,
        content_hash=hashlib.sha256(text.encode("utf-8")).hexdigest() if text else None,
        content_length=len(text) if text else None,
        fetched_at=CREATED_AT,
        discovered_at=CREATED_AT,
        error_message=err,
    ))
    print(f"  {rec['report_month']}  {fetch_status:<11} {err or ''}")

# COMMAND ----------

raw_schema = StructType([
    StructField("source_id", StringType()),
    StructField("report_month", StringType()),
    StructField("report_year", IntegerType()),
    StructField("source_url", StringType()),
    StructField("source_domain", StringType()),
    StructField("headline_total", IntegerType()),
    StructField("page_title", StringType()),
    StructField("http_status", IntegerType()),
    StructField("fetch_status", StringType()),
    StructField("extracted_text", StringType()),
    StructField("content_hash", StringType()),
    StructField("content_length", IntegerType()),
    StructField("fetched_at", TimestampType()),
    StructField("discovered_at", TimestampType()),
    StructField("error_message", StringType()),
])

if rows:
    spark.createDataFrame(rows, raw_schema).createOrReplaceTempView("vinfast_raw_incoming")

    # A failed fetch must update only attempt metadata. `UPDATE SET *` here would
    # write NULL over the stored release text, and this table is the only copy -
    # that is exactly how two months of Hyundai history were destroyed.
    spark.sql(f"""
    MERGE INTO {FULL_SCHEMA}.vinfast_ir_sources t
    USING vinfast_raw_incoming s
      ON t.report_month = s.report_month
    WHEN MATCHED AND s.fetch_status = 'ok' THEN UPDATE SET *
    WHEN MATCHED AND s.fetch_status <> 'ok' THEN UPDATE SET
      t.fetch_status = s.fetch_status,
      t.http_status = s.http_status,
      t.fetched_at = s.fetched_at,
      t.error_message = s.error_message
    WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

summary = spark.sql(f"""
SELECT fetch_status, COUNT(*) AS n, MIN(report_month) AS first_month, MAX(report_month) AS last_month
FROM {FULL_SCHEMA}.vinfast_ir_sources
GROUP BY fetch_status ORDER BY n DESC
""")
summary.show(truncate=False)

ok_count = spark.sql(f"""
SELECT COUNT(*) AS n FROM {FULL_SCHEMA}.vinfast_ir_sources WHERE fetch_status = 'ok'
""").collect()[0]["n"]

metrics = {
    "discovered": len(discovered),
    "attempted": len(rows),
    "succeeded": sum(1 for r in rows if r["fetch_status"] == "ok"),
    "skipped_already_ok": len(skipped),
    "total_ok_in_table": ok_count,
    "index_errors": index_errors,
}
print(json.dumps(metrics, indent=2))

# Every attempt failing is infrastructure, not absence of data.
if rows and metrics["succeeded"] == 0:
    raise RuntimeError(
        f"All {len(rows)} VinFast source fetches failed. "
        f"First error: {next((r['error_message'] for r in rows if r['error_message']), '')}"
    )

dbutils.notebook.exit(json.dumps(metrics, default=str))
