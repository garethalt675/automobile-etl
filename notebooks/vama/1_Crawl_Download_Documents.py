# Databricks notebook source
# DBTITLE 1,VAMA Step 1 - Crawl and Download Documents
# MAGIC %md
# MAGIC # Step 1: Crawl VAMA sales-report PDFs and download them
# MAGIC
# MAGIC Source: `http://vama.org.vn/vn/bao-cao-ban-hang.html?Page=1`
# MAGIC
# MAGIC Outputs:
# MAGIC - `market_data.vama.vama_documents_url`: discovered PDF URLs + inferred metadata
# MAGIC - `market_data.vama.document_processing_log`: download/parse/extraction tracking
# MAGIC - `/Volumes/market_data/vama/download_docs/<report_year>/...`: downloaded PDFs

# COMMAND ----------

import hashlib
import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse, unquote

import requests
from pyspark.sql import functions as F
from pyspark.sql.types import *

BASE_URL = "http://vama.org.vn/vn/bao-cao-ban-hang.html?Page=1"
SITE_ROOT = "http://vama.org.vn"
CATALOG = "market_data"
SCHEMA = "vama"
VOLUME = "download_docs"
VOLUME_BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
MAX_PAGES = 200
MAX_RETRIES = 3
REQUEST_TIMEOUT = 60
SLEEP_SECONDS = 0.5

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
os.makedirs(VOLUME_BASE_PATH, exist_ok=True)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.vama_documents_url (
  document_id STRING NOT NULL,
  url STRING NOT NULL,
  title STRING,
  filename STRING,
  source_page INT,
  report_year INT,
  report_month INT,
  report_month_name STRING,
  report_month_key STRING,
  report_type STRING,
  is_primary_detail BOOLEAN,
  discovered_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.document_processing_log (
  document_id STRING NOT NULL,
  document_url STRING NOT NULL,
  title STRING,
  filename STRING,
  report_year INT,
  report_month INT,
  report_month_key STRING,
  report_type STRING,
  local_path STRING,
  download_status STRING,
  download_timestamp TIMESTAMP,
  download_attempts INT,
  download_error_message STRING,
  parse_status STRING,
  parse_timestamp TIMESTAMP,
  parse_error_message STRING,
  extraction_status STRING,
  extraction_timestamp TIMESTAMP,
  extraction_error_message STRING,
  extraction_rows_inserted INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

MONTH_MAP = {
    'jan': 1, 'january': 1, 'tháng 1': 1, 'thang 1': 1,
    'feb': 2, 'february': 2, 'tháng 2': 2, 'thang 2': 2,
    'mar': 3, 'march': 3, 'tháng 3': 3, 'thang 3': 3,
    'apr': 4, 'april': 4, 'tháng 4': 4, 'thang 4': 4,
    'may': 5, 'tháng 5': 5, 'thang 5': 5,
    'jun': 6, 'june': 6, 'tháng 6': 6, 'thang 6': 6,
    'jul': 7, 'july': 7, 'tháng 7': 7, 'thang 7': 7,
    'aug': 8, 'august': 8, 'tháng 8': 8, 'thang 8': 8,
    'sep': 9, 'sept': 9, 'september': 9, 'tháng 9': 9, 'thang 9': 9,
    'oct': 10, 'october': 10, 'tháng 10': 10, 'thang 10': 10,
    'nov': 11, 'november': 11, 'tháng 11': 11, 'thang 11': 11,
    'dec': 12, 'december': 12, 'tháng 12': 12, 'thang 12': 12,
}

def document_id(url: str) -> str:
    return hashlib.md5(url.encode('utf-8')).hexdigest()[:16]

def clean_text(s):
    return re.sub(r"\s+", " ", (s or "")).strip()

def infer_report_type(title, filename):
    text = f"{title} {filename}".lower()
    if 'detail' in text:
        return 'detail'
    if 'summary' in text:
        return 'summary'
    if 'bmw' in text or 'mini' in text:
        return 'bmw_mini'
    if 'lexus' in text:
        return 'lexus'
    if 'cover' in text or 'letter' in text or '(vie)' in text:
        return 'cover_letter'
    return 'other'

def infer_period(title, filename, url):
    text = clean_text(f"{title} {filename} {unquote(url)}").lower()
    year_match = re.search(r'(20\d{2})', text)
    year = int(year_match.group(1)) if year_match else None
    month = None
    month_name = None
    # Numeric Vietnamese title: tháng 3 năm 2026
    m = re.search(r'th[aá]ng\s*(\d{1,2})', text)
    if m:
        month = int(m.group(1))
        month_name = f"month_{month:02d}"
    else:
        for key, value in sorted(MONTH_MAP.items(), key=lambda kv: -len(kv[0])):
            if re.search(rf'\b{re.escape(key)}\b', text):
                month = value
                month_name = key
                break
    month_key = f"{year:04d}-{month:02d}" if year and month else None
    return year, month, month_name, month_key

def parse_listing(html, page_no):
    # Lightweight parser: each report link appears as an <a href="...pdf">title</a> in document-list.
    links = re.findall(r'<a\s+[^>]*href=["\']([^"\']+\.pdf)[^"\']*["\'][^>]*>(.*?)</a>', html, flags=re.I|re.S)
    rows = []
    seen = set()
    for href, inner in links:
        url = urljoin(SITE_ROOT, href)
        if url in seen:
            continue
        seen.add(url)
        title = clean_text(re.sub(r'<[^>]+>', ' ', inner)).lstrip('*').strip()
        filename = unquote(urlparse(url).path.split('/')[-1])
        year, month, month_name, month_key = infer_period(title, filename, url)
        report_type = infer_report_type(title, filename)
        rows.append({
            'document_id': document_id(url),
            'url': url,
            'title': title,
            'filename': filename,
            'source_page': page_no,
            'report_year': year,
            'report_month': month,
            'report_month_name': month_name,
            'report_month_key': month_key,
            'report_type': report_type,
            'is_primary_detail': report_type == 'detail',
            'discovered_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
        })
    return rows

def fetch(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r

print('='*80)
print('CRAWLING VAMA REPORT LISTINGS')
print('='*80)
all_rows = []
empty_streak = 0
seen_urls = set()
for page in range(1, MAX_PAGES + 1):
    page_url = f"http://vama.org.vn/vn/bao-cao-ban-hang.html?Page={page}"
    print(f"Page {page}: {page_url}")
    try:
        html = fetch(page_url).text
    except Exception as e:
        print(f"  stop: fetch failed: {e}")
        break
    rows = parse_listing(html, page)
    new_rows = [r for r in rows if r['url'] not in seen_urls]
    for r in new_rows:
        seen_urls.add(r['url'])
    print(f"  links={len(rows)}, new={len(new_rows)}")
    if not new_rows:
        empty_streak += 1
        if empty_streak >= 2:
            print('  stopping after two pages with no new URLs')
            break
    else:
        empty_streak = 0
        all_rows.extend(new_rows)
    time.sleep(SLEEP_SECONDS)

print(f"Discovered unique documents: {len(all_rows)}")

if all_rows:
    schema = StructType([
        StructField('document_id', StringType(), False),
        StructField('url', StringType(), False),
        StructField('title', StringType(), True),
        StructField('filename', StringType(), True),
        StructField('source_page', IntegerType(), True),
        StructField('report_year', IntegerType(), True),
        StructField('report_month', IntegerType(), True),
        StructField('report_month_name', StringType(), True),
        StructField('report_month_key', StringType(), True),
        StructField('report_type', StringType(), True),
        StructField('is_primary_detail', BooleanType(), True),
        StructField('discovered_at', TimestampType(), True),
        StructField('updated_at', TimestampType(), True),
    ])
    df = spark.createDataFrame(all_rows, schema=schema)
    df.createOrReplaceTempView('vama_discovered_urls')
    spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.vama_documents_url AS target
    USING vama_discovered_urls AS source
    ON target.document_id = source.document_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# DBTITLE 1,Download newly discovered PDFs
urls_df = spark.table(f"{CATALOG}.{SCHEMA}.vama_documents_url")
log_df = spark.table(f"{CATALOG}.{SCHEMA}.document_processing_log")
if log_df.count() > 0:
    done_df = log_df.filter(F.col('download_status') == 'success').select('document_url').distinct()
    to_download_df = urls_df.join(done_df, urls_df.url == done_df.document_url, 'left_anti')
else:
    to_download_df = urls_df

to_download = to_download_df.orderBy(F.desc('report_year'), F.desc('report_month'), 'report_type').collect()
print(f"Documents to download: {len(to_download)}")

results = []
for idx, row in enumerate(to_download, 1):
    url = row['url']
    year_dir = str(row['report_year'] or 'unknown_year')
    month_dir = row['report_month_key'] or 'unknown_month'
    safe_name = re.sub(r'[\\/:*?"<>|]+', '_', row['filename'] or f"{row['document_id']}.pdf")
    out_dir = f"{VOLUME_BASE_PATH}/{year_dir}/{month_dir}"
    os.makedirs(out_dir, exist_ok=True)
    local_path = f"{out_dir}/{safe_name}"
    print(f"[{idx}/{len(to_download)}] {row['report_month_key']} {row['report_type']} -> {safe_name}")

    success = False
    error = None
    attempts = 0
    for attempt in range(1, MAX_RETRIES + 1):
        attempts = attempt
        try:
            resp = fetch(url)
            if not resp.content:
                raise ValueError('empty response body')
            if 'pdf' not in (resp.headers.get('content-type') or '').lower() and not url.lower().endswith('.pdf'):
                raise ValueError(f"unexpected content-type: {resp.headers.get('content-type')}")
            with open(local_path, 'wb') as f:
                f.write(resp.content)
            success = True
            error = None
            print(f"  ✓ downloaded {len(resp.content)/1024:.1f} KB")
            break
        except Exception as e:
            error = str(e)
            print(f"  ✗ attempt {attempt}: {error}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)

    now = datetime.utcnow()
    results.append({
        'document_id': row['document_id'],
        'document_url': url,
        'title': row['title'],
        'filename': safe_name,
        'report_year': row['report_year'],
        'report_month': row['report_month'],
        'report_month_key': row['report_month_key'],
        'report_type': row['report_type'],
        'local_path': local_path if success else None,
        'download_status': 'success' if success else 'failed',
        'download_timestamp': now,
        'download_attempts': attempts,
        'download_error_message': error,
        'parse_status': 'pending' if success else None,
        'parse_timestamp': None,
        'parse_error_message': None,
        'extraction_status': None,
        'extraction_timestamp': None,
        'extraction_error_message': None,
        'extraction_rows_inserted': None,
        'created_at': now,
        'updated_at': now,
    })

if results:
    schema = StructType([
        StructField('document_id', StringType(), False),
        StructField('document_url', StringType(), False),
        StructField('title', StringType(), True),
        StructField('filename', StringType(), True),
        StructField('report_year', IntegerType(), True),
        StructField('report_month', IntegerType(), True),
        StructField('report_month_key', StringType(), True),
        StructField('report_type', StringType(), True),
        StructField('local_path', StringType(), True),
        StructField('download_status', StringType(), True),
        StructField('download_timestamp', TimestampType(), True),
        StructField('download_attempts', IntegerType(), True),
        StructField('download_error_message', StringType(), True),
        StructField('parse_status', StringType(), True),
        StructField('parse_timestamp', TimestampType(), True),
        StructField('parse_error_message', StringType(), True),
        StructField('extraction_status', StringType(), True),
        StructField('extraction_timestamp', TimestampType(), True),
        StructField('extraction_error_message', StringType(), True),
        StructField('extraction_rows_inserted', IntegerType(), True),
        StructField('created_at', TimestampType(), True),
        StructField('updated_at', TimestampType(), True),
    ])
    spark.createDataFrame(results, schema=schema).createOrReplaceTempView('vama_download_results')
    spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.document_processing_log AS target
    USING vama_download_results AS source
    ON target.document_id = source.document_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

summary = spark.sql(f"""
SELECT report_year, report_month_key, report_type, download_status, COUNT(*) AS documents
FROM {CATALOG}.{SCHEMA}.document_processing_log
GROUP BY report_year, report_month_key, report_type, download_status
ORDER BY report_year DESC, report_month_key DESC, report_type, download_status
""")
display(summary)