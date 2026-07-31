# Databricks notebook source
# MAGIC %md
# MAGIC # Hyundai source discovery
# MAGIC Discovers TC Group/Thanh Cong Hyundai monthly sales pages from sitemap first, then bounded deterministic URL-pattern probes.
# MAGIC
# MAGIC Sprint 001 constraints:
# MAGIC - no broad web search
# MAGIC - no sales extraction
# MAGIC - complete reliably on Databricks serverless

# COMMAND ----------
import datetime as dt
import hashlib
import html
import re
import socket
import urllib.error
import urllib.request
from urllib.parse import urlparse

from pyspark.sql import Row

FULL_SCHEMA = "market_data.hyundai_vinfast"
START_YEAR = 2019
now = dt.date.today()
END_YEAR, END_MONTH = now.year, now.month

SITEMAP_INDEX = "https://thanhcong.vn/wp-sitemap.xml"
FALLBACK_SITEMAPS = ["https://thanhcong.vn/wp-sitemap-posts-post-1.xml"]

# Keep probes intentionally bounded. The sitemap is the primary discovery source; probes only fill likely gaps.
MAX_PROBES = 96
PROBE_TIMEOUT_SECONDS = 3
URL_TIMEOUT_SECONDS = 12
socket.setdefaulttimeout(max(URL_TIMEOUT_SECONDS, PROBE_TIMEOUT_SECONDS + 2))

PATTERNS = [
    ("tc_group_no_xe", "https://thanhcong.vn/tin-tuc/tc-group-thong-bao-ket-qua-ban-hang-hyundai-thang-{m}-{y}.html"),
    ("tc_group_no_xe_0", "https://thanhcong.vn/tin-tuc/tc-group-thong-bao-ket-qua-ban-hang-hyundai-thang-{mm}-{y}.html"),
    ("thanh_cong_group", "https://thanhcong.vn/tin-tuc/tap-doan-thanh-cong-thong-bao-ket-qua-ban-hang-hyundai-thang-{m}-{y}.html"),
    ("thanh_cong_group_0", "https://thanhcong.vn/tin-tuc/tap-doan-thanh-cong-thong-bao-ket-qua-ban-hang-hyundai-thang-{mm}-{y}.html"),
    ("short", "https://thanhcong.vn/tin-tuc/thong-bao-ket-qua-ban-hang-hyundai-thang-{m}-{y}.html"),
    ("short_0", "https://thanhcong.vn/tin-tuc/thong-bao-ket-qua-ban-hang-hyundai-thang-{mm}-{y}.html"),
    ("tc_group_xe", "https://thanhcong.vn/tin-tuc/tc-group-thong-bao-ket-qua-ban-hang-xe-hyundai-thang-{m}-{y}.html"),
    ("tc_group_xe_0", "https://thanhcong.vn/tin-tuc/tc-group-thong-bao-ket-qua-ban-hang-xe-hyundai-thang-{mm}-{y}.html"),
    # 2026-05 was published without "hyundai" anywhere in the slug:
    # tc-group-thong-bao-ket-qua-ban-hang-thang-5-2026.html
    # Every other pattern here requires the brand name, which is why that month
    # showed up as "no source candidate" rather than as a fetch failure.
    ("tc_group_no_brand", "https://thanhcong.vn/tin-tuc/tc-group-thong-bao-ket-qua-ban-hang-thang-{m}-{y}.html"),
    ("tc_group_no_brand_0", "https://thanhcong.vn/tin-tuc/tc-group-thong-bao-ket-qua-ban-hang-thang-{mm}-{y}.html"),
]

STRICT_SALES_HINTS = (
    "ket-qua-ban-hang-hyundai",
    "ket-qua-ban-hang-xe-hyundai",
    # Brand-less variants: a TC Group / Thanh Cong monthly sales announcement.
    "tc-group-thong-bao-ket-qua-ban-hang-thang",
    "tap-doan-thanh-cong-thong-bao-ket-qua-ban-hang-thang",
)


def fetch_text(url: str, timeout: int = URL_TIMEOUT_SECONDS, max_bytes: int = 3_000_000):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 OpenClaw Hyundai VinFast source discovery",
            "Accept": "text/html,application/xhtml+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read(max_bytes)
        return resp.status, resp.headers.get("content-type", ""), raw.decode("utf-8", "ignore")


def sitemap_urls():
    sitemap_locs = []
    try:
        status, _ctype, xml = fetch_text(SITEMAP_INDEX)
        if status == 200:
            sitemap_locs = [html.unescape(x.strip()) for x in re.findall(r"<loc>(.*?)</loc>", xml)]
    except Exception as exc:
        print("sitemap_index_failed", type(exc).__name__, str(exc)[:200])

    post_sitemaps = [u for u in sitemap_locs if "wp-sitemap-posts-post" in u]
    if not post_sitemaps:
        post_sitemaps = FALLBACK_SITEMAPS

    urls = []
    for sm_url in post_sitemaps:
        try:
            status, _ctype, xml = fetch_text(sm_url)
            if status == 200:
                urls.extend(html.unescape(x.strip()) for x in re.findall(r"<loc>(.*?)</loc>", xml))
        except Exception as exc:
            print("post_sitemap_failed", sm_url, type(exc).__name__, str(exc)[:200])
    return urls


def month_from_url(url: str):
    low = url.lower()
    patterns = [
        r"thang[-_/ ]*(0?[1-9]|1[0-2])(?:[-_/ ]*(?:va[-_/ ]*)?nam)?[-_/ ]*(20\d{2})",
        r"(20\d{2}).{0,30}thang[-_/ ]*(0?[1-9]|1[0-2])",
    ]
    for pat in patterns:
        match = re.search(pat, low, re.I)
        if match:
            if match.group(1).startswith("20"):
                return int(match.group(1)), int(match.group(2))
            return int(match.group(2)), int(match.group(1))
    return None, None


def is_likely_hyundai_sales_url(url: str):
    low = url.lower()
    # Exclude global-sales or prize-program articles that happen to include "ban-hang".
    # Requiring "hyundai" in the slug used to be the first gate, but TC Group does
    # not always put the brand in it (see 2026-05), so the hints carry that weight
    # now: they are specific enough to be a monthly sales announcement on their own.
    return any(hint in low for hint in STRICT_SALES_HINTS)


def report_months_desc():
    months = []
    for y in range(START_YEAR, END_YEAR + 1):
        max_m = END_MONTH if y == END_YEAR else 12
        for m in range(1, max_m + 1):
            months.append((y, m, f"{y}-{m:02d}"))
    return list(reversed(months))


def source_id_for(url: str):
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]


rows = []
seen_ids = set()
ts = dt.datetime.utcnow()

# 1) Primary discovery: sitemap.
for rank, url in enumerate(sitemap_urls(), start=1):
    if not is_likely_hyundai_sales_url(url):
        continue
    y, m = month_from_url(url)
    if not y or y < START_YEAR or m < 1 or m > 12:
        continue
    sid = source_id_for(url)
    if sid in seen_ids:
        continue
    seen_ids.add(sid)
    rows.append(Row(
        source_id=sid,
        report_year=int(y),
        report_month_int=int(m),
        report_month=f"{y}-{m:02d}",
        url=url,
        title="",
        source_domain=urlparse(url).netloc,
        source_type="sitemap",
        source_priority=10,
        discovered_by="sitemap",
        pattern_name="",
        candidate_rank=int(rank),
        is_selected=False,
        selection_reason="",
        created_at=ts,
        updated_at=ts,
    ))

sitemap_months = {r.report_month for r in rows}
print("sitemap_candidate_rows", len(rows), "sitemap_months", len(sitemap_months))

# 2) Secondary discovery: deterministic URL probes for recent/gap months only, with a hard budget.
probe_jobs = []
for y, m, rm in report_months_desc():
    if rm in sitemap_months:
        continue
    for pidx, (pname, tmpl) in enumerate(PATTERNS, start=1):
        url = tmpl.format(y=y, m=m, mm=f"{m:02d}")
        sid = source_id_for(url)
        if sid in seen_ids:
            continue
        probe_jobs.append((y, m, rm, pidx, pname, url, sid))
        if len(probe_jobs) >= MAX_PROBES:
            break
    if len(probe_jobs) >= MAX_PROBES:
        break

probe_hits = 0
for job_rank, (y, m, rm, pidx, pname, url, sid) in enumerate(probe_jobs, start=1):
    try:
        status, _ctype, body = fetch_text(url, timeout=PROBE_TIMEOUT_SECONDS, max_bytes=500_000)
        if status != 200:
            continue
        body_low = body.lower()
        if "hyundai" not in body_low or ("ban" not in body_low and "bán" not in body_low):
            continue
        if sid in seen_ids:
            continue
        seen_ids.add(sid)
        probe_hits += 1
        rows.append(Row(
            source_id=sid,
            report_year=int(y),
            report_month_int=int(m),
            report_month=rm,
            url=url,
            title="",
            source_domain=urlparse(url).netloc,
            source_type="url_probe",
            source_priority=20 + int(pidx),
            discovered_by="url_pattern_probe_bounded",
            pattern_name=pname,
            candidate_rank=100000 + int(job_rank),
            is_selected=False,
            selection_reason="",
            created_at=ts,
            updated_at=ts,
        ))
    except Exception:
        # Probes are best-effort and deliberately silent per-URL to keep notebook output compact.
        continue

print("url_probe_jobs", len(probe_jobs), "url_probe_hits", probe_hits)

if not rows:
    raise RuntimeError("No Hyundai source candidates discovered from sitemap or bounded probes")

# COMMAND ----------

df = spark.createDataFrame(rows).dropDuplicates(["source_id"])
df.createOrReplaceTempView("new_hyundai_candidates")

spark.sql(f"""
MERGE INTO {FULL_SCHEMA}.hyundai_source_candidates t
USING new_hyundai_candidates s
ON t.source_id = s.source_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# Select one candidate per month by priority/rank. Limit selection to non-null report_months.
spark.sql(f"UPDATE {FULL_SCHEMA}.hyundai_source_candidates SET is_selected=false, selection_reason=NULL")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW ranked_hyundai_candidates AS
SELECT
  source_id,
  row_number() OVER (
    PARTITION BY report_month
    ORDER BY source_priority ASC, candidate_rank ASC, url ASC
  ) AS rn
FROM {FULL_SCHEMA}.hyundai_source_candidates
WHERE report_month IS NOT NULL
""")
spark.sql(f"""
MERGE INTO {FULL_SCHEMA}.hyundai_source_candidates t
USING (SELECT source_id FROM ranked_hyundai_candidates WHERE rn=1) s
ON t.source_id=s.source_id
WHEN MATCHED THEN UPDATE SET
  is_selected=true,
  selection_reason='lowest priority/rank candidate selected by sprint 001 discovery notebook',
  updated_at=current_timestamp()
""")

summary = spark.sql(f"""
SELECT
  count(*) AS candidate_rows,
  count(DISTINCT report_month) AS candidate_months,
  sum(CASE WHEN is_selected THEN 1 ELSE 0 END) AS selected_rows,
  count(DISTINCT CASE WHEN is_selected THEN report_month END) AS selected_months
FROM {FULL_SCHEMA}.hyundai_source_candidates
""")
display(summary)

display(spark.sql(f"""
SELECT report_month, count(*) AS candidates, max(CASE WHEN is_selected THEN url END) AS selected_url
FROM {FULL_SCHEMA}.hyundai_source_candidates
GROUP BY report_month
ORDER BY report_month
"""))
