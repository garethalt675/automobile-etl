# Databricks notebook source
# MAGIC %md
# MAGIC # Hyundai deterministic extraction
# MAGIC Extract model-level monthly/YTD sales from fetched official TC Group Hyundai pages.

# COMMAND ----------

import datetime as dt
import hashlib
import html
import re
from urllib.parse import urlparse
from pyspark.sql import Row

FULL_SCHEMA = "market_data.hyundai_vinfast"

MODEL_NAMES = [
    "Hyundai Grand i10",
    "Hyundai Accent",
    "Hyundai Elantra",
    "Hyundai Venue",
    "Hyundai Tucson",
    "Hyundai SantaFe",
    "Hyundai Santa Fe",
    "Hyundai Creta",
    "Hyundai Palisade",
    "Hyundai Custin",
    "Hyundai Stargazer",
    "Hyundai Kona",
    "Hyundai Avante",
    "Hyundai Solati",
    "XE THƯƠNG MẠI",
    "Xe thương mại",
    "Các mẫu xe thương mại",
    "Mẫu khác",
    "Các mẫu xe khác",
]

CANONICAL_MODEL = {
    "hyundai santafe": "Hyundai Santa Fe",
    "xe thương mại": "XE THƯƠNG MẠI",
    "các mẫu xe thương mại": "XE THƯƠNG MẠI",
    "mẫu khác": "Mẫu khác",
    "các mẫu xe khác": "Mẫu khác",
}

NUM_RE = re.compile(r"(?<![\w/])\d{1,3}(?:[\.,]\d{1,3})*(?![\w/])|(?<![\w/])\d+(?![\w/])")
AMT = r"([0-9]{1,3}(?:[\.,][0-9]{3})+|[0-9]+)"


def compact_text(raw_html, fallback_text):
    text = raw_html or fallback_text or ""
    text = re.sub(r"<script[\s\S]*?</script>|<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def parse_int_token(token):
    if token is None:
        return None
    cleaned = re.sub(r"[^0-9]", "", token)
    if not cleaned:
        return None
    return int(cleaned)


def find_report_month_in_text(text):
    # Some TC Group pages contain unrelated dates and one known typo (`12/2034` on
    # the Dec-2023 page).  Use the first plausible sales-period expression, and
    # support the title form "tháng 12 và năm 2023".
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


def month_dates(report_month):
    y, m = [int(x) for x in report_month.split("-")]
    start = dt.date(y, m, 1)
    if m == 12:
        end = dt.date(y, 12, 31)
    else:
        end = dt.date(y, m + 1, 1) - dt.timedelta(days=1)
    return y, start, end


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
    # Prefer explicit total row if a flattened table exists.
    has_header = False
    table_start = text.find("Mẫu xe CBU/CKD")
    has_header = table_start >= 0
    if table_start < 0:
        m_header = re.search(r"Mẫu\s+xe\s+CBU/CKD", text, flags=re.I)
        table_start = m_header.start() if m_header else -1
        has_header = bool(m_header)
    if table_start < 0:
        model_positions = [m.start() for model in MODEL_NAMES for m in re.finditer(re.escape(model), text, flags=re.I)]
        total_pos = text.find("TỔNG")
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



PROSE_MODEL_PATTERNS = {
    "Hyundai Accent": [r"Hyundai\s+Accent[^.]{0,180}?" + AMT + r"\s+xe"],
    "Hyundai Creta": [r"Hyundai\s+Creta[^.]{0,180}?" + AMT + r"\s+xe"],
    "Hyundai Santa Fe": [r"Hyundai\s+Santa\s*Fe[^.]{0,200}?" + AMT + r"\s+xe"],
    "Hyundai Grand i10": [r"Hyundai\s+Grand\s+i10[^.]{0,200}?" + AMT + r"\s+xe"],
    "Hyundai Tucson": [r"Hyundai\s+Tucson[^.]{0,180}?" + AMT + r"\s+xe"],
    "Hyundai Venue": [r"Hyundai\s+Venue[^.]{0,180}?" + AMT + r"\s+xe"],
    "Hyundai Stargazer": [r"Hyundai\s+Stargazer[^.]{0,180}?" + AMT + r"\s+xe"],
    "Hyundai Custin": [r"Hyundai\s+Custin[^.]{0,180}?" + AMT + r"\s+xe"],
    "Hyundai Elantra": [r"Hyundai\s+Elantra[^.]{0,200}?" + AMT + r"\s+xe"],
    "Hyundai Palisade": [r"Hyundai\s+Palisade[^.]{0,200}?" + AMT + r"\s+xe"],
    "XE TH??NG M?I": [
        r"(?:C?c|CA.c|CA?c)?\s*m.{0,12}u\s+xe\s+th.{0,40}ng\s+m.{0,20}i\s+Hyundai[^.]{0,220}?" + AMT + r"\s+xe",
        r"xe\s+th.{0,40}ng\s+m.{0,20}i\s+Hyundai[^.]{0,220}?" + AMT + r"\s+xe",
    ],
}


def extract_prose_rows(raw, text, report_month, report_year, start, end, source_total, month_mismatch, found_month):
    # Fallback for 2024-09/10/11 and 2025-01 style TC Group articles where
    # the WordPress page text contains model-level prose but no extractable table.
    rows = []
    seen = set()
    for canonical, patterns in PROSE_MODEL_PATTERNS.items():
        for pat in patterns:
            m = re.search(pat, text, flags=re.I)
            if not m:
                continue
            monthly_total = parse_int_token(m.group(1))
            if monthly_total is None:
                continue
            evidence = m.group(0)[:500]
            doc_id = hashlib.sha256(f"{raw.source_id}|{report_month}|{canonical}".encode("utf-8")).hexdigest()
            rows.append(Row(
                document_id=doc_id,
                document_url=raw.url,
                filename=urlparse(raw.url).path.rsplit("/", 1)[-1],
                report_year=report_year,
                report_month=report_month,
                report_start_date=start,
                report_end_date=end,
                maker="Hyundai",
                model_name=canonical,
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
                source_row_index=len(rows) + 1,
                extracted_timestamp=dt.datetime.utcnow(),
                parsing_method="deterministic_tcgroup_prose_model_rows_v1" + ("_month_mismatch" if month_mismatch else ""),
                source_id=raw.source_id,
                source_title=None,
                source_domain=urlparse(raw.url).netloc,
                regional_granularity=None,
                is_commercial_vehicle=(canonical == "XE TH??NG M?I"),
                is_other_bucket=False,
                cbu_ckd=None,
                extraction_confidence=0.82 if not month_mismatch else 0.55,
                validation_status="warning",
                validation_message="Prose fallback extraction; YTD not available in prose rows" + (f"; source text month {found_month} differs from candidate {report_month}" if month_mismatch else ""),
                raw_evidence=evidence,
                created_at=dt.datetime.utcnow(),
                updated_at=dt.datetime.utcnow(),
            ))
            seen.add(canonical)
            break
    if rows and source_total is not None:
        explicit = sum(int(r.monthly_total or 0) for r in rows)
        residual = int(source_total) - explicit
        if residual > 0:
            canonical = "M?u kh?c"
            doc_id = hashlib.sha256(f"{raw.source_id}|{report_month}|{canonical}".encode("utf-8")).hexdigest()
            rows.append(Row(
                document_id=doc_id,
                document_url=raw.url,
                filename=urlparse(raw.url).path.rsplit("/", 1)[-1],
                report_year=report_year,
                report_month=report_month,
                report_start_date=start,
                report_end_date=end,
                maker="Hyundai",
                model_name=canonical,
                vama_classification=None,
                seat=None,
                monthly_north=None,
                monthly_central=None,
                monthly_south=None,
                monthly_total=residual,
                ytd_north=None,
                ytd_central=None,
                ytd_south=None,
                ytd_total=None,
                source_table_index=None,
                source_row_index=len(rows) + 1,
                extracted_timestamp=dt.datetime.utcnow(),
                parsing_method="deterministic_tcgroup_prose_residual_v1",
                source_id=raw.source_id,
                source_title=None,
                source_domain=urlparse(raw.url).netloc,
                regional_granularity=None,
                is_commercial_vehicle=False,
                is_other_bucket=True,
                cbu_ckd=None,
                extraction_confidence=0.70,
                validation_status="warning",
                validation_message=f"Computed residual from source total {source_total} minus explicit prose model rows {explicit}",
                raw_evidence=f"M?u kh?c residual = source total {source_total} - explicit prose model rows {explicit} = {residual}",
                created_at=dt.datetime.utcnow(),
                updated_at=dt.datetime.utcnow(),
            ))
    return rows


def extract_rows(raw):
    text = compact_text(raw.raw_html, raw.extracted_text)
    report_month = raw.report_month
    found_month = find_report_month_in_text(text)
    month_mismatch = bool(found_month and found_month != report_month)
    report_year, start, end = month_dates(report_month)

    # Restrict to an explicit flattened sales table. Older pages with prose-only summaries
    # are left unresolved instead of guessing model/YTD rows from narrative percentages.
    table_start = text.find("Mẫu xe CBU/CKD")
    if table_start < 0:
        m_header = re.search(r"Mẫu\s+xe\s+CBU/CKD", text, flags=re.I)
        table_start = m_header.start() if m_header else -1
    if table_start < 0:
        # Encoding glitches can damage the Vietnamese header while preserving model
        # rows.  Anchor to the first known model only when a later total row exists.
        model_positions = [m.start() for model in MODEL_NAMES for m in re.finditer(re.escape(model), text, flags=re.I)]
        total_pos = text.find("TỔNG")
        candidates = [p for p in model_positions if total_pos < 0 or p < total_pos]
        table_start = min(candidates) if candidates and total_pos > min(candidates) else -1
    if table_start < 0 or "TỔNG" not in text[table_start:]:
        return []
    table_end = text.find("Chia sẻ", table_start)
    segment = text[table_start:table_end if table_end > table_start else len(text)]

    hits = []
    for m in re.finditer("TỔNG", segment, flags=re.I):
        hits.append((m.start(), m.end(), "__TOTAL_DELIMITER__", "TỔNG"))
    for model in MODEL_NAMES:
        for m in re.finditer(re.escape(model), segment, flags=re.I):
            canonical = CANONICAL_MODEL.get(model.lower(), model)
            hits.append((m.start(), m.end(), canonical, model))
    # Keep first occurrence for each canonical model in the table segment.
    hits = sorted(hits, key=lambda x: (x[0], -len(x[3])))
    dedup = []
    seen = set()
    for h in hits:
        if h[2] not in seen:
            dedup.append(h)
            seen.add(h[2])
    hits = dedup

    source_total, source_ytd = source_totals(text)
    rows = []
    for idx, (start_pos, end_pos, canonical, matched) in enumerate(hits):
        next_pos = hits[idx + 1][0] if idx + 1 < len(hits) else len(segment)
        chunk = segment[start_pos:next_pos]
        if canonical == "__TOTAL_DELIMITER__":
            continue
        total_delim = re.search(r"TỔNG", chunk, flags=re.I)
        if total_delim:
            chunk = chunk[:total_delim.start()]
        nums = [n.group(0) for n in NUM_RE.finditer(chunk)]
        parsed = [parse_int_token(n) for n in nums]
        parsed = [n for n in parsed if n is not None]
        monthly_total = None
        ytd_total = None
        if len(parsed) >= 2:
            if report_month.endswith("-01") and len(parsed) == 2:
                # January pages often show prior December and current January only;
                # YTD equals current-month sales, not the prior December value.
                monthly_total = parsed[-1]
                ytd_total = parsed[-1]
            else:
                monthly_total = parsed[-2]
                ytd_total = parsed[-1]
        elif len(parsed) == 1:
            # Rows flattened as `-- -- N` carry YTD-only retired/other buckets.
            monthly_total = 0
            ytd_total = parsed[0]
        else:
            continue

        # Rare TC Group page typo/flattening case: commercial row `1.34` is missing trailing zero;
        # body prose and total row make 1,340 clear. Repair only this exact safe case.
        if canonical == "XE THƯƠNG MẠI" and monthly_total is not None and monthly_total < 200 and source_total:
            body = re.search(r"(?:xe\s+thương\s+mại|dòng\s+xe\s+thương\s+mại)[^.]{0,140}?([0-9][0-9\.,]*)\s+xe", text, flags=re.I)
            body_val = parse_int_token(body.group(1)) if body else None
            if body_val and body_val > monthly_total:
                monthly_total = body_val

        evidence = chunk[:500]
        doc_id = hashlib.sha256(f"{raw.source_id}|{report_month}|{canonical}".encode("utf-8")).hexdigest()
        rows.append(Row(
            document_id=doc_id,
            document_url=raw.url,
            filename=urlparse(raw.url).path.rsplit("/", 1)[-1],
            report_year=report_year,
            report_month=report_month,
            report_start_date=start,
            report_end_date=end,
            maker="Hyundai",
            model_name=canonical,
            vama_classification=None,
            seat=None,
            monthly_north=None,
            monthly_central=None,
            monthly_south=None,
            monthly_total=int(monthly_total) if monthly_total is not None else None,
            ytd_north=None,
            ytd_central=None,
            ytd_south=None,
            ytd_total=int(ytd_total) if ytd_total is not None else None,
            source_table_index=0 if table_start >= 0 else None,
            source_row_index=idx + 1,
            extracted_timestamp=dt.datetime.utcnow(),
            parsing_method="deterministic_flattened_tcgroup_table_v1" + ("_month_mismatch" if month_mismatch else ""),
            source_id=raw.source_id,
            source_title=None,
            source_domain=urlparse(raw.url).netloc,
            regional_granularity=None,
            is_commercial_vehicle=(canonical == "XE THƯƠNG MẠI"),
            is_other_bucket=(canonical == "Mẫu khác"),
            cbu_ckd=("CKD/CBU" if canonical == "XE THƯƠNG MẠI" and "CKD/CBU" in chunk else ("CBU" if " CBU " in f" {chunk} " else ("CKD" if " CKD " in f" {chunk} " else None))),
            extraction_confidence=0.90 if table_start >= 0 and not month_mismatch else 0.60,
            validation_status="warning" if month_mismatch else None,
            validation_message=(f"Source text month {found_month} differs from candidate {report_month}" if month_mismatch else None),
            raw_evidence=evidence,
            created_at=dt.datetime.utcnow(),
            updated_at=dt.datetime.utcnow(),
        ))
    return rows

# COMMAND ----------

raw_rows = spark.sql(f"""
SELECT r.source_id, r.report_month, r.url, r.fetch_status, r.http_status, r.raw_html, r.extracted_text
FROM {FULL_SCHEMA}.hyundai_raw_sources r
JOIN {FULL_SCHEMA}.hyundai_source_candidates c ON r.source_id = c.source_id
WHERE c.is_selected = true AND r.fetch_status = 'ok' AND r.http_status BETWEEN 200 AND 299
ORDER BY r.report_month
""").collect()

out = []
for raw in raw_rows:
    out.extend(extract_rows(raw))

selected_ids = [r.source_id for r in raw_rows]
if selected_ids:
    ids_sql = ",".join(["'" + x.replace("'", "''") + "'" for x in selected_ids])
    spark.sql(f"DELETE FROM {FULL_SCHEMA}.hyundai_sales_by_model WHERE source_id IN ({ids_sql})")

if out:
    target_schema = spark.table(f"{FULL_SCHEMA}.hyundai_sales_by_model").schema
    df = spark.createDataFrame(out, schema=target_schema)
    df.createOrReplaceTempView("new_hyundai_sales_by_model")
    spark.sql(f"INSERT INTO {FULL_SCHEMA}.hyundai_sales_by_model SELECT * FROM new_hyundai_sales_by_model")

print(f"Fetched ok sources parsed: {len(raw_rows)}; extracted rows: {len(out)}")
display(spark.sql(f"""
SELECT report_month, model_name, monthly_total, ytd_total, is_commercial_vehicle, is_other_bucket, raw_evidence
FROM {FULL_SCHEMA}.hyundai_sales_by_model
ORDER BY report_month, source_row_index
"""))
