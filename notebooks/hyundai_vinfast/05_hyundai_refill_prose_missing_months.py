# Databricks notebook source
# MAGIC %md
# MAGIC # Hyundai prose-style missing-month refill
# MAGIC
# MAGIC Reproducible refill for official TC Group Hyundai monthly sales pages that contain model-level prose but no extractable flattened table.
# MAGIC
# MAGIC Current covered official pages:
# MAGIC - 2024-09
# MAGIC - 2024-10
# MAGIC - 2024-11
# MAGIC - 2024-12
# MAGIC - 2025-01
# MAGIC - 2025-10
# MAGIC
# MAGIC The refill:
# MAGIC - reads only selected official `hyundai_raw_sources` / `hyundai_source_candidates`
# MAGIC - extracts explicit model monthly totals using model-name-anchored regex
# MAGIC - inserts `Mẫu khác` residual only when source monthly total exceeds explicit rows
# MAGIC - promotes monthly validation to `pass` only when extracted sum reconciles exactly to official monthly total

# COMMAND ----------
FULL_SCHEMA = "market_data.hyundai_vinfast"
TARGET_MONTHS = ["2024-09", "2024-10", "2024-11", "2024-12", "2025-01", "2025-10"]
months_sql = ",".join([f"'{m}'" for m in TARGET_MONTHS])

# COMMAND ----------
# The replacement rows are materialised BEFORE anything is deleted, and only the
# months that actually produced rows are cleared.
#
# This notebook used to open with an unconditional
#     DELETE ... WHERE report_month IN (all six TARGET_MONTHS)
# followed by an INSERT that re-derived the rows from hyundai_raw_sources. That is
# only safe while every source page still fetches. On 2026-07-30 the pages for
# 2024-12 and 2025-10 had gone unretrievable (fetch_status='error', empty
# extracted_text), so the delete removed them and the insert produced nothing -
# both months vanished from hyundai_sales_by_model. Because hyundai_extract runs
# first and re-inserts only what it can parse, nothing else put them back.
#
# The monthly-total casts below are wrapped in NULLIF(..., ''): regexp_extract
# returns '' rather than NULL when a page fetches fine but matches no pattern, and
# CAST('' AS INT) raises CAST_INVALID_INPUT. That is now the normal state of the
# 2024-12 and 2025-10 pages, which serve a generic shell.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW hyundai_prose_refill_rows AS
WITH raw AS (
  SELECT r.source_id, r.report_month, r.url, r.extracted_text,
         c.report_year,
         to_date(concat(r.report_month,'-01')) AS report_start_date,
         last_day(to_date(concat(r.report_month,'-01'))) AS report_end_date,
         CAST(NULLIF(regexp_replace(regexp_extract(r.extracted_text, 't.{{0,20}}ng doanh s.{{0,20}} xe Hyundai th.{{0,20}}ng [0-9]{{1,2}} .{{0,20}}t ([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe', 1), '[^0-9]', ''), '') AS INT) AS source_total
  FROM {FULL_SCHEMA}.hyundai_raw_sources r
  JOIN {FULL_SCHEMA}.hyundai_source_candidates c ON r.source_id=c.source_id
  WHERE r.report_month IN ({months_sql})
    AND c.is_selected = true
), patterns AS (
  SELECT * FROM VALUES
    ('Hyundai Accent', false, false, 'Hyundai Accent[^.]{{0,220}}?([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe'),
    ('Hyundai Creta', false, false, 'Hyundai Creta[^.]{{0,220}}?([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe'),
    ('Hyundai Santa Fe', false, false, 'Hyundai Santa Fe[^.]{{0,260}}?([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe'),
    ('Hyundai Grand i10', false, false, 'Hyundai Grand i10[^.]{{0,260}}?([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe'),
    ('Hyundai Tucson', false, false, 'Hyundai Tucson[^.]{{0,220}}?([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe'),
    ('Hyundai Venue', false, false, 'Hyundai Venue[^.]{{0,220}}?([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe'),
    ('Hyundai Stargazer', false, false, 'Hyundai Stargazer[^.]{{0,220}}?([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe'),
    ('Hyundai Custin', false, false, 'Hyundai Custin[^.]{{0,220}}?([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe'),
    ('Hyundai Elantra', false, false, 'Hyundai Elantra[^.]{{0,260}}?([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe'),
    ('Hyundai Palisade', false, false, 'Hyundai Palisade[^.]{{0,260}}?([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe'),
    ('XE THƯƠNG MẠI', true, false, 'm.{{0,12}}u xe th.{{0,50}}ng m.{{0,30}}i Hyundai[^.]{{0,260}}?([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe')
  AS patterns(model_name, is_commercial_vehicle, is_other_bucket, pat)
), explicit AS (
  SELECT
    sha2(concat(raw.source_id,'|',raw.report_month,'|',patterns.model_name),256) AS document_id,
    raw.url AS document_url,
    element_at(split(parse_url(raw.url,'PATH'),'/'),-1) AS filename,
    raw.report_year,
    raw.report_month,
    raw.report_start_date,
    raw.report_end_date,
    'Hyundai' AS maker,
    patterns.model_name,
    CAST(NULL AS STRING) AS vama_classification,
    CAST(NULL AS STRING) AS seat,
    CAST(NULL AS INT) AS monthly_north,
    CAST(NULL AS INT) AS monthly_central,
    CAST(NULL AS INT) AS monthly_south,
    CAST(regexp_replace(regexp_extract(raw.extracted_text, patterns.pat, 1), '[^0-9]', '') AS INT) AS monthly_total,
    CAST(NULL AS INT) AS ytd_north,
    CAST(NULL AS INT) AS ytd_central,
    CAST(NULL AS INT) AS ytd_south,
    CAST(NULL AS INT) AS ytd_total,
    CAST(NULL AS INT) AS source_table_index,
    row_number() OVER (PARTITION BY raw.report_month ORDER BY patterns.model_name) AS source_row_index,
    current_timestamp() AS extracted_timestamp,
    'deterministic_tcgroup_prose_sql_refill_v1' AS parsing_method,
    raw.source_id,
    CAST(NULL AS STRING) AS source_title,
    parse_url(raw.url,'HOST') AS source_domain,
    CAST(NULL AS STRING) AS regional_granularity,
    patterns.is_commercial_vehicle,
    patterns.is_other_bucket,
    CAST(NULL AS STRING) AS cbu_ckd,
    CAST(0.82 AS DOUBLE) AS extraction_confidence,
    'warning' AS validation_status,
    'Official TC Group prose fallback; YTD not available in prose row' AS validation_message,
    regexp_extract(raw.extracted_text, patterns.pat, 0) AS raw_evidence,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
  FROM raw CROSS JOIN patterns
  WHERE regexp_extract(raw.extracted_text, patterns.pat, 1) <> ''
), residual AS (
  SELECT
    sha2(concat(raw.source_id,'|',raw.report_month,'|Mẫu khác'),256) AS document_id,
    raw.url AS document_url,
    element_at(split(parse_url(raw.url,'PATH'),'/'),-1) AS filename,
    raw.report_year,
    raw.report_month,
    raw.report_start_date,
    raw.report_end_date,
    'Hyundai' AS maker,
    'Mẫu khác' AS model_name,
    CAST(NULL AS STRING) AS vama_classification,
    CAST(NULL AS STRING) AS seat,
    CAST(NULL AS INT) AS monthly_north,
    CAST(NULL AS INT) AS monthly_central,
    CAST(NULL AS INT) AS monthly_south,
    CAST(raw.source_total - SUM(explicit.monthly_total) AS INT) AS monthly_total,
    CAST(NULL AS INT) AS ytd_north,
    CAST(NULL AS INT) AS ytd_central,
    CAST(NULL AS INT) AS ytd_south,
    CAST(NULL AS INT) AS ytd_total,
    CAST(NULL AS INT) AS source_table_index,
    99 AS source_row_index,
    current_timestamp() AS extracted_timestamp,
    'deterministic_tcgroup_prose_residual_sql_refill_v1' AS parsing_method,
    raw.source_id,
    CAST(NULL AS STRING) AS source_title,
    parse_url(raw.url,'HOST') AS source_domain,
    CAST(NULL AS STRING) AS regional_granularity,
    false AS is_commercial_vehicle,
    true AS is_other_bucket,
    CAST(NULL AS STRING) AS cbu_ckd,
    CAST(0.70 AS DOUBLE) AS extraction_confidence,
    'warning' AS validation_status,
    concat('Computed residual from source total ', raw.source_total, ' minus explicit prose rows ', SUM(explicit.monthly_total)) AS validation_message,
    concat('Mẫu khác residual = ', raw.source_total, ' - ', SUM(explicit.monthly_total), ' = ', raw.source_total - SUM(explicit.monthly_total)) AS raw_evidence,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
  FROM raw JOIN explicit ON raw.report_month=explicit.report_month AND raw.source_id=explicit.source_id
  GROUP BY raw.source_id, raw.report_month, raw.url, raw.report_year, raw.report_start_date, raw.report_end_date, raw.source_total
  HAVING raw.source_total - SUM(explicit.monthly_total) > 0
)
SELECT * FROM explicit
UNION ALL
SELECT * FROM residual
""")

refill_months = [r[0] for r in spark.sql(
    "SELECT DISTINCT report_month FROM hyundai_prose_refill_rows ORDER BY report_month").collect()]
skipped = [m for m in TARGET_MONTHS if m not in refill_months]
print(f"prose refill produced rows for: {refill_months}")
if skipped:
    print(f"NO rows derived for {skipped} - their existing rows are being left "
          f"untouched rather than deleted. Check hyundai_raw_sources.fetch_status "
          f"for those months; an unretrievable source page is the usual cause.")

if refill_months:
    refill_months_sql = ",".join(f"'{m}'" for m in refill_months)
    spark.sql(f"""
    DELETE FROM {FULL_SCHEMA}.hyundai_sales_by_model
    WHERE report_month IN ({refill_months_sql})
    """)
    spark.sql(f"""
    INSERT INTO {FULL_SCHEMA}.hyundai_sales_by_model
    SELECT * FROM hyundai_prose_refill_rows
    """)

# COMMAND ----------
spark.sql(f"""
MERGE INTO {FULL_SCHEMA}.hyundai_monthly_validation t
USING (
  SELECT
    c.report_month,
    c.source_id AS selected_source_id,
    CAST(NULLIF(regexp_replace(regexp_extract(r.extracted_text, 't.{{0,20}}ng doanh s.{{0,20}} xe Hyundai th.{{0,20}}ng [0-9]{{1,2}} .{{0,20}}t ([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe', 1), '[^0-9]', ''), '') AS INT) AS source_total,
    CAST(SUM(h.monthly_total) AS INT) AS sum_model_monthly_total,
    CAST(NULL AS INT) AS source_ytd_total,
    CAST(NULL AS INT) AS sum_model_ytd_total,
    CAST(NULL AS INT) AS previous_ytd_total,
    CAST(NULL AS INT) AS derived_current_from_ytd,
    CASE WHEN CAST(NULLIF(regexp_replace(regexp_extract(r.extracted_text, 't.{{0,20}}ng doanh s.{{0,20}} xe Hyundai th.{{0,20}}ng [0-9]{{1,2}} .{{0,20}}t ([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe', 1), '[^0-9]', ''), '') AS INT) = CAST(SUM(h.monthly_total) AS INT)
         THEN 'pass' ELSE 'fail' END AS validation_status,
    CASE WHEN CAST(NULLIF(regexp_replace(regexp_extract(r.extracted_text, 't.{{0,20}}ng doanh s.{{0,20}} xe Hyundai th.{{0,20}}ng [0-9]{{1,2}} .{{0,20}}t ([0-9]{{1,3}}(?:[.][0-9]{{3}})+|[0-9]+) xe', 1), '[^0-9]', ''), '') AS INT) = CAST(SUM(h.monthly_total) AS INT)
         THEN 'Monthly model rows reconcile exactly to official TC Group monthly total; YTD unavailable in prose page, accepted as monthly-only refill.'
         ELSE 'Prose refill monthly rows do not reconcile to official source total.' END AS validation_message,
    current_timestamp() AS checked_at
  FROM {FULL_SCHEMA}.hyundai_source_candidates c
  JOIN {FULL_SCHEMA}.hyundai_raw_sources r ON c.source_id=r.source_id
  JOIN {FULL_SCHEMA}.hyundai_sales_by_model h ON c.source_id=h.source_id AND c.report_month=h.report_month
  WHERE c.report_month IN ({months_sql}) AND c.is_selected=true
  GROUP BY c.report_month, c.source_id, r.extracted_text
) s
ON t.report_month=s.report_month AND t.selected_source_id=s.selected_source_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
result = spark.sql(f"""
SELECT report_month, COUNT(*) AS rows, SUM(monthly_total) AS monthly_total_sum
FROM {FULL_SCHEMA}.hyundai_sales_by_model
WHERE report_month IN ({months_sql})
GROUP BY report_month
ORDER BY report_month
""").collect()
print([r.asDict() for r in result])
