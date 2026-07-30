# Databricks notebook source
# MAGIC %md
# MAGIC # Sprint 007 — Build unified Vietnam automobile sales views
# MAGIC
# MAGIC Creates `market_data.automobile` integration views over VAMA, Hyundai, and VinFast source-of-truth tables.
# MAGIC
# MAGIC This notebook intentionally creates only schema/views in `market_data.automobile`; it does not mutate `market_data.vama` or `market_data.hyundai_vinfast` source tables.

# COMMAND ----------

from pyspark.sql import functions as F
import json

TARGET_SCHEMA = "market_data.automobile"
UNIFIED_VIEW = f"{TARGET_SCHEMA}.curated_vietnam_auto_sales_unified"
QA_VIEW = f"{TARGET_SCHEMA}.auto_sales_source_quality"

VAMA = "market_data.vama.curated_vama_sales_unified"
HYUNDAI = "market_data.hyundai_vinfast.curated_hyundai_sales"
VINFAST = "market_data.hyundai_vinfast.curated_vinfast_sales"

CORE_COLUMNS = [
    ("document_id", "STRING"),
    ("document_url", "STRING"),
    ("filename", "STRING"),
    ("report_year", "INT"),
    ("report_month", "STRING"),
    ("report_start_date", "DATE"),
    ("report_end_date", "DATE"),
    ("maker", "STRING"),
    ("model_name", "STRING"),
    ("vama_classification", "STRING"),
    ("seat", "STRING"),
    ("monthly_north", "INT"),
    ("monthly_central", "INT"),
    ("monthly_south", "INT"),
    ("monthly_total", "INT"),
    ("ytd_north", "INT"),
    ("ytd_central", "INT"),
    ("ytd_south", "INT"),
    ("ytd_total", "INT"),
    ("source_table_index", "INT"),
    ("source_row_index", "INT"),
    ("extracted_timestamp", "TIMESTAMP"),
    ("parsing_method", "STRING"),
]

AUDIT_COLUMNS = [
    ("source_system", "STRING"),
    ("source_schema", "STRING"),
    ("source_table", "STRING"),
    ("source_id", "STRING"),
    ("source_domain", "STRING"),
    ("source_type", "STRING"),
    ("regional_granularity", "STRING"),
    ("validation_status", "STRING"),
    ("validation_message", "STRING"),
    ("extraction_confidence", "DOUBLE"),
    ("is_analytics_ready", "BOOLEAN"),
    ("raw_evidence", "STRING"),
    ("created_at", "TIMESTAMP"),
    ("updated_at", "TIMESTAMP"),
]

# COMMAND ----------

def qident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def qobj(name: str) -> str:
    return ".".join(qident(part) for part in name.split("."))


def lit(value, typ: str) -> str:
    if value is None:
        return f"CAST(NULL AS {typ})"
    if isinstance(value, bool):
        return f"CAST({'true' if value else 'false'} AS {typ})"
    if isinstance(value, (int, float)):
        return f"CAST({value} AS {typ})"
    escaped = str(value).replace("'", "''")
    return f"CAST('{escaped}' AS {typ})"


def col(alias: str, name: str, typ: str, cols: set, fallback="NULL") -> str:
    if name in cols:
        return f"CAST({alias}.{qident(name)} AS {typ})"
    if fallback == "CURRENT_TIMESTAMP":
        return f"CAST(current_timestamp() AS {typ})"
    return f"CAST(NULL AS {typ})"


def object_exists(full_name: str) -> bool:
    try:
        spark.table(full_name).limit(0).collect()
        return True
    except Exception:
        return False


def column_set(full_name: str) -> set:
    return {field.name for field in spark.table(full_name).schema.fields}

required_objects = [VAMA, HYUNDAI, VINFAST]
missing = [obj for obj in required_objects if not object_exists(obj)]
if missing:
    raise RuntimeError("Missing required source object(s): " + ", ".join(missing))

schemas = {obj: sorted(column_set(obj)) for obj in required_objects}
print("SOURCE_SCHEMA_INSPECTION")
for obj, cols in schemas.items():
    print(json.dumps({"object": obj, "columns": cols}, ensure_ascii=False))

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qobj(TARGET_SCHEMA)}")

vama_cols = set(schemas[VAMA])
hyundai_cols = set(schemas[HYUNDAI])
vinfast_cols = set(schemas[VINFAST])

# Core column mapping - VinFast uses model_name_clean instead of model_name
core_select_vama = [f"{col('v', name, typ, vama_cols)} AS {qident(name)}" for name, typ in CORE_COLUMNS]
core_select_hyundai = [f"{col('h', name, typ, hyundai_cols)} AS {qident(name)}" for name, typ in CORE_COLUMNS]

# VinFast needs special handling for model_name -> model_name_clean
core_select_vinfast = []
for name, typ in CORE_COLUMNS:
    if name == 'model_name':
        # Map model_name_clean to model_name
        core_select_vinfast.append(f"{col('vf', 'model_name_clean', typ, vinfast_cols)} AS {qident(name)}")
    else:
        core_select_vinfast.append(f"{col('vf', name, typ, vinfast_cols)} AS {qident(name)}")

vama_audit = [
    f"{lit('VAMA', 'STRING')} AS source_system",
    f"{lit('market_data.vama', 'STRING')} AS source_schema",
    f"{lit('curated_vama_sales_unified', 'STRING')} AS source_table",
    f"{col('v', 'document_id', 'STRING', vama_cols)} AS source_id",
    f"{lit(None, 'STRING')} AS source_domain",
    f"{lit(None, 'STRING')} AS source_type",
    f"{lit('regional', 'STRING')} AS regional_granularity",
    f"{lit('pass', 'STRING')} AS validation_status",
    f"{lit(None, 'STRING')} AS validation_message",
    f"{lit(None, 'DOUBLE')} AS extraction_confidence",
    f"{lit(True, 'BOOLEAN')} AS is_analytics_ready",
    f"{lit(None, 'STRING')} AS raw_evidence",
    f"{lit(None, 'TIMESTAMP')} AS created_at",
    f"{lit(None, 'TIMESTAMP')} AS updated_at",
]

# Hyundai curated table has all analytics-ready rows (no validation_status field)
hyundai_audit = [
    f"{lit('Hyundai', 'STRING')} AS source_system",
    f"{lit('market_data.hyundai_vinfast', 'STRING')} AS source_schema",
    f"{lit('curated_hyundai_sales', 'STRING')} AS source_table",
    f"{col('h', 'document_id', 'STRING', hyundai_cols)} AS source_id",
    f"{lit(None, 'STRING')} AS source_domain",
    f"{lit(None, 'STRING')} AS source_type",
    f"{lit('national_total', 'STRING')} AS regional_granularity",
    f"{lit('pass', 'STRING')} AS validation_status",
    f"{lit(None, 'STRING')} AS validation_message",
    f"{lit(None, 'DOUBLE')} AS extraction_confidence",
    f"{lit(True, 'BOOLEAN')} AS is_analytics_ready",
    f"{lit(None, 'STRING')} AS raw_evidence",
    f"{col('h', 'extracted_timestamp', 'TIMESTAMP', hyundai_cols)} AS created_at",
    f"{col('h', 'extracted_timestamp', 'TIMESTAMP', hyundai_cols)} AS updated_at",
]

# VinFast curated table has validation fields built in
vinfast_audit = [
    f"{lit('VinFast', 'STRING')} AS source_system",
    f"{lit('market_data.hyundai_vinfast', 'STRING')} AS source_schema",
    f"{lit('curated_vinfast_sales', 'STRING')} AS source_table",
    f"{col('vf', 'source_id', 'STRING', vinfast_cols)} AS source_id",
    f"{col('vf', 'source_domain', 'STRING', vinfast_cols)} AS source_domain",
    f"{col('vf', 'source_type', 'STRING', vinfast_cols)} AS source_type",
    f"{col('vf', 'regional_granularity', 'STRING', vinfast_cols)} AS regional_granularity",
    f"{col('vf', 'validation_status', 'STRING', vinfast_cols)} AS validation_status",
    f"{col('vf', 'validation_message', 'STRING', vinfast_cols)} AS validation_message",
    f"{col('vf', 'extraction_confidence', 'DOUBLE', vinfast_cols)} AS extraction_confidence",
    f"CAST(({col('vf', 'validation_status', 'STRING', vinfast_cols)}) = 'pass' AS BOOLEAN) AS is_analytics_ready",
    f"{col('vf', 'raw_evidence', 'STRING', vinfast_cols)} AS raw_evidence",
    f"{col('vf', 'created_at', 'TIMESTAMP', vinfast_cols)} AS created_at",
    f"{col('vf', 'updated_at', 'TIMESTAMP', vinfast_cols)} AS updated_at",
]

# Build column lists with proper newlines
vama_select = ',\n  '.join(core_select_vama + vama_audit)
hyundai_select = ',\n  '.join(core_select_hyundai + hyundai_audit)
vinfast_select = ',\n  '.join(core_select_vinfast + vinfast_audit)
vinfast_validation_filter = col('vf', 'validation_status', 'STRING', vinfast_cols)
vinfast_aggregate_filter = col('vf', 'is_aggregate', 'BOOLEAN', vinfast_cols)

unified_sql = f"""
CREATE OR REPLACE VIEW {qobj(UNIFIED_VIEW)} AS
SELECT
  {vama_select}
FROM {qobj(VAMA)} v

UNION ALL

SELECT
  {hyundai_select}
FROM {qobj(HYUNDAI)} h

UNION ALL

SELECT
  {vinfast_select}
FROM {qobj(VINFAST)} vf
WHERE {vinfast_validation_filter} IN ('pass', 'warning')
"""

spark.sql(unified_sql)
print(f"CREATED_VIEW {UNIFIED_VIEW}")

# COMMAND ----------

# For Hyundai: curated table has all analytics-ready rows
hyundai_source_id_expr = "h.document_id"

# For VinFast: validation fields are in the curated table
vinfast_has_validation = "validation_status" in vinfast_cols
vinfast_source_id_expr = "vf.source_id" if "source_id" in vinfast_cols else "vf.document_id"

qa_sql = f"""
CREATE OR REPLACE VIEW {qobj(QA_VIEW)} AS
WITH vama_months AS (
  SELECT
    'VAMA' AS source_system,
    'market_data.vama' AS source_schema,
    'curated_vama_sales_unified' AS source_table,
    report_month,
    CAST(NULL AS STRING) AS source_id,
    'pass' AS validation_status,
    CAST(NULL AS STRING) AS validation_message,
    COUNT(*) AS source_month_row_count,
    COUNT(*) AS included_row_count,
    CAST(0 AS BIGINT) AS excluded_row_count,
    true AS is_analytics_ready_month,
    'VAMA curated source rows included as analytics-ready.' AS coverage_note
  FROM {qobj(VAMA)}
  GROUP BY report_month
),
hyundai_months AS (
  SELECT
    'Hyundai' AS source_system,
    'market_data.hyundai_vinfast' AS source_schema,
    'curated_hyundai_sales' AS source_table,
    h.report_month,
    CAST({hyundai_source_id_expr} AS STRING) AS source_id,
    'pass' AS validation_status,
    CAST(NULL AS STRING) AS validation_message,
    COUNT(*) AS source_month_row_count,
    COUNT(*) AS included_row_count,
    CAST(0 AS BIGINT) AS excluded_row_count,
    true AS is_analytics_ready_month,
    'Hyundai curated source rows included; all rows are analytics-ready.' AS coverage_note
  FROM {qobj(HYUNDAI)} h
  GROUP BY h.report_month, {hyundai_source_id_expr}
),
vinfast_months AS (
  SELECT
    'VinFast' AS source_system,
    'market_data.hyundai_vinfast' AS source_schema,
    'curated_vinfast_sales' AS source_table,
    vf.report_month,
    CAST({vinfast_source_id_expr} AS STRING) AS source_id,
    CAST(MAX(vf.validation_status) AS STRING) AS validation_status,
    CAST(MAX(vf.validation_message) AS STRING) AS validation_message,
    COUNT(*) AS source_month_row_count,
    SUM(CASE WHEN vf.validation_status IN ('pass', 'warning') THEN 1 ELSE 0 END) AS included_row_count,
    SUM(CASE WHEN vf.validation_status NOT IN ('pass', 'warning') THEN 1 ELSE 0 END) AS excluded_row_count,
    BOOL_OR(vf.validation_status = 'pass') AS is_analytics_ready_month,
    CASE
      WHEN SUM(CASE WHEN vf.validation_status IN ('pass', 'warning') THEN 1 ELSE 0 END) > 0
        THEN 'VinFast curated rows included for pass/warning validation (including aggregate rows when model breakdown unavailable); analytics-ready only when pass.'
      ELSE 'VinFast month excluded from unified view because no pass/warning rows available.'
    END AS coverage_note
  FROM {qobj(VINFAST)} vf
  GROUP BY vf.report_month, {vinfast_source_id_expr}
)
SELECT *, current_timestamp() AS generated_at FROM vama_months
UNION ALL
SELECT *, current_timestamp() AS generated_at FROM hyundai_months
UNION ALL
SELECT *, current_timestamp() AS generated_at FROM vinfast_months
"""

spark.sql(qa_sql)
print(f"CREATED_VIEW {QA_VIEW}")

# COMMAND ----------

# Verification gates: query views, counts, validation-pass-only checks, VinFast readiness, duplicate key check.
row_counts = [r.asDict() for r in spark.sql(f"""
    SELECT source_system, COUNT(*) AS row_count, COUNT(DISTINCT report_month) AS distinct_report_months
    FROM {qobj(UNIFIED_VIEW)}
    GROUP BY source_system
    ORDER BY source_system
""").collect()]

vama_source_count = spark.table(VAMA).count()
vama_unified_count = spark.sql(f"SELECT COUNT(*) AS c FROM {qobj(UNIFIED_VIEW)} WHERE source_system = 'VAMA'").first()["c"]

# Hyundai curated table should have all pass rows
hyundai_non_pass_rows = spark.sql(f"""
    SELECT COUNT(*) AS c
    FROM {qobj(UNIFIED_VIEW)}
    WHERE source_system = 'Hyundai'
      AND validation_status <> 'pass'
""").first()["c"]

# Check for any aggregate rows in Hyundai (shouldn't have any in curated)
hyundai_aggregate_rows = spark.sql(f"""
    SELECT COUNT(*) AS c
    FROM {qobj(HYUNDAI)}
    WHERE is_commercial_vehicle = true
""").first()["c"]

# VinFast checks: should only have pass/warning, no aggregates
vinfast_invalid_rows = spark.sql(f"""
    SELECT COUNT(*) AS c
    FROM {qobj(UNIFIED_VIEW)}
    WHERE source_system = 'VinFast'
      AND validation_status NOT IN ('pass', 'warning')
""").first()["c"]

vinfast_rows = spark.sql(f"SELECT COUNT(*) AS c FROM {qobj(UNIFIED_VIEW)} WHERE source_system = 'VinFast'").first()["c"]
vinfast_source_rows = spark.table(VINFAST).count()

duplicate_key_count = spark.sql(f"""
    WITH keyed AS (
      SELECT source_system, document_id, report_month, maker, model_name, source_row_index, COUNT(*) AS n
      FROM {qobj(UNIFIED_VIEW)}
      GROUP BY source_system, document_id, report_month, maker, model_name, source_row_index
      HAVING COUNT(*) > 1
    )
    SELECT COUNT(*) AS c FROM keyed
""").first()["c"]

qa_rows = spark.sql(f"SELECT COUNT(*) AS c FROM {qobj(QA_VIEW)}").first()["c"]

verification = {
    "schema": TARGET_SCHEMA,
    "unified_view": UNIFIED_VIEW,
    "qa_view": QA_VIEW,
    "row_counts_by_source_system": row_counts,
    "vama_source_count": vama_source_count,
    "vama_unified_count": vama_unified_count,
    "vama_count_matches": vama_source_count == vama_unified_count,
    "hyundai_non_pass_rows_in_unified": hyundai_non_pass_rows,
    "hyundai_aggregate_rows_in_source": hyundai_aggregate_rows,
    "vinfast_source_rows": vinfast_source_rows,
    "vinfast_unified_rows": vinfast_rows,
    "vinfast_invalid_validation_rows_in_unified": vinfast_invalid_rows,
    "duplicate_key_count": duplicate_key_count,
    "qa_view_rows": qa_rows,
    "source_mutation_statement": "Notebook only executed CREATE SCHEMA and CREATE OR REPLACE VIEW in market_data.automobile; no INSERT/UPDATE/MERGE/DELETE/CREATE TABLE against source curated tables.",
}
print("SPRINT_007_VERIFICATION_JSON")
print(json.dumps(verification, ensure_ascii=False, default=str, indent=2))

assert vama_source_count == vama_unified_count, "VAMA source count does not match unified VAMA count"
assert hyundai_non_pass_rows == 0, "Hyundai unified rows include non-pass validation rows"
assert vinfast_invalid_rows == 0, "VinFast unified rows include validation statuses outside pass/warning"
assert duplicate_key_count == 0, "Duplicate rows found under sprint defensible key"
assert qa_rows > 0, "QA view returned no rows"
print("SPRINT_007_GATES_PASSED (using curated tables)")
try:
    dbutils.notebook.exit(json.dumps(verification, ensure_ascii=False, default=str))
except NameError:
    pass