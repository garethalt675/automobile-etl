# Databricks notebook source
# MAGIC %md
# MAGIC # Create schema and common Hyundai/VinFast tables
# MAGIC Generated from `specs_hyundai_vinfast_sales_workflow.md`.

# COMMAND ----------

CATALOG = "market_data"
SCHEMA = "hyundai_vinfast"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA} COMMENT 'Standalone Hyundai and VinFast monthly vehicle sales workflow'")
spark.sql(f"USE {FULL_SCHEMA}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.hyundai_source_candidates (
  source_id STRING NOT NULL,
  report_year INT,
  report_month_int INT,
  report_month STRING,
  url STRING,
  title STRING,
  source_domain STRING,
  source_type STRING,
  source_priority INT,
  discovered_by STRING,
  pattern_name STRING,
  candidate_rank INT,
  is_selected BOOLEAN,
  selection_reason STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.hyundai_raw_sources (
  source_id STRING NOT NULL,
  report_month STRING,
  url STRING,
  fetch_status STRING,
  http_status INT,
  fetched_at TIMESTAMP,
  content_type STRING,
  raw_html STRING,
  extracted_text STRING,
  content_hash STRING,
  error_message STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.hyundai_sales_by_model (
  document_id STRING NOT NULL,
  document_url STRING,
  filename STRING,
  report_year INT,
  report_month STRING,
  report_start_date DATE,
  report_end_date DATE,
  maker STRING,
  model_name STRING,
  vama_classification STRING,
  seat STRING,
  monthly_north INT,
  monthly_central INT,
  monthly_south INT,
  monthly_total INT,
  ytd_north INT,
  ytd_central INT,
  ytd_south INT,
  ytd_total INT,
  source_table_index INT,
  source_row_index INT,
  extracted_timestamp TIMESTAMP,
  parsing_method STRING,
  source_id STRING,
  source_title STRING,
  source_domain STRING,
  regional_granularity STRING,
  is_commercial_vehicle BOOLEAN,
  is_other_bucket BOOLEAN,
  cbu_ckd STRING,
  extraction_confidence DOUBLE,
  validation_status STRING,
  validation_message STRING,
  raw_evidence STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.hyundai_monthly_validation (
  report_month STRING,
  selected_source_id STRING,
  source_total INT,
  sum_model_monthly_total INT,
  source_ytd_total INT,
  sum_model_ytd_total INT,
  previous_ytd_total INT,
  derived_current_from_ytd INT,
  validation_status STRING,
  validation_message STRING,
  checked_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.vinfast_source_candidates (
  source_id STRING NOT NULL,
  report_year INT,
  report_month_int INT,
  report_month STRING,
  url STRING,
  title STRING,
  source_domain STRING,
  source_type STRING,
  source_priority INT,
  discovered_by STRING,
  search_query STRING,
  candidate_rank INT,
  is_selected BOOLEAN,
  selection_reason STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.vinfast_raw_sources (
  source_id STRING NOT NULL,
  report_month STRING,
  url STRING,
  fetch_status STRING,
  http_status INT,
  fetched_at TIMESTAMP,
  content_type STRING,
  raw_html STRING,
  extracted_text STRING,
  content_hash STRING,
  error_message STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.vinfast_sales_by_model (
  document_id STRING NOT NULL,
  document_url STRING,
  filename STRING,
  report_year INT,
  report_month STRING,
  report_start_date DATE,
  report_end_date DATE,
  maker STRING,
  model_name STRING,
  vama_classification STRING,
  seat STRING,
  monthly_north INT,
  monthly_central INT,
  monthly_south INT,
  monthly_total INT,
  ytd_north INT,
  ytd_central INT,
  ytd_south INT,
  ytd_total INT,
  source_table_index INT,
  source_row_index INT,
  extracted_timestamp TIMESTAMP,
  parsing_method STRING,
  source_id STRING,
  source_title STRING,
  source_domain STRING,
  source_type STRING,
  regional_granularity STRING,
  is_official_source BOOLEAN,
  is_total_only_row BOOLEAN,
  extraction_confidence DOUBLE,
  validation_status STRING,
  validation_message STRING,
  raw_evidence STRING,
  llm_model STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.vinfast_monthly_validation (
  report_month STRING,
  selected_source_id STRING,
  source_total INT,
  sum_model_monthly_total INT,
  source_ytd_total INT,
  sum_model_ytd_total INT,
  previous_ytd_total INT,
  derived_current_from_ytd INT,
  selected_source_type STRING,
  source_conflict_count INT,
  confidence_reason STRING,
  validation_status STRING,
  validation_message STRING,
  checked_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

for table_name in [
    'hyundai_source_candidates','hyundai_raw_sources','hyundai_sales_by_model','hyundai_monthly_validation',
    'vinfast_source_candidates','vinfast_raw_sources','vinfast_sales_by_model','vinfast_monthly_validation'
]:
    print(table_name)
    spark.sql(f"DESCRIBE TABLE {FULL_SCHEMA}.{table_name}").show(80, truncate=False)
