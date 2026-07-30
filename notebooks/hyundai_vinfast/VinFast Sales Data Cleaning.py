# Databricks notebook source
# DBTITLE 1,Verify model name cleaning - before and after
# MAGIC %sql
# MAGIC -- Verify the cleaning: show original vs cleaned names
# MAGIC SELECT DISTINCT
# MAGIC   model_name_original,
# MAGIC   model_name_clean,
# MAGIC   is_aggregate,
# MAGIC   COUNT(*) as record_count
# MAGIC FROM market_data.hyundai_vinfast.curated_vinfast_sales
# MAGIC GROUP BY model_name_original, model_name_clean, is_aggregate
# MAGIC ORDER BY is_aggregate, model_name_clean

# COMMAND ----------

# DBTITLE 1,Create curated VinFast sales view with cleaned model names
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW market_data.hyundai_vinfast.curated_vinfast_sales AS
# MAGIC SELECT 
# MAGIC   document_id,
# MAGIC   document_url,
# MAGIC   filename,
# MAGIC   report_year,
# MAGIC   report_month,
# MAGIC   report_start_date,
# MAGIC   report_end_date,
# MAGIC   maker,
# MAGIC   
# MAGIC   -- Cleaned model name: strip "VinFast " prefix and standardize
# MAGIC   TRIM(REGEXP_REPLACE(model_name, '^VinFast\\s+', '')) AS model_name_clean,
# MAGIC   
# MAGIC   -- Original model name for reference
# MAGIC   model_name AS model_name_original,
# MAGIC   
# MAGIC   -- Flag for aggregate/combined entries
# MAGIC   CASE 
# MAGIC     WHEN UPPER(model_name) LIKE '%TOTAL%' THEN TRUE
# MAGIC     WHEN UPPER(model_name) LIKE '%OTHER%' THEN TRUE
# MAGIC     WHEN model_name LIKE '%,%' THEN TRUE
# MAGIC     WHEN model_name LIKE '%(combined)%' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_aggregate,
# MAGIC   
# MAGIC   vama_classification,
# MAGIC   seat,
# MAGIC   monthly_north,
# MAGIC   monthly_central,
# MAGIC   monthly_south,
# MAGIC   monthly_total,
# MAGIC   ytd_north,
# MAGIC   ytd_central,
# MAGIC   ytd_south,
# MAGIC   ytd_total,
# MAGIC   source_table_index,
# MAGIC   source_row_index,
# MAGIC   extracted_timestamp,
# MAGIC   parsing_method,
# MAGIC   source_id,
# MAGIC   source_title,
# MAGIC   source_domain,
# MAGIC   source_type,
# MAGIC   regional_granularity,
# MAGIC   is_official_source,
# MAGIC   is_total_only_row,
# MAGIC   extraction_confidence,
# MAGIC   validation_status,
# MAGIC   validation_message,
# MAGIC   raw_evidence,
# MAGIC   llm_model,
# MAGIC   created_at,
# MAGIC   updated_at
# MAGIC   
# MAGIC FROM market_data.hyundai_vinfast.vinfast_sales_by_model