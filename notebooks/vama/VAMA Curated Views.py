# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # VAMA Curated Sales View - BI/Genie Ready Layer
# MAGIC
# MAGIC This notebook creates a unified analytical view combining vehicle sales data from two source tables:
# MAGIC - `market_data.vama.sales_by_model_region` - Detailed model-level sales data
# MAGIC - `market_data.vama.sales_by_other_makers` - Aggregated "Other Makers" sales data
# MAGIC
# MAGIC ## Purpose
# MAGIC Provide a single, clean source of truth for BI dashboards and Genie spaces by:
# MAGIC - **Unifying** both source tables into one view
# MAGIC - **Simplifying** the schema (excluding market share columns)
# MAGIC - **Tracking** data lineage with source_table column
# MAGIC - **Maintaining** row-level detail for maximum analytical flexibility
# MAGIC
# MAGIC ## View Details
# MAGIC - **Target View:** `market_data.vama.curated_vama_sales_unified`
# MAGIC - **Strategy:** UNION ALL (no overlap between source tables)
# MAGIC - **Columns:** 24 fields (23 business + 1 source tracking)

# COMMAND ----------

# DBTITLE 1,Create Unified Curated View
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW market_data.vama.curated_vama_sales_unified
# MAGIC (
# MAGIC   document_id COMMENT 'Unique identifier for the source document',
# MAGIC   document_url COMMENT 'URL of the source document',
# MAGIC   filename COMMENT 'Source document filename',
# MAGIC   report_year COMMENT 'Year of the sales report',
# MAGIC   report_month COMMENT 'Month of the sales report in YYYY-MM format',
# MAGIC   report_start_date COMMENT 'Start date of the reporting period',
# MAGIC   report_end_date COMMENT 'End date of the reporting period',
# MAGIC   maker COMMENT 'Vehicle manufacturer or brand name',
# MAGIC   model_name COMMENT 'Vehicle model name',
# MAGIC   vama_classification COMMENT 'VAMA vehicle classification or category',
# MAGIC   seat COMMENT 'Vehicle seating capacity',
# MAGIC   monthly_north COMMENT 'Monthly sales units in Northern region',
# MAGIC   monthly_central COMMENT 'Monthly sales units in Central region',
# MAGIC   monthly_south COMMENT 'Monthly sales units in Southern region',
# MAGIC   monthly_total COMMENT 'Total monthly sales units across all regions',
# MAGIC   ytd_north COMMENT 'Year-to-date sales units in Northern region',
# MAGIC   ytd_central COMMENT 'Year-to-date sales units in Central region',
# MAGIC   ytd_south COMMENT 'Year-to-date sales units in Southern region',
# MAGIC   ytd_total COMMENT 'Total year-to-date sales units across all regions',
# MAGIC   source_table_index COMMENT 'Index of source table within the document',
# MAGIC   source_row_index COMMENT 'Row index within the source table',
# MAGIC   extracted_timestamp COMMENT 'Timestamp when data was extracted from source',
# MAGIC   parsing_method COMMENT 'Method used to parse the document (html or llm)',
# MAGIC   source_table COMMENT 'Source table name: sales_by_model_region or sales_by_other_makers'
# MAGIC )
# MAGIC AS
# MAGIC
# MAGIC -- Source: Detailed model-level sales
# MAGIC SELECT 
# MAGIC   document_id,
# MAGIC   document_url,
# MAGIC   filename,
# MAGIC   report_year,
# MAGIC   report_month,
# MAGIC   report_start_date,
# MAGIC   report_end_date,
# MAGIC   maker,
# MAGIC   model_name,
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
# MAGIC   'sales_by_model_region' AS source_table
# MAGIC FROM market_data.vama.sales_by_model_region
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Source: Aggregated "Other Makers" sales
# MAGIC SELECT 
# MAGIC   document_id,
# MAGIC   document_url,
# MAGIC   filename,
# MAGIC   report_year,
# MAGIC   report_month,
# MAGIC   report_start_date,
# MAGIC   report_end_date,
# MAGIC   maker,
# MAGIC   model_name,
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
# MAGIC   'sales_by_other_makers' AS source_table
# MAGIC FROM market_data.vama.sales_by_other_makers

# COMMAND ----------

# DBTITLE 1,Add Metadata: Table and Column Comments
# MAGIC %sql
# MAGIC -- Add table-level description
# MAGIC COMMENT ON TABLE market_data.vama.curated_vama_sales_unified IS 
# MAGIC 'Unified curated view combining vehicle sales data from detailed model-level and aggregated Other Makers sources. Provides BI-ready, row-level sales metrics by maker, model, region, and time period. Excludes market share percentages. Includes data lineage tracking via source_table column.';
# MAGIC
# MAGIC SELECT 'Metadata added successfully' AS status;

# COMMAND ----------

# DBTITLE 1,Validation: Row Count Summary
# MAGIC %sql
# MAGIC -- Verify row counts match expectations
# MAGIC SELECT 
# MAGIC   source_table,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   COUNT(DISTINCT document_id) AS unique_documents,
# MAGIC   MIN(report_start_date) AS earliest_date,
# MAGIC   MAX(report_end_date) AS latest_date
# MAGIC FROM market_data.vama.curated_vama_sales_unified
# MAGIC GROUP BY source_table
# MAGIC ORDER BY source_table

# COMMAND ----------

# DBTITLE 1,Validation: Sample Data Preview
# MAGIC %sql
# MAGIC -- Sample data from each source table
# MAGIC SELECT 
# MAGIC   source_table,
# MAGIC   report_month,
# MAGIC   maker,
# MAGIC   model_name,
# MAGIC   vama_classification,
# MAGIC   monthly_total,
# MAGIC   ytd_total,
# MAGIC   parsing_method
# MAGIC FROM market_data.vama.curated_vama_sales_unified
# MAGIC WHERE report_start_date >= DATE '2026-01-01'
# MAGIC ORDER BY source_table, report_month DESC, monthly_total DESC
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Data Quality Summary by Parsing Method
# MAGIC %sql
# MAGIC -- Assess data quality across parsing methods and source tables
# MAGIC SELECT 
# MAGIC   source_table,
# MAGIC   parsing_method,
# MAGIC   COUNT(*) AS record_count,
# MAGIC   COUNT(DISTINCT report_month) AS unique_months,
# MAGIC   COUNT(DISTINCT maker) AS unique_makers,
# MAGIC   COUNT(DISTINCT model_name) AS unique_models,
# MAGIC   SUM(CASE WHEN monthly_north IS NULL THEN 1 ELSE 0 END) AS null_monthly_north,
# MAGIC   SUM(CASE WHEN monthly_total IS NULL THEN 1 ELSE 0 END) AS null_monthly_total,
# MAGIC   SUM(CASE WHEN ytd_total IS NULL THEN 1 ELSE 0 END) AS null_ytd_total
# MAGIC FROM market_data.vama.curated_vama_sales_unified
# MAGIC GROUP BY source_table, parsing_method
# MAGIC ORDER BY source_table, parsing_method