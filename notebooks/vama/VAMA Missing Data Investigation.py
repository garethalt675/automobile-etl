# Databricks notebook source
# DBTITLE 1,Search for Toyota/Suzuki in ALL parsed JSONs for 2021-06
# MAGIC %sql
# MAGIC -- Check which parsed documents contain Toyota or Suzuki
# MAGIC SELECT document_id, 
# MAGIC        filename, 
# MAGIC        report_type,
# MAGIC        CASE 
# MAGIC          WHEN LOWER(parsed_json) LIKE '%toyota%' THEN 'CONTAINS Toyota'
# MAGIC          ELSE 'NO Toyota'
# MAGIC        END as has_toyota,
# MAGIC        CASE 
# MAGIC          WHEN LOWER(parsed_json) LIKE '%suzuki%' THEN 'CONTAINS Suzuki'
# MAGIC          ELSE 'NO Suzuki'
# MAGIC        END as has_suzuki
# MAGIC FROM market_data.vama.parsed_documents_raw
# MAGIC WHERE report_year = 2021 AND report_month = 6
# MAGIC ORDER BY report_type

# COMMAND ----------

# DBTITLE 1,Phase 1: Check document status for 2021-06
# MAGIC %sql
# MAGIC -- Check if documents exist and their processing status
# MAGIC SELECT document_id, filename, report_type, 
# MAGIC        download_status, parse_status, extraction_status,
# MAGIC        download_error_message, parse_error_message, extraction_error_message
# MAGIC FROM market_data.vama.document_processing_log
# MAGIC WHERE report_year = 2021 AND report_month = 6
# MAGIC ORDER BY filename

# COMMAND ----------

# DBTITLE 1,Phase 2: Search for Toyota/Suzuki in raw extracted tables
# MAGIC %sql
# MAGIC -- Check if Toyota and Suzuki exist in raw extracted cells
# MAGIC SELECT document_id, filename, table_index, row_index, column_name, cell_value
# MAGIC FROM market_data.vama.extracted_tables_long
# MAGIC WHERE document_id IN (
# MAGIC   SELECT document_id 
# MAGIC   FROM market_data.vama.document_processing_log
# MAGIC   WHERE report_year = 2021 AND report_month = 6
# MAGIC )
# MAGIC AND (LOWER(cell_value) LIKE '%toyota%' OR LOWER(cell_value) LIKE '%suzuki%')
# MAGIC ORDER BY document_id, table_index, row_index

# COMMAND ----------

# DBTITLE 1,Check what makers ARE in Detail.pdf for 2021-06
# MAGIC %sql
# MAGIC -- See what makers were extracted from the Detail PDF
# MAGIC SELECT DISTINCT cell_value AS maker
# MAGIC FROM market_data.vama.extracted_tables_long
# MAGIC WHERE document_id = '366ff22025ed17db' -- Detail.pdf for June 2021
# MAGIC   AND column_name IN ('col_0', 'Maker', 'maker')
# MAGIC   AND cell_value IS NOT NULL
# MAGIC   AND cell_value != ''
# MAGIC ORDER BY maker

# COMMAND ----------

# DBTITLE 1,Check monthly totals to identify all problematic months
# MAGIC %sql
# MAGIC -- Identify months with low totals or missing major makers
# MAGIC SELECT report_month,
# MAGIC        COUNT(DISTINCT maker) AS maker_count,
# MAGIC        COUNT(DISTINCT model_name) AS model_count,
# MAGIC        SUM(monthly_total) AS total_monthly_sales,
# MAGIC        SUM(ytd_total) AS total_ytd_sales
# MAGIC FROM market_data.vama.sales_by_model_region
# MAGIC GROUP BY report_month
# MAGIC ORDER BY report_month

# COMMAND ----------

# DBTITLE 1,Compare document types: 2021-06 vs recent complete month
# MAGIC %sql
# MAGIC -- Compare what document types exist for incomplete vs complete months
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN report_year = 2021 AND report_month = 6 THEN '2021-06 (INCOMPLETE)'
# MAGIC     WHEN report_year = 2026 AND report_month = 3 THEN '2026-03 (COMPLETE)'
# MAGIC   END AS month_category,
# MAGIC   report_type,
# MAGIC   filename
# MAGIC FROM market_data.vama.document_processing_log
# MAGIC WHERE (report_year = 2021 AND report_month = 6)
# MAGIC    OR (report_year = 2026 AND report_month = 3)
# MAGIC ORDER BY month_category, report_type

# COMMAND ----------

# DBTITLE 1,Check makers in 2026-03 Detail.pdf (complete month)
# MAGIC %sql
# MAGIC -- See what makers are in the 2026-03 Detail PDF
# MAGIC SELECT DISTINCT cell_value AS maker
# MAGIC FROM market_data.vama.extracted_tables_long
# MAGIC WHERE document_id IN (
# MAGIC     SELECT document_id 
# MAGIC     FROM market_data.vama.document_processing_log
# MAGIC     WHERE report_year = 2026 AND report_month = 3 AND report_type = 'detail'
# MAGIC   )
# MAGIC   AND column_name IN ('col_0', 'Maker', 'maker')
# MAGIC   AND cell_value IS NOT NULL
# MAGIC   AND cell_value != ''
# MAGIC   AND cell_value NOT RLIKE '^[0-9]+$' -- Exclude pure numbers
# MAGIC ORDER BY maker

# COMMAND ----------

# DBTITLE 1,Check for separate Toyota/Suzuki PDFs we might be missing
# MAGIC %sql
# MAGIC -- Check all report_types across all months to see if there are brand-specific PDFs we're not processing
# MAGIC SELECT DISTINCT report_type, COUNT(*) as month_count
# MAGIC FROM market_data.vama.document_processing_log
# MAGIC GROUP BY report_type
# MAGIC ORDER BY report_type

# COMMAND ----------

# DBTITLE 1,Check what's in the 'other' report type
# MAGIC %sql
# MAGIC -- See what documents are classified as 'other'
# MAGIC SELECT report_year, report_month, filename, extraction_status
# MAGIC FROM market_data.vama.document_processing_log
# MAGIC WHERE report_type = 'other'
# MAGIC ORDER BY report_year, report_month
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,ERROR SUMMARY: Root cause of missing data
# MAGIC %md
# MAGIC ## Investigation Findings: Missing Toyota/Suzuki/Mitsubishi Data
# MAGIC
# MAGIC ### USER VERIFICATION
# MAGIC **User manually confirmed**: The actual PDF files DO contain Toyota, Suzuki, and Mitsubishi data.
# MAGIC
# MAGIC ### PRIMARY ERROR: ai_parse_document Timeout/Error (CRITICAL)
# MAGIC **Location**: `2_Parse_Documents` notebook - PDF parsing stage
# MAGIC
# MAGIC **Evidence**:
# MAGIC - Detail.pdf for 2021-06 shows `BarnacleTimeoutError` in parsed_json
# MAGIC - Parsing failed on page 0 before reaching Toyota/Suzuki/Mitsubishi sections
# MAGIC - Last parsed: 2026-05-07 (17 days ago) - no retry attempted
# MAGIC - Parsing status marked as "success" despite timeout
# MAGIC
# MAGIC **Scope**: SYSTEMIC ISSUE
# MAGIC - **100+ months** of Detail.pdf files have timeout or error status
# MAGIC - 2024-2026: Mostly "ERROR" status (partial parsing)
# MAGIC - 2022-2024: Mostly "TIMEOUT" status (early failure)
# MAGIC - 2021 and earlier: Mix of TIMEOUT and ERROR
# MAGIC
# MAGIC **Impact**:
# MAGIC - Major manufacturers missing from 26+ months (Toyota, Suzuki, Mitsubishi, Ford)
# MAGIC - Estimated 30-50% of sales data not extracted due to incomplete parsing
# MAGIC - Silent failures - pipeline marks documents as "success" even with errors
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### SECONDARY ERROR: Hardcoded Filter Blocks Brand-Specific PDFs
# MAGIC **Location**: `3_Extract_Tables` notebook, function `extract_sales_rows()`
# MAGIC
# MAGIC **Code Issue**:
# MAGIC ```python
# MAGIC if doc['report_type'] != 'detail':
# MAGIC     return rows  # Blocks Lexus.pdf and BMW-Mini.pdf
# MAGIC ```
# MAGIC
# MAGIC **Impact**: Even when brand PDFs (Lexus, BMW-Mini) are successfully parsed, they're never loaded into sales_by_model_region
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### TERTIARY ERROR: No Parsing Validation
# MAGIC **Location**: ETL pipeline design
# MAGIC
# MAGIC **Issue**: No checks for:
# MAGIC - Error status in parsed JSON
# MAGIC - Expected manufacturer count (15-20)
# MAGIC - Minimum sales threshold (20,000+)
# MAGIC - Presence of major brands (Toyota, Ford, Honda)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## RECOMMENDATIONS
# MAGIC
# MAGIC ### IMMEDIATE ACTION REQUIRED
# MAGIC 1. **Re-parse ALL Detail.pdf files** with increased timeout (300+ seconds)
# MAGIC 2. **Add validation** to check for error_status in parsed JSON
# MAGIC 3. **Update extraction logic** to process Lexus.pdf and BMW-Mini.pdf
# MAGIC 4. **Implement retry logic** for failed parses
# MAGIC
# MAGIC ### Expected Recovery
# MAGIC - Toyota data: 26+ months
# MAGIC - Suzuki data: 26+ months  
# MAGIC - Mitsubishi/Ford/Isuzu: 15-20 months
# MAGIC - Lexus models: 144 months
# MAGIC - BMW-Mini models: 63 months
# MAGIC
# MAGIC **Total estimated recovery**: 30-50% increase in extracted sales data

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from market_data.vama.parsed_documents_raw
# MAGIC where report_month_key = '2021-06'

# COMMAND ----------

# DBTITLE 1,Check for parsing errors in Detail.pdf JSON
# MAGIC %sql
# MAGIC -- Check if there are parsing errors or timeouts mentioned in the JSON
# MAGIC SELECT 
# MAGIC   document_id,
# MAGIC   filename,
# MAGIC   report_type,
# MAGIC   LENGTH(parsed_json) as json_size,
# MAGIC   CASE 
# MAGIC     WHEN parsed_json LIKE '%BarnacleTimeoutError%' THEN 'TIMEOUT ERROR'
# MAGIC     WHEN parsed_json LIKE '%error_status%' THEN 'HAS ERROR STATUS'
# MAGIC     ELSE 'NO ERROR DETECTED'
# MAGIC   END as parsing_status,
# MAGIC   CASE WHEN parsed_json LIKE '%toyota%' THEN 'YES' ELSE 'NO' END as has_toyota,
# MAGIC   CASE WHEN parsed_json LIKE '%suzuki%' THEN 'YES' ELSE 'NO' END as has_suzuki,
# MAGIC   CASE WHEN parsed_json LIKE '%mitsubishi%' THEN 'YES' ELSE 'NO' END as has_mitsubishi
# MAGIC FROM market_data.vama.parsed_documents_raw
# MAGIC WHERE report_year = 2021 AND report_month = 6
# MAGIC ORDER BY report_type

# COMMAND ----------

# DBTITLE 1,Check timeout errors across ALL months
# MAGIC %sql
# MAGIC -- Count how many months have timeout errors in Detail.pdf parsing
# MAGIC SELECT 
# MAGIC   report_month_key,
# MAGIC   filename,
# MAGIC   CASE 
# MAGIC     WHEN parsed_json LIKE '%BarnacleTimeoutError%' THEN 'TIMEOUT'
# MAGIC     WHEN parsed_json LIKE '%error_status%' THEN 'ERROR'
# MAGIC     ELSE 'OK'
# MAGIC   END as status,
# MAGIC   LENGTH(parsed_json) as json_size
# MAGIC FROM market_data.vama.parsed_documents_raw
# MAGIC WHERE report_type = 'detail'
# MAGIC   AND (parsed_json LIKE '%BarnacleTimeoutError%' OR parsed_json LIKE '%error%')
# MAGIC ORDER BY report_month_key DESC
# MAGIC LIMIT 50

# COMMAND ----------

# DBTITLE 1,Check parsing timestamps and attempts
# MAGIC %sql
# MAGIC -- Check when Detail.pdf was parsed and if there were multiple attempts
# MAGIC SELECT 
# MAGIC   document_id,
# MAGIC   filename,
# MAGIC   report_type,
# MAGIC   parsed_timestamp,
# MAGIC   DATEDIFF(CURRENT_TIMESTAMP(), parsed_timestamp) as days_since_parse
# MAGIC FROM market_data.vama.parsed_documents_raw
# MAGIC WHERE report_year = 2021 AND report_month = 6
# MAGIC   AND report_type = 'detail'
# MAGIC ORDER BY parsed_timestamp DESC

# COMMAND ----------

# DBTITLE 1,Extract Detail.pdf parsed JSON to examine raw content
# MAGIC %sql
# MAGIC -- Get the parsed JSON for Detail.pdf to see if Toyota/Suzuki are actually there
# MAGIC SELECT document_id, filename, report_type, 
# MAGIC        LENGTH(parsed_json) as json_length,
# MAGIC        parsed_json
# MAGIC FROM market_data.vama.parsed_documents_raw
# MAGIC WHERE report_year = 2021 AND report_month = 6 
# MAGIC   AND report_type = 'detail'
# MAGIC LIMIT 1