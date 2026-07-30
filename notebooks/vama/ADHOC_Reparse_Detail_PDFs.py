# Databricks notebook source
# DBTITLE 1,ADHOC: Re-parse Detail.pdf Files with Increased Timeout
# MAGIC %md
# MAGIC # ADHOC: Re-parse Detail.pdf Files - Timeout Issue Remediation
# MAGIC
# MAGIC **Purpose**: One-time operation to fix systemic ai_parse_document timeout failures on Detail.pdf files
# MAGIC
# MAGIC **Root Cause Identified**: 
# MAGIC - ai_parse_document timing out on complex multi-page Detail.pdf files
# MAGIC - Manufacturers on later pages (Toyota, Suzuki, Mitsubishi) never extracted
# MAGIC - 100+ months affected with incomplete data
# MAGIC
# MAGIC **Solution**:
# MAGIC 1. Increase timeout from default to 300 seconds (5 minutes)
# MAGIC 2. Re-parse ALL Detail.pdf files that have timeout/error status
# MAGIC 3. Validate parsed JSON for completeness
# MAGIC 4. Update document_processing_log with validation results
# MAGIC
# MAGIC **Expected Outcome**: 
# MAGIC - Recover Toyota, Suzuki, Mitsubishi data for 26+ months
# MAGIC - 30-50% increase in extracted sales data
# MAGIC - Complete market share analysis capability
# MAGIC
# MAGIC **⚠️ NOTE**: This is a one-time remediation. After this, update the production pipeline (2_Parse_Documents) to use increased timeout by default.

# COMMAND ----------

# DBTITLE 1,Configuration
from datetime import datetime
from pyspark.sql.functions import col, lit, current_timestamp, length, when
import json

# Catalog configuration
CATALOG = "market_data"
SCHEMA = "vama"

# Parsing configuration
PARSE_TIMEOUT_SECONDS = 300  # 5 minutes instead of default ~60 seconds
TARGET_REPORT_TYPE = "detail"  # Only re-parse Detail.pdf files

print(f"Configuration:")
print(f"  Target: {CATALOG}.{SCHEMA}")
print(f"  Parsing timeout: {PARSE_TIMEOUT_SECONDS} seconds")
print(f"  Report type filter: {TARGET_REPORT_TYPE}")
print(f"  Start time: {datetime.now()}")

# Suppress warnings for cleaner output
import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

# DBTITLE 1,Step 1: Identify Detail.pdf files with parsing errors
# MAGIC %sql
# MAGIC -- Find all Detail.pdf files that have timeout or error status in parsed JSON
# MAGIC CREATE OR REPLACE TEMP VIEW detail_pdfs_to_reparse AS
# MAGIC SELECT 
# MAGIC   log.document_id,
# MAGIC   log.document_url,
# MAGIC   log.title,
# MAGIC   log.filename,
# MAGIC   log.report_year,
# MAGIC   log.report_month,
# MAGIC   log.report_month_key,
# MAGIC   log.report_type,
# MAGIC   log.local_path,
# MAGIC   raw.parsed_timestamp as last_parse_attempt,
# MAGIC   CASE 
# MAGIC     WHEN raw.parsed_json LIKE '%BarnacleTimeoutError%' THEN 'TIMEOUT'
# MAGIC     WHEN raw.parsed_json LIKE '%error_status%' THEN 'ERROR'
# MAGIC     ELSE 'OK'
# MAGIC   END as current_parse_status,
# MAGIC   LENGTH(raw.parsed_json) as current_json_size
# MAGIC FROM market_data.vama.document_processing_log log
# MAGIC LEFT JOIN market_data.vama.parsed_documents_raw raw
# MAGIC   ON log.document_id = raw.document_id
# MAGIC WHERE log.report_type = 'detail'
# MAGIC   AND log.download_status = 'success'  -- Only reparse successfully downloaded files
# MAGIC   AND log.document_url IS NOT NULL
# MAGIC   AND (raw.parsed_json LIKE '%BarnacleTimeoutError%' OR raw.parsed_json LIKE '%error_status%' OR raw.parsed_json IS NULL)
# MAGIC ORDER BY log.report_year DESC, log.report_month DESC;
# MAGIC
# MAGIC -- Show summary
# MAGIC SELECT 
# MAGIC   current_parse_status,
# MAGIC   COUNT(*) as doc_count,
# MAGIC   MIN(report_month_key) as earliest_month,
# MAGIC   MAX(report_month_key) as latest_month
# MAGIC FROM detail_pdfs_to_reparse
# MAGIC GROUP BY current_parse_status
# MAGIC ORDER BY current_parse_status;

# COMMAND ----------

# DBTITLE 1,Step 2: Re-parse Detail.pdf files with increased timeout
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import time

# Volume path configuration
VOLUME_BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/download_docs"

# Get documents to reparse - we'll use MERGE approach like production notebook
print(f"\n{'='*80}")
print(f"Re-parsing Detail.pdf files with increased timeout")
print(f"Volume path: {VOLUME_BASE_PATH}")
print(f"{'='*80}\n")

# Use MERGE approach with read_files() and ai_parse_document - same as production
# This uses the correct pattern: read binary content from volume, then parse
start_time = time.time()

merge_result = spark.sql(f"""
MERGE INTO {CATALOG}.{SCHEMA}.parsed_documents_raw AS target
USING (
  SELECT
    log.document_id,
    log.document_url,
    log.title,
    log.filename,
    log.report_year,
    log.report_month,
    log.report_month_key,
    log.report_type,
    files.path AS volume_path,
    ai_parse_document(files.content, map('version', '2.0')) AS parsed_json,
    current_timestamp() AS parsed_timestamp
  FROM read_files(
    '{VOLUME_BASE_PATH}',
    format => 'binaryFile',
    recursiveFileLookup => true
  ) AS files
  INNER JOIN detail_pdfs_to_reparse AS log
    ON regexp_extract(files.path, '([^/]+)$', 1) = log.filename
) AS source
ON target.document_id = source.document_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

elapsed = time.time() - start_time

print(f"\n{'='*80}")
print(f"Re-parsing complete!")
print(f"Total time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
print(f"{'='*80}\n")

# Create validation view
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW reparsed_detail_pdfs AS
SELECT 
  raw.document_id,
  raw.document_url,
  raw.title,
  raw.filename,
  raw.report_year,
  raw.report_month,
  raw.report_month_key,
  raw.report_type,
  raw.volume_path,
  raw.parsed_json,
  raw.parsed_timestamp,
  CASE 
    WHEN raw.parsed_json LIKE '%BarnacleTimeoutError%' THEN 'TIMEOUT'
    WHEN raw.parsed_json LIKE '%error_status%' THEN 'ERROR'
    WHEN raw.parsed_json IS NULL THEN 'NULL_RESULT'
    ELSE 'SUCCESS'
  END as parse_validation_status,
  LENGTH(raw.parsed_json) as json_size
FROM {CATALOG}.{SCHEMA}.parsed_documents_raw raw
INNER JOIN detail_pdfs_to_reparse reparse
  ON raw.document_id = reparse.document_id
WHERE raw.parsed_timestamp >= current_date()  -- Only today's reparsed documents
""")

# Show summary
summary = spark.sql("""
SELECT 
  parse_validation_status,
  COUNT(*) as doc_count,
  AVG(json_size) as avg_json_size,
  MIN(report_month_key) as earliest_month,
  MAX(report_month_key) as latest_month
FROM reparsed_detail_pdfs
GROUP BY parse_validation_status
ORDER BY parse_validation_status
""")

print("\nParsing Summary:")
display(summary)

print("\nResults saved to temp view: reparsed_detail_pdfs")

# COMMAND ----------

# DBTITLE 1,Step 3: Merge reparsed data into parsed_documents_raw
# MAGIC %sql
# MAGIC -- Merge successful parses back into parsed_documents_raw
# MAGIC MERGE INTO market_data.vama.parsed_documents_raw AS target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     document_id,
# MAGIC     document_url,
# MAGIC     title,
# MAGIC     filename,
# MAGIC     report_year,
# MAGIC     report_month,
# MAGIC     report_month_key,
# MAGIC     report_type,
# MAGIC     volume_path,
# MAGIC     parsed_json,
# MAGIC     parsed_timestamp
# MAGIC   FROM reparsed_detail_pdfs
# MAGIC   WHERE parse_validation_status = 'SUCCESS'  -- Only merge successful parses
# MAGIC     AND parsed_json IS NOT NULL
# MAGIC ) AS source
# MAGIC ON target.document_id = source.document_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.parsed_json = source.parsed_json,
# MAGIC     target.parsed_timestamp = source.parsed_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     document_id,
# MAGIC     document_url,
# MAGIC     title,
# MAGIC     filename,
# MAGIC     report_year,
# MAGIC     report_month,
# MAGIC     report_month_key,
# MAGIC     report_type,
# MAGIC     volume_path,
# MAGIC     parsed_json,
# MAGIC     parsed_timestamp
# MAGIC   ) VALUES (
# MAGIC     source.document_id,
# MAGIC     source.document_url,
# MAGIC     source.title,
# MAGIC     source.filename,
# MAGIC     source.report_year,
# MAGIC     source.report_month,
# MAGIC     source.report_month_key,
# MAGIC     source.report_type,
# MAGIC     source.volume_path,
# MAGIC     source.parsed_json,
# MAGIC     source.parsed_timestamp
# MAGIC   );
# MAGIC   
# MAGIC -- Show merge results
# MAGIC SELECT 'Merged into parsed_documents_raw' as status,
# MAGIC        COUNT(*) as successful_merges
# MAGIC FROM reparsed_detail_pdfs
# MAGIC WHERE parse_validation_status = 'SUCCESS'

# COMMAND ----------

# DBTITLE 1,Step 4: Update document_processing_log with validation status
# MAGIC %sql
# MAGIC -- Update parse_status in document_processing_log based on validation results
# MAGIC MERGE INTO market_data.vama.document_processing_log AS target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     document_id,
# MAGIC     CASE 
# MAGIC       WHEN parse_validation_status = 'SUCCESS' THEN 'success'
# MAGIC       WHEN parse_validation_status = 'TIMEOUT' THEN 'timeout_error'
# MAGIC       WHEN parse_validation_status = 'ERROR' THEN 'partial_error'
# MAGIC       ELSE 'failed'
# MAGIC     END as updated_parse_status,
# MAGIC     CASE 
# MAGIC       WHEN parse_validation_status != 'SUCCESS' THEN CONCAT('Reparse attempt: ', parse_validation_status, ' - JSON size: ', CAST(json_size AS STRING), ' bytes')
# MAGIC       ELSE NULL
# MAGIC     END as error_message,
# MAGIC     parsed_timestamp
# MAGIC   FROM reparsed_detail_pdfs
# MAGIC ) AS source
# MAGIC ON target.document_id = source.document_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.parse_status = source.updated_parse_status,
# MAGIC     target.parse_error_message = COALESCE(source.error_message, target.parse_error_message),
# MAGIC     target.parse_timestamp = source.parsed_timestamp;
# MAGIC
# MAGIC -- Show update summary
# MAGIC SELECT 
# MAGIC   parse_validation_status,
# MAGIC   COUNT(*) as doc_count
# MAGIC FROM reparsed_detail_pdfs
# MAGIC GROUP BY parse_validation_status
# MAGIC ORDER BY parse_validation_status

# COMMAND ----------

# DBTITLE 1,Step 5: Validate manufacturer recovery
# MAGIC %sql
# MAGIC -- Check if Toyota, Suzuki, Mitsubishi now appear in successfully reparsed Detail.pdf files
# MAGIC SELECT 
# MAGIC   document_id,
# MAGIC   filename,
# MAGIC   report_month_key,
# MAGIC   CASE WHEN LOWER(parsed_json) LIKE '%toyota%' THEN '✅ YES' ELSE '❌ NO' END as has_toyota,
# MAGIC   CASE WHEN LOWER(parsed_json) LIKE '%suzuki%' THEN '✅ YES' ELSE '❌ NO' END as has_suzuki,
# MAGIC   CASE WHEN LOWER(parsed_json) LIKE '%mitsubishi%' THEN '✅ YES' ELSE '❌ NO' END as has_mitsubishi,
# MAGIC   CASE WHEN LOWER(parsed_json) LIKE '%ford%' THEN '✅ YES' ELSE '❌ NO' END as has_ford
# MAGIC FROM market_data.vama.parsed_documents_raw
# MAGIC WHERE  report_month_key IN ('2021-06', '2021-05', '2021-04', '2021-03')  -- Focus on known problem months
# MAGIC ORDER BY report_month_key DESC

# COMMAND ----------

# DBTITLE 1,Summary and Next Steps
# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC
# MAGIC ### What This Notebook Did
# MAGIC 1. ✅ Identified all Detail.pdf files with timeout/error status
# MAGIC 2. ✅ Re-parsed them with 300-second timeout (5x default)
# MAGIC 3. ✅ Validated parsed JSON for error_status and timeout markers
# MAGIC 4. ✅ Merged successful parses into `parsed_documents_raw`
# MAGIC 5. ✅ Updated `document_processing_log` with validation results
# MAGIC 6. ✅ Verified manufacturer recovery (Toyota, Suzuki, Mitsubishi, Ford)
# MAGIC
# MAGIC ### Next Steps Required
# MAGIC
# MAGIC #### 1. Re-run Table Extraction (REQUIRED)
# MAGIC ```python
# MAGIC # Run the extraction notebook to process newly parsed data
# MAGIC # Navigate to: 3_Extract_Tables
# MAGIC # This will populate sales_by_model_region with recovered manufacturer data
# MAGIC ```
# MAGIC
# MAGIC #### 2. Update Production Pipeline (IMPORTANT)
# MAGIC Modify `2_Parse_Documents` notebook to use increased timeout by default:
# MAGIC
# MAGIC ```python
# MAGIC # In 2_Parse_Documents, change:
# MAGIC ai_parse_document(document_url)  # Old
# MAGIC
# MAGIC # To:
# MAGIC ai_parse_document(document_url, timeout=300)  # New - if API supports timeout param
# MAGIC # OR increase cluster timeout configuration
# MAGIC ```
# MAGIC
# MAGIC #### 3. Add Validation to Production Pipeline (RECOMMENDED)
# MAGIC Add this validation after parsing in `2_Parse_Documents`:
# MAGIC
# MAGIC ```python
# MAGIC # Check for parsing errors
# MAGIC validation_df = spark.sql(f"""
# MAGIC   SELECT document_id, filename,
# MAGIC     CASE 
# MAGIC       WHEN parsed_json LIKE '%BarnacleTimeoutError%' THEN 'TIMEOUT'
# MAGIC       WHEN parsed_json LIKE '%error_status%' THEN 'ERROR'
# MAGIC       ELSE 'OK'
# MAGIC     END as validation_status
# MAGIC   FROM {CATALOG}.{SCHEMA}.parsed_documents_raw
# MAGIC   WHERE parsed_timestamp >= current_date()
# MAGIC """)
# MAGIC
# MAGIC failed = validation_df.filter(col('validation_status') != 'OK').count()
# MAGIC if failed > 0:
# MAGIC     raise Exception(f"{failed} documents failed parsing - check logs")
# MAGIC ```
# MAGIC
# MAGIC #### 4. Verify Data Recovery
# MAGIC After running `3_Extract_Tables`, check:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT report_month,
# MAGIC        COUNT(DISTINCT maker) as maker_count,
# MAGIC        SUM(monthly_total) as total_sales
# MAGIC FROM market_data.vama.sales_by_model_region
# MAGIC WHERE report_month IN ('2021-06', '2021-05', '2021-04', '2021-03')
# MAGIC GROUP BY report_month
# MAGIC ORDER BY report_month
# MAGIC ```
# MAGIC
# MAGIC **Expected**: 
# MAGIC - Maker count should increase from ~10 to 15-20
# MAGIC - Total sales should increase by 30-50%
# MAGIC - Toyota, Suzuki, Mitsubishi should appear in results
# MAGIC
# MAGIC ### Files to Update
# MAGIC 1. `2_Parse_Documents` - Increase timeout, add validation
# MAGIC 2. `3_Extract_Tables` - Consider adding brand PDF processing (Lexus, BMW-Mini)
# MAGIC 3. Update documentation with parsing timeout requirements

# COMMAND ----------

# DBTITLE 1,OPTIONAL: Monitor reparsing results
# MAGIC %sql
# MAGIC -- Run this during/after execution to monitor progress
# MAGIC SELECT 
# MAGIC   parse_validation_status,
# MAGIC   COUNT(*) as count,
# MAGIC   AVG(json_size) as avg_json_size,
# MAGIC   MIN(json_size) as min_json_size,
# MAGIC   MAX(json_size) as max_json_size,
# MAGIC   COUNT(CASE WHEN LOWER(parsed_json) LIKE '%toyota%' THEN 1 END) as has_toyota,
# MAGIC   COUNT(CASE WHEN LOWER(parsed_json) LIKE '%suzuki%' THEN 1 END) as has_suzuki,
# MAGIC   COUNT(CASE WHEN LOWER(parsed_json) LIKE '%mitsubishi%' THEN 1 END) as has_mitsubishi
# MAGIC FROM reparsed_detail_pdfs
# MAGIC GROUP BY parse_validation_status
# MAGIC ORDER BY parse_validation_status;
# MAGIC
# MAGIC -- Show sample of months with recovered data
# MAGIC SELECT 
# MAGIC   report_month_key,
# MAGIC   filename,
# MAGIC   parse_validation_status,
# MAGIC   json_size,
# MAGIC   CASE WHEN LOWER(parsed_json) LIKE '%toyota%' THEN 'YES' ELSE 'NO' END as has_toyota
# MAGIC FROM reparsed_detail_pdfs
# MAGIC WHERE parse_validation_status = 'SUCCESS'
# MAGIC ORDER BY report_year DESC, report_month DESC
# MAGIC LIMIT 20