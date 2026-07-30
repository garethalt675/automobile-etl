# Databricks notebook source
# DBTITLE 1,VAMA Step 2 - Parse Documents with ai_parse_document
# MAGIC %md
# MAGIC # Step 2: Parse downloaded VAMA PDFs
# MAGIC
# MAGIC Uses Databricks `ai_parse_document()` to convert PDFs into JSON containing text and table HTML.
# MAGIC
# MAGIC Output: `market_data.vama.parsed_documents_raw`

# COMMAND ----------

CATALOG = "market_data"
SCHEMA = "vama"
VOLUME = "download_docs"
VOLUME_BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# COMMAND ----------



# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.parsed_documents_raw (
#   document_id STRING NOT NULL,
#   document_url STRING,
#   title STRING,
#   filename STRING,
#   report_year INT,
#   report_month INT,
#   report_month_key STRING,
#   report_type STRING,
#   volume_path STRING,
#   parsed_json STRING,
#   parsed_timestamp TIMESTAMP
# ) USING DELTA
# """)

# COMMAND ----------

# DBTITLE 1,Reset documents for re-parse (April 2025 onwards)
# # Reset the 12 Detail PDFs with timeout errors for re-parsing
# # These documents from April 2025 - March 2026 have BarnacleTimeoutError

# documents_to_reset = [
#     '6c400b20b6beea1a',
# '64212a0225db6757',
# '99c968efbcdf0e4c',
# '2679ac8469a0b2fb',
# 'eb42c5db6b3cfa00',
# 'b19417498a33ad86',
# 'e32d16051c34912b',
# 'f559303ca4a23f3d',
# '774f8e05999f8816',
# '4522b7b5744ce481',
# '59e352d92bcf6d76',
# '1f848a2ab05ac05a',
# 'e3aca6f403725a89',
# 'a058f255e87ce8a3',
# '7cd3d97b245a3ae5',
# '1a40b6d047f5ec3c',
# 'b6380a9e1194d30c',
# '7da4017d17edee0c',
# '1966bf68dc15caf3',
# 'd1ca0b453aa00df2',
# '01526541a1719a65',
# '9e6deb551645bd2f',
# '999f81b0203fe030',
# 'e58c7a959c664490'
# ]

# # Build the IN clause
# doc_ids_str = "','".join(documents_to_reset)

# # Step 1: Delete old failed parses from parsed_documents_raw
# deleted = spark.sql(f"""
# DELETE FROM {CATALOG}.{SCHEMA}.parsed_documents_raw
# WHERE document_id IN ('{doc_ids_str}')
# """)

# print(f"Deleted {deleted.first()['num_affected_rows']} old parse records from parsed_documents_raw")

# # Step 2: Reset parse and extraction status in processing log
# reset = spark.sql(f"""
# UPDATE {CATALOG}.{SCHEMA}.document_processing_log
# SET 
#   parse_status = NULL,
#   parse_timestamp = NULL,
#   parse_error_message = NULL,
#   extraction_status = NULL,
#   extraction_timestamp = NULL,
#   extraction_error_message = NULL,
#   extraction_rows_inserted = NULL,
#   updated_at = current_timestamp()
# WHERE document_id IN ('{doc_ids_str}')
# """)

# print(f"Reset {reset.first()['num_affected_rows']} documents in processing log")

# # Step 3: Verify reset
# verify = spark.sql(f"""
# SELECT 
#   report_month_key,
#   filename,
#   parse_status,
#   extraction_status
# FROM {CATALOG}.{SCHEMA}.document_processing_log
# WHERE document_id IN ('{doc_ids_str}')
# ORDER BY report_year, report_month
# """)

# print("\nVerification - Documents ready for re-parse:")
# display(verify)

# print("\n✓ Ready to re-run Cell 5 (Parse pending PDFs)")

# COMMAND ----------

# DBTITLE 1,Parse pending PDFs
spark.sql(f"""
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
  INNER JOIN {CATALOG}.{SCHEMA}.document_processing_log AS log
    ON regexp_extract(files.path, '([^/]+)$', 1) = log.filename
  WHERE log.download_status = 'success'
    AND (log.parse_status IS NULL OR log.parse_status IN ('pending', 'failed'))
) AS source
ON target.document_id = source.document_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# DBTITLE 1,Update processing log
# Update log for ALL successfully parsed documents (deduplicated by latest parse)
spark.sql(f"""
MERGE INTO {CATALOG}.{SCHEMA}.document_processing_log AS target
USING (
  SELECT
    document_id,
    'success' AS parse_status,
    MAX(parsed_timestamp) AS parse_timestamp,
    CAST(NULL AS STRING) AS parse_error_message,
    'pending' AS extraction_status,
    current_timestamp() AS updated_at
  FROM {CATALOG}.{SCHEMA}.parsed_documents_raw
  GROUP BY document_id
  HAVING document_id NOT IN (
    SELECT document_id FROM {CATALOG}.{SCHEMA}.document_processing_log WHERE parse_status = 'success'
  )
) AS source
ON target.document_id = source.document_id
WHEN MATCHED THEN UPDATE SET
  parse_status = source.parse_status,
  parse_timestamp = source.parse_timestamp,
  parse_error_message = source.parse_error_message,
  extraction_status = source.extraction_status,
  updated_at = source.updated_at
""")

summary = spark.sql(f"""
SELECT report_year, report_month_key, report_type, COUNT(*) AS parsed_documents
FROM {CATALOG}.{SCHEMA}.parsed_documents_raw
GROUP BY report_year, report_month_key, report_type
ORDER BY report_year DESC, report_month_key DESC, report_type
""")
display(summary)