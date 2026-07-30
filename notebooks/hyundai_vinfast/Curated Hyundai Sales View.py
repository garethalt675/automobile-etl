# Databricks notebook source
# DBTITLE 1,Create Curated Hyundai/VinFast Sales View
# MAGIC %sql
# MAGIC -- Curated view for Hyundai/VinFast sales data
# MAGIC -- Modeled after market_data.vama.curated_vama_sales_unified
# MAGIC -- Excludes document_url, filename and technical metadata
# MAGIC -- Cleans model names and keeps only BI-relevant columns
# MAGIC
# MAGIC CREATE OR REPLACE VIEW market_data.hyundai_vinfast.curated_hyundai_sales AS
# MAGIC SELECT 
# MAGIC   -- Document identifiers (for traceability)
# MAGIC   document_id,
# MAGIC   
# MAGIC   -- Temporal dimensions
# MAGIC   report_year,
# MAGIC   report_month,
# MAGIC   report_start_date,
# MAGIC   report_end_date,
# MAGIC   
# MAGIC   -- Product dimensions
# MAGIC   maker,
# MAGIC   
# MAGIC   -- Clean model name: remove redundant "Hyundai " prefix and trim whitespace
# MAGIC   TRIM(
# MAGIC     CASE 
# MAGIC       WHEN LOWER(model_name) LIKE 'hyundai %' THEN REGEXP_REPLACE(model_name, '^[Hh]yundai\\s+', '')
# MAGIC       WHEN LOWER(model_name) LIKE 'vinfast %' THEN REGEXP_REPLACE(model_name, '^[Vv]infast\\s+', '')
# MAGIC       ELSE model_name
# MAGIC     END
# MAGIC   ) AS model_name,
# MAGIC   
# MAGIC   vama_classification,
# MAGIC   seat,
# MAGIC   
# MAGIC   -- Business attributes
# MAGIC   cbu_ckd,
# MAGIC   is_commercial_vehicle,
# MAGIC   
# MAGIC   -- Monthly sales metrics by region
# MAGIC   monthly_north,
# MAGIC   monthly_central,
# MAGIC   monthly_south,
# MAGIC   monthly_total,
# MAGIC   
# MAGIC   -- Year-to-date sales metrics by region
# MAGIC   ytd_north,
# MAGIC   ytd_central,
# MAGIC   ytd_south,
# MAGIC   ytd_total,
# MAGIC   
# MAGIC   -- Data lineage (for debugging and validation)
# MAGIC   source_table_index,
# MAGIC   source_row_index,
# MAGIC   extracted_timestamp
# MAGIC   
# MAGIC FROM market_data.hyundai_vinfast.hyundai_sales_by_model
# MAGIC
# MAGIC -- Optional: Add data quality filters if needed
# MAGIC -- WHERE validation_status = 'valid' OR validation_status IS NULL
# MAGIC
# MAGIC ORDER BY report_start_date DESC, maker, model_name

# COMMAND ----------

# DBTITLE 1,Verify Curated View - Sample Recent Data
# MAGIC %sql
# MAGIC -- Verify the curated view with recent data
# MAGIC -- Check model name cleaning and column selection
# MAGIC
# MAGIC SELECT 
# MAGIC   maker,
# MAGIC   model_name,  -- Should be cleaned (no "Hyundai" prefix)
# MAGIC   report_month,
# MAGIC   monthly_total,
# MAGIC   ytd_total,
# MAGIC   cbu_ckd,
# MAGIC   is_commercial_vehicle
# MAGIC FROM market_data.hyundai_vinfast.curated_hyundai_sales
# MAGIC WHERE report_start_date >= current_date() - INTERVAL 90 DAYS
# MAGIC ORDER BY report_start_date DESC, monthly_total DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,ETL Summary
# MAGIC %md
# MAGIC ## ✅ Curated View Created: `market_data.hyundai_vinfast.curated_hyundai_sales_unified`
# MAGIC
# MAGIC ### Transformation Summary
# MAGIC
# MAGIC **Source:** `market_data.hyundai_vinfast.hyundai_sales_by_model` (36 columns)  
# MAGIC **Target:** `market_data.hyundai_vinfast.curated_hyundai_sales_unified` (23 columns)  
# MAGIC **Reference Pattern:** `market_data.vama.curated_vama_sales_unified`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Transformations
# MAGIC
# MAGIC #### 1. Model Name Cleaning
# MAGIC * **Before:** `"Hyundai Stargazer"`, `"Hyundai Tucson"`, `"Hyundai Creta"`
# MAGIC * **After:** `"Stargazer"`, `"Tucson"`, `"Creta"`
# MAGIC * **Logic:** Removed redundant brand prefixes ("Hyundai ", "VinFast ") using regex pattern matching
# MAGIC * **Preserved:** Vietnamese names (`"XE THƯƠNG MẠI"`, `"Mẫu khác"`)
# MAGIC
# MAGIC #### 2. Columns EXCLUDED (Not BI-Relevant)
# MAGIC * ❌ `document_url` - Source URL (as requested)
# MAGIC * ❌ `filename` - File name (as requested)
# MAGIC * ❌ `source_id`, `source_title`, `source_domain` - Redundant source metadata
# MAGIC * ❌ `regional_granularity` - Internal flag
# MAGIC * ❌ `is_other_bucket` - Internal classification flag
# MAGIC * ❌ `extraction_confidence`, `validation_status`, `validation_message` - Data quality metrics (not for end users)
# MAGIC * ❌ `raw_evidence` - Technical debugging data
# MAGIC * ❌ `parsing_method` - ETL implementation detail
# MAGIC * ❌ `created_at`, `updated_at` - System timestamps
# MAGIC
# MAGIC #### 3. Columns INCLUDED (BI-Ready)
# MAGIC
# MAGIC **Identifiers & Lineage**
# MAGIC * `document_id` - Unique document identifier
# MAGIC * `source_table_index`, `source_row_index` - For debugging/validation
# MAGIC * `extracted_timestamp` - When data was captured
# MAGIC
# MAGIC **Temporal Dimensions**
# MAGIC * `report_year`, `report_month` - Reporting period
# MAGIC * `report_start_date`, `report_end_date` - Date range
# MAGIC
# MAGIC **Product Dimensions**
# MAGIC * `maker` - Brand (Hyundai/VinFast)
# MAGIC * `model_name` - **Cleaned** vehicle model
# MAGIC * `vama_classification` - Vehicle category
# MAGIC * `seat` - Seating capacity
# MAGIC
# MAGIC **Business Attributes**
# MAGIC * `cbu_ckd` - Import type (CBU/CKD)
# MAGIC * `is_commercial_vehicle` - Vehicle type flag
# MAGIC
# MAGIC **Sales Metrics**
# MAGIC * `monthly_north`, `monthly_central`, `monthly_south`, `monthly_total` - Monthly regional sales
# MAGIC * `ytd_north`, `ytd_central`, `ytd_south`, `ytd_total` - Year-to-date regional sales
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Data Quality Note
# MAGIC * Optional data quality filter is commented out in the view definition
# MAGIC * To enable: Uncomment the WHERE clause to filter by `validation_status`
# MAGIC * Current behavior: Includes all records regardless of validation status

# COMMAND ----------

