# Databricks notebook source
# MAGIC %md
# MAGIC # Spec: Standalone Hyundai + VinFast Monthly Sales Workflows
# MAGIC 
# MAGIC ## 1. Decisions
# MAGIC 
# MAGIC Build Hyundai and VinFast as **two separate workflows** under one project area.
# MAGIC 
# MAGIC They should use:
# MAGIC 
# MAGIC - separate notebooks
# MAGIC - separate source-candidate tables
# MAGIC - separate raw-source tables
# MAGIC - separate normalized sales tables
# MAGIC - separate validation tables
# MAGIC 
# MAGIC Reason: Hyundai has a semi-stable official TC Group web source; VinFast has variable sources and needs source ranking/search + evidence. Keeping them separate makes debugging much easier.
# MAGIC 
# MAGIC Historical range: process from **2019 to now**.
# MAGIC 
# MAGIC Hyundai `XE THƯƠNG MẠI` must be included.
# MAGIC 
# MAGIC ## 2. Databricks Layout
# MAGIC 
# MAGIC ### Workspace folder
# MAGIC 
# MAGIC Create a new folder parallel to VAMA:
# MAGIC 
# MAGIC `/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/4. Hyundai VinFast Sales`
# MAGIC 
# MAGIC ### Unity Catalog schema
# MAGIC 
# MAGIC Create new schema:
# MAGIC 
# MAGIC `market_data.hyundai_vinfast`
# MAGIC 
# MAGIC Do **not** write Hyundai/VinFast rows into `market_data.vama` tables.
# MAGIC 
# MAGIC ## 3. Existing VAMA Shape to Mirror
# MAGIC 
# MAGIC Reference folder:
# MAGIC 
# MAGIC `/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/2. VAMA`
# MAGIC 
# MAGIC Reference view:
# MAGIC 
# MAGIC `market_data.vama.curated_vama_sales_unified`
# MAGIC 
# MAGIC Reference VAMA-compatible sales columns:
# MAGIC 
# MAGIC - `document_id STRING NOT NULL`
# MAGIC - `document_url STRING`
# MAGIC - `filename STRING`
# MAGIC - `report_year INT`
# MAGIC - `report_month STRING` — `YYYY-MM`
# MAGIC - `report_start_date DATE`
# MAGIC - `report_end_date DATE`
# MAGIC - `maker STRING`
# MAGIC - `model_name STRING`
# MAGIC - `vama_classification STRING`
# MAGIC - `seat STRING`
# MAGIC - `monthly_north INT`
# MAGIC - `monthly_central INT`
# MAGIC - `monthly_south INT`
# MAGIC - `monthly_total INT`
# MAGIC - `ytd_north INT`
# MAGIC - `ytd_central INT`
# MAGIC - `ytd_south INT`
# MAGIC - `ytd_total INT`
# MAGIC - `source_table_index INT`
# MAGIC - `source_row_index INT`
# MAGIC - `extracted_timestamp TIMESTAMP`
# MAGIC - `parsing_method STRING`
# MAGIC 
# MAGIC Hyundai/VinFast tables should mirror this shape where possible. Regional fields are usually null because sources report national totals only.
# MAGIC 
# MAGIC ## 4. Notebook Design
# MAGIC 
# MAGIC ### 4.1 Shared setup notebooks
# MAGIC 
# MAGIC - `00_specs_hyundai_vinfast_sales`
# MAGIC - `00_create_schema_and_common_helpers`
# MAGIC 
# MAGIC ### 4.2 Hyundai notebooks
# MAGIC 
# MAGIC - `01_hyundai_discover_sources`
# MAGIC - `02_hyundai_fetch_sources`
# MAGIC - `03_hyundai_extract_sales`
# MAGIC - `04_hyundai_validate`
# MAGIC 
# MAGIC ### 4.3 VinFast notebooks
# MAGIC 
# MAGIC - `11_vinfast_discover_sources`
# MAGIC - `12_vinfast_fetch_sources`
# MAGIC - `13_vinfast_extract_sales`
# MAGIC - `14_vinfast_validate`
# MAGIC 
# MAGIC ### 4.4 Optional integration notebook
# MAGIC 
# MAGIC - `90_build_cross_market_view_optional`
# MAGIC 
# MAGIC This optional notebook can later build a union view from VAMA + Hyundai + VinFast, but should not mutate the VAMA workflow.
# MAGIC 
# MAGIC ## 5. Package Design
# MAGIC 
# MAGIC Package folder:
# MAGIC 
# MAGIC `hyundai_vinfast_parser`
# MAGIC 
# MAGIC Suggested modules:
# MAGIC 
# MAGIC - `config.py`
# MAGIC - `schemas.py`
# MAGIC - `utils.py`
# MAGIC - `hyundai_discovery.py`
# MAGIC - `hyundai_extract.py`
# MAGIC - `hyundai_validation.py`
# MAGIC - `vinfast_discovery.py`
# MAGIC - `vinfast_extract.py`
# MAGIC - `vinfast_validation.py`
# MAGIC - `llm_extract.py`
# MAGIC 
# MAGIC ## 6. Hyundai Tables
# MAGIC 
# MAGIC ### 6.1 `market_data.hyundai_vinfast.hyundai_source_candidates`
# MAGIC 
# MAGIC One row per discovered candidate Hyundai page.
# MAGIC 
# MAGIC Columns:
# MAGIC 
# MAGIC - `source_id STRING NOT NULL`
# MAGIC - `report_year INT`
# MAGIC - `report_month_int INT`
# MAGIC - `report_month STRING` — `YYYY-MM`
# MAGIC - `url STRING`
# MAGIC - `title STRING`
# MAGIC - `source_domain STRING`
# MAGIC - `source_type STRING` — official_tcgroup, sitemap, url_probe, search_result, manual_seed
# MAGIC - `source_priority INT`
# MAGIC - `discovered_by STRING` — sitemap, url_pattern_probe, web_search, manual_seed
# MAGIC - `pattern_name STRING`
# MAGIC - `candidate_rank INT`
# MAGIC - `is_selected BOOLEAN`
# MAGIC - `selection_reason STRING`
# MAGIC - `created_at TIMESTAMP`
# MAGIC - `updated_at TIMESTAMP`
# MAGIC 
# MAGIC ### 6.2 `market_data.hyundai_vinfast.hyundai_raw_sources`
# MAGIC 
# MAGIC Fetched Hyundai page content.
# MAGIC 
# MAGIC Columns:
# MAGIC 
# MAGIC - `source_id STRING NOT NULL`
# MAGIC - `report_month STRING`
# MAGIC - `url STRING`
# MAGIC - `fetch_status STRING`
# MAGIC - `http_status INT`
# MAGIC - `fetched_at TIMESTAMP`
# MAGIC - `content_type STRING`
# MAGIC - `raw_html STRING`
# MAGIC - `extracted_text STRING`
# MAGIC - `content_hash STRING`
# MAGIC - `error_message STRING`
# MAGIC 
# MAGIC ### 6.3 `market_data.hyundai_vinfast.hyundai_sales_by_model`
# MAGIC 
# MAGIC Normalized Hyundai output. Mirror VAMA-compatible columns, then append evidence fields.
# MAGIC 
# MAGIC VAMA-compatible columns:
# MAGIC 
# MAGIC - `document_id STRING NOT NULL`
# MAGIC - `document_url STRING`
# MAGIC - `filename STRING`
# MAGIC - `report_year INT`
# MAGIC - `report_month STRING`
# MAGIC - `report_start_date DATE`
# MAGIC - `report_end_date DATE`
# MAGIC - `maker STRING` — always `Hyundai`
# MAGIC - `model_name STRING`
# MAGIC - `vama_classification STRING`
# MAGIC - `seat STRING`
# MAGIC - `monthly_north INT`
# MAGIC - `monthly_central INT`
# MAGIC - `monthly_south INT`
# MAGIC - `monthly_total INT`
# MAGIC - `ytd_north INT`
# MAGIC - `ytd_central INT`
# MAGIC - `ytd_south INT`
# MAGIC - `ytd_total INT`
# MAGIC - `source_table_index INT`
# MAGIC - `source_row_index INT`
# MAGIC - `extracted_timestamp TIMESTAMP`
# MAGIC - `parsing_method STRING`
# MAGIC 
# MAGIC Hyundai-specific appended columns:
# MAGIC 
# MAGIC - `source_id STRING`
# MAGIC - `source_title STRING`
# MAGIC - `source_domain STRING`
# MAGIC - `regional_granularity STRING` — `national`
# MAGIC - `is_commercial_vehicle BOOLEAN`
# MAGIC - `is_other_bucket BOOLEAN`
# MAGIC - `cbu_ckd STRING`
# MAGIC - `extraction_confidence DOUBLE`
# MAGIC - `validation_status STRING`
# MAGIC - `validation_message STRING`
# MAGIC - `raw_evidence STRING`
# MAGIC - `created_at TIMESTAMP`
# MAGIC - `updated_at TIMESTAMP`
# MAGIC 
# MAGIC Important Hyundai row rules:
# MAGIC 
# MAGIC - Include `XE THƯƠNG MẠI` as a sales row with `is_commercial_vehicle = true`.
# MAGIC - Include `Mẫu khác` as a sales row with `is_other_bucket = true`.
# MAGIC - Do not insert `TỔNG` as a model row unless extraction fails and only total is available; in that case mark warning and model `TOTAL_UNSPECIFIED`.
# MAGIC 
# MAGIC ### 6.4 `market_data.hyundai_vinfast.hyundai_monthly_validation`
# MAGIC 
# MAGIC One row per Hyundai month.
# MAGIC 
# MAGIC Columns:
# MAGIC 
# MAGIC - `report_month STRING`
# MAGIC - `selected_source_id STRING`
# MAGIC - `source_total INT`
# MAGIC - `sum_model_monthly_total INT`
# MAGIC - `source_ytd_total INT`
# MAGIC - `sum_model_ytd_total INT`
# MAGIC - `previous_ytd_total INT`
# MAGIC - `derived_current_from_ytd INT`
# MAGIC - `validation_status STRING`
# MAGIC - `validation_message STRING`
# MAGIC - `checked_at TIMESTAMP`
# MAGIC 
# MAGIC ## 7. VinFast Tables
# MAGIC 
# MAGIC ### 7.1 `market_data.hyundai_vinfast.vinfast_source_candidates`
# MAGIC 
# MAGIC One row per candidate VinFast source page.
# MAGIC 
# MAGIC Columns:
# MAGIC 
# MAGIC - `source_id STRING NOT NULL`
# MAGIC - `report_year INT`
# MAGIC - `report_month_int INT`
# MAGIC - `report_month STRING`
# MAGIC - `url STRING`
# MAGIC - `title STRING`
# MAGIC - `source_domain STRING`
# MAGIC - `source_type STRING` — official_vinfast, official_vingroup, press, aggregator, search_result, manual_seed
# MAGIC - `source_priority INT`
# MAGIC - `discovered_by STRING` — web_search, manual_seed, sitemap, llm_search_assist
# MAGIC - `search_query STRING`
# MAGIC - `candidate_rank INT`
# MAGIC - `is_selected BOOLEAN`
# MAGIC - `selection_reason STRING`
# MAGIC - `created_at TIMESTAMP`
# MAGIC - `updated_at TIMESTAMP`
# MAGIC 
# MAGIC ### 7.2 `market_data.hyundai_vinfast.vinfast_raw_sources`
# MAGIC 
# MAGIC Fetched VinFast candidate content.
# MAGIC 
# MAGIC Columns:
# MAGIC 
# MAGIC - `source_id STRING NOT NULL`
# MAGIC - `report_month STRING`
# MAGIC - `url STRING`
# MAGIC - `fetch_status STRING`
# MAGIC - `http_status INT`
# MAGIC - `fetched_at TIMESTAMP`
# MAGIC - `content_type STRING`
# MAGIC - `raw_html STRING`
# MAGIC - `extracted_text STRING`
# MAGIC - `content_hash STRING`
# MAGIC - `error_message STRING`
# MAGIC 
# MAGIC ### 7.3 `market_data.hyundai_vinfast.vinfast_sales_by_model`
# MAGIC 
# MAGIC Normalized VinFast output. Mirror VAMA-compatible columns, then append evidence fields.
# MAGIC 
# MAGIC VAMA-compatible columns are the same as Hyundai, with `maker = 'VinFast'`.
# MAGIC 
# MAGIC VinFast-specific appended columns:
# MAGIC 
# MAGIC - `source_id STRING`
# MAGIC - `source_title STRING`
# MAGIC - `source_domain STRING`
# MAGIC - `source_type STRING`
# MAGIC - `regional_granularity STRING` — `national`
# MAGIC - `is_official_source BOOLEAN`
# MAGIC - `is_total_only_row BOOLEAN`
# MAGIC - `extraction_confidence DOUBLE`
# MAGIC - `validation_status STRING`
# MAGIC - `validation_message STRING`
# MAGIC - `raw_evidence STRING`
# MAGIC - `llm_model STRING`
# MAGIC - `created_at TIMESTAMP`
# MAGIC - `updated_at TIMESTAMP`
# MAGIC 
# MAGIC Rules:
# MAGIC 
# MAGIC - Prefer model-level rows when available.
# MAGIC - If only a monthly total is available, insert one row with `model_name = 'TOTAL_UNSPECIFIED'`, `is_total_only_row = true`, and `validation_status = 'warning'`.
# MAGIC - Store raw evidence for every extracted row.
# MAGIC 
# MAGIC ### 7.4 `market_data.hyundai_vinfast.vinfast_monthly_validation`
# MAGIC 
# MAGIC One row per VinFast month.
# MAGIC 
# MAGIC Columns mirror Hyundai validation, plus:
# MAGIC 
# MAGIC - `selected_source_type STRING`
# MAGIC - `source_conflict_count INT`
# MAGIC - `confidence_reason STRING`
# MAGIC 
# MAGIC ## 8. Hyundai Source Discovery: 2019 to Now
# MAGIC 
# MAGIC Hyundai URL path is **not fixed**. It changes by year and sometimes by month.
# MAGIC 
# MAGIC Observed quick probe results:
# MAGIC 
# MAGIC - 2023 April works with zero-padded month:
# MAGIC   - `https://thanhcong.vn/tin-tuc/tc-group-thong-bao-ket-qua-ban-hang-hyundai-thang-04-2023.html`
# MAGIC - 2025 April works without zero-padding:
# MAGIC   - `https://thanhcong.vn/tin-tuc/tc-group-thong-bao-ket-qua-ban-hang-hyundai-thang-4-2025.html`
# MAGIC - 2026 includes mixed prefixes:
# MAGIC   - Jan: `tc-group-thong-bao-ket-qua-ban-hang-hyundai-thang-1-2026.html`
# MAGIC   - Feb: `tap-doan-thanh-cong-thong-bao-ket-qua-ban-hang-hyundai-thang-2-2026.html`
# MAGIC   - Mar/Apr: `tc-group-thong-bao-ket-qua-ban-hang-hyundai-thang-3-2026.html`, `...thang-4-2026.html`
# MAGIC - Sitemap probe found examples:
# MAGIC   - 2022: `tc-group-thong-bao-ket-qua-ban-hang-xe-hyundai-thang-6-2022.html`
# MAGIC   - 2022: `thong-bao-ket-qua-ban-hang-hyundai-thang-10-2022.html`
# MAGIC   - 2023: `thong-bao-ket-qua-ban-hang-hyundai-thang-1-2023.html`, `...thang-02-2023.html`, `tc-group...thang-03-2023.html`
# MAGIC   - 2024: `thong-bao-ket-qua-ban-hang-hyundai-thang-1-2024.html`, `...thang-4-2024.html`
# MAGIC   - 2025: `tc-group-thong-bao-ket-qua-ban-hang-hyundai-thang-1-2025.html`, etc.
# MAGIC 
# MAGIC Recommended Hyundai discovery order:
# MAGIC 
# MAGIC 1. Fetch WordPress sitemap:
# MAGIC    - `https://thanhcong.vn/wp-sitemap-posts-post-1.xml`
# MAGIC 2. Extract URLs containing Hyundai + sales-report keywords:
# MAGIC    - `hyundai`
# MAGIC    - `ban-hang`
# MAGIC    - month/year tokens
# MAGIC 3. Infer month/year from title, URL, and article body.
# MAGIC 4. For missing months, run URL-pattern probes with variants:
# MAGIC    - `tc-group-thong-bao-ket-qua-ban-hang-hyundai-thang-{m}-{year}.html`
# MAGIC    - `tc-group-thong-bao-ket-qua-ban-hang-hyundai-thang-{mm}-{year}.html`
# MAGIC    - `tap-doan-thanh-cong-thong-bao-ket-qua-ban-hang-hyundai-thang-{m}-{year}.html`
# MAGIC    - `tap-doan-thanh-cong-thong-bao-ket-qua-ban-hang-hyundai-thang-{mm}-{year}.html`
# MAGIC    - `thong-bao-ket-qua-ban-hang-hyundai-thang-{m}-{year}.html`
# MAGIC    - `thong-bao-ket-qua-ban-hang-hyundai-thang-{mm}-{year}.html`
# MAGIC    - `tc-group-thong-bao-ket-qua-ban-hang-xe-hyundai-thang-{m}-{year}.html`
# MAGIC    - `tc-group-thong-bao-ket-qua-ban-hang-xe-hyundai-thang-{mm}-{year}.html`
# MAGIC 5. For still-missing 2019–2021 or gap months, use web search and/or manual seeds.
# MAGIC 6. Keep all candidates in `hyundai_source_candidates`; select one per report month with reason.
# MAGIC 
# MAGIC Important: 2019–2021 may not be fully covered by current TC Group sitemap. Treat web search/manual-seed fallback as part of the workflow, not an exception.
# MAGIC 
# MAGIC ## 9. Hyundai Extraction
# MAGIC 
# MAGIC Hyundai extraction should be deterministic first:
# MAGIC 
# MAGIC 1. Fetch selected source.
# MAGIC 2. Confirm source month/year from title/body.
# MAGIC 3. Locate table/body section near `KẾT QUẢ BÁN HÀNG XE Ô TÔ HYUNDAI` or equivalent text.
# MAGIC 4. Extract rows for known/found model labels, including:
# MAGIC    - passenger models
# MAGIC    - `XE THƯƠNG MẠI`
# MAGIC    - `Mẫu khác`
# MAGIC 5. Detect header columns and pick the current-month column.
# MAGIC 6. Extract YTD from `Cả năm` if present.
# MAGIC 7. Validate sum of rows equals `TỔNG`.
# MAGIC 8. Use Gemini fallback only when deterministic parsing fails.
# MAGIC 
# MAGIC ## 10. VinFast Source Discovery and Extraction
# MAGIC 
# MAGIC VinFast has no stable source.
# MAGIC 
# MAGIC Source priority:
# MAGIC 
# MAGIC 1. VinFast official newsroom.
# MAGIC 2. Vingroup official release.
# MAGIC 3. Reputable Vietnamese business/auto press quoting official data.
# MAGIC 4. Aggregators only for discovery, not final source if better source exists.
# MAGIC 
# MAGIC Search templates:
# MAGIC 
# MAGIC - `VinFast doanh số tháng {month} {year}`
# MAGIC - `VinFast bàn giao xe tháng {month} {year}`
# MAGIC - `doanh số VinFast VF 3 VF 5 VF 6 VF 7 VF 8 VF 9 tháng {month} {year}`
# MAGIC - `VinFast sales Vietnam {month_name} {year}`
# MAGIC 
# MAGIC Use Gemini extraction with strict JSON schema only after fetching candidate pages and storing raw evidence.
# MAGIC 
# MAGIC ## 11. Gemini API Key Handling
# MAGIC 
# MAGIC Databricks workspace env path found:
# MAGIC 
# MAGIC `/Users/tuckeyhue@gmail.com/env/.env.`
# MAGIC 
# MAGIC Use it inside Databricks runtime. Do not print values.
# MAGIC 
# MAGIC ## 12. Validation Rules
# MAGIC 
# MAGIC Required pass rules:
# MAGIC 
# MAGIC - article/source month matches target month
# MAGIC - numeric values parsed cleanly
# MAGIC - no duplicate `(report_month, model_name)` rows in each maker table
# MAGIC - Hyundai rows including `XE THƯƠNG MẠI` and `Mẫu khác` sum to source `TỔNG`
# MAGIC - if YTD is present, current month should be consistent with previous YTD where prior month exists
# MAGIC 
# MAGIC Warning rules:
# MAGIC 
# MAGIC - missing source month after all discovery steps
# MAGIC - VinFast source is non-official press
# MAGIC - only total is available, no model breakdown
# MAGIC - LLM extraction used without deterministic confirmation
# MAGIC - missing YTD
# MAGIC - source conflict between candidates
# MAGIC 
# MAGIC Fail rules:
# MAGIC 
# MAGIC - source month mismatch
# MAGIC - no reliable monthly value
# MAGIC - model rows materially disagree with source total
# MAGIC - multiple candidate sources disagree and no official/preferred source resolves it
# MAGIC 
# MAGIC ## 13. Optional Cross-Market Integration
# MAGIC 
# MAGIC Keep standalone tables as source of truth:
# MAGIC 
# MAGIC - `market_data.hyundai_vinfast.hyundai_sales_by_model`
# MAGIC - `market_data.hyundai_vinfast.vinfast_sales_by_model`
# MAGIC 
# MAGIC Later, create a separate view such as:
# MAGIC 
# MAGIC `market_data.vehicle_sales.curated_vietnam_auto_sales_unified`
# MAGIC 
# MAGIC This can UNION ALL:
# MAGIC 
# MAGIC - `market_data.vama.curated_vama_sales_unified`
# MAGIC - `market_data.hyundai_vinfast.hyundai_sales_by_model`
# MAGIC - `market_data.hyundai_vinfast.vinfast_sales_by_model`
# MAGIC 
# MAGIC Do not modify `market_data.vama.curated_vama_sales_unified` directly.
# MAGIC 
# MAGIC ## 14. Implementation Phases
# MAGIC 
# MAGIC ### Phase 1 — Foundation
# MAGIC 
# MAGIC - Create workspace folder and schema.
# MAGIC - Create Hyundai tables and VinFast tables separately.
# MAGIC - Upload shared package skeleton.
# MAGIC 
# MAGIC ### Phase 2 — Hyundai discovery + extraction 2019-now
# MAGIC 
# MAGIC - Sitemap discovery.
# MAGIC - URL-pattern fallback.
# MAGIC - Search/manual seed fallback for gaps.
# MAGIC - Deterministic extraction.
# MAGIC - Validation table.
# MAGIC 
# MAGIC ### Phase 3 — VinFast discovery + extraction 2019-now
# MAGIC 
# MAGIC - Search/candidate ranking.
# MAGIC - Raw source storage.
# MAGIC - Gemini structured extraction.
# MAGIC - Validation and confidence scoring.
# MAGIC 
# MAGIC ### Phase 4 — Optional unified BI layer
# MAGIC 
# MAGIC - Create separate `vehicle_sales` schema/view if needed.
# MAGIC 
