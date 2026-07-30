# automobile-etl

Vietnam monthly automobile sales ETL on Databricks. Consolidates three workspace
folders that used to be worked on separately:

| Repo directory | Databricks workspace folder | Role |
| --- | --- | --- |
| `notebooks/vama/` | `Market Research/1. Data ETL/2. VAMA` | Source 1 — official VAMA member sales reports |
| `notebooks/hyundai_vinfast/` | `Market Research/1. Data ETL/4. Hyundai VinFast Sales` | Source 2 — Hyundai and VinFast, who do not report through VAMA |
| `notebooks/automobile/` | `Market Research/1. Data ETL/5. Automobile` | Gold — unified cross-source views |

Workspace host: `dbc-5a6b7518-84a8.cloud.databricks.com`, catalog `market_data`,
serverless warehouse `7eb5fd2336243915`.

The monthly cadence runs as Databricks job **`etl data automobile`** (id
`647704685836737`), 08:00 on the 15th, `Asia/Ho_Chi_Minh`. The DAG lives in
`jobs/etl-data-automobile.json` and deploys with `scripts/deploy_job.py` —
see **`docs/monthly_workflow.md`** for its shape, the non-obvious ordering
constraints, and the three scheduling bugs that had to be fixed first.

`docs/state_2026-07-30.md` is the data snapshot taken when the folders were
consolidated; note all three sources stopped at **2026-04** at that point.

## The three pipelines

### VAMA — `notebooks/vama/`

VAMA publishes monthly sales-report PDFs at `vama.org.vn/vn/bao-cao-ban-hang.html`.

```
1_Crawl_Download_Documents      discover PDF URLs -> vama_documents_url,
                                document_processing_log, and the PDFs into
                                /Volumes/market_data/vama/download_docs/<year>/
2_Parse_Documents               Databricks ai_parse_document() -> parsed_documents_raw
3_Extract_Tables                mainstream members -> extracted_tables_long,
                                sales_by_model_region; falls back to Gemini when
                                deterministic validation fails
3b Extract Other Makers         BMW, Lexus, Mercedes-Benz Vietnam ->
                                sales_by_other_makers (same schema)
VAMA Curated Views              UNION ALL of the two -> curated_vama_sales_unified
```

Support code: `vama_parser/` (a real Python module in the workspace, not a
notebook) wraps the Gemini fallback parser. `ADHOC_Reparse_Detail_PDFs`,
`EXAMPLE_Gemini_Parser_Usage` and `VAMA Missing Data Investigation` are
operator/diagnostic notebooks, not part of the monthly run.

### Hyundai + VinFast — `notebooks/hyundai_vinfast/`

Neither maker reports through VAMA, so each month has to be found and extracted
from the makers' own publications.

```
00_specs_hyundai_vinfast_sales          the spec this workflow was generated from
00_create_schema_and_common_helpers     DDL for the market_data.hyundai_vinfast schema
01_hyundai_discover_sources             sitemap first, then bounded URL-pattern
                                        probes of TC Group / Thanh Cong Hyundai
02_hyundai_fetch_sources                fetch selected pages -> hyundai_raw_sources
03_hyundai_extract_sales                deterministic model-level extraction
04_hyundai_validate                     one row per month; 'pass' only when model
                                        rows reconcile exactly to the official total
05_hyundai_refill_prose_missing_months  months published as prose with no table
15_vinfast_ai_search_assisted_extract   VinFast via Gemini + Google Search
                                        grounding, resolving redirects to canonical
                                        publisher URLs; unresolved months stay 'fail'
Curated Hyundai Sales View              -> curated_hyundai_sales
VinFast Sales Data Cleaning             model-name cleaning -> curated_vinfast_sales
```

The numbering is sparse because it follows sprint numbers, not run order.

### Unified gold — `notebooks/automobile/`

```
90_build_automobile_unified_sales   curated_vama_sales_unified
                                  + curated_hyundai_sales
                                  + curated_vinfast_sales
                                  -> market_data.automobile.curated_vietnam_auto_sales_unified
                                     market_data.automobile.auto_sales_source_quality
```

Creates views only; it never mutates the `vama` or `hyundai_vinfast` source
tables. This notebook existed in **two** workspace copies that had drifted apart
— see `docs/unified_notebook_duplicate.md` for which one won and why, and for the
one workspace reconciliation still outstanding.

## Tables

`market_data.vama`
: `vama_documents_url`, `document_processing_log`, `parsed_documents_raw`,
  `extracted_tables_long`, `sales_by_model_region`,
  `sales_by_model_region_gemini`, `sales_by_other_makers`,
  view `curated_vama_sales_unified`

`market_data.hyundai_vinfast`
: `hyundai_source_candidates`, `hyundai_raw_sources`, `hyundai_sales_by_model`,
  `hyundai_monthly_validation`, `vinfast_source_candidates`,
  `vinfast_raw_sources`, `vinfast_sales_by_model`, `vinfast_monthly_validation`,
  `vinfast_ai_search_queries`, `vinfast_ai_search_claims`,
  views `curated_hyundai_sales`, `curated_vinfast_sales`

`market_data.automobile`
: views `curated_vietnam_auto_sales_unified`, `auto_sales_source_quality`

## Working on this

**The Databricks workspace is the source of truth.** Pushing to GitHub does not
update Databricks. Always diff before editing either side:

```bash
pip install -r scripts/requirements.txt
python scripts/databricks_sync.py diff      # -v for line diffs
python scripts/databricks_sync.py pull      # workspace -> repo
python scripts/databricks_sync.py push      # repo -> workspace  (ships notebooks)
python scripts/deploy_job.py --dry-run      # jobs/*.json -> Databricks (ships the DAG)
python scripts/export_jobs.py               # Databricks -> jobs/*.json
```

Two separate deploys: `databricks_sync.py push` ships notebook changes,
`deploy_job.py` ships DAG changes. Pushing to GitHub ships neither.

Auth comes from the `DEFAULT` profile in `~/.databrickscfg`, or from
`DATABRICKS_HOST` / `DATABRICKS_TOKEN`. A cloud sandbox has neither — add them as
environment secrets to run sync or live queries there.

`diff` currently reports one expected difference (the stale unified notebook,
above). Anything else is real drift.

## Secrets

No credentials are in this repo. The Gemini key is read at runtime from a
workspace env file (`/Workspace/Users/tuckeyhue@gmail.com/env/.env.`) and never
logged. Do not commit `.databrickscfg`, `dbx config.txt`, or
`.claude/settings.local.json` — all three are gitignored and the last one hides a
live `dapi...` token inside a Bash permission rule.
