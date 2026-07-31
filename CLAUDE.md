# automobile-etl — working notes for Claude

**Picking this up fresh? Read `docs/handoff_2026-07-31.md` first** — current state,
the one open bug, and what changed most recently.

Read `README.md` for the pipeline map and `docs/state_2026-07-30.md` for what the
data actually looks like. This file is only the things that will bite you.

## Open right now: VAMA 2026-06 extracts zero rows and reports success

`3_Extract_Tables` produced 0 rows for the June 2026 detail document while
`extraction_status = 'success'`. The PDF downloaded and parsed fine (3,240 cells in
`extracted_tables_long`). Suspected cause is an unresolved year-to-date header —
June has `col_8` where May has `Sales - YTM 2026`. ~24,000 units missing from gold,
and nothing will flag it on the 15th. Detail and a read-only test query are in
`docs/handoff_2026-07-31.md`.

## The workspace is authoritative, not this repo

Unlike `customs-etl` (where job tasks are `source: GIT` and pushing to `main` is
the deploy step), here the job tasks are `source: WORKSPACE`, so **Databricks runs
the workspace copy**. So:

- `git push` changes nothing in Databricks.
- Run `python scripts/databricks_sync.py diff` **before** editing either side —
  someone may have edited a notebook in the Databricks UI since the last pull.
- `databricks_sync.py push` deploys notebooks; `deploy_job.py` deploys the DAG.

`source: GIT` is not an option here without code changes:
`3_Extract_Tables` and `3b Extract Other Makers` hardcode
`sys.path.insert(0, '/Workspace/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/2. VAMA')`
to import `vama_parser`, which a Git checkout path would break.

## Job ordering has two constraints that look arbitrary

`hyundai_refill_prose` after `hyundai_extract`, and
`vama_extract_other_makers` after `vama_extract`. Both matter — reversing either
silently drops data rather than failing. `docs/monthly_workflow.md` explains why.

## `diff` has one expected difference

`automobile/90_build_automobile_unified_sales.py` will report as differing until
someone pushes. That is the stale duplicate reconciliation described in
`docs/unified_notebook_duplicate.md` — not drift you introduced. Every other
difference is real.

## The raw source tables are the only copy — never let a failure overwrite them

`hyundai_raw_sources.extracted_text` is the sole copy of each Hyundai source page,
and `03`/`05` both re-derive from it. A `MERGE ... WHEN MATCHED THEN UPDATE SET *`
fed by a failed fetch nulls it and the month becomes unrecoverable — that is
exactly how 2024-12 and 2025-10 were lost on 2026-07-30. Any write into a raw
source table must be conditional on the fetch having succeeded.

The same shape — delete a range, then conditionally re-insert — is the other half
of that failure. If an insert can produce nothing, the delete must be scoped to
what the insert actually produced. This bug existed independently in
`3b Extract Other Makers` and `05_hyundai_refill_prose_missing_months`; both are
fixed, but assume it in anything new.

Do not reach for `max_retries: 0` to contain a destructive task. On 2026-07-30
`hyundai_refill_prose` had no configured retries and still ran twice, because the
run ended in `INTERNAL_ERROR` and Databricks retries that itself. The task has to be
safe to fail; retry settings are not a control here.

## Credentials come from Databricks secrets, not the `.env.` file

`/Workspace/Users/tuckeyhue@gmail.com/env/.env.` still exists and still holds a
`GEMINI_API_KEY`, but it is **dead**. The live key is the secret
`news-signal/gemini-api-key`. Both loaders read the secret first now; the file is
fallback only. Rotating the file changes nothing, and rotating the secret while a
loader still preferred the file is exactly the bug that wasted 2026-07-24..30 —
the key had already been rotated and nothing read it. Log the key's *length*, never
its value.

## Do not "fix" the failing months

`hyundai_monthly_validation` has 6 `fail` rows and `vinfast_monthly_validation`
has 5 `fail` + 16 `warning`. These are deliberate. `04_hyundai_validate` promotes
a month to `pass` only when extracted model rows reconcile **exactly** to the
official monthly total, and `15_vinfast_ai_search_assisted_extract` leaves a
month `fail` rather than accept ungrounded numbers. Loosening either check to
make the dashboard look complete would be fabricating sales figures.

Same logic in `15_vinfast_ai_search_assisted_extract`'s guardrail: on Gemini 429
quota exhaustion it keeps existing rows rather than writing an empty month.

## Things that look like bugs but are not

- Sparse notebook numbering (`00`, `01`…`05`, then `15`, then `90`) follows sprint
  numbers, not run order.
- `report_month` is a **STRING** (`'2026-04'`) in the VAMA tables, not an int.
  `report_year * 100 + report_month` will throw `CAST_INVALID_INPUT`.
- `sales_by_model_region_gemini` is a parallel LLM-parsed variant, not a
  replacement for `sales_by_model_region`.
- `vama_parser/` is a workspace *file* module, not a notebook — `3_Extract_Tables`
  imports it and `importlib.reload`s it, so edits need a push plus a re-run.

## Excluded from sync

`vama/Shinhan FX Daily/` exists inside the `2. VAMA` workspace folder but is
banking FX work, unrelated to automobiles. It is in `EXCLUDE` in
`scripts/databricks_sync.py`; leave it out.

## Open work

- `notebooks/vama/HANDOFF_RowMisalignment_Investigation_2026-05-26.md` is an
  unresolved extraction-accuracy investigation. `sales_by_other_makers.maker`
  holding ~24 numeric junk values (`'0'`, `'178'`, `'489'`) is probably the same
  root cause.
- 21 VAMA documents never downloaded and 1 failed extraction, out of 722
  discovered.
- The Gemini fallback excludes `parsing_method='llm'` rows from validation, so the
  2,390 rows in that state are never re-checked.
- `05_hyundai_refill_prose_missing_months` has a hardcoded `TARGET_MONTHS` list; a
  new prose-format month needs a code change to be picked up. Two of its six months
  (2024-12, 2025-10) can no longer be derived — the source articles are gone from
  the site — so they will report as skipped on every run. That is correct
  behaviour now, not a failure.
- **VinFast published no monthly figures for most of 2024** (monthly by model
  through 2023-07, then quarterly, resuming 2024-11). `2024-03` and `2024-05` held
  other months' figures and were deleted 2026-07-30. Do not try to "fill" them, and
  do not accept a source claiming a monthly 2024 number without checking it against
  the quarterly totals — Q1 2024 was 9,689 *globally*. `docs/vinfast_crawl.md`.
- The curated VinFast view does **not** filter on `validation_status`, so `warning`
  and `fail` rows reach gold unchanged. Worth knowing before trusting a row count.
- Nothing needs **Google Search grounding** any more; the VinFast rewrite designed
  it out, and the key's project has no grounding quota anyway. Do not reintroduce a
  grounded call without reading `docs/gemini_key.md`.
- Hyundai `2026-05` has no source candidate at all (discovery found none), so that
  month is absent for a different reason than the two lost ones.
