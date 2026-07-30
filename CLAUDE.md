# automobile-etl — working notes for Claude

Read `README.md` for the pipeline map and `docs/state_2026-07-30.md` for what the
data actually looks like. This file is only the things that will bite you.

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
- **VinFast extraction is blocked on Google Search grounding quota, not on the
  key.** The key lives in the Databricks secret `news-signal/gemini-api-key` and
  works — but every *grounded* call returns `429 RESOURCE_EXHAUSTED` while
  ungrounded calls on the same key return 200, i.e. the key's Google project has no
  grounding allowance. Grounding is the whole mechanism of
  `15_vinfast_ai_search_assisted_extract`, so it stays blocked; removing grounding
  would turn it into a model inventing sales figures. Enabling billing on that
  Google project is the fix. `docs/gemini_key.md` has the detail.
  The VAMA Gemini fallback does **not** use grounding and is working again.
- Hyundai `2026-05` has no source candidate at all (discovery found none), so that
  month is absent for a different reason than the two lost ones.
