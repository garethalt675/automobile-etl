# automobile-etl — working notes for Claude

Read `README.md` for the pipeline map and `docs/state_2026-07-30.md` for what the
data actually looks like. This file is only the things that will bite you.

## The workspace is authoritative, not this repo

Unlike `customs-etl` (where jobs run `source: GIT` and pushing to `main` is the
deploy step), here **Databricks runs the workspace copy**. There is no Git folder
and there are no jobs. So:

- `git push` changes nothing in Databricks.
- Run `python scripts/databricks_sync.py diff` **before** editing either side —
  someone may have edited a notebook in the Databricks UI since the last pull.
- `python scripts/databricks_sync.py push` is the deploy step.

## `diff` has one expected difference

`automobile/90_build_automobile_unified_sales.py` will report as differing until
someone pushes. That is the stale duplicate reconciliation described in
`docs/unified_notebook_duplicate.md` — not drift you introduced. Every other
difference is real.

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

- No monthly Workflow exists. Creating one is the main missing piece; name it with
  a prefix `scripts/export_jobs.py` matches so the DAG gets version-controlled.
- All sources stop at 2026-04; 2026-05 and 2026-06 need a run.
- `notebooks/vama/HANDOFF_RowMisalignment_Investigation_2026-05-26.md` is an
  unresolved extraction-accuracy investigation.
- 21 VAMA documents never downloaded and 1 failed extraction, out of 722
  discovered.
