# The two copies of `90_build_automobile_unified_sales`

Recorded 2026-07-30, when the three workspace folders were pulled into this repo.

Databricks had the same notebook in two places:

| Workspace path | Last modified | Reads Hyundai/VinFast from |
| --- | --- | --- |
| `4. Hyundai VinFast Sales/90_build_automobile_unified_sales` | 2026-06-05 08:29 | `curated_hyundai_sales`, `curated_vinfast_sales` (views) |
| `5. Automobile/90_build_automobile_unified_sales` | 2026-06-04 08:05 | `hyundai_sales_by_model`, `vinfast_sales_by_model` + the two `*_monthly_validation` tables |

The **`4. Hyundai VinFast Sales` copy is the authoritative one**, on three
independent pieces of evidence:

1. It is the newer of the two by about a day.
2. `SHOW CREATE VIEW market_data.automobile.curated_vietnam_auto_sales_unified`
   plus `SELECT DISTINCT source_table` on that view return
   `curated_hyundai_sales` / `curated_vinfast_sales` / `curated_vama_sales_unified`
   — i.e. the live views were built by this copy, not the other one.
3. The `5. Automobile` copy contains
   `f"CAST('{str(value).replace("'", "''")}' AS {typ})"` — a nested same-quote
   f-string, which is a `SyntaxError` on anything below Python 3.12. The newer
   copy hoists that into a separate `escaped = ...` statement, which reads like a
   deliberate fix of exactly that problem.

## What this repo does about it

- `notebooks/automobile/90_build_automobile_unified_sales.py` holds the
  **authoritative (newer) content**, because the unified gold layer belongs in
  the `automobile` part of the tree.
- `scripts/databricks_sync.py` maps `notebooks/automobile/` to the `5. Automobile`
  workspace folder and lists `hyundai_vinfast/90_build_automobile_unified_sales.py`
  in `EXCLUDE`, so `pull` cannot resurrect the duplicate.
- Consequence: `python scripts/databricks_sync.py diff` reports exactly one
  difference until the reconciliation below is done. That is expected, not a bug.

## Outstanding reconciliation (not yet performed)

Nothing in Databricks has been changed. To finish the cleanup:

1. `python scripts/databricks_sync.py push` — overwrites the stale
   `5. Automobile` copy with the authoritative content. `diff` then comes back
   clean.
2. Delete `4. Hyundai VinFast Sales/90_build_automobile_unified_sales` from the
   workspace, so only one copy exists. Verify it is byte-identical to
   `notebooks/automobile/90_build_automobile_unified_sales.py` first.

Both steps mutate the workspace and were deliberately left for a human to
approve.
