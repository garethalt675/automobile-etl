# The monthly Workflow

Job **`etl data automobile`**, job id `647704685836737`, created 2026-07-30 from
`jobs/etl-data-automobile.json`.

Schedule: `0 0 8 15 * ?` in `Asia/Ho_Chi_Minh` — 08:00 on the 15th of each month.
Failure email goes to `garethhoang@gmail.com`. Serverless, environment version 5,
`max_concurrent_runs: 1` with queueing on.

Deploy the DAG with `python scripts/deploy_job.py` (add `--dry-run` to preview,
`--pause` to land it with the trigger off). The script refuses to deploy if any
task's notebook path is missing from the workspace, and refuses if two jobs share
the name. Note it deploys **only the DAG** — the tasks point at workspace
notebooks, so shipping a notebook change is still `databricks_sync.py push`.

## Shape

Two independent source branches that converge on the unified build:

```
vama_crawl ─ vama_parse ─ vama_extract ─ vama_extract_other_makers ─ vama_curated_views ─┐
                                                                                         │
hv_schema ─┬─ hyundai_discover ─ hyundai_fetch ─ hyundai_extract ─ hyundai_validate ─┐    ├─ build_unified
           │                                     hyundai_refill_prose ─ hyundai_curated_view
           │
           └─ vinfast_extract ─ vinfast_curated_view ───────────────────────────────────┘
```

`build_unified` waits on all three curated layers.

## Ordering constraints that are not obvious

**`vama_extract_other_makers` must follow `vama_extract`.** It reads
`sales_by_other_makers` to decide what to do, and its documents are the same
Detail PDFs `vama_extract` walks. Running it first is harmless; running it
*instead* is not.

**`hyundai_refill_prose` must follow `hyundai_extract`.** `hyundai_extract` does
`DELETE ... WHERE source_id IN (...)` across every selected source and re-inserts,
which wipes the six prose-format months (2024-09..2025-01, 2025-10) it cannot
parse. `05_hyundai_refill_prose_missing_months` puts them back. Reverse the order
and those months silently disappear from `hyundai_sales_by_model` on every run.

**`vinfast_extract` does not depend on the Hyundai chain.** They share only the
schema DDL, so they run in parallel.

## Task parameters worth knowing

| Task | Parameter | Default | Use |
| --- | --- | --- | --- |
| `vama_extract` | `reextract_all` | `false` | `true` rebuilds every parsed document — use after changing extraction logic |
| `vama_extract` | `only_months` | `""` | e.g. `2026-05,2026-06` to re-do specific months |
| `vama_extract_other_makers` | same two | same | same |
| `vinfast_extract` | `lookback_months` | `3` | how many complete months back the rolling window reaches |
| `vinfast_extract` | `target_start` / `target_end` | `""` | set both to override the rolling window for a backfill |
| `vinfast_extract` | `replace_existing` | `true` | `false` keeps existing rows for months in the window |

Retries are set only where a failure is plausibly transient — the crawl, the
Hyundai discovery and fetch (2 retries, 5 min apart) and the VinFast Gemini step
(1 retry, 10 min apart). The deterministic extract and view steps have none, so a
logic error surfaces instead of being retried three times.

## Five bugs fixed to make scheduling safe

All five produced correct-looking manual runs and would have silently corrupted
scheduled ones. Numbers 4 and 5 were caught only by actually running the job —
they cost two months of Hyundai history before they were understood.

**1. `3_Extract_Tables` dropped its output table on every run.**
`DROP TABLE IF EXISTS sales_by_model_region` at the top forced a full re-extract
of all 725 parsed documents, re-spent Gemini quota rebuilding the 2,390
`parsing_method='llm'` rows, and left the table empty for the duration — or
permanently, had the run failed part-way, taking
`curated_vama_sales_unified` and the gold view down with it. Now
`CREATE TABLE IF NOT EXISTS`, with document selection restricted to documents that
were never extracted, failed, or have been re-parsed since. The writes were
already `MERGE`s, so nothing else had to change. The Gemini fallback query already
filtered `parsing_method = 'html'`, so persisting the `llm` rows is what stops the
repeat spend.

**2. `3b Extract Other Makers` would have emptied its table on the first run.**
It opened with an unconditional `DELETE FROM sales_by_other_makers`, then selected
documents with `extraction_status <> 'success'` — but `3_Extract_Tables` marks
every document it touches, BMW/Lexus/MBV included, as `'success'`. So the
selection matched **zero** documents while the delete removed all 500 rows. (The
predicate was also NULL-unsafe: a never-extracted document has
`extraction_status IS NULL`, and `NULL <> 'success'` is not true.) It now tracks
progress against `sales_by_other_makers` itself — that column belongs to notebook
3 — and scopes the delete to the batch being appended.

**3. `15_vinfast_ai_search_assisted_extract` was pinned to 2024.**
`TARGET_END = "2024-12"` with the rolling expression commented out one line above
it, `TARGET_START = "2024-01"`, `replace_existing=true`. On a schedule that would
delete and re-Gemini all twelve months of 2024 every run and never advance past
2024-12 — which is why VinFast coverage stalled at 136 rows. Replaced with a
rolling `lookback_months` window ending at the previous complete month, verified
across year boundaries, with explicit `target_start`/`target_end` still winning
for backfills.

**4. `02_hyundai_fetch_sources` overwrote good content with failures.**
`WHEN MATCHED THEN UPDATE SET *` replaced `raw_html` / `extracted_text` /
`content_hash` with `NULL` whenever a page that had previously fetched fine failed
this time. The raw source table was the *only* copy of that text, and everything
downstream re-derives from it, so one bad fetch permanently destroyed a month's
inputs. Failures now update only `fetch_status`, `http_status`, `fetched_at` and
`error_message`, leaving the last good payload intact.

**5. The same notebook could not fetch part of its own source host.**
`hyundai.thanhcong.vn` requires TLS renegotiation that OpenSSL 3.x refuses by
default, raising `SSL: UNSAFE_LEGACY_RENEGOTIATION_DISABLED`. Only some pages on
the host are affected, so a single probe looks healthy — 42 of 44 sources fetched
fine. `fetch()` now passes an SSL context with `OP_LEGACY_SERVER_CONNECT` set;
certificate and hostname verification stay fully enabled. Verified directly: the
default context reproduces the error, the new context returns HTTP 200.

## Data lost on 2026-07-30, and why it cannot be recovered

Hyundai net went from **447 to 435** rows: `2026-06` gained (+12), **`2024-12` and
`2025-10` permanently lost** (-24).

The chain: bug 5 made those two fetches fail → bug 4 wrote the failure over their
stored `extracted_text` → the old unconditional delete in
`05_hyundai_refill_prose_missing_months` removed their rows → the re-derive had no
text to work from → nothing was written back.

It then fired a second time, and that part is worth knowing about. Fixing bug 5
made those two pages fetch successfully again, which turned their stored text from
`NULL` into a generic shell page. `regexp_extract` returns `''` (not `NULL`) for
text that matches nothing, and `CAST('' AS INT)` raises `CAST_INVALID_INPUT` — so
on the second run the old notebook executed its unconditional `DELETE` and *then*
threw, taking the four still-good prose months (2024-09/10/11, 2025-01, 45 rows)
down with it and leaving nothing behind. Those four were recoverable: their source
pages still serve the prose, so a one-off run of the fixed notebook rebuilt them
(390 → 435). The casts are now wrapped in `NULLIF(..., '')`.

The lesson is the ordering, not the cast: **a notebook that deletes before it knows
the replacement rows are valid destroys data every time it fails.** Pushing the fix
mid-run did not help either — the task's retry had already been scheduled and ran
the old code 64 seconds after the first failure.

Neither recovery route is open:

- **Delta time travel:** the pre-run version (v19, 2026-06-03) is beyond
  `delta.deletedFileRetentionDuration` (168 hours), so
  `VERSION AS OF 19` fails outright.
- **Re-deriving from source:** with the SSL fix both URLs now return HTTP 200, but
  both strip to exactly 3,104 characters of a generic shell and match none of the
  prose patterns. The articles have been removed from the site. For contrast, the
  four surviving prose months (2024-09/10/11, 2025-01) return byte-identical text
  to what is stored and still match, which is how the difference was confirmed.

So the loss of those two months is real and permanent. The fixes stop it happening
again — that is all they can do.

## Verified state after 2026-07-30

| Table / view | Rows | Through |
| --- | --- | --- |
| `vama.sales_by_model_region` | 12,802 | 2026-05 |
| `vama.sales_by_other_makers` | 506 | 2026-06 |
| `hyundai_vinfast.hyundai_sales_by_model` | 435 | 2026-06 |
| `hyundai_vinfast.vinfast_sales_by_model` | 136 | 2026-04 |
| `automobile.curated_vietnam_auto_sales_unified` | 13,879 | 2026-06 |

The incremental extract was verified against a pre-change snapshot: no month lost
rows, `2026-05` added (+88), and `parsing_method='llm'` held at exactly 2,390 —
i.e. Gemini output is no longer re-bought every run.

VinFast still stops at 2026-04 because the API key is dead, not because of the
window logic. The rolling window provably targeted 2026-04..2026-06 (all three
months appear in `vinfast_ai_search_queries`).

## Not in the job

`00_specs_hyundai_vinfast_sales` (a spec document),
`ADHOC_Reparse_Detail_PDFs`, `EXAMPLE_Gemini_Parser_Usage`,
`VAMA Missing Data Investigation` (operator/diagnostic notebooks).

## Still open

- `05_hyundai_refill_prose_missing_months` carries a hardcoded
  `TARGET_MONTHS` list. A *new* prose-format month will not be picked up until
  someone adds it. Deterministic and cheap, so running it monthly is safe.
- The Gemini fallback excludes `parsing_method='llm'` rows from validation
  entirely, so a bad LLM parse is never re-checked. 2,390 rows are in that state.
- `sales_by_other_makers.maker` holds ~24 rows of misparsed numeric values
  (`'0'`, `'178'`, `'489'`...), and `MERCEDES-BENZ` stops at 2019-03. Probably the
  same root cause as
  `notebooks/vama/HANDOFF_RowMisalignment_Investigation_2026-05-26.md`.
