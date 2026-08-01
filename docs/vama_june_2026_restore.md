# VAMA 2026-06 — why the extraction "failed", and how it was restored

Resolved 2026-08-01. Every claim here was verified against the live workspace or
against the published PDF; none of it is carried over from the earlier handoff,
which got the cause wrong.

## It was not an extraction failure

The extractor worked. The rows were written and then deleted 15 seconds later.
From the Delta history of `market_data.vama.sales_by_model_region`:

```
v50  2026-07-30T01:49:56  MERGE     175 rows written   <- HTML extract; June's 87 among them
v51  2026-07-30T01:50:11  DELETE     87 rows removed   <- document_id = a8d5c421a1c7f7b9
v52  2026-07-30T01:50:13  OPTIMIZE
```

No INSERT follows v51, and `sales_by_model_region_gemini` holds no June rows. The
chain:

1. The HTML extract produced 87 correct rows and merged them.
2. The validation query scored the month as failed. It reads `column_index = 7`
   of the grand-total row as the monthly total; June's grand-total row is
   misaligned by three columns, so column 7 held a year-to-date figure.
3. The Gemini fallback **deleted the document's rows before materialising its
   replacement** — the exact shape `CLAUDE.md` warns about, which had already been
   fixed in two other notebooks and existed a third time here.
4. The Gemini re-parse returned nothing, so the delete was never replaced.
5. The failure path never wrote to `document_processing_log`, so the document kept
   the `extraction_status = 'success'` the HTML extract had given it minutes
   earlier. Nothing retried it and nothing flagged it.

### The year-to-date header hypothesis was wrong

`docs/handoff_2026-07-31.md` suspected an unresolved YTD header — June showing
`col_8` where May showed `Sales - YTM 2026`. That is not what happened. The
published PDF carries `Sales - YTM 2026` on all three pages, and replaying the
extract against the stored `parsed_json` returns 87 rows. Do not go looking for a
header bug again.

## The code was already fixed; only the data was missing

`python scripts/databricks_sync.py diff` reports **0 workspace-only, 0 repo-only,
0 differing**, so the workspace is already running the repaired
`3_Extract_Tables`: the fallback materialises and counts its rows before deleting
anything, the failure path records `failed_llm_fallback`, and the validation
refuses to trust a grand-total row whose own regional cells do not add up to it.

What the fix could **not** do is bring June back. Worse, it would never have run
against it: the incremental filter selects documents where

```
extraction_status IS NULL OR = 'failed' OR extraction_timestamp IS NULL
  OR parsed_timestamp > extraction_timestamp
```

and June's document satisfied none of them —

```
extraction_status  success
extraction_ts      2026-07-30T01:49:46Z
parsed_ts          2026-07-30T01:48:10Z   (older, so no re-parse trigger)
```

The 15 August scheduled run would have skipped it silently.

## The restore

The 87 rows were still one Delta version back and no `VACUUM` had run, so they
were recoverable exactly — no re-extraction, no Gemini, no new parsing code:

```sql
INSERT INTO market_data.vama.sales_by_model_region
SELECT s.*
FROM market_data.vama.sales_by_model_region VERSION AS OF 50 s
LEFT ANTI JOIN market_data.vama.sales_by_model_region c
  ON  c.document_id        = s.document_id
  AND c.source_table_index = s.source_table_index
  AND c.source_row_index   = s.source_row_index
WHERE s.document_id = 'a8d5c421a1c7f7b9'
```

Insert-only, and idempotent by construction: the anti-join drops rows that already
exist, so nothing is ever deleted or overwritten. `source_row_index` alone is not
unique within the document (68 distinct across 87 rows) — the key needs
`source_table_index` too.

```
v53  2026-08-01T16:25:57  WRITE  87 rows   <- restore
v54  2026-08-01T16:26:13  WRITE   0 rows   <- re-run, correctly a no-op
```

Result, in `market_data.automobile.curated_vietnam_auto_sales_unified`:

| Month | Before | After |
| --- | ---: | ---: |
| 2026-06 VAMA rows | 3 | 90 |
| 2026-06 VAMA units | 418 | 23,946 |

`sales_by_model_region` went 12,802 → 12,889 rows. 2026-03/04/05 are unchanged.
The gold view is a view, so it picked the rows up with no rebuild.

## What the restored rows are worth

`monthly_total` was checked against the published PDF, parsed independently with
pdfplumber. Every maker sub-total the PDF prints matches:

| Maker | Restored | PDF sub-total |
| --- | ---: | ---: |
| Toyota | 6,494 | 6,494 |
| Mitsubishi | 3,158 | 3,158 |
| Ford | 2,741 | 2,741 |
| THACO MAZDA | 2,361 | 2,361 |
| THACO TRUCK | 2,173 | 2,173 |
| Honda | 2,002 | 2,002 |
| Isuzu | 900 | 900 |
| Suzuki | 654 | 654 |
| Hino | 229 | 229 |
| BUS THACO | 156 | 156 |
| SAMCO | 10 | 10 |

**`ytd_total` in these rows is shifted by one position.** The extract carries an
extra `Outlander` row the PDF does not list as a data row, and every YTD value
below it slides up one:

| Model | Restored `ytd_total` | PDF |
| --- | ---: | ---: |
| Pajero Sport | 6,299 | 81 |
| Xforce | 717 | 6,299 |
| Attrage | 1,588 | 717 |
| Triton | 21,617 | 1,540 |

21,617 is the Mitsubishi **sub-total**, sitting in Triton's row. This is the
table-wide year-to-date defect described in
`docs/data_quality_2026-08-01.md` — not something June-specific, and not a reason
to withhold the restore, since the monthly figures are correct.

## Two gaps that remain

- June totals **23,528** against the PDF's grand-total of **24,356** — 828 units,
  3.4% short. Part of that is one THACO KIA row worth 409 units that the extract
  misses entirely (restored THACO KIA 2,266 vs the PDF's 2,675). The rest is the
  same row-misalignment defect. Every other month in the table under-reads by
  about the same proportion, so June is now *consistent*, not *exact*.
- `document_processing_log` still reads `extraction_status = 'success'` for this
  document. That is now accurate — it does have rows — so it was left alone.

Fixing the 828-unit gap means fixing
`notebooks/vama/HANDOFF_RowMisalignment_Investigation_2026-05-26.md`, which is
upstream of the extractor in `ai_parse_document` output and affects the whole
table. Extracting the PDF directly is not the shortcut it looks like: the report
carries per-maker, per-category and per-percentage subtotal tiers, and a
from-scratch pdfplumber pass over the June file reconciles to 26,572 against a
true 24,356 until every one of those tiers is classified — which is the work the
existing extractor already does.
