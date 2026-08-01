# Data quality report — `market_data.automobile` gold output

Assessed 2026-08-01 against the live workspace
(`dbc-5a6b7518-84a8`, warehouse `7eb5fd2336243915`). Every figure below was read
from Databricks with read-only SQL; nothing here is inferred from the notebooks.

Subject: `market_data.automobile.curated_vietnam_auto_sales_unified` (13,895 rows)
and its companion `auto_sales_source_quality`.

## Verdict

**The monthly-total column is broadly trustworthy for VAMA back-history. The
year-to-date columns are not usable at all, the newest month (2026-06) is 98%
empty, and the quality view reports every one of these problems as `pass`.**

The most damaging property of this dataset is not any single wrong number — it is
that `validation_status` and `is_analytics_ready` are hardcoded literals for 99%
of rows, so nothing in the output distinguishes a clean month from a destroyed
one.

| | Rows | Share |
| --- | ---: | ---: |
| Total rows in gold | 13,895 | 100% |
| Carrying a usable `monthly_total` | 11,842 | 85.2% |
| `monthly_total` NULL | 2,053 | 14.8% |
| No figures at all (monthly *and* YTD NULL) | 764 | 5.5% |
| Labelled `validation_status='pass'` | 13,852 | 99.7% |
| …of those, labels actually computed from a check | 97 | 0.7% |
| Rows whose status came from a real validator at all | 140 | 1.0% |

## Coverage and freshness

Last extraction 2026-07-30/31; all three sources nominally reach 2026-06.

| Source | Rows | Months | Range | Units |
| --- | ---: | ---: | --- | ---: |
| VAMA | 13,308 | 145 | 2014-01 – 2026-06 | 2,842,214 |
| Hyundai | 447 | 42 | 2022-10 – 2026-06 | 212,392 |
| VinFast | 140 | 32 | 2022-11 – 2026-06 | 373,209 |

Missing months, measured against a continuous calendar from each source's first
month:

| Source | Missing | Months |
| --- | ---: | --- |
| VAMA | 6 | 2016-01, 2018-08, 2018-09, 2018-10, 2018-11, 2019-02 |
| Hyundai | 3 | 2022-12, 2024-12, 2025-10 |
| VinFast | 12 | 2023-06, 2023-08 – 2024-06 |

The VAMA gaps trace to `document_processing_log`: 21 documents never downloaded
(17 of them in 2018), 1 parsed but failed extraction (2018-09 detail). The
VinFast gap is expected — the publisher issued no monthly figures for most of
2024 (`docs/vinfast_crawl.md`). Hyundai `2024-12` and `2025-10` are recoverable
via the publisher's JSON API (`docs/hyundai_api_recovery.md`) but are not
ingested.

---

## Critical

### 1. 2026-06 is present but 98.3% empty, and nothing will fix it

June 2026 carries **418 units against a true ~24,356** (the published PDF's own
grand-total row, per `docs/handoff_2026-07-31.md`).

| Month | VAMA rows | VAMA units |
| --- | ---: | ---: |
| 2026-03 | 81 | 30,806 |
| 2026-04 | 85 | 24,115 |
| 2026-05 | 91 | 23,764 |
| **2026-06** | **3** | **418** |

The three surviving rows are the Lexus (2) and BMW-Mini (1) documents. The detail
document contributed nothing. This is the incident already root-caused in the
handoff: the Gemini fallback deleted the 87 correctly-extracted rows and its
re-parse produced nothing to replace them.

**The fix is in the code but has not taken effect, and the schedule will not
trigger it.** The document still carries `extraction_status = 'success'`:

```
document_id        a8d5c421a1c7f7b9   (VAMA sales report June 2026 - Detail.pdf)
extraction_status  success
extraction_ts      2026-07-30T01:49:46Z
parsed_ts          2026-07-30T01:48:10Z   (older, so no re-parse trigger)
```

The incremental filter in `3_Extract_Tables` selects documents where
`extraction_status IS NULL OR = 'failed' OR extraction_timestamp IS NULL OR
parsed_timestamp > extraction_timestamp`. All four are false here, so the
15 August run **skips this document**. Recovering June needs an explicit
`only_months=2026-06` or `reextract_all=true` run.

### 2. Every YTD column is structurally wrong across the whole history

The `ytd_*` columns are not merely sparse — they hold the wrong source columns,
spanning 2015-01 to 2026-05.

| Check | Result |
| --- | --- |
| `ytd_north` NULL (VAMA) | 44.3% of 13,308 rows |
| `ytd_total` < `monthly_total` | 777 rows — impossible for a year-to-date figure |
| YTD *decreasing* month-over-month within a year | **1,468 of 9,581 comparable pairs (15.3%)** |
| `ytd_north + ytd_central + ytd_south ≠ ytd_total` | 77 rows |

The monotonicity test is the decisive one: a genuine year-to-date series cannot
fall. 15.3% of it does.

Cause, per the handoff: `detect_column_structure` derived the YTD block by fixed
offset (`data_start + 4..7`), but a `Share` column sits between the monthly and
YTD blocks. `ytd_north` was reading the monthly share percentage (`'11.0%'` →
NULL), and `ytd_total` was holding the YTD *South* figure. Fixed in
`3_Extract_Tables` by locating the block from its own `Sales - YTM <year>`
header — but **correcting history requires a `reextract_all` run that has not
been done.**

> Any year-to-date number read off the gold view today is wrong. Treat the
> `ytd_*` columns as unavailable, not as approximate.

### 3. The quality layer certifies the failures

`validation_status` and `is_analytics_ready` are **string literals** in
`90_build_automobile_unified_sales.py` — `'pass'` / `true` for VAMA (lines
144, 147) and Hyundai (161, 163). Only VinFast's 140 rows carry a computed
status.

So `auto_sales_source_quality` reports the June collapse as healthy:

```
source_system  report_month  validation_status  rows  excluded  is_analytics_ready_month
VAMA           2026-05       pass                91         0   true
VAMA           2026-06       pass                 3         0   true      ← 98% of the month gone
```

Two further defects in that view:

- **Zero excluded rows are ever reported** for VAMA and Hyundai, by construction
  (`CAST(0 AS BIGINT) AS excluded_row_count`).
- **The Hyundai rollup is degenerate.** It groups by `report_month, document_id`,
  and `document_id` is unique per row in `curated_hyundai_sales` (447 rows, 447
  distinct ids). The result is 447 one-row "months" — not a monthly summary.

The view's own verification gates in the build notebook pass cleanly, because
they assert on the hardcoded values.

---

## High

### 4. Hyundai months the pipeline itself failed are published as `pass`

`hyundai_monthly_validation` holds **8** `fail` months. Six of them reach gold,
relabelled `pass` / `is_analytics_ready = true` by the hardcoded literal.

| Month | Official total | Extracted | In gold | Gap |
| --- | ---: | ---: | ---: | --- |
| 2023-04 | 4,592 | 1,885 | 1,885 | **−2,707 (−59%)** |
| 2024-03 | 4,542 | 4,597 | 4,597 | +55 |
| 2025-02 | 3,022 | 3,022 | 3,022 | monthly OK, YTD off by 10 |
| 2025-05 | 4,063 | 4,063 | 4,063 | monthly OK, YTD off by 2,351 |
| 2025-06 | 4,197 | 4,197 | 4,197 | monthly OK, source YTD corrupt (`24`) |
| 2022-06 | 4,278 | — | 0 rows | month absent |

2023-04 is the serious one: it understates Hyundai by 59% and carries a `pass`
label. The validator caught it; the gold build discards the answer.

(Note: `CLAUDE.md` records 6 fail months and 5 VinFast fails. Live counts are now
**8** Hyundai fail, and VinFast **16 pass / 16 warning / 4 fail**.)

### 5. 14.8% of rows carry no monthly figure

2,053 rows have a NULL `monthly_total`; 764 have neither monthly nor YTD. They
concentrate in the LLM-parsed path:

| Source | `parsing_method` | Rows | NULL `monthly_total` | Rate |
| --- | --- | ---: | ---: | ---: |
| VAMA | `html` | 10,412 | 1,091 | 10.5% |
| VAMA | `llm` | 2,390 | 962 | **40.3%** |
| VAMA | `gemini_html` | 506 | 0 | 0% |
| Hyundai | — | 447 | 0 | 0% |
| VinFast | (4 methods) | 140 | 0 | 0% |

The `llm` rows are the ones the Gemini fallback excludes from validation, so a
bad LLM parse is never re-checked — and 40% of them arrive with no sales number
at all. All 2,390 are labelled `pass`.

### 6. Aggregate rows are mixed into model-level data with the flag dropped

Both source tables carry an aggregate flag. **Neither is propagated to gold** —
the columns are simply not in the view's column list, and the build notebook
computes a `vinfast_aggregate_filter` (line 194) that it then never uses.

| Source | Flag | Rows | Units | Appears in gold as |
| --- | --- | ---: | ---: | --- |
| Hyundai | `is_commercial_vehicle` | 42 | 39,831 | model `XE THƯƠNG MẠI` |
| VinFast | `is_aggregate` | 31 | 74,325 | models `Other models`, `TOTAL_UNSPECIFIED` |

**114,156 units — 3.5% of all volume — are rollups wearing a model name.**
`XE THƯƠNG MẠI` ("commercial vehicles") is Hyundai's second-largest "model" in
gold, ahead of Creta.

One point in the design's favour: VinFast's 8 `TOTAL_UNSPECIFIED` months carry no
model rows, so aggregates substitute rather than duplicate. The problem is purely
that a consumer cannot tell which is which.

---

## Medium

### 7. Row-count anomalies suggest fragmented parses

Three detail documents produce ~70% more rows than neighbouring months while
reporting *lower* volume — the signature of split or misaligned rows, not growth:

| Month | Detail rows | Detail units |
| --- | ---: | ---: |
| 2025-05 | 151 | 22,417 |
| 2026-01 | 146 | 29,315 |
| 2026-02 | 146 | 15,548 |
| 2026-05 (normal) | 88 | 23,253 |

### 8. Duplicate model-months

136 duplicate `(source_system, report_month, maker, model_name, seat)` groups
covering 286 rows. Most are Mitsubishi Pajero Sport appearing 3× within a single
2015–2016 document with unrelated totals (e.g. 2015-09: 20 / 12 / 168) — the
row-misalignment defect tracked in
`notebooks/vama/HANDOFF_RowMisalignment_Investigation_2026-05-26.md`.

The build notebook's duplicate gate does not catch these: it keys on
`source_row_index`, which differs per row.

### 9. Field-level junk

| Issue | Rows |
| --- | ---: |
| NULL `report_month` (all `report_year = 2019`) | 107 |
| NULL/empty `model_name` | 66 |
| Numeric `maker` values (`'0'`, `'178'`, `'489'`, …, 12 distinct) | 24 |
| `monthly_north + central + south ≠ monthly_total` (VAMA) | 100 |

The 107 NULL-month rows cannot be attributed to any period and silently drop out
of every month-grouped query.

### 10. Hyundai commercial vehicles are counted through two channels

The VAMA slice contains Hyundai `Mighty CKD` / `Mighty CBU` light trucks and
`County` buses — 3,344 units across 25 months that overlap the Hyundai source
(2022-10 – 2024-12). Hyundai's own `XE THƯƠNG MẠI` rollup covers commercial
vehicles too, so summing "Hyundai" across `source_system` values likely
double-counts. Not quantifiable exactly without a model-level mapping, but the
overlap window is real.

---

## What this means for use

| Column group | Verdict |
| --- | --- |
| `monthly_total`, `monthly_north/central/south` | **Usable with caveats** — 85% populated, ~3.4% systematic under-read from row misalignment, 2026-06 unusable |
| `ytd_north/central/south/total` | **Do not use** — structurally wrong across the full history |
| `validation_status`, `is_analytics_ready` | **Do not use** — hardcoded for 99% of rows |
| `report_month` | Reliable except 107 NULL rows |
| `maker`, `model_name` | Reliable except 24 junk makers, 66 NULL models, and unflagged aggregates |

Safe aggregate query today: sum `monthly_total`, filter
`report_month IS NOT NULL AND report_month <> '2026-06'`, and exclude
`model_name IN ('XE THƯƠNG MẠI','Mẫu khác','Other models','TOTAL_UNSPECIFIED')`
for model-level work.

## Recommended order of work

1. **Re-extract 2026-06** (`only_months=2026-06`). Restores ~24,000 units.
   Highest impact, smallest change.
2. **Stop the QA layer lying.** Replace the hardcoded `'pass'` / `true` in
   `90_build_automobile_unified_sales.py` with the real status from
   `hyundai_monthly_validation`, and add a month-over-month volume check so a
   98%-empty month cannot report `pass`.
3. **`reextract_all` to correct YTD history** — the column-location fix is
   already in `3_Extract_Tables` but has never been applied to stored rows.
   Expect the ~11% row-misalignment corruption to survive it (that defect is in
   `ai_parse_document` output, upstream of the extractor).
4. **Propagate `is_commercial_vehicle` / `is_aggregate` into gold**, or filter
   them out. The `vinfast_aggregate_filter` variable is already computed and
   unused.
5. **Bring the 2,390 `llm` rows into validation**, or mark them
   `is_analytics_ready = false`. 40% have no sales figure.
6. Ingest Hyundai 2024-12 and 2025-10 per `docs/hyundai_api_recovery.md`.

## Reproducing this

All queries were read-only against the live warehouse. The runner used is not
committed (it reads `host` / `token` from the environment); the checks are
plain SQL against `market_data.automobile.curated_vietnam_auto_sales_unified`,
`market_data.automobile.auto_sales_source_quality`,
`market_data.vama.document_processing_log`,
`market_data.hyundai_vinfast.hyundai_monthly_validation` and the two curated
source tables.
