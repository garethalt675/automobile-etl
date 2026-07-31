# VinFast: crawl the release, don't search for it

Built 2026-07-30. Replaces `15_vinfast_ai_search_assisted_extract` in the job with
`16_vinfast_discover_and_fetch` → `17_vinfast_extract_from_sources`.

## Why the old method had to go

Notebook 15 asked Gemini, with Google Search grounding, to *find* a source for a
given month. Two things follow from letting the model pick the source:

**It mislabelled data.** `2024-03` held `2026-03`'s figures — Limo Green 6,795,
VF 3 4,729, VF 5 4,218, VF 6 3,152, VF 7 1,732 — sourced from a `plo.vn` article.
Limo Green did not exist in March 2024. `2024-05` had the same problem against
`2025-05`. Both months were stored `warning`, not `fail`, so nothing downstream
objected. There is no way to tell which other months are affected except by
checking each one by hand.

**It was billing-dependent.** Grounding requires quota the current API key's Google
project does not have (`docs/gemini_key.md`).

## What replaces it

```
16_vinfast_discover_and_fetch   crawl the IR newsroom, fetch each month's release
17_vinfast_extract_from_sources Gemini (ungrounded) turns fetched text into rows
```

The month now comes from a URL we chose. Gemini never decides which month or which
source a number belongs to — it only reformats text already fetched for a month
already selected. That removes the mislabelling failure by construction, and it
removes the grounding dependency as a side effect.

### Discovery

VinFast has used at least five slug shapes for the same monthly release:

```
vinfast-reports-deliveries-of-17955-electric-vehicles-in-june-2026-in
vinfast-delivered-10922-electric-vehicles-in-vietnam-in-august-2025
vinfast-delivers-23186-electric-vehicles-in-november-2025-in-vietnam
vinfast-sets-another-record-delivering-more-than-20000-cars-in-october-2025
vinfast-announces-4q24-global-deliveries-january-2025-domestic-deliveries
```

So discovery matches "a delivery release naming a month and a year" rather than
enumerating shapes, and pulls the exact total out separately. The last two shapes
carry no exact monthly total (`more-than` is a rounded claim), so `headline_total`
is NULL for those and notebook 17 skips the cross-check rather than failing.

`?page=N` on the IR index is **not** pagination — every page returns the same
~11 recent releases. There is no archive route from this source.

### The four guards

Applied after the model, before anything is written:

| # | Guard | Catches |
| --- | --- | --- |
| 0 | Page must mention the month and year the slug claimed | A mis-parsed slug filing one month under another |
| 1 | Every number must appear literally in the fetched text | Invented figures |
| 2 | Monthly total must equal the total in the URL slug | Wrong article, wrong total — slug is never shown to the model |
| 3 | Model rows may not exceed the monthly total | Row-level nonsense; the residual becomes an explicit `Other models` row |

A month failing any guard is dropped, not written. A month whose release states no
monthly total is **skipped**, because without a total there is no residual and the
month would be written understated — worse than what may already be stored.

## Validation

Ten months extracted. The eight that already had data reproduced their existing
totals exactly:

| Month | Total | | Month | Total |
| --- | --- | --- | --- | --- |
| 2025-04 | 9,588 | | 2025-11 | 23,186 |
| 2025-05 | 11,496 | | 2026-03 | 27,609 |
| 2025-06 | 11,382 | | 2026-04 | 24,774 |
| 2025-08 | 10,922 | | **2026-05** | **19,503** (new) |
| 2025-10 | 20,380 | | **2026-06** | **17,955** (new) |

Before/after was compared **per month**, not as a table total — a whole-table count
hides a month lost against a month gained. Result: no month lost, no existing month
changed, two months gained, 136 → 151 rows.

## Still open

**`2024-03` and `2024-05` were deleted on 2026-07-30** (11 rows). They are not a
gap that can be filled — see the section below.

**There is a richer source already fetched.** `vinfast_raw_sources` — a *different*
table from the `vinfast_ir_sources` this branch writes — holds 22 fetched
Vietnamese-language articles from `vinfast.vn` spanning 2022-11 to 2026-04,
including months the US IR site never carries (2022, 2023, 2024-11, 2024-12,
2025-01/02/03, 2026-01/02). Its slugs also encode month and total
(`vinfast-ban-giao-11-496-xe-o-to-dien-trong-thang-5-2025`). Adding it as a second
discovery source in notebook 16 would extend coverage considerably. Not done here.

**Do not confuse the two tables.** `vinfast_raw_sources` is the legacy
`vinfast.vn` table with a different schema (`url`, `raw_html`, `content_type`).
`vinfast_ir_sources` is the one this branch writes. Notebook 16 originally targeted
the former, and `CREATE TABLE IF NOT EXISTS` silently did nothing against the
existing schema — the MERGE then failed on an unresolvable `url` column.

## Why 2024-03 and 2024-05 cannot be filled

VinFast **did not publish monthly delivery figures for most of 2024**. It reported
monthly by model through July 2023, switched to quarterly, and only resumed monthly
Vietnam releases in November 2024.

Evidence, in descending order of strength:

- **VinFast's own sitemap.** A full crawl of `vinfast.vn` (1,040 URLs across 12
  sub-sitemaps) contains exactly **two** 2024 monthly delivery articles: November
  2024 and December 2024. Nothing for March or May. This is the publisher's entire
  archive, not a search result.
- **The Q1 2024 6-K** (`tm2411945d1_ex99-1.htm`) reports *quarterly only*: "EV
  Deliveries 9,689" for 1Q2024, against 13,513 for 4Q2023 and 1,780 for 1Q2023. No
  monthly split anywhere in the filing.
- **The H1 2024 release** likewise gives Q2 2024 = 12,058 and H1 = 21,747, with no
  monthly or per-model breakdown.
- **Vietnamese Wikipedia's VinFast sales table** is monthly by model through July
  2023, then quarterly, then a 2024 annual figure only.

The stored values were also arithmetically impossible, which is independent of
sourcing. Both are **global** ceilings, and Vietnam is a subset:

| Month | Was stored | Official quarterly ceiling | |
| --- | --- | --- | --- |
| `2024-03` | 27,609 | Q1 2024 = **9,689** for all three months | 2.8× the whole quarter |
| `2024-05` | 11,496 | Q2 2024 = **12,058** for all three months | leaves 562 for April + June |

So the rows were deleted rather than corrected. Absence is the truthful state, and
it matches how 2023-06 and 2023-08..2024-06 are already absent. Backup of the 11
deleted rows was taken before the delete.

**Do not "fill" these from third-party estimates.** Registration-based or
press-estimated monthly numbers are not what the rest of this table contains, and
mixing them in silently is a milder version of the bug that created the problem.
If quarterly figures are ever wanted, they exist and are official — but the table's
grain is monthly, and Q1/Q2 2024 are global rather than Vietnam.

## Notebook 15

Kept in the repo for provenance, removed from the job. The rows it produced are
still in `vinfast_sales_by_model` under
`parsing_method = 'gemini_google_search_grounding_live_databricks'` — 73 of them, in
months the new branch has not re-extracted.
