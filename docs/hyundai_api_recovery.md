# Hyundai source recovery via the hyundai.thanhcong.vn JSON API

Written 2026-08-01, after `2024-12` and `2025-10` — both recorded as permanently
lost — were retrieved intact. **Both months are recoverable.** This supersedes the
"not recoverable" note in `CLAUDE.md` and `docs/handoff_2026-07-31.md`.

## What was actually wrong

`hyundai.thanhcong.vn` is a Nuxt SPA. Fetching the article URL over plain HTTP
returns a shell: SEO metadata and the product-navigation menu, no article body.
Every previous check read that shell, saw no sales table, and concluded the article
was gone. The conclusion did not follow — the body is fetched client-side, so
"absent from the HTML" only ever meant "absent before JavaScript runs".

Two things made this hard to notice:

- The model names *are* present in the shell HTML (`Grand i10`, `Accent`,
  `Santa Fe`…) — but as product-menu entries, so a keyword check for model names
  finds hits and looks reassuring while the table is still missing.
- The article's attachment is named `Doanh so TC Group- Hyundai-2024.jpg`
  ("Doanh so" = sales), which reads like the sales table. It is a Santa Fe press
  photo. Do not treat that filename as evidence of a table.

Headless Chromium does **not** work around it either: the host resets the
connection (`ERR_CONNECTION_RESET`). Use the API below instead.

## Transport: legacy TLS renegotiation is required

`hyundai.thanhcong.vn` and `hyundai-api.thanhcong.vn` need unsafe legacy
renegotiation; without it OpenSSL 3 fails with
`error:0A000152:SSL routines::unsafe legacy renegotiation disabled`.

```ini
# legacy.cnf
openssl_conf = openssl_init
[openssl_init]
ssl_conf = ssl_sect
[ssl_sect]
system_default = sysdef
[sysdef]
Options = UnsafeLegacyRenegotiation,UnsafeLegacyServerConnect
CipherString = DEFAULT@SECLEVEL=0
```

`OPENSSL_CONF=legacy.cnf curl …`. In Python, set the equivalent on the SSL
context (`ssl.OP_LEGACY_SERVER_CONNECT`) rather than disabling verification.

Both hosts also return sporadic 404/503 on the first hit and succeed on retry —
the existing retry-with-backoff convention applies.

## The endpoints

API base is `https://hyundai-api.thanhcong.vn/api/front` (found in
`/_nuxt/app.*.js`; the site itself calls `/post/show?slug=`).

**Index of every company-news post** — 350 posts, 67 of them monthly sales
releases, covering **2021-03 → 2026-06 with no gaps**, plus a stray 2018-08:

```
GET /post?group=htv-tin-cong-ty&itemsPerPage=-1&limit=500
    → data.posts[] : {id, title, slug, sort_description, created_at, …}
```

This is a far better discovery source than guessing sitemap slug shapes — it is
the publisher's own index, and it is what should feed `01_hyundai_discover_sources`.
Filter on `'ban-hang' in slug`, then read the month from the slug
(`thang-(\d{1,2})-(\d{4})`) — **but confirm it against the article title**, since
slugs in this project have named the wrong month before.

**Full article body, including the sales table:**

```
GET /post/show?slug=<slug>
    → data.post.content : article HTML with the <table>
    → data.post.sort_description : prose summary carrying the month total
```

`sort_description` is a useful independent cross-check: for `2024-12` it reads
"tổng doanh số xe Hyundai tháng 12 đạt 8.316 xe", matching the table's `TỔNG`.

Note `id` in the SSR payload (`seo:{id:626,…}`) is the **SEO record id**, not the
post id (632). `?id=` is ignored by the list endpoint — it returns an unfiltered
page. Only `slug` selects a single article.

## Parsing rule: align columns from the RIGHT

The table is ragged. Only `Grand i10`, `Stargazer` and `XE THƯƠNG MẠI` carry the
`CBU/CKD` cell; every other model row omits it entirely, so a fixed left-hand
column index puts `Accent`'s previous-month figure in the CBU/CKD slot. This is
the same class of defect as
`notebooks/vama/HANDOFF_RowMisalignment_Investigation_2026-05-26.md`.

**Take the last three cells of each row**: `[prev month, report month, year-to-date]`.
That holds for every row in both months, including the short `Mẫu khác` row in
2024-12 (`['Mẫu khác', '', '32', '35']` → prev blank, 32, 35).

Do not key on the header text either — it is not stable:

| month | header |
| --- | --- |
| 2024-12 | `Mẫu xe \| CBU/CKD \| T11 - 2024 \| T12 - 2024 \| Cộng dồn 2024` |
| 2025-10 | `Mẫu xe \| CBU/CKD \| Tháng 9 \| Tháng 10 \| Cả năm 2025` |

Take the reporting month from the article title, not the column label.

## The two recovered months

Both reconcile **exactly** on all three columns — which is what `04_hyundai_validate`
requires to promote a month to `pass`, so neither needs any loosening of the check.

### 2024-12 — `TỔNG` 8,316 · YTD 67,168

| Model | CBU/CKD | T11-2024 | T12-2024 | Cộng dồn 2024 |
| --- | --- | ---: | ---: | ---: |
| Grand i10 | CKD | 1,035 | 751 | 5,831 |
| Accent | | 2,052 | 1,861 | 13,538 |
| Elantra | | 222 | 146 | 1,845 |
| Venue | | 690 | 566 | 4,465 |
| Tucson | | 1,584 | 970 | 6,641 |
| Santa Fe | | 1,206 | 829 | 6,941 |
| Creta | | 1,330 | 901 | 8,640 |
| Palisade | | 242 | 191 | 1,587 |
| Custin | | 395 | 444 | 3,101 |
| Stargazer | CBU | 468 | 704 | 4,159 |
| XE THƯƠNG MẠI | CKD/CBU | 1,079 | 921 | 10,385 |
| Mẫu khác | | — | 32 | 35 |
| **TỔNG** | | **10,303** | **8,316** | **67,168** |

Sums: 10,303 ✓ · 8,316 ✓ · 67,168 ✓

### 2025-10 — `TỔNG` 5,260 · YTD 41,062

| Model | CBU/CKD | Tháng 9 | Tháng 10 | Cả năm 2025 |
| --- | --- | ---: | ---: | ---: |
| Grand i10 | CKD | 224 | 362 | 2,617 |
| Accent | | 406 | 609 | 5,644 |
| Elantra | | 44 | 61 | 539 |
| Venue | | 140 | 184 | 1,837 |
| Tucson | | 952 | 929 | 6,839 |
| Santa Fe | | 202 | 208 | 2,179 |
| Creta | | 915 | 1,022 | 6,114 |
| Palisade | | 120 | 115 | 916 |
| Custin | | 106 | 117 | 1,184 |
| Stargazer | CBU | 220 | 285 | 2,673 |
| XE THƯƠNG MẠI | CKD/CBU | 956 | 1,363 | 10,458 |
| Mẫu khác | | 11 | 5 | 62 |
| **TỔNG** | | **4,296** | **5,260** | **41,062** |

Sums: 4,296 ✓ · 5,260 ✓ · 41,062 ✓

Note `2025-10`'s previous-month column (Tháng 9 = 4,296) is an independent check on
`2025-09`, which is already in the table.

## What still has to happen

None of this is in Databricks yet — the 2026-08-01 session had no credential, and
the workspace is authoritative, so nothing here has been ingested. Remaining work:

1. Point `01_hyundai_discover_sources` at the index endpoint instead of sitemap
   slug guessing; it resolves the brand-less-slug problem too.
2. Give `02_hyundai_fetch_sources` the API path and the legacy-TLS transport, so
   `hyundai_raw_sources.extracted_text` gets a real body for these months. Per
   `CLAUDE.md`, that write must stay conditional on the fetch having succeeded —
   an SPA shell is a *successful HTTP 200 with no data*, which is exactly the
   shape that nulls the only stored copy. Validate that the body contains a
   `<table>` with a `TỔNG` row before writing.
3. Drop `2024-12` and `2025-10` from the "cannot be derived" list in
   `05_hyundai_refill_prose_missing_months`, and reconsider its hardcoded
   `TARGET_MONTHS` now that the index gives every published month.
