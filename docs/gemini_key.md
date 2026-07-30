# The Gemini API key

## Where it lives

**Databricks secret `news-signal` / `gemini-api-key`.** That is the only place to
rotate it.

The workspace file `/Workspace/Users/tuckeyhue@gmail.com/env/.env.` also has a
`GEMINI_API_KEY` entry. It is **stale and dead** and is kept only as a fallback for
code that predates the secret. Do not rotate there.

## Why the read order matters

Until 2026-07-30, both `15_vinfast_ai_search_assisted_extract` and
`vama_parser.GeminiParser` read the `.env.` file **first** and treated a Databricks
secret as an afterthought. The key was rotated into the secret on **2026-07-24**,
and it changed nothing — both call paths kept picking up the dead `.env.` copy and
every call came back `400 API_KEY_INVALID`.

So the failure was never "the key is expired". It was "the rotated key was never
read". Both loaders now try the secret first and fall back to the file, logging
which source won and the key's *length* only.

Confirmed from inside a serverless job run:

| | |
| --- | --- |
| secret resolves | yes, length 53 |
| `.env.` file entry | length 39, **different key** |
| ungrounded `generateContent` | HTTP 200 |
| grounded (`googleSearch`) | HTTP 429 |
| `gemini-2.5-flash` | HTTP 404 |

## The two things that broke with the new key

**1. `gemini-2.5-flash` no longer exists for this project.** It returns
`404 ... no longer available to new users`. The new key belongs to a different
Google project, and that model is closed to projects that had not already used it.
`15_...` was pinned to it (a 2026-07-30 change made "due to quota") and is now back
on `gemini-3.1-flash-lite`, which the key can reach.

**2. Google Search grounding has zero quota on this key's project.** Every
grounded call returns `429 RESOURCE_EXHAUSTED`, on every model and both tool
spellings (`google_search`, `googleSearch`, `google_search_retrieval`), while an
*ungrounded* call to the same model on the same key returns 200 instantly. That
rules out a per-model or per-minute limit — the project simply has no grounding
allowance, which on the Gemini API requires billing to be enabled.

This is the one thing code cannot fix.

## What that means per consumer

| Consumer | Uses grounding? | Status |
| --- | --- | --- |
| `vama_parser.GeminiParser` (VAMA fallback) | no | **working** — restored by the secret fix |
| `17_vinfast_extract_from_sources` | no | **working** — grounding designed out |
| `15_vinfast_ai_search_assisted_extract` | yes, it *is* the mechanism | **retired**, out of the job |

**Nothing in the pipeline needs Google Search grounding any more.** VinFast used to,
and the rewrite in `docs/vinfast_crawl.md` removed the need: the month now comes from
a URL we choose, and Gemini only reformats text we already fetched. Enabling billing
is therefore optional — it would only bring notebook 15 back to life, and notebook 15
is the one that mislabelled data.

If you do want to check whether grounding is available, a single grounded call is the
whole test: 200 means go, 429 means still no quota.

## Failure mode is contained

Both `16_` and `17_` raise when every month fails, so the failure is loud rather than
a green task ingesting nothing. `vinfast_curated_view` is `run_if: ALL_DONE`, so a
dead Gemini key no longer blocks the gold rebuild — see `docs/monthly_workflow.md`.
