# Workflow definitions

Empty by design as of 2026-07-30 — the automobile pipeline has no Databricks
Workflows yet, so `python scripts/export_jobs.py` has nothing to export. Every
notebook is currently run by hand.

Once monthly jobs exist, name them with a prefix `export_jobs.py` matches (see
`JOB_NAME_PREFIXES`) and re-run it to land the DAGs here.
