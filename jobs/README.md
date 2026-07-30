# Workflow definitions

`etl-data-automobile.json` is the **source of truth** for the monthly job, not a
dump of it. Edit it here and deploy:

```bash
python scripts/deploy_job.py --dry-run
python scripts/deploy_job.py
```

`deploy_job.py` matches the existing job by name and `reset`s it — a full replace,
so anything changed in the Databricks UI and not written back here is lost. Run
`python scripts/export_jobs.py` first if you are not sure the file is current.

See `docs/monthly_workflow.md` for the DAG shape and its ordering constraints.
