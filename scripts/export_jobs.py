#!/usr/bin/env python
"""Export the live automobile Databricks job definitions to jobs/exported/*.json.

    python scripts/export_jobs.py

This reads Databricks and writes what is actually deployed. It writes into
`jobs/exported/`, deliberately NOT over `jobs/*.json` - those are the
hand-maintained source of truth that `deploy_job.py` deploys, and the API returns
them peppered with server-side defaults and reordered keys. Use this to see drift
(diff the two) or to recover a definition someone changed in the UI, then fold the
change into `jobs/*.json` by hand.

Run IDs, timestamps and other per-run state are stripped - only settings are kept.
"""
import json
import os
import re
import sys

from databricks.sdk import WorkspaceClient

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOBS_DIR = os.path.join(REPO_ROOT, "jobs", "exported")

# Match anything whose name starts with one of these (case-insensitive).
JOB_NAME_PREFIXES = ("etl data automobile", "automobile", "vama", "hyundai", "vinfast")


def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    os.makedirs(JOBS_DIR, exist_ok=True)

    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    w = WorkspaceClient() if os.environ.get("DATABRICKS_TOKEN") else WorkspaceClient(profile=profile)

    found = 0
    for job in w.jobs.list():
        name = job.settings.name if job.settings else ""
        if not name or not name.lower().startswith(JOB_NAME_PREFIXES):
            continue
        settings = w.jobs.get(job.job_id).settings.as_dict()
        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
        path = os.path.join(JOBS_DIR, f"{slug}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(settings, fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        print(f"  {slug}.json   ({len(settings.get('tasks', []))} tasks)  <- job {job.job_id}")
        found += 1

    print(f"\n{found} job definition(s) exported to {JOBS_DIR}")
    if not found:
        print(f"  (nothing matched {JOB_NAME_PREFIXES} - check you are on the right workspace)")


if __name__ == "__main__":
    main()
