#!/usr/bin/env python
"""Create or update the automobile Databricks Workflow from jobs/*.json.

    python scripts/deploy_job.py --dry-run     # show what would change
    python scripts/deploy_job.py               # create or reset the job
    python scripts/deploy_job.py --pause       # deploy with the schedule paused

`jobs/etl-data-automobile.json` is the source of truth for the DAG. Deploying
matches on job *name*: if a job with that name exists its settings are `reset` to
the file's contents, otherwise the job is created. Reset is a full replace, so
anything changed in the Databricks UI and not written back to the file is lost -
run `python scripts/export_jobs.py` first if you are unsure.

Note this deploys the DAG only. The notebooks it points at are workspace copies,
so shipping a notebook change is still `databricks_sync.py push`.

Auth comes from the `DEFAULT` profile in ~/.databrickscfg, or from
DATABRICKS_HOST / DATABRICKS_TOKEN.
"""
import argparse
import glob
import json
import os
import sys

from databricks.sdk import WorkspaceClient

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOBS_DIR = os.path.join(REPO_ROOT, "jobs")


def client():
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        return WorkspaceClient()
    return WorkspaceClient(profile=profile)


def existing_by_name(w):
    found = {}
    for job in w.jobs.list():
        name = job.settings.name if job.settings else None
        if name:
            found.setdefault(name, []).append(job.job_id)
    return found


def check_notebooks_exist(w, settings):
    """Fail before deploying rather than at 08:00 on the 15th."""
    missing = []
    for task in settings.get("tasks", []):
        nb = task.get("notebook_task") or {}
        path = nb.get("notebook_path")
        if not path or nb.get("source") != "WORKSPACE":
            continue
        try:
            w.workspace.get_status(path)
        except Exception:
            missing.append((task["task_key"], path))
    return missing


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="report what would happen, change nothing")
    parser.add_argument("--pause", action="store_true", help="deploy with schedule.pause_status=PAUSED")
    parser.add_argument("--file", help="deploy only this jobs/*.json file")
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    w = client()
    print(f"host : {w.config.host}\n")

    paths = [args.file] if args.file else sorted(glob.glob(os.path.join(JOBS_DIR, "*.json")))
    if not paths:
        print(f"no job definitions found in {JOBS_DIR}")
        return

    by_name = existing_by_name(w)

    for path in paths:
        with open(path, encoding="utf-8") as fh:
            settings = json.load(fh)
        name = settings["name"]

        if args.pause and settings.get("schedule"):
            settings["schedule"]["pause_status"] = "PAUSED"

        missing = check_notebooks_exist(w, settings)
        if missing:
            print(f"REFUSING to deploy {name!r} - notebook path(s) not in the workspace:")
            for task_key, nb_path in missing:
                print(f"    {task_key}: {nb_path}")
            sys.exit(1)

        job_ids = by_name.get(name, [])
        tasks = len(settings.get("tasks", []))
        sched = settings.get("schedule", {})
        cron = sched.get("quartz_cron_expression", "(none)")
        pause = sched.get("pause_status", "-")

        if len(job_ids) > 1:
            print(f"REFUSING to deploy {name!r} - {len(job_ids)} jobs share that name: {job_ids}")
            sys.exit(1)

        verb = "reset" if job_ids else "create"
        print(f"{verb:7} {name!r}  ({tasks} tasks, cron {cron}, {pause})")
        if args.dry_run:
            continue

        # Posted as raw JSON rather than through the typed SDK helpers, so the
        # file is the literal request body and no field needs a matching
        # dataclass in whatever SDK version happens to be installed.
        if job_ids:
            job_id = job_ids[0]
            w.api_client.do("POST", "/api/2.2/jobs/reset",
                            body={"job_id": job_id, "new_settings": settings})
        else:
            job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=settings)["job_id"]
        print(f"        job_id {job_id}")
        print(f"        {w.config.host}/jobs/{job_id}")

    if args.dry_run:
        print("\n(dry run - nothing changed)")


if __name__ == "__main__":
    main()
