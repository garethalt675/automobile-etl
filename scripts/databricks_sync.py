#!/usr/bin/env python
"""Sync notebooks between this repo and the Databricks workspace folders.

    python scripts/databricks_sync.py diff    # show what differs (run this first)
    python scripts/databricks_sync.py pull    # workspace -> repo
    python scripts/databricks_sync.py push    # repo -> workspace

The automobile pipeline lives in three sibling workspace folders that this repo
flattens into one tree under `notebooks/`:

    notebooks/vama/             <- ".../1. Data ETL/2. VAMA"
    notebooks/hyundai_vinfast/  <- ".../1. Data ETL/4. Hyundai VinFast Sales"
    notebooks/automobile/       <- ".../1. Data ETL/5. Automobile"

`EXCLUDE` skips workspace subtrees that are not part of this project (the
Shinhan FX folder that was parked inside "2. VAMA" belongs to the banking work).

The Databricks workspace is the source of truth: pushing to GitHub does NOT
update Databricks, only `push` here or a Databricks Git folder does. Always
`diff` before editing either side.

Auth comes from the `DEFAULT` profile in ~/.databrickscfg, or from
DATABRICKS_HOST / DATABRICKS_TOKEN. Nothing here reads or writes credentials.
"""
import argparse
import base64
import difflib
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_DIR = os.path.join(REPO_ROOT, "notebooks")

WS_ROOT = os.environ.get(
    "AUTOMOBILE_WS_ROOT",
    "/Workspace/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL",
)

# repo subdirectory under notebooks/  ->  workspace folder name under WS_ROOT
FOLDERS = {
    "vama": "2. VAMA",
    "hyundai_vinfast": "4. Hyundai VinFast Sales",
    "automobile": "5. Automobile",
}

# Workspace-relative prefixes (repo-style paths) to leave alone entirely.
#
#   vama/Shinhan FX Daily
#       banking FX work that was parked inside "2. VAMA"; not part of this project.
#
#   hyundai_vinfast/90_build_automobile_unified_sales.py
#       a SECOND copy of the unified build notebook lives in "4. Hyundai VinFast
#       Sales". As of 2026-07-30 it is the newer of the two (2026-06-05 vs
#       2026-06-04) and is the one that actually built the live
#       market_data.automobile views, so the repo tracks it once, under
#       notebooks/automobile/. It is excluded here so `pull` cannot resurrect the
#       duplicate. See docs/unified_notebook_duplicate.md.
EXCLUDE = (
    "vama/Shinhan FX Daily",
    "hyundai_vinfast/90_build_automobile_unified_sales.py",
)

# Notebook source extension <-> Databricks language
EXT_TO_LANG = {".py": Language.PYTHON, ".sql": Language.SQL, ".scala": Language.SCALA, ".r": Language.R}
LANG_TO_EXT = {"PYTHON": ".py", "SQL": ".sql", "SCALA": ".scala", "R": ".r"}

BOM = "﻿".encode("utf-8")


def client():
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        return WorkspaceClient()
    return WorkspaceClient(profile=profile)


def excluded(rel):
    posix = rel.replace("\\", "/")
    return any(posix == e or posix.startswith(e + "/") for e in EXCLUDE)


def _walk(w, ws_path, rel):
    for obj in sorted(w.workspace.list(ws_path), key=lambda o: o.path):
        name = obj.path.rsplit("/", 1)[-1]
        kind = obj.object_type.value
        child = f"{rel}/{name}" if rel else name
        if kind == "NOTEBOOK":
            # Exclusions are matched against the repo path, which carries the
            # extension the notebook is exported with.
            child += LANG_TO_EXT.get(obj.language.value if obj.language else "", ".py")
        if excluded(child):
            continue
        if kind == "DIRECTORY":
            yield from _walk(w, obj.path, child)
        elif kind == "NOTEBOOK":
            yield child, obj.path, True
        elif kind == "FILE":
            yield child, obj.path, False


def workspace_files(w):
    """Yield (repo-relative path with extension, workspace path, is_notebook)."""
    for sub, folder in FOLDERS.items():
        yield from _walk(w, f"{WS_ROOT}/{folder}", sub)


def repo_files():
    for root, _, files in os.walk(NOTEBOOK_DIR):
        for name in sorted(files):
            full = os.path.join(root, name)
            rel = os.path.relpath(full, NOTEBOOK_DIR)
            if not excluded(rel):
                yield rel.replace("\\", "/"), full


def ws_path_for(rel):
    """Map a repo-relative path back to its workspace path."""
    sub, _, tail = rel.partition("/")
    if sub not in FOLDERS:
        raise ValueError(f"{rel!r} is not under one of {sorted(FOLDERS)}")
    stem, ext = os.path.splitext(tail)
    leaf = stem if ext.lower() in EXT_TO_LANG else tail
    return f"{WS_ROOT}/{FOLDERS[sub]}/{leaf}"


def fetch(w, ws_path):
    res = w.workspace.export(ws_path, format=ExportFormat.SOURCE)
    # A leading BOM makes a notebook fail as a job task with SyntaxError, so it is
    # stripped on the way in and never written back out.
    return base64.b64decode(res.content).replace(BOM, b"")


def cmd_pull(w, args):
    changed = 0
    for rel, ws_path, _ in workspace_files(w):
        data = fetch(w, ws_path)
        local = os.path.join(NOTEBOOK_DIR, rel)
        os.makedirs(os.path.dirname(local), exist_ok=True)
        old = open(local, "rb").read() if os.path.exists(local) else None
        if old == data:
            continue
        if not args.dry_run:
            with open(local, "wb") as fh:
                fh.write(data)
        print(f"  {'would update' if args.dry_run else 'updated'}  {rel}")
        changed += 1
    print(f"\n{changed} file(s) {'would change' if args.dry_run else 'changed'}")


def cmd_push(w, args):
    remote = {rel: ws for rel, ws, _ in workspace_files(w)}
    changed = 0
    for rel, local in repo_files():
        ext = os.path.splitext(rel)[1].lower()
        is_notebook = ext in EXT_TO_LANG
        data = open(local, "rb").read().replace(BOM, b"")
        if rel in remote and fetch(w, remote[rel]) == data:
            continue
        if args.dry_run:
            print(f"  would upload  {rel}")
            changed += 1
            continue
        ws_path = ws_path_for(rel)
        if is_notebook:
            w.workspace.import_(
                path=ws_path, format=ImportFormat.SOURCE,
                language=EXT_TO_LANG[ext],
                content=base64.b64encode(data).decode("ascii"), overwrite=True,
            )
        else:
            w.workspace.upload(path=ws_path, content=data, overwrite=True)
        print(f"  uploaded  {rel}")
        changed += 1
    print(f"\n{changed} file(s) {'would upload' if args.dry_run else 'uploaded'}")


def cmd_diff(w, args):
    remote = {rel: ws for rel, ws, _ in workspace_files(w)}
    local = dict(repo_files())

    only_remote = sorted(set(remote) - set(local))
    only_local = sorted(set(local) - set(remote))
    both = sorted(set(remote) & set(local))

    for rel in only_remote:
        print(f"  workspace only : {rel}")
    for rel in only_local:
        print(f"  repo only      : {rel}")

    differing = 0
    for rel in both:
        rdata = fetch(w, remote[rel]).decode("utf-8", "replace").splitlines()
        ldata = open(local[rel], "rb").read().replace(BOM, b"").decode("utf-8", "replace").splitlines()
        if rdata == ldata:
            continue
        differing += 1
        print(f"\n  differs        : {rel}")
        if args.verbose:
            for line in difflib.unified_diff(rdata, ldata, "workspace", "repo", lineterm="", n=1):
                print("      " + line)

    print(f"\n{len(only_remote)} workspace-only, {len(only_local)} repo-only, {differing} differing")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["pull", "push", "diff"])
    parser.add_argument("--dry-run", action="store_true", help="report changes without writing")
    parser.add_argument("-v", "--verbose", action="store_true", help="show line diffs (diff only)")
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    w = client()
    print(f"workspace : {WS_ROOT}")
    for sub, folder in FOLDERS.items():
        print(f"            {folder!r} -> notebooks/{sub}/")
    print(f"repo      : {NOTEBOOK_DIR}")
    print(f"host      : {w.config.host}\n")

    {"pull": cmd_pull, "push": cmd_push, "diff": cmd_diff}[args.command](w, args)


if __name__ == "__main__":
    main()
