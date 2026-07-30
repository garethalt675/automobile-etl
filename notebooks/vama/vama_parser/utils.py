"""Utility functions for VAMA Gemini parser."""

import base64
import os
import re
from typing import Dict, List, Optional

import requests

from .config import TARGET_COLS


def read_workspace_file(path: str, dbutils) -> str:
    """Read a Workspace file using dbutils.fs where possible, fallback to Workspace REST.
    
    Args:
        path: Workspace path (e.g., "/Users/user@example.com/file.txt")
        dbutils: Databricks utilities instance
        
    Returns:
        File content as string
    """
    local_path = "/Workspace" + path
    try:
        with open(local_path, "r", encoding="utf-8-sig") as f:
            return f.read()
    except Exception:
        pass

    # Fallback: Workspace export API using notebook context token
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = ctx.apiUrl().get().rstrip("/")
    token = ctx.apiToken().get()
    resp = requests.get(
        f"{host}/api/2.0/workspace/export",
        headers={"Authorization": f"Bearer {token}"},
        params={"path": path, "format": "SOURCE"},
        timeout=60,
    )
    resp.raise_for_status()
    return base64.b64decode(resp.json()["content"]).decode("utf-8-sig")


def parse_env(text: str) -> Dict[str, str]:
    """Parse .env file content into dictionary.
    
    Args:
        text: Content of .env file
        
    Returns:
        Dictionary of environment variables
    """
    vals = {}
    for line in text.splitlines():
        m = re.match(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"?([^"#\r\n]+)"?\s*$', line)
        if m:
            vals[m.group(1)] = m.group(2).strip()
    return vals


def extract_json(text: str) -> str:
    """Extract JSON object from text that may contain markdown code blocks.
    
    Args:
        text: Text potentially containing JSON
        
    Returns:
        Extracted JSON string
    """
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < 0:
        raise ValueError("No JSON object found in Gemini response")
    return text[start : end + 1]


def normalize_rows(obj: dict) -> List[dict]:
    """Normalize rows from Gemini response to match target schema.
    
    Args:
        obj: Parsed JSON response from Gemini
        
    Returns:
        List of normalized row dictionaries
    """
    cols = obj.get("cols") or obj.get("columns") or TARGET_COLS
    rows = []
    for item in obj.get("rows", []):
        if isinstance(item, dict):
            row = {c: item.get(c) for c in TARGET_COLS}
        else:
            tmp = {cols[i]: item[i] if i < len(item) else None for i in range(len(cols))}
            row = {c: tmp.get(c) for c in TARGET_COLS}
        rows.append(row)
    return rows


def parse_int(v) -> Optional[int]:
    """Parse value to integer, handling various formats.
    
    Args:
        v: Value to parse (str, int, float, or None)
        
    Returns:
        Parsed integer or None
    """
    if v is None or v == "" or str(v).lower() == "null":
        return None
    if isinstance(v, str):
        s = v.strip()
        # Gemini can occasionally put a share percentage into an integer slot
        if "%" in s:
            return None
        s = s.replace(",", "").strip()
        if s in ("", "-"):
            return None
        v = s
    return int(float(v))


def parse_share(v) -> Optional[float]:
    """Parse share value to float (0-1 range).
    
    Args:
        v: Value to parse (str, float, or None)
        
    Returns:
        Parsed share as float (0-1 range) or None
    """
    if v is None or v == "" or str(v).lower() == "null":
        return None
    s = str(v).replace(",", "").replace("%", "").strip()
    if not s or s == "-":
        return None
    out = float(s)
    return out / 100.0 if out > 1 else out
