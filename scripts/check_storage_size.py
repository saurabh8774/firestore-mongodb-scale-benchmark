#!/usr/bin/env python3
"""Checks total Firestore Enterprise storage size (Data + Indexes) in GiB / GB.

Queries the Cloud Monitoring metric:
  firestore.googleapis.com/storage/data_and_index_storage_bytes
using `gcloud auth print-access-token` (stdlib only, no extra SDK required).

Usage:
  python3 scripts/check_storage_size.py --project <PROJECT_ID> --database <DB_ID>
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request


def resolve_project(explicit_project: str) -> str:
  if explicit_project:
    return explicit_project
  proc = subprocess.run(
      ["gcloud", "config", "get-value", "project"],
      capture_output=True,
      text=True,
      check=False,
  )
  proj = proc.stdout.strip()
  if proc.returncode == 0 and proj and proj != "(unset)":
    return proj
  print(
      "ERROR: Specify --project <YOUR_GCP_PROJECT_ID> or export PROJECT_ID.",
      file=sys.stderr,
  )
  sys.exit(1)


def main() -> None:
  parser = argparse.ArgumentParser(
      description="Query Firestore Enterprise Data + Index Storage Size in GiB/GB"
  )
  parser.add_argument(
      "--project",
      default=os.environ.get("PROJECT_ID", ""),
      help="GCP Project ID (defaults to PROJECT_ID env var or gcloud config)",
  )
  parser.add_argument(
      "--database",
      default=os.environ.get("DB_ID", "voicebot-demo"),
      help="Firestore Enterprise Database ID (default: voicebot-demo)",
  )
  parser.add_argument(
      "--lookback-hours",
      type=int,
      default=6,
      help="Hours to look back for the latest Cloud Monitoring gauge sample",
  )
  args = parser.parse_args()

  project_id = resolve_project(args.project)
  tok_proc = subprocess.run(
      ["gcloud", "auth", "print-access-token"],
      capture_output=True,
      text=True,
      check=False,
  )
  token = tok_proc.stdout.strip()
  if tok_proc.returncode != 0 or not token:
    print(
        "ERROR: Failed to obtain OAuth2 token via `gcloud auth print-access-token`.",
        file=sys.stderr,
    )
    sys.exit(1)

  now = datetime.now(timezone.utc)
  start = (now - timedelta(hours=max(1, args.lookback_hours))).strftime(
      "%Y-%m-%dT%H:%M:%SZ"
  )
  end = now.strftime("%Y-%m-%dT%H:%M:%SZ")
  metric_filter = (
      'metric.type="firestore.googleapis.com/storage/data_and_index_storage_bytes" '
      f'AND resource.labels.database_id="{args.database}"'
  )
  params = urllib.parse.urlencode({
      "filter": metric_filter,
      "interval.startTime": start,
      "interval.endTime": end,
  })
  url = f"https://monitoring.googleapis.com/v3/projects/{project_id}/timeSeries?{params}"

  req = urllib.request.Request(
      url, headers={"Authorization": f"Bearer {token}"}
  )
  with urllib.request.urlopen(req, timeout=15.0) as resp:
    data = json.loads(resp.read().decode("utf-8"))

  series_list = data.get("timeSeries", [])
  if not series_list:
    print(
        f"No storage gauge sample found yet for database '{args.database}' in "
        f"project '{project_id}' over the last {args.lookback_hours}h.\n"
        "Note: Cloud Monitoring storage gauges emit periodically; for newly "
        "loaded data, check instantaneous logical BSON size via mongosh in "
        "scripts/live_validation_queries.js."
    )
    return

  total_bytes = 0
  latest_ts = ""
  print(f"Database: {args.database} (Project: {project_id})")
  print("-" * 68)
  for ts in series_list:
    label_type = ts.get("metric", {}).get("labels", {}).get("type", "total")
    pt = ts["points"][0]
    val = int(
        pt["value"].get("int64Value") or pt["value"].get("doubleValue") or 0
    )
    sample_time = pt.get("interval", {}).get("endTime", "")
    if sample_time > latest_ts:
      latest_ts = sample_time
    total_bytes += val
    print(
        f"  Component [{label_type:<12}]: {val / (1024**3):8.3f} GiB "
        f"({val / 1e9:8.3f} GB | {val:,} bytes)"
    )
  print("-" * 68)
  print(
      f"  TOTAL (Data + Indexes) : {total_bytes / (1024**3):8.3f} GiB "
      f"({total_bytes / 1e9:8.3f} GB)"
  )
  if latest_ts:
    print(f"  Latest Sample Timestamp: {latest_ts}")


if __name__ == "__main__":
  main()
