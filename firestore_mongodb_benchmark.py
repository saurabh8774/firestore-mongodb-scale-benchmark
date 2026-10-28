#!/usr/bin/env python3
"""Firestore Enterprise (MongoDB Compatibility) Scale & Consistency Benchmark v2.3.

Designed for 2-VM Workload Isolation (e.g., two e2-highcpu-16 GCE VMs in
asia-south1) as well as single-machine / laptop smoke-test execution:

  * VM 1 (vm-bau-primary | --role bau-primary):
      - Establishes exactly 1,000 live BAU TLS connections across 16 worker
        processes (1 process per vCPU on e2-highcpu-16) driving 5,000 BAU ops/s
        (<15 ms read/write target, <<30,000 ms SLA, 0 timeouts):
          1. Real-time point lookups (_id full 34-field fetch & covered
             phone_number_input_date dedup lookup)
          2. 100% covered voicebot dialer queue fetches (vbcalltrigger)
          3. Live call-state updates (disjoint per-thread active voicebot call
             slots: 85% single-split mid-call conversation turn updates
             + 15% terminal call-disposition composite-index updates)
          4. 100% covered campaign dashboard aggregations ($group)
      - Triggers Concurrent Online Composite Index Build / Rebuild on
        customerCRTId + immutable CRM metadata + inserted_on with zero-fork
        HTTPS REST LRO progress tracking and automatic cleanup of prior
        high-churn / stale demo indexes.
      - Runs Zero-Delay Index Consistency Probes (0.0 ms write -> .hint() scan).
      - Continuously drains compact binary worker metrics (`array('f')`) so the
        64 KB Linux OS pipe never bottlenecks or aliases 5-second QPS windows,
        printing the unified 7-metric real-time dashboard.

  * VM 2 (vm-bulk-worker | --role bulk-worker):
      - Runs parallel bulk-ingestion worker processes ingesting 1M to 10M
        full 34-field voicebot call documents via insert_many(..., ordered=False)
        with staggered smooth pacing, synchronized start/stop, and 1-second
        telemetry updates to VM 1 (100% wire-compatible with v2.2 & v2.3).

  * Single-Host / Laptop (--role all-in-one):
      - Runs all four concurrent workloads from a single machine.
"""

import argparse
from array import array
from datetime import datetime, timedelta, timezone
import hashlib
import json
import multiprocessing as mp
import os
import queue
import random
import resource
import shutil
import socket
import ssl
import subprocess
import sys
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse, urlunparse
from urllib.request import Request, urlopen
import uuid

from bson import ObjectId
from bson.int64 import Int64
from pymongo import MongoClient, monitoring
from pymongo.errors import (
    AutoReconnect,
    BulkWriteError,
    ExecutionTimeout,
    NetworkTimeout,
    OperationFailure,
    PyMongoError,
    ServerSelectionTimeoutError,
    WTimeoutError,
)

BENCHMARK_VERSION = "v2.3"
STAGE_CODES = ("DSCN", "INIT", "FLUP", "COMP", "RETRY")
CALL_STATUSES = ("ANSWERED", "NO_ANSWER", "BUSY", "FAILED")
LANGUAGES = ("hindi", "english", "kannada", "tamil", "telugu")

# Prime flow slot count per 1M-doc cohort (coprime to len(STAGE_CODES)=5 and
# len(CALL_STATUSES)=4) prevents modulo aliasing and multi-run bucket bloat.
FLOWS_PER_COHORT = 5003
FLOW_PREFIX = "prod-voicebot-v2"
SEED_INPUT_DATE = "14-10-2026"
SEED_CAMPAIGN_TAG = "active"
SYNC_COLLECTION = "voicebot_bench_sync"

# Bijective 10-digit Indian mobile permutation over [6000000000 .. 9999999999].
# Because 4,000,000,000 = 2^11 * 5^9 and 2654435761 is odd and not divisible
# by 5, gcd(PHONE_MULT, PHONE_SPAN) == 1 -> strict 1-to-1 bijection with zero
# collisions and zero monotonic prefix hotspotting on phone_number_input_date.
PHONE_SPAN = 4_000_000_000
PHONE_MULT = 2654435761
PHONE_ADD = 1013904223

# Candidate CRM history composite indexes (all leading with customerCRTId,
# using ONLY immutable per-call CRM attributes, and trailing with inserted_on:-1).
# NOTE: `sequence_number` is intentionally excluded because it increments on
# every mid-call utterance turn (which would force 2PC secondary index churn on
# 100% of live call writes).
ONLINE_INDEX_CANDIDATES: list[tuple[str, list[tuple[str, int]]]] = [
    ("idx_crm_history_online", [("customerCRTId", 1), ("inserted_on", -1)]),
    (
        "idx_crm_active_hist_online",
        [("customerCRTId", 1), ("active_call", 1), ("inserted_on", -1)],
    ),
    (
        "idx_crm_trig_hist_online",
        [("customerCRTId", 1), ("triggered_from", 1), ("inserted_on", -1)],
    ),
    (
        "idx_crm_dup_hist_online",
        [("customerCRTId", 1), ("duplicate", 1), ("inserted_on", -1)],
    ),
    (
        "idx_crm_maxtrig_hist_online",
        [("customerCRTId", 1), ("max_trigger", 1), ("inserted_on", -1)],
    ),
    (
        "idx_crm_analysis_hist_online",
        [("customerCRTId", 1), ("call_analysis", 1), ("inserted_on", -1)],
    ),
    (
        "idx_crm_src_hist_online",
        [("customerCRTId", 1), ("inserted_from", 1), ("inserted_on", -1)],
    ),
]

# Legacy high-churn demo index names/fields from earlier versions to auto-drop
# during pre-flight setup so live mid-call turn writes stay 1-phase single-split.
LEGACY_HIGH_CHURN_INDEX_NAMES = frozenset({"idx_crm_seq_hist_online"})

CANDIDATE_EQUALITY_FIELDS: dict[str, object] = {
    "active_call": 1,
    "triggered_from": "vbcalltrigger",
    "duplicate": 1,
    "max_trigger": 3,
    "call_analysis": 0,
    "inserted_from": "dashboard",
}

# Process-local cached OAuth2 token for zero-fork REST LRO progress polling
_CACHED_OAUTH_TOKEN: list[str] = [""]
_TOKEN_LOCK = threading.Lock()


def raise_nofile_limit(target_soft: int = 65535) -> tuple[int, int]:
  """Raises soft RLIMIT_NOFILE so 1,000+ TLS sockets never hit Errno 24."""
  soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
  desired = min(max(soft, target_soft), hard)
  if desired > soft:
    try:
      resource.setrlimit(resource.RLIMIT_NOFILE, (desired, hard))
      soft = desired
    except (ValueError, OSError):
      pass
  return soft, hard


def detect_gce_vm_zone() -> str:
  """Detects GCE VM zone via local metadata server in <0.4s (zero subprocess)."""
  try:
    req = Request(
        "http://metadata.google.internal/computeMetadata/v1/instance/zone",
        headers={"Metadata-Flavor": "Google"},
    )
    with urlopen(req, timeout=0.4) as resp:
      raw = resp.read().decode("utf-8", errors="ignore").strip()
      if raw:
        return raw.rsplit("/", 1)[-1]
  except Exception:
    pass
  return "non-gce"


def scatter_phone(seq: int) -> int:
  """1-to-1 deterministic bijection mapping seq -> [6000000000, 9999999999]."""
  return 6000000000 + ((seq * PHONE_MULT + PHONE_ADD) % PHONE_SPAN)


def scatter_crt_id(seq: int) -> str:
  """Deterministic scattered 19-digit numeric customerCRTId across [10^18..9.99*10^18]."""
  bucket = seq % 2_000_000
  h = ((bucket * 11400714819323198485) + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
  return str(1_000_000_000_000_000_000 + (h % 8_999_999_999_999_999_999))


def make_flow_id(seq: int, input_date: str = SEED_INPUT_DATE) -> str:
  """Deterministic campaign flow_id scoped by campaign tag and 1M-doc cohort."""
  tag = SEED_CAMPAIGN_TAG if input_date == SEED_INPUT_DATE else input_date
  cohort = seq // 1_000_000
  slot = (seq % FLOWS_PER_COHORT) + 1
  return f"{FLOW_PREFIX}-{tag}-c{cohort}-{slot}"


def make_scattered_oid(dedup_key: str) -> ObjectId:
  """Deterministic 12-byte ObjectId from SHA-256(phone_number_input_date)."""
  return ObjectId(hashlib.sha256(dedup_key.encode("utf-8")).hexdigest()[:24])


def build_call_doc(
    seq: int,
    base_time: datetime,
    input_date: str = SEED_INPUT_DATE,
    custom_dedup_key: str | None = None,
    custom_crt_id: str | None = None,
) -> dict:
  """Builds a 34-field BSON voicebot call document (~1.16 KiB)."""
  phone_num = scatter_phone(seq)
  dedup_key = custom_dedup_key or f"{phone_num}--{input_date}"
  flow_id = make_flow_id(seq, input_date)
  call_status = CALL_STATUSES[seq % len(CALL_STATUSES)]
  is_answered = 1 if call_status == "ANSWERED" else 0
  crt_id = custom_crt_id or scatter_crt_id(seq)
  sec_offset = ((seq * 40503) & 0xFFFF) % 86400

  return {
      "_id": make_scattered_oid(dedup_key),
      "phone_number": Int64(phone_num),
      "full_name": f"Customer_{seq}",
      "language": LANGUAGES[seq % len(LANGUAGES)],
      "duplicate": 1,
      "month": "October",
      "last_triggered_date": base_time - timedelta(minutes=sec_offset % 120),
      "input_date": input_date,
      "next_trigger_date": base_time + timedelta(seconds=sec_offset),
      "phone_number_input_date": dedup_key,
      "sequence_number": 1,
      "answered_seq": is_answered,
      "no_answer_seq": 0 if is_answered else 1,
      "call_status": call_status,
      "setupTime": 20 + (seq % 25),
      "sts": "Answered" if is_answered else "NotAnswered",
      "trigger_call": False,
      "flow_id": flow_id,
      "STAGE_CODE": STAGE_CODES[seq % len(STAGE_CODES)],
      "stage": "initial",
      "call_comp_flag": "no",
      "call_sequence_mapping": {
          "13/10/2026": {
              "calls_triggered": 1,
              "answered": is_answered,
              "not_answered": 0 if is_answered else 1,
              "max_trigger": 3,
          }
      },
      "conversation_log": [],
      "conversation_id": "",
      "call_analysis": 0,
      "fallback_count": 0,
      "fallback_failure": False,
      "repeat_count": 0,
      "repeat_failure": False,
      "tracker_log": [],
      "fallback_conv": [],
      "customerCRTId": crt_id,
      "active_call": 1,
      "max_trigger": 3,
      "ringingTime": seq % 15,
      "inserted_by": "voicebot-scheduler@example.com",
      "inserted_from": "dashboard",
      "inserted_on": base_time - timedelta(seconds=sec_offset % 3600),
      "triggered_from": "vbcalltrigger",
      "sender_id": crt_id,
      "stop_calling": False,
      "total_connects": is_answered,
  }


class PoolConnectionTracker(monitoring.ConnectionPoolListener):
  """Tracks live established TCP/TLS connections via PyMongo CMAP events.

  PyMongo's ConnectionPoolListener defines 11 abstract event callbacks that
  raise NotImplementedError unless overridden. All 11 callbacks are implemented
  here, and ready sockets are tracked by (address, connection_id) so failed
  connection attempts never skew the live counter.
  """

  def __init__(self, shared_counter: mp.Value):
    self._shared = shared_counter
    self._local_lock = threading.Lock()
    self._ready_conn_ids: set[tuple[object, object]] = set()
    self.local_ready = 0

  def pool_created(self, event):
    pass

  def pool_ready(self, event):
    pass

  def pool_cleared(self, event):
    pass

  def pool_closed(self, event):
    addr = getattr(event, "address", None)
    removed_count = 0
    with self._local_lock:
      stale_keys = [k for k in self._ready_conn_ids if k[0] == addr]
      for k in stale_keys:
        self._ready_conn_ids.discard(k)
      removed_count = len(stale_keys)
      self.local_ready = len(self._ready_conn_ids)
    if removed_count > 0:
      with self._shared.get_lock():
        self._shared.value = max(0, self._shared.value - removed_count)

  def connection_created(self, event):
    pass

  def connection_ready(self, event):
    key = (
        getattr(event, "address", None),
        getattr(event, "connection_id", id(event)),
    )
    added = False
    with self._local_lock:
      if key not in self._ready_conn_ids:
        self._ready_conn_ids.add(key)
        self.local_ready = len(self._ready_conn_ids)
        added = True
    if added:
      with self._shared.get_lock():
        self._shared.value += 1

  def connection_closed(self, event):
    key = (
        getattr(event, "address", None),
        getattr(event, "connection_id", id(event)),
    )
    removed = False
    with self._local_lock:
      if key in self._ready_conn_ids:
        self._ready_conn_ids.discard(key)
        self.local_ready = len(self._ready_conn_ids)
        removed = True
    if removed:
      with self._shared.get_lock():
        self._shared.value = max(0, self._shared.value - 1)

  def connection_check_out_started(self, event):
    pass

  def connection_check_out_failed(self, event):
    pass

  def connection_checked_out(self, event):
    pass

  def connection_checked_in(self, event):
    pass


# Forward-compatibility guard for any future PyMongo CMAP listener callbacks
for _cmap_attr in dir(monitoring.ConnectionPoolListener):
  if (
      not _cmap_attr.startswith("_")
      and _cmap_attr not in PoolConnectionTracker.__dict__
  ):
    setattr(PoolConnectionTracker, _cmap_attr, lambda self, event: None)


def percentile(sorted_vals: list[float], pct: float) -> float:
  if not sorted_vals:
    return 0.0
  idx = min(int(len(sorted_vals) * pct), len(sorted_vals) - 1)
  return sorted_vals[idx]


# ---------------------------------------------------------------------------
# 1. BAU Worker Process Pool (1,000 Concurrent Connections @ 5,000 Ops/sec)
# ---------------------------------------------------------------------------


def bau_process_main(
    proc_id: int,
    uri: str,
    database_id: str,
    coll_name: str,
    conns_this_proc: int,
    threads_this_proc: int,
    global_thread_offset: int,
    total_bau_threads: int,
    target_ops_this_proc: float,
    ramp_seconds: float,
    disposition_write_ratio: float,
    baseline_seed_docs: int,
    campaign_date_shared: mp.Array,
    start_epoch_shared: mp.Value,
    max_seq_shared: mp.Value,
    bau_conns_shared: mp.Value,
    ready_procs_shared: mp.Value,
    start_gate: mp.Event,
    stop_event: mp.Event,
    metrics_q: mp.Queue,
):
  raise_nofile_limit()
  tracker = PoolConnectionTracker(bau_conns_shared)
  client_lock = threading.Lock()
  state: dict[str, object] = {"uri": uri}

  def _make_bau_client(cur_uri: str) -> MongoClient:
    return MongoClient(
        cur_uri,
        minPoolSize=conns_this_proc,
        maxPoolSize=conns_this_proc,
        maxConnecting=conns_this_proc,
        maxIdleTimeMS=None,
        socketTimeoutMS=15000,
        connectTimeoutMS=15000,
        waitQueueTimeoutMS=10000,
        event_listeners=[tracker],
    )

  client = _make_bau_client(uri)
  state["client"] = client
  state["db"] = client[database_id]
  state["coll"] = client[database_id][coll_name]

  def _warm_all_pool_sockets(target_db):
    barrier = threading.Barrier(conns_this_proc)

    def _ping_one():
      try:
        barrier.wait(timeout=20.0)
        target_db.command("ping")
      except Exception:
        pass

    w_threads = [
        threading.Thread(target=_ping_one, daemon=True)
        for _ in range(conns_this_proc)
    ]
    for t in w_threads:
      t.start()
    for t in w_threads:
      t.join(timeout=25.0)

  # Pre-warm all conns_this_proc TLS sockets so 1,000 live sockets are open
  _warm_all_pool_sockets(state["db"])

  # Wait up to 10s for minPoolSize background maintainer to finish TLS handshakes
  t_wait = time.perf_counter()
  while (
      tracker.local_ready < conns_this_proc
      and (time.perf_counter() - t_wait) < 10.0
  ):
    time.sleep(0.1)

  with ready_procs_shared.get_lock():
    ready_procs_shared.value += 1

  start_gate.wait()
  if stop_event.is_set():
    client.close()
    return

  campaign_date = (
      campaign_date_shared.value.decode("utf-8") or SEED_INPUT_DATE
  )

  local_lock = threading.Lock()
  pt_reads = array("f")
  q_reads = array("f")
  aggs = array("f")
  writes = array("f")
  counts = {"timeouts": 0, "errors": 0, "over_30s": 0}
  last_err_msg: list[str] = []
  last_reauth_ts: list[float] = [0.0]

  worker_count = max(1, min(threads_this_proc, conns_this_proc))
  per_thread_rate = max(target_ops_this_proc / worker_count, 0.1)
  base_interval = 1.0 / per_thread_rate
  min_trigger = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
  active_write_pool = max(1000, baseline_seed_docs)
  tot_threads = max(1, total_bau_threads)

  def _keepalive_loop():
    # Exercises all conns_this_proc sockets every 180s so the 600s load-balancer idle
    # timeout never closes bottom-of-LIFO sockets during long 10M-doc runs.
    while not stop_event.wait(timeout=180.0):
      cur_db = state["db"]
      _warm_all_pool_sockets(cur_db)

  keepalive_thread = threading.Thread(target=_keepalive_loop, daemon=True)
  keepalive_thread.start()

  def _try_reauth_bau():
    with client_lock:
      now_m = time.perf_counter()
      if (now_m - last_reauth_ts[0]) < 15.0:
        return
      new_uri = refresh_access_token_in_uri(str(state["uri"]))
      if new_uri != state["uri"]:
        last_reauth_ts[0] = now_m
        old_client = state["client"]
        new_client = _make_bau_client(new_uri)
        state["uri"] = new_uri
        state["client"] = new_client
        state["db"] = new_client[database_id]
        state["coll"] = new_client[database_id][coll_name]
        _warm_all_pool_sockets(state["db"])
        try:
          old_client.close()  # type: ignore[union-attr]
        except Exception:
          pass

  def thread_loop(tid: int):
    rng = random.Random((proc_id << 16) ^ tid ^ int(time.time() * 1000))
    next_deadline = time.perf_counter() + rng.uniform(0.0, base_interval)

    # Disjoint active voicebot call ownership for this thread:
    # In production outbound dialers, each active phone call belongs to one
    # voicebot session. Partitioning active_write_pool by global_tid eliminates
    # artificial same-_id write-lock contention across concurrent worker threads.
    global_tid = (global_thread_offset + tid) % tot_threads
    slots_this_thread = max(
        1, ((active_write_pool - 1 - global_tid) // tot_threads) + 1
    )

    while not stop_event.is_set():
      now_perf = time.perf_counter()
      s_epoch = start_epoch_shared.value
      elapsed = max(0.0, time.time() - s_epoch) if s_epoch > 0 else 0.0
      if ramp_seconds > 0.0 and elapsed < ramp_seconds:
        # Smooth ramp from 25% -> 100% QPS over ramp_seconds
        scale = 0.25 + 0.75 * (elapsed / ramp_seconds)
      else:
        scale = 1.0
      cur_interval = base_interval / scale

      if next_deadline > now_perf:
        time.sleep(min(next_deadline - now_perf, 0.25))
        if stop_event.is_set():
          break

      # Allow up to 3 intervals of smooth catch-up slack so normal 15-40ms
      # writes never drop target QPS slots, while bounding recovery after stalls.
      now_after = time.perf_counter()
      if next_deadline + (3.0 * cur_interval) < now_after:
        next_deadline = now_after + rng.uniform(0.0, cur_interval * 0.25)
      else:
        next_deadline += cur_interval

      coll = state["coll"]
      upper_key = max(1000, max_seq_shared.value)
      roll = rng.random()
      t0 = time.perf_counter()
      try:
        if roll < 0.48:
          # 48%: Real-Time Voicebot Point Lookup (_id full fetch & covered dedup check)
          seq = rng.randint(0, upper_key - 1)
          row_date = (
              SEED_INPUT_DATE if seq < baseline_seed_docs else campaign_date
          )
          dedup_key = f"{scatter_phone(seq)}--{row_date}"
          if rng.random() < 0.70:
            doc_id = make_scattered_oid(dedup_key)
            coll.find_one({"_id": doc_id})  # type: ignore[union-attr]
          else:
            coll.find_one(  # type: ignore[union-attr]
                {"phone_number_input_date": dedup_key},
                {"_id": 1, "phone_number_input_date": 1},
            )
          ms = (time.perf_counter() - t0) * 1000.0
          with local_lock:
            pt_reads.append(ms)
            if ms > 30000.0:
              counts["over_30s"] += 1

        elif roll < 0.68:
          # 20%: Voicebot Dialer Queue Fetch (100% Index-Covered IDXSCAN)
          seq = rng.randint(0, upper_key - 1)
          row_date = (
              SEED_INPUT_DATE if seq < baseline_seed_docs else campaign_date
          )
          flow_id = make_flow_id(seq, row_date)
          list(
              coll.find(  # type: ignore[union-attr]
                  {
                      "flow_id": flow_id,
                      "stop_calling": False,
                      "call_status": "ANSWERED",
                      "next_trigger_date": {"$gte": min_trigger},
                  },
                  {
                      "_id": 1,
                      "flow_id": 1,
                      "stop_calling": 1,
                      "call_status": 1,
                      "next_trigger_date": 1,
                  },
              )
              .sort("next_trigger_date", 1)
              .limit(10)
          )
          ms = (time.perf_counter() - t0) * 1000.0
          with local_lock:
            q_reads.append(ms)
            if ms > 30000.0:
              counts["over_30s"] += 1

        elif roll < 0.70:
          # 2%: Live Supervisor Campaign Dashboard Covered Aggregation ($group)
          seq = rng.randint(0, upper_key - 1)
          row_date = (
              SEED_INPUT_DATE if seq < baseline_seed_docs else campaign_date
          )
          flow_id = make_flow_id(seq, row_date)
          stage = STAGE_CODES[seq % len(STAGE_CODES)]
          list(
              coll.aggregate([  # type: ignore[union-attr]
                  {"$match": {"flow_id": flow_id, "STAGE_CODE": stage}},
                  {
                      "$group": {
                          "_id": "$call_status",
                          "count": {"$sum": 1},
                          "connects": {"$sum": "$total_connects"},
                      }
                  },
              ])
          )
          ms = (time.perf_counter() - t0) * 1000.0
          with local_lock:
            aggs.append(ms)
            if ms > 30000.0:
              counts["over_30s"] += 1

        else:
          # 30%: Live Call-State Update on this thread's disjoint active call pool.
          # - (1 - disposition_write_ratio) [default 85%]: Live in-call utterance
          #   turn update (last_triggered_date, sequence_number, conversation_log).
          #   Touches 0 secondary index keys -> commits in a single storage
          #   split round trip (~12-14 ms P50 in-region)!
          # - disposition_write_ratio [default 15%]: Terminal call-disposition
          #   update mutating call_status, sts, answered_seq, total_connects across
          #   operational dialer & supervisor composite indexes via atomic 2PC.
          slot_k = rng.randint(0, slots_this_thread - 1)
          w_seq = min(
              active_write_pool - 1, global_tid + (slot_k * tot_threads)
          )
          w_date = (
              SEED_INPUT_DATE if baseline_seed_docs > 0 else campaign_date
          )
          w_dedup = f"{scatter_phone(w_seq)}--{w_date}"
          w_doc_id = make_scattered_oid(w_dedup)
          now_dt = datetime.now(timezone.utc)

          if rng.random() >= disposition_write_ratio:
            update_doc = {
                "$set": {
                    "last_triggered_date": now_dt,
                },
                "$inc": {
                    "sequence_number": 1,
                },
                "$push": {
                    "conversation_log": {
                        "$each": [{
                            "turn": 1,
                            "speaker": "voicebot",
                            "utterance": "Hello, calling with an update regarding your account.",
                            "ts": now_dt,
                        }],
                        "$slice": -5,
                    }
                },
            }
          else:
            is_ans = 1 if rng.random() < 0.5 else 0
            new_status = "ANSWERED" if is_ans else "NO_ANSWER"
            update_doc = {
                "$set": {
                    "call_status": new_status,
                    "sts": "Answered" if is_ans else "NotAnswered",
                    "last_triggered_date": now_dt,
                },
                "$inc": {
                    "answered_seq": is_ans,
                    "sequence_number": 1,
                    "total_connects": is_ans,
                },
                "$push": {
                    "conversation_log": {
                        "$each": [{
                            "turn": 1,
                            "speaker": "voicebot",
                            "utterance": "Thank you for speaking with our automated assistant.",
                            "ts": now_dt,
                        }],
                        "$slice": -5,
                    }
                },
            }

          try:
            coll.update_one({"_id": w_doc_id}, update_doc)  # type: ignore[union-attr]
          except AutoReconnect:
            coll.update_one({"_id": w_doc_id}, update_doc)  # type: ignore[union-attr]
          ms = (time.perf_counter() - t0) * 1000.0
          with local_lock:
            writes.append(ms)
            if ms > 30000.0:
              counts["over_30s"] += 1

      except (
          NetworkTimeout,
          ExecutionTimeout,
          WTimeoutError,
          ServerSelectionTimeoutError,
      ):
        with local_lock:
          counts["timeouts"] += 1
      except OperationFailure as ofe:
        if ofe.code == 391:
          _try_reauth_bau()
        else:
          with local_lock:
            counts["errors"] += 1
            if not last_err_msg:
              last_err_msg.append(
                  f"OperationFailure(code={ofe.code}): {str(ofe)[:90]}"
              )
      except PyMongoError as pme:
        with local_lock:
          counts["errors"] += 1
          if not last_err_msg:
            last_err_msg.append(f"{type(pme).__name__}: {str(pme)[:90]}")

  workers = [
      threading.Thread(target=thread_loop, args=(tid,), daemon=True)
      for tid in range(worker_count)
  ]
  for w in workers:
    w.start()

  # Flush compact C float32 byte buffers every 0.5s (<700 bytes/packet,
  # drained continuously by main() so the 64 KB Linux pipe never fills).
  while not stop_event.is_set():
    time.sleep(0.5)
    with local_lock:
      err_sample = last_err_msg[0] if last_err_msg else ""
      batch = (
          pt_reads.tobytes(),
          q_reads.tobytes(),
          aggs.tobytes(),
          writes.tobytes(),
          counts["timeouts"],
          counts["errors"],
          counts["over_30s"],
          err_sample,
      )
      del pt_reads[:]
      del q_reads[:]
      del aggs[:]
      del writes[:]
      counts["timeouts"] = 0
      counts["errors"] = 0
      counts["over_30s"] = 0
      last_err_msg.clear()
    try:
      metrics_q.put_nowait(batch)
    except queue.Full:
      pass

  for w in workers:
    w.join(timeout=2.0)
  try:
    state["client"].close()  # type: ignore[union-attr]
  except Exception:
    pass


# ---------------------------------------------------------------------------
# 2. Parallel Bulk Ingestion Pool (1M to 10M 34-Field Call Documents)
# ---------------------------------------------------------------------------


def bulk_worker_main(
    worker_id: int,
    total_workers: int,
    uri: str,
    database_id: str,
    coll_name: str,
    rel_offset: int,
    doc_count: int,
    batch_size: int,
    per_worker_target_rate: float,
    bulk_start_offset_shared: mp.Value,
    campaign_date_shared: mp.Array,
    bulk_conns_shared: mp.Value,
    ready_bulk_shared: mp.Value,
    bulk_loaded_shared: mp.Value,
    max_seq_shared: mp.Value,
    bulk_errors_shared: mp.Value,
    start_gate: mp.Event,
    stop_event: mp.Event,
):
  raise_nofile_limit()
  tracker = PoolConnectionTracker(bulk_conns_shared)
  client = MongoClient(
      uri,
      minPoolSize=1,
      maxPoolSize=4,
      maxIdleTimeMS=300000,
      socketTimeoutMS=60000,
      connectTimeoutMS=15000,
      event_listeners=[tracker],
  )
  db = client[database_id]
  coll = db[coll_name]
  try:
    db.command("ping")
  except Exception:
    pass

  with ready_bulk_shared.get_lock():
    ready_bulk_shared.value += 1

  start_gate.wait()
  if stop_event.is_set():
    client.close()
    return

  campaign_date = (
      campaign_date_shared.value.decode("utf-8") or SEED_INPUT_DATE
  )
  start_seq = bulk_start_offset_shared.value + rel_offset
  end_seq = start_seq + doc_count
  cursor = start_seq
  base_time = datetime(2026, 10, 15, 3, 0, 0, tzinfo=timezone.utc)

  if per_worker_target_rate > 0.0:
    batch_interval = batch_size / per_worker_target_rate
    # Stagger workers evenly across one batch_interval so parallel bulk workers
    # stream smoothly instead of firing synchronized microbursts.
    stagger = (worker_id / max(total_workers, 1)) * min(batch_interval, 2.0)
    if stagger > 0.0:
      time.sleep(stagger)
    next_batch_deadline = time.perf_counter()
  else:
    batch_interval = 0.0
    next_batch_deadline = 0.0

  while cursor < end_seq and not stop_event.is_set():
    if batch_interval > 0.0:
      now_p = time.perf_counter()
      if next_batch_deadline > now_p:
        time.sleep(min(next_batch_deadline - now_p, 0.5))
        if stop_event.is_set():
          break

    upper = min(cursor + batch_size, end_seq)
    n_batch = upper - cursor
    if batch_interval > 0.0:
      step_iv = (n_batch / batch_size) * batch_interval
      now_p2 = time.perf_counter()
      if next_batch_deadline + (2.0 * step_iv) < now_p2:
        next_batch_deadline = now_p2 + step_iv
      else:
        next_batch_deadline += step_iv

    batch = [
        build_call_doc(seq, base_time, input_date=campaign_date)
        for seq in range(cursor, upper)
    ]
    inserted_now = 0
    for attempt in range(3):
      try:
        res = coll.insert_many(batch, ordered=False)
        inserted_now = len(res.inserted_ids)
        break
      except BulkWriteError as bwe:
        write_errs = bwe.details.get("writeErrors", [])
        non_dup = [e for e in write_errs if e.get("code") != 11000]
        inserted_now = int(
            bwe.details.get("nInserted", len(batch) - len(write_errs))
        )
        if not non_dup:
          inserted_now = len(batch)
        else:
          with bulk_errors_shared.get_lock():
            bulk_errors_shared.value += len(non_dup)
        break
      except AutoReconnect:
        if attempt < 2:
          time.sleep(0.15 * (attempt + 1))
          continue
        with bulk_errors_shared.get_lock():
          bulk_errors_shared.value += len(batch)
      except OperationFailure as ofe:
        if ofe.code == 391:
          new_uri = refresh_access_token_in_uri(uri)
          if new_uri != uri:
            uri = new_uri
            try:
              client.close()
            except Exception:
              pass
            client = MongoClient(
                uri,
                minPoolSize=1,
                maxPoolSize=4,
                maxIdleTimeMS=300000,
                socketTimeoutMS=60000,
                connectTimeoutMS=15000,
                event_listeners=[tracker],
            )
            coll = client[database_id][coll_name]
            if attempt < 2:
              continue
        if attempt < 2:
          time.sleep(0.25 * (attempt + 1))
          continue
        with bulk_errors_shared.get_lock():
          bulk_errors_shared.value += len(batch)
      except PyMongoError:
        if attempt < 2:
          time.sleep(0.25 * (attempt + 1))
          continue
        with bulk_errors_shared.get_lock():
          bulk_errors_shared.value += len(batch)

    cursor = upper
    if inserted_now > 0:
      with bulk_loaded_shared.get_lock():
        bulk_loaded_shared.value += inserted_now
      with max_seq_shared.get_lock():
        if upper > max_seq_shared.value:
          max_seq_shared.value = upper

  client.close()


def spawn_bulk_worker_pool(
    uri: str,
    database_id: str,
    coll_name: str,
    bulk_docs: int,
    bulk_workers: int,
    batch_size: int,
    bulk_target_rate: float,
    bulk_start_offset_shared: mp.Value,
    campaign_date_shared: mp.Array,
    bulk_conns_shared: mp.Value,
    ready_bulk_shared: mp.Value,
    bulk_loaded_shared: mp.Value,
    max_seq_shared: mp.Value,
    bulk_errors_shared: mp.Value,
    start_gate: mp.Event,
    stop_event: mp.Event,
) -> list[mp.Process]:
  """Spawns parallel bulk-ingestion worker processes with optional rate pacing."""
  procs: list[mp.Process] = []
  if bulk_docs <= 0 or bulk_workers <= 0:
    return procs
  docs_per_w, rem_docs = divmod(bulk_docs, bulk_workers)
  active_workers = sum(
      1 for wid in range(bulk_workers) if (docs_per_w + (1 if wid < rem_docs else 0)) > 0
  )
  per_worker_rate = (
      (bulk_target_rate / active_workers)
      if (bulk_target_rate > 0.0 and active_workers > 0)
      else 0.0
  )
  rel_off = 0
  for wid in range(bulk_workers):
    w_count = docs_per_w + (1 if wid < rem_docs else 0)
    if w_count <= 0:
      continue
    bp = mp.Process(
        target=bulk_worker_main,
        args=(
            wid,
            active_workers,
            uri,
            database_id,
            coll_name,
            rel_off,
            w_count,
            batch_size,
            per_worker_rate,
            bulk_start_offset_shared,
            campaign_date_shared,
            bulk_conns_shared,
            ready_bulk_shared,
            bulk_loaded_shared,
            max_seq_shared,
            bulk_errors_shared,
            start_gate,
            stop_event,
        ),
    )
    bp.start()
    procs.append(bp)
    rel_off += w_count
  return procs


# ---------------------------------------------------------------------------
# 3. Online Index Build/Rebuild & Zero-Delay Index Consistency Probe
# ---------------------------------------------------------------------------


class OnlineIndexAndConsistencyCoordinator:
  """Coordinates Concurrent Online Index Build/Rebuild & 0-Delay Index Probes.

  1. Concurrent Online Index Build & Rebuild Proof:
     - Inserts pre-build sentinel records before triggering online createIndex
       on one of the zero-churn CRM history candidate specs, and tracks
       mid-build sentinels inserted while the LRO backfill runs.
     - Polls LRO backfill progress via zero-fork in-process HTTPS REST requests
       to `firestore.googleapis.com` (never spawning `gcloud` subprocesses
       during live BAU traffic!).
     - Immediately verifies 100% of pre-build + mid-build sentinels via forced
       `.hint(active_online_keys)` the instant the build completes.
  2. Consistent Index Maintenance (0-Delay Write-to-Indexed-Read):
     - Continuously inserts fresh 3-document cohorts and with ZERO sleep
       (0.0 ms delay) queries them back using `.hint()` on both pre-existing
       and newly built online indexes.
  """

  def __init__(
      self,
      uri: str,
      database_id: str,
      coll_name: str,
      project_id: str,
      trigger_delay_sec: float,
      rebuild_if_exists: bool,
      stop_event: mp.Event,
  ):
    self.uri = uri
    self.database_id = database_id
    self.coll_name = coll_name
    self.project_id = project_id
    self.trigger_delay_sec = trigger_delay_sec
    self.rebuild_if_exists = rebuild_if_exists
    self.stop_event = stop_event
    self._ssl_ctx = ssl.create_default_context()

    self._lock = threading.Lock()
    self.index_state = f"QUEUED (T-{int(trigger_delay_sec)}s)"
    self.online_index_ready = False
    self.is_rebuilding_now = False
    self.rebuild_verified = False
    self.active_online_name: str = ONLINE_INDEX_CANDIDATES[0][0]
    self.active_online_keys: list[tuple[str, int]] = ONLINE_INDEX_CANDIDATES[0][1]
    self._stale_demo_index_names: list[str] = []
    self.sentinel_crt_ids: list[str] = []

    self.window_probe_passes = 0
    self.window_probe_misses = 0
    self.window_probe_latencies: list[float] = []
    self.total_probe_passes = 0
    self.total_probe_misses = 0

  def set_index_state(self, text: str):
    with self._lock:
      self.index_state = text

  @staticmethod
  def _build_crm_query(
      crt_id: str, index_keys: list[tuple[str, int]]
  ) -> dict[str, object]:
    q: dict[str, object] = {"customerCRTId": crt_id}
    for field_name, _ in index_keys:
      if field_name in CANDIDATE_EQUALITY_FIELDS:
        q[field_name] = CANDIDATE_EQUALITY_FIELDS[field_name]
    return q

  def snapshot_probe(self) -> tuple[str, str, int, int]:
    with self._lock:
      idx_st = self.index_state
      passes = self.window_probe_passes
      misses = self.window_probe_misses
      lats = sorted(self.window_probe_latencies)
      tot_pass = self.total_probe_passes
      tot_miss = self.total_probe_misses
      dual = self.online_index_ready
      verified = self.rebuild_verified
      self.window_probe_passes = 0
      self.window_probe_misses = 0
      self.window_probe_latencies.clear()

    avg_ms = (sum(lats) / len(lats)) if lats else 0.0
    if misses > 0:
      probe_str = f"WARN {misses} lag miss! ({passes} ok)"
    elif passes == 0:
      probe_str = "WARMING..."
    else:
      mode = (
          "Dual+Rebuild"
          if (dual and verified)
          else ("Dual-IDX" if dual else "Live-IDX")
      )
      probe_str = f"0ms ({mode} {passes}/{passes}, {avg_ms:.1f}ms)"
    return idx_st, probe_str, tot_pass, tot_miss

  def _poll_lro_rest(self) -> str | None:
    """Zero-fork HTTPS REST query to Firestore Operations API for LRO progress."""
    if not self.project_id:
      return None
    tok = get_cached_oauth_token(allow_mint=False)
    if not tok:
      return None
    url = (
        f"https://firestore.googleapis.com/v1/projects/{self.project_id}"
        f"/databases/{self.database_id}/operations"
    )
    for attempt in range(2):
      try:
        req = Request(
            url,
            headers={
                "Authorization": f"Bearer {tok}",
                "Accept": "application/json",
            },
        )
        with urlopen(req, timeout=3.0, context=self._ssl_ctx) as resp:
          payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
        ops = (
            payload.get("operations", [])
            if isinstance(payload, dict)
            else payload
        )
        if not isinstance(ops, list):
          return None
        for op in ops:
          if op.get("done") is True:
            continue
          meta = op.get("metadata", {})
          if "IndexOperationMetadata" in meta.get("@type", "") or "index" in meta:
            state_str = str(meta.get("state", "")).upper()
            if "DELET" in state_str or "CANCEL" in state_str:
              continue
            prog = meta.get("progressDocuments", {})
            done_docs = int(prog.get("completedWork", 0))
            est_docs = int(prog.get("estimatedWork", 0))
            if est_docs > 0:
              pct = min(99.9, (done_docs / est_docs) * 100.0)
              return f"BUILDING {pct:4.1f}% ({done_docs:,}/{est_docs:,})"
            return "BUILDING (LRO active)"
        return None
      except HTTPError as he:
        if he.code == 401 and attempt == 0:
          tok = get_cached_oauth_token(allow_mint=True, force_refresh=True)
          if tok:
            continue
        return None
      except (URLError, TimeoutError, ValueError, OSError):
        return None
    return None

  def _run_index_lifecycle(self):
    # socketTimeoutMS=None because createIndexes blocks until server LRO finishes
    admin_client = MongoClient(
        self.uri,
        maxPoolSize=4,
        socketTimeoutMS=None,
        connectTimeoutMS=15000,
    )
    coll = admin_client[self.database_id][self.coll_name]
    base_time = datetime(2026, 10, 15, 3, 0, 0, tzinfo=timezone.utc)

    t0 = time.perf_counter()
    while not self.stop_event.is_set():
      rem = self.trigger_delay_sec - (time.perf_counter() - t0)
      if rem <= 0:
        break
      self.set_index_state(f"QUEUED (T-{int(rem) + 1}s)")
      time.sleep(min(1.0, rem))

    if self.stop_event.is_set():
      admin_client.close()
      return

    try:
      idx_info = coll.index_information()
      candidate_by_spec = {
          tuple(keys): (cname, keys) for cname, keys in ONLINE_INDEX_CANDIDATES
      }
      existing_demo_indexes: list[tuple[str, list[tuple[str, int]]]] = []
      existing_specs: set[tuple[tuple[str, int], ...]] = set()
      for actual_name, spec in idx_info.items():
        norm_key = tuple((k, int(v)) for k, v in spec.get("key", []))
        existing_specs.add(norm_key)
        if norm_key in candidate_by_spec:
          existing_demo_indexes.append((actual_name, list(norm_key)))

      if existing_demo_indexes and not self.rebuild_if_exists:
        chosen_name, chosen_keys = existing_demo_indexes[0]
        with self._lock:
          self.active_online_name = chosen_name
          self.active_online_keys = chosen_keys
          self.online_index_ready = True
          self.rebuild_verified = True
        self.set_index_state("READY (pre-existing)")
        admin_client.close()
        return

      with self._lock:
        self._stale_demo_index_names = [n for n, _ in existing_demo_indexes]

      unused_candidates = [
          (cname, keys)
          for cname, keys in ONLINE_INDEX_CANDIDATES
          if tuple(keys) not in existing_specs
      ]
      if not unused_candidates:
        oldest_name, oldest_keys = existing_demo_indexes[0]
        self.set_index_state("DROPPING oldest idx...")
        coll.drop_index(oldest_name)
        with self._lock:
          self._stale_demo_index_names = [
              n for n in self._stale_demo_index_names if n != oldest_name
          ]
        unused_candidates = [(oldest_name, oldest_keys)]

      # Insert pre-build sentinel cohort before triggering online createIndex
      pre_crt = f"CRT-PRE-REBUILD-{int(time.time() * 1000)}"
      pre_docs = [
          build_call_doc(
              95000000 + i,
              base_time,
              custom_dedup_key=f"998888888{i}--PRE-{pre_crt}",
              custom_crt_id=pre_crt,
          )
          for i in range(3)
      ]
      coll.insert_many(pre_docs, ordered=False)
      with self._lock:
        self.sentinel_crt_ids.append(pre_crt)
        self.is_rebuilding_now = True

      self.set_index_state("BUILDING (starting LRO)")
      build_done = threading.Event()
      build_err: list[str] = []

      def _create_worker():
        # Try unused candidates in order; if one is still in background DELETING
        # state from a previous run, immediately advance to the next candidate!
        for round_num in range(3):
          for cand_name, cand_keys in unused_candidates:
            if self.stop_event.is_set():
              build_done.set()
              return
            try:
              coll.create_index(cand_keys, name=cand_name)
              with self._lock:
                self.active_online_name = cand_name
                self.active_online_keys = cand_keys
              build_done.set()
              return
            except OperationFailure as ofe:
              msg = str(ofe).lower()
              if (
                  "being deleted" in msg
                  or "already exists" in msg
                  or "matching definition" in msg
              ):
                continue
              if round_num < 2:
                time.sleep(3.0)
                continue
              build_err.append(str(ofe)[:40])
              build_done.set()
              return
            except Exception as exc:
              build_err.append(str(exc)[:40])
              build_done.set()
              return
          if round_num < 2:
            time.sleep(4.0)
        build_err.append("All candidate indexes busy deleting")
        build_done.set()

      t_create = threading.Thread(target=_create_worker, daemon=True)
      t_create.start()
      t_lro_start = time.perf_counter()

      while not build_done.is_set() and not self.stop_event.is_set():
        lro_str = self._poll_lro_rest()
        if lro_str:
          self.set_index_state(lro_str)
        else:
          el = int(time.perf_counter() - t_lro_start)
          self.set_index_state(f"BUILDING (LRO T+{el}s, 0 locks)")
        build_done.wait(timeout=12.0)

      if self.stop_event.is_set():
        admin_client.close()
        return

      if build_err:
        self.set_index_state(f"ERR: {build_err[0]}")
        admin_client.close()
        return

      # Immediately (0ms delay) verify 100% of pre-build + mid-build sentinels
      with self._lock:
        self.is_rebuilding_now = False
        sentinels_to_verify = list(self.sentinel_crt_ids)
        built_keys = list(self.active_online_keys)

      missing_cohorts = 0
      for crt_key in sentinels_to_verify:
        q_filter = self._build_crm_query(crt_key, built_keys)
        found = list(
            coll.find(q_filter, {"_id": 1})
            .sort("inserted_on", -1)
            .hint(built_keys)
        )
        if len(found) < 3:
          missing_cohorts += 1

      with self._lock:
        self.online_index_ready = True
        if missing_cohorts == 0:
          self.rebuild_verified = True
          self.index_state = (
              f"READY (100% of {len(sentinels_to_verify) * 3} sentinels!)"
          )
        else:
          self.total_probe_misses += missing_cohorts
          self.index_state = f"READY ({missing_cohorts} cohort miss)"

    except Exception as exc:
      self.set_index_state(f"IDX ERR: {type(exc).__name__}")
    finally:
      admin_client.close()

  def _run_zero_delay_probe(self):
    probe_client = MongoClient(
        self.uri,
        maxPoolSize=4,
        socketTimeoutMS=15000,
        connectTimeoutMS=15000,
    )
    coll = probe_client[self.database_id][self.coll_name]
    base_time = datetime(2026, 10, 15, 3, 0, 0, tzinfo=timezone.utc)
    probe_seq = 0

    while not self.stop_event.is_set():
      time.sleep(2.5)
      if self.stop_event.is_set():
        break

      probe_seq += 1
      stamp = f"{int(time.time() * 1000000)}-{probe_seq}"
      probe_phone = scatter_phone(98000000 + probe_seq)
      probe_dedup = f"{probe_phone}--PROBE-{stamp}"
      probe_crt = f"CRT-LIVE-{stamp}"

      docs = [
          build_call_doc(
              98000000 + (probe_seq * 3) + i,
              base_time,
              custom_dedup_key=f"{probe_dedup}-{i}",
              custom_crt_id=probe_crt,
          )
          for i in range(3)
      ]
      for i, d in enumerate(docs):
        d["_id"] = make_scattered_oid(f"{probe_dedup}-oid-{i}")
        d["phone_number_input_date"] = probe_dedup

      try:
        coll.insert_many(docs, ordered=False)

        with self._lock:
          rebuilding = self.is_rebuilding_now
          check_online_idx = self.online_index_ready
          online_keys = list(self.active_online_keys)
          if rebuilding and len(self.sentinel_crt_ids) < 25:
            self.sentinel_crt_ids.append(probe_crt)

        # ZERO-DELAY (0.0 ms sleep) immediate indexed query via hint()
        t_q = time.perf_counter()
        res1 = list(
            coll.find(
                {"phone_number_input_date": probe_dedup}, {"_id": 1}
            ).hint([("phone_number_input_date", 1)])
        )
        ok = len(res1) == 3
        n_scans = 1

        if check_online_idx:
          q_filter = self._build_crm_query(probe_crt, online_keys)
          res2 = list(
              coll.find(q_filter, {"_id": 1})
              .sort("inserted_on", -1)
              .hint(online_keys)
          )
          ok = ok and (len(res2) == 3)
          n_scans = 2

        per_scan_ms = ((time.perf_counter() - t_q) * 1000.0) / n_scans
        with self._lock:
          self.window_probe_latencies.append(per_scan_ms)
          if ok:
            self.window_probe_passes += 1
            self.total_probe_passes += 1
          else:
            self.window_probe_misses += 1
            self.total_probe_misses += 1
      except Exception:
        with self._lock:
          self.window_probe_misses += 1
          self.total_probe_misses += 1

    probe_client.close()

  def cleanup_previous_demo_indexes(self):
    """Quietly drops prior-run demo candidate indexes after benchmark exit."""
    with self._lock:
      stale = [
          n
          for n in self._stale_demo_index_names
          if n != self.active_online_name
      ]
    if not stale:
      return
    client = MongoClient(
        self.uri, maxPoolSize=2, socketTimeoutMS=20000, connectTimeoutMS=10000
    )
    try:
      coll = client[self.database_id][self.coll_name]
      for idx_name in stale[:2]:
        try:
          coll.drop_index(idx_name)
        except Exception:
          pass
    finally:
      client.close()

  def start(self):
    t1 = threading.Thread(target=self._run_index_lifecycle, daemon=True)
    t2 = threading.Thread(target=self._run_zero_delay_probe, daemon=True)
    t1.start()
    t2.start()
    return t1, t2


# ---------------------------------------------------------------------------
# 4. Pre-Flight Index Setup, High-Churn Index Cleanup & Initial Seed Helper
# ---------------------------------------------------------------------------


def ensure_operational_indexes_and_seed(
    uri: str,
    database_id: str,
    coll_name: str,
    seed_docs: int,
    seed_workers: int,
    rebuild_online_index: bool = True,
) -> int:
  """Ensures Operational Indexes 1-3 exist, drops legacy high-churn indexes, and seeds baseline."""
  client = MongoClient(uri, socketTimeoutMS=None, connectTimeoutMS=15000)
  coll = client[database_id][coll_name]

  print("[Setup] Ensuring 3 pre-created operational indexes exist...")
  coll.create_index([("phone_number_input_date", 1)])
  coll.create_index([
      ("flow_id", 1),
      ("stop_calling", 1),
      ("call_status", 1),
      ("next_trigger_date", 1),
  ])
  coll.create_index([
      ("flow_id", 1),
      ("STAGE_CODE", 1),
      ("call_status", 1),
      ("total_connects", 1),
  ])

  # Inspect existing indexes:
  # 1. Immediately drop any legacy index indexing `sequence_number` (e.g.
  #    `idx_crm_seq_hist_online`), which forced multi-key 2PC index updates on
  #    100% of mid-call utterance turn updates!
  # 2. If >1 prior demo candidate indexes accumulated from interrupted runs,
  #    trim up to 2 excess stale demo indexes so writes don't maintain 8+ indexes.
  try:
    idx_info = coll.index_information()
    cand_specs = {tuple(keys) for _, keys in ONLINE_INDEX_CANDIDATES}
    high_churn_to_drop: list[str] = []
    stale_demo_to_trim: list[str] = []

    for idx_name, spec in idx_info.items():
      if idx_name == "_id_":
        continue
      norm_keys = tuple((k, int(v)) for k, v in spec.get("key", []))
      field_names = {k for k, _ in norm_keys}
      if idx_name in LEGACY_HIGH_CHURN_INDEX_NAMES or (
          "customerCRTId" in field_names and "sequence_number" in field_names
      ):
        high_churn_to_drop.append(idx_name)
      elif norm_keys in cand_specs:
        stale_demo_to_trim.append(idx_name)

    # Also attempt spec-based drop of (customerCRTId:1, sequence_number:1, inserted_on:-1)
    # in case it is still in CREATING state from a stopped v2.2 run.
    dropped_any_churn = False
    for idx_name in high_churn_to_drop:
      try:
        coll.drop_index(idx_name)
        dropped_any_churn = True
        print(
            f"[Setup] Dropped high-churn index '{idx_name}' (indexed per-turn "
            "sequence_number -> forced multi-split 2PC on 100% of writes)."
        )
      except Exception:
        pass
    if not dropped_any_churn:
      try:
        coll.drop_index(
            [("customerCRTId", 1), ("sequence_number", 1), ("inserted_on", -1)]
        )
        print(
            "[Setup] Dropped building high-churn index on "
            "(customerCRTId, sequence_number, inserted_on)."
        )
      except Exception:
        pass

    if rebuild_online_index and len(stale_demo_to_trim) > 1:
      for stale_name in stale_demo_to_trim[:2]:
        try:
          coll.drop_index(stale_name)
          print(
              f"[Setup] Trimmed leftover prior-run demo index '{stale_name}' "
              "to minimize secondary index write amplification."
          )
        except Exception:
          pass
  except Exception:
    pass

  check_seq = max(0, seed_docs - 1)
  sample_oid = make_scattered_oid(
      f"{scatter_phone(check_seq)}--{SEED_INPUT_DATE}"
  )
  sample_doc = coll.find_one({"_id": sample_oid}, {"_id": 1, "flow_id": 1})
  client.close()

  if (
      sample_doc is not None
      and str(sample_doc.get("flow_id", "")).startswith(FLOW_PREFIX)
  ) or seed_docs <= 0:
    print(f"[Setup] Scattered baseline records (0..{seed_docs:,}) already present.")
    return max(seed_docs, 1000)

  print(
      f"[Setup] Seeding {seed_docs:,} scattered 34-field active campaign records "
      f"across {seed_workers} workers so BAU queries hit hotspot-free data..."
  )
  seed_offset_shared = mp.Value("q", 0)
  seed_date_shared = mp.Array("c", 64)
  seed_date_shared.value = SEED_INPUT_DATE.encode("utf-8")
  dummy_conns = mp.Value("i", 0)
  dummy_ready = mp.Value("i", 0)
  seeded_cnt = mp.Value("q", 0)
  max_seq = mp.Value("q", 0)
  errs = mp.Value("q", 0)
  gate = mp.Event()
  gate.set()
  stop = mp.Event()

  procs = spawn_bulk_worker_pool(
      uri=uri,
      database_id=database_id,
      coll_name=coll_name,
      bulk_docs=seed_docs,
      bulk_workers=seed_workers,
      batch_size=200,
      bulk_target_rate=0.0,
      bulk_start_offset_shared=seed_offset_shared,
      campaign_date_shared=seed_date_shared,
      bulk_conns_shared=dummy_conns,
      ready_bulk_shared=dummy_ready,
      bulk_loaded_shared=seeded_cnt,
      max_seq_shared=max_seq,
      bulk_errors_shared=errs,
      start_gate=gate,
      stop_event=stop,
  )
  for p in procs:
    p.join()
  print(f"[Setup] Seeded {seeded_cnt.value:,} scattered baseline docs.")
  return max(seed_docs, 1000)


# ---------------------------------------------------------------------------
# 5. Dedicated Bulk-Worker VM Mode (--role bulk-worker on VM 2)
# ---------------------------------------------------------------------------


def run_dedicated_bulk_worker_vm(
    args: argparse.Namespace,
    uri: str,
    database_id: str,
    bulk_docs: int,
    bulk_workers: int,
    bulk_target_rate: float,
):
  """Runs on VM 2 (bulk-worker) and streams live ingestion metrics to VM 1."""
  hostname = socket.gethostname()
  vm_zone = detect_gce_vm_zone()
  ctrl_id = f"{args.run_id}:control"
  bulk_id = f"{args.run_id}:bulk"

  bulk_start_offset_shared = mp.Value("q", args.bulk_start_offset)
  campaign_date_shared = mp.Array("c", 64)
  campaign_date_shared.value = b"27-10-2026"
  bulk_conns_shared = mp.Value("i", 0)
  ready_bulk_shared = mp.Value("i", 0)
  bulk_loaded_shared = mp.Value("q", 0)
  max_seq_shared = mp.Value("q", args.bulk_start_offset)
  bulk_errors_shared = mp.Value("q", 0)
  start_gate = mp.Event()
  stop_event = mp.Event()

  rate_desc = (
      f"paced @ {bulk_target_rate:,.0f} docs/s"
      if bulk_target_rate > 0
      else "unthrottled max rate"
  )
  print(
      f"\n[Firestore Benchmark {BENCHMARK_VERSION} | VM2 Bulk Tier | {hostname} ({vm_zone})] "
      f"Pre-warming {bulk_workers} parallel bulk worker processes for "
      f"{bulk_docs:,} 34-field call docs ({rate_desc}, "
      f"batch={args.bulk_batch_size}, run_id={args.run_id})..."
  )
  bulk_processes = spawn_bulk_worker_pool(
      uri=uri,
      database_id=database_id,
      coll_name=args.collection,
      bulk_docs=bulk_docs,
      bulk_workers=bulk_workers,
      batch_size=args.bulk_batch_size,
      bulk_target_rate=bulk_target_rate,
      bulk_start_offset_shared=bulk_start_offset_shared,
      campaign_date_shared=campaign_date_shared,
      bulk_conns_shared=bulk_conns_shared,
      ready_bulk_shared=ready_bulk_shared,
      bulk_loaded_shared=bulk_loaded_shared,
      max_seq_shared=max_seq_shared,
      bulk_errors_shared=bulk_errors_shared,
      start_gate=start_gate,
      stop_event=stop_event,
  )

  # Open sync_client AFTER forking bulk_processes to guarantee PyMongo fork-safety
  sync_client = MongoClient(
      uri, maxPoolSize=4, socketTimeoutMS=15000, connectTimeoutMS=15000
  )
  sync_coll = sync_client[database_id][SYNC_COLLECTION]

  t_warm = time.perf_counter()
  while (
      ready_bulk_shared.value < len(bulk_processes)
      and (time.perf_counter() - t_warm) < 25.0
  ):
    time.sleep(0.2)

  print(
      f"[VM2 Bulk Tier | {hostname}] {ready_bulk_shared.value}/{len(bulk_processes)} "
      f"workers warm ({bulk_conns_shared.value} TLS conns). Waiting for VM 1 "
      f"(--role bau-primary --run-id {args.run_id}) start signal..."
  )

  acked_session: str | None = None
  t_wait_sync = time.perf_counter()
  try:
    while not stop_event.is_set():
      if (time.perf_counter() - t_wait_sync) > args.sync_timeout:
        print(
            f"ERROR: Timed out after {args.sync_timeout}s waiting for VM 1 "
            f"(--role bau-primary --run-id {args.run_id}).",
            file=sys.stderr,
        )
        stop_event.set()
        start_gate.set()
        break

      try:
        ctrl = sync_coll.find_one({"_id": ctrl_id})
      except OperationFailure as ofe:
        if ofe.code == 391:
          new_uri = refresh_access_token_in_uri(uri)
          if new_uri != uri:
            uri = new_uri
            sync_client.close()
            sync_client = MongoClient(
                uri, maxPoolSize=4, socketTimeoutMS=15000, connectTimeoutMS=15000
            )
            sync_coll = sync_client[database_id][SYNC_COLLECTION]
            continue
        raise
      if ctrl:
        sess = ctrl.get("session_id")
        state = ctrl.get("state")
        if state == "WAITING_FOR_BULK" and sess and sess != acked_session:
          sync_coll.update_one(
              {"_id": bulk_id},
              {
                  "$set": {
                      "session_id": sess,
                      "status": "READY",
                      "host": hostname,
                      "target_docs": bulk_docs,
                      "workers": len(bulk_processes),
                      "bulk_target_rate": bulk_target_rate,
                      "bulk_conns": bulk_conns_shared.value,
                      "bulk_loaded": 0,
                      "max_seq": int(
                          ctrl.get("bulk_start_offset", args.bulk_start_offset)
                      ),
                      "bulk_errors": 0,
                      "done": False,
                      "updated_epoch": time.time(),
                  }
              },
              upsert=True,
          )
          acked_session = sess
          print(
              f"[VM2 Bulk Tier | {hostname}] Handshake ACK sent to VM 1 "
              f"(session={sess}); standing by for simultaneous release..."
          )
        elif state == "RUNNING" and sess and (
            sess == acked_session
            or (time.time() - float(ctrl.get("updated_epoch", 0))) < 30.0
        ):
          acked_session = sess
          c_date = str(ctrl.get("campaign_date", "27-10-2026"))
          b_off = int(ctrl.get("bulk_start_offset", args.bulk_start_offset))
          campaign_date_shared.value = c_date.encode("utf-8")[:63]
          bulk_start_offset_shared.value = b_off
          max_seq_shared.value = b_off
          start_gate.set()
          print(
              f"[VM2 Bulk Tier | {hostname}] GO! Launching {bulk_docs:,} doc "
              f"bulk ingestion ({rate_desc}, campaign_date={c_date}, "
              f"start_seq={b_off:,})..."
          )
          break
      time.sleep(0.4)

    prev_loaded = 0
    t_prev = time.perf_counter()
    tick = 0
    while not stop_event.is_set():
      time.sleep(1.0)
      tick += 1
      alive = any(bp.is_alive() for bp in bulk_processes)
      cur_loaded = bulk_loaded_shared.value
      cur_conns = bulk_conns_shared.value
      cur_max_seq = max_seq_shared.value
      cur_errs = bulk_errors_shared.value

      try:
        sync_coll.update_one(
            {"_id": bulk_id},
            {
                "$set": {
                    "session_id": acked_session,
                    "status": "RUNNING" if alive else "DONE",
                    "host": hostname,
                    "target_docs": bulk_docs,
                    "workers": len(bulk_processes),
                    "bulk_target_rate": bulk_target_rate,
                    "bulk_conns": cur_conns if alive else 0,
                    "bulk_loaded": cur_loaded,
                    "max_seq": cur_max_seq,
                    "bulk_errors": cur_errs,
                    "done": not alive,
                    "updated_epoch": time.time(),
                }
            },
            upsert=True,
        )
      except OperationFailure as ofe:
        if ofe.code == 391:
          new_uri = refresh_access_token_in_uri(uri)
          if new_uri != uri:
            uri = new_uri
            sync_client.close()
            sync_client = MongoClient(
                uri, maxPoolSize=4, socketTimeoutMS=15000, connectTimeoutMS=15000
            )
            sync_coll = sync_client[database_id][SYNC_COLLECTION]
        else:
          raise

      if tick % 5 == 0 or not alive:
        t_now = time.perf_counter()
        dt = max(t_now - t_prev, 1e-6)
        rate = max(0, cur_loaded - prev_loaded) / dt
        t_prev = t_now
        prev_loaded = cur_loaded
        pct = (cur_loaded / bulk_docs * 100.0) if bulk_docs > 0 else 100.0
        ts_utc = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(
            f"[VM2 Bulk -> VM1 Dashboard] {ts_utc} | Conns: {cur_conns:2d} | "
            f"Rate: {rate:7,.0f} docs/s | "
            f"Progress: {cur_loaded:10,}/{bulk_docs:,} ({pct:5.1f}%) | "
            f"Errors: {cur_errs}"
        )

      if not alive:
        print(
            f"[VM2 Bulk Tier | {hostname}] Completed ingesting "
            f"{bulk_loaded_shared.value:,} 34-field call docs!"
        )
        break

      try:
        ctrl = sync_coll.find_one(
            {"_id": ctrl_id}, {"state": 1, "session_id": 1}
        )
      except OperationFailure as ofe:
        if ofe.code == 391:
          new_uri = refresh_access_token_in_uri(uri)
          if new_uri != uri:
            uri = new_uri
            sync_client.close()
            sync_client = MongoClient(
                uri, maxPoolSize=4, socketTimeoutMS=15000, connectTimeoutMS=15000
            )
            sync_coll = sync_client[database_id][SYNC_COLLECTION]
            ctrl = None
          else:
            raise
        else:
          raise
      if (
          ctrl
          and ctrl.get("session_id") == acked_session
          and ctrl.get("state") == "STOPPED"
      ):
        print(f"[VM2 Bulk Tier | {hostname}] Received STOPPED from VM 1.")
        stop_event.set()
        break

  except KeyboardInterrupt:
    print(f"\n[VM2 Bulk Tier | {hostname}] Stopping bulk workers gracefully...")
  finally:
    stop_event.set()
    start_gate.set()
    for bp in bulk_processes:
      bp.join(timeout=5.0)
      if bp.is_alive():
        bp.terminate()
    try:
      if acked_session:
        sync_coll.update_one(
            {"_id": bulk_id},
            {
                "$set": {
                    "done": True,
                    "status": "DONE",
                    "bulk_conns": 0,
                    "bulk_loaded": bulk_loaded_shared.value,
                    "bulk_errors": bulk_errors_shared.value,
                    "updated_epoch": time.time(),
                }
            },
        )
    except Exception:
      pass
    sync_client.close()


# ---------------------------------------------------------------------------
# 6. Main Orchestrator & Unified Real-Time Telemetry Dashboard
# ---------------------------------------------------------------------------


def parse_db_name_from_uri(uri: str) -> str | None:
  parsed = urlparse(uri)
  path = (parsed.path or "").lstrip("/")
  db_name = path.split("/")[0] if path else ""
  return db_name or None


def parse_db_region_from_uri(uri: str) -> str:
  host = (urlparse(uri).hostname or "").lower()
  # Format: <uid>.<region>.firestore.goog
  parts = host.split(".")
  if len(parts) >= 4 and parts[-2] == "firestore" and parts[-1] == "goog":
    return parts[-3]
  return "unknown"


def mint_gcloud_access_token() -> str | None:
  """Mints a fresh 60-minute OAuth2 access token via gcloud CLI if available."""
  if not shutil.which("gcloud"):
    return None
  try:
    proc = subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        capture_output=True,
        text=True,
        timeout=10.0,
        check=False,
    )
    tok = proc.stdout.strip()
    if proc.returncode == 0 and tok:
      with _TOKEN_LOCK:
        _CACHED_OAUTH_TOKEN[0] = tok
      return tok
  except Exception:
    pass
  return None


def get_cached_oauth_token(
    allow_mint: bool = True, force_refresh: bool = False
) -> str:
  with _TOKEN_LOCK:
    cur = _CACHED_OAUTH_TOKEN[0]
  if cur and not force_refresh:
    return cur
  if not allow_mint:
    return ""
  return mint_gcloud_access_token() or ""


def refresh_access_token_in_uri(uri: str) -> str:
  """If uri uses gcloud access_token (PLAIN auth), replaces token with a fresh 60m token."""
  parsed = urlparse(uri)
  is_access_token_auth = (
      parsed.username == "access_token"
      or "authmechanism=plain" in (parsed.query or "").lower()
  )
  if not is_access_token_auth:
    return uri
  fresh_tok = mint_gcloud_access_token()
  if not fresh_tok or not parsed.hostname:
    return uri
  user = parsed.username or "access_token"
  host_port = (
      f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname
  )
  new_netloc = f"{user}:{quote(fresh_tok, safe='')}@{host_port}"
  return urlunparse((
      parsed.scheme,
      new_netloc,
      parsed.path,
      parsed.params,
      parsed.query,
      parsed.fragment,
  ))


def resolve_or_refresh_mongodb_uri(
    raw_uri: str | None,
    database_arg: str | None,
    project_id: str,
) -> tuple[str, str]:
  """Resolves MONGODB_URI and auto-refreshes expired gcloud OAuth2 access tokens (code 391)."""
  if raw_uri:
    db_id = (
        database_arg or parse_db_name_from_uri(raw_uri) or "voicebot-demo"
    )
    parsed = urlparse(raw_uri)
    if (
        parsed.username == "access_token"
        or "authmechanism=plain" in (parsed.query or "").lower()
    ):
      refreshed = refresh_access_token_in_uri(raw_uri)
      if refreshed != raw_uri:
        print(
            "[Auth] Auto-refreshed 60-minute OAuth2 access token via "
            "`gcloud auth print-access-token`."
        )
        return refreshed, db_id
    else:
      # Pre-cache one OAuth2 token before benchmark start for zero-fork REST LRO polling
      mint_gcloud_access_token()
    return raw_uri, db_id

  # MONGODB_URI not exported: auto-build from gcloud if available
  db_id = database_arg or "voicebot-demo"
  if not project_id and shutil.which("gcloud"):
    try:
      p_proc = subprocess.run(
          ["gcloud", "config", "get-value", "project"],
          capture_output=True,
          text=True,
          timeout=8.0,
          check=False,
      )
      cfg_proj = p_proc.stdout.strip()
      if p_proc.returncode == 0 and cfg_proj and cfg_proj != "(unset)":
        project_id = cfg_proj
    except Exception:
      pass
  if not project_id:
    print(
        "ERROR: Pass --project <YOUR_GCP_PROJECT_ID> (or export PROJECT_ID / MONGODB_URI).",
        file=sys.stderr,
    )
    sys.exit(1)
  fresh_tok = mint_gcloud_access_token()
  if fresh_tok and shutil.which("gcloud"):
    try:
      proc = subprocess.run(
          [
              "gcloud",
              "firestore",
              "databases",
              "describe",
              f"--database={db_id}",
              f"--project={project_id}",
              "--format=json",
          ],
          capture_output=True,
          text=True,
          timeout=12.0,
          check=False,
      )
      if proc.returncode == 0 and proc.stdout.strip():
        meta = json.loads(proc.stdout)
        uid = meta.get("uid", "")
        loc = meta.get("locationId", "")
        if uid and loc:
          db_path = "_" if db_id == "(default)" else db_id
          enc_tok = quote(fresh_tok, safe="")
          auto_uri = (
              f"mongodb://access_token:{enc_tok}@{uid}.{loc}.firestore.goog:443/"
              f"{db_path}?loadBalanced=true&tls=true&retryWrites=false"
              f"&authMechanism=PLAIN&authSource=%24external"
          )
          print(
              f"[Auth] Auto-constructed MONGODB_URI for '{db_id}' ({loc}) "
              "using fresh 60-minute gcloud access token."
          )
          return auto_uri, db_id
    except Exception:
      pass

  print(
      "ERROR: MONGODB_URI is not set and could not be auto-constructed via gcloud.",
      file=sys.stderr,
  )
  sys.exit(1)


def preflight_verify_connection(
    uri: str, database_id: str, project_id: str
) -> None:
  """Fails fast before spawning workers if MONGODB_URI has placeholders or auth/DNS issues."""
  placeholder_tokens = (
      "<DB_USER>",
      "<SECURE_PASSWORD>",
      "<YOUR_SECURE_PASSWORD>",
      "<DB_UID>",
      "<PASSWORD>",
      "<USERNAME>",
      "<USER>",
      "YOUR_URL_ENCODED_PASSWORD",
      "YOUR_SCRAM_USERNAME",
  )
  parsed_host = urlparse(uri).hostname or ""
  if any(tok in uri for tok in placeholder_tokens) or "<" in parsed_host:
    print(
        "ERROR: MONGODB_URI still contains unreplaced placeholder tokens "
        "(e.g. <DB_USER>, <SECURE_PASSWORD>, or <DB_UID>).\n"
        f"  -> Run the following command to fetch the exact connection string "
        f"template for '{database_id}':\n"
        f"     gcloud firestore databases connection-string "
        f"--database={database_id} --auth=scram-sha-256 --project={project_id}\n"
        "  -> Then substitute your real SCRAM username & URL-encoded password.",
        file=sys.stderr,
    )
    sys.exit(1)

  probe_counter = mp.Value("i", 0)
  probe_tracker = PoolConnectionTracker(probe_counter)
  probe_client = MongoClient(
      uri,
      maxPoolSize=1,
      serverSelectionTimeoutMS=12000,
      connectTimeoutMS=12000,
      socketTimeoutMS=12000,
      event_listeners=[probe_tracker],
  )
  try:
    probe_client[database_id].command("ping")
  except Exception as exc:
    print(
        f"ERROR: Pre-flight connectivity/auth check failed for '{database_id}': "
        f"{type(exc).__name__}: {exc}\n"
        f"  -> Verify your credentials and DB UID via:\n"
        f"     gcloud firestore databases connection-string "
        f"--database={database_id} --auth=scram-sha-256 --project={project_id}",
        file=sys.stderr,
    )
    sys.exit(1)
  finally:
    probe_client.close()


def main():
  parser = argparse.ArgumentParser(
      description="Firestore Enterprise (MongoDB Compatibility) Scale & Consistency Benchmark v2.3"
  )
  parser.add_argument(
      "--role",
      choices=["all-in-one", "bau-primary", "bulk-worker"],
      default="all-in-one",
      help=(
          "'bau-primary': run on VM 1 for 1,000 BAU conns @ "
          "5,000 ops/s + Online Index Build + 0-Delay Consistency + Unified "
          "Dashboard. 'bulk-worker': run on VM 2 "
          "for dedicated 1M-10M bulk load. "
          "'all-in-one': run everything on one host."
      ),
  )
  parser.add_argument(
      "--run-id",
      default="voicebot-live-bench",
      help="Shared coordination ID linking VM 1 (bau-primary) and VM 2 (bulk-worker)",
  )
  parser.add_argument(
      "--sync-timeout",
      type=float,
      default=300.0,
      help="Max seconds to wait for peer VM handshake in 2-VM mode",
  )
  parser.add_argument(
      "--preset",
      choices=["full-scale", "local-smoke"],
      default="full-scale",
      help=(
          "'full-scale' (default): 1,000 BAU conns @ 5,000 BAU ops/s + bulk "
          "load (for GCE VMs in asia-south1). 'local-smoke': 100 BAU conns "
          "@ 500 ops/s + 100k bulk load for laptop testing."
      ),
  )
  parser.add_argument(
      "--database",
      default=os.environ.get("DB_ID"),
      help="Database ID (defaults to DB_ID, URI path, or 'voicebot-demo')",
  )
  parser.add_argument("--collection", default="calls", help="Collection name")
  parser.add_argument(
      "--project",
      default=os.environ.get("PROJECT_ID", ""),
      help="GCP Project ID (enables zero-fork REST LRO %% polling)",
  )
  parser.add_argument(
      "--bau-conns", type=int, default=None, help="BAU concurrent connections"
  )
  parser.add_argument(
      "--bau-procs", type=int, default=None, help="BAU worker processes"
  )
  parser.add_argument(
      "--threads-per-proc",
      type=int,
      default=None,
      help=(
          "Active request threads per BAU process multiplexing across the "
          "pre-warmed connection pool (default: 24 -> 384 active threads across "
          "1,000 live open TLS connections for steady 5,000 ops/s)"
      ),
  )
  parser.add_argument(
      "--target-bau-ops", type=float, default=None, help="Target BAU ops/sec"
  )
  parser.add_argument(
      "--ramp-seconds",
      type=float,
      default=20.0,
      help="Smooth BAU QPS ramp duration in seconds (0 for instant 5k/s)",
  )
  parser.add_argument(
      "--disposition-write-ratio",
      type=float,
      default=0.15,
      help=(
          "Fraction of BAU writes [0.0..1.0] that perform terminal call_status "
          "disposition transitions mutating composite indexes (default: 0.15; "
          "remaining 0.85 are live in-call conversation turns)"
      ),
  )
  parser.add_argument(
      "--bulk-docs",
      type=int,
      default=None,
      help="Concurrent bulk ingestion document count (1,000,000 to 10,000,000)",
  )
  parser.add_argument(
      "--bulk-workers",
      type=int,
      default=None,
      help="Parallel bulk worker processes",
  )
  parser.add_argument(
      "--bulk-batch-size",
      type=int,
      default=100,
      help="Docs per insert_many batch (default: 100 for low tail latency)",
  )
  parser.add_argument(
      "--bulk-target-rate",
      type=float,
      default=None,
      help=(
          "Target aggregate bulk ingestion rate in docs/sec across all bulk "
          "workers (default: 4000 for full-scale so live BAU stays <15ms, "
          "500 for local-smoke; pass 0 for unthrottled max-saturation stress)"
      ),
  )
  parser.add_argument(
      "--bulk-start-offset",
      type=int,
      default=100000,
      help="Starting sequence ID for bulk load",
  )
  parser.add_argument(
      "--seed-docs",
      type=int,
      default=100000,
      help="Initial active campaign baseline docs before run",
  )
  parser.add_argument(
      "--index-trigger-delay",
      type=float,
      default=15.0,
      help="Seconds after benchmark start to trigger online index build",
  )
  parser.add_argument(
      "--no-rebuild-online-index",
      action="store_true",
      help="Reuse existing online CRM history index instead of building a fresh one",
  )
  parser.add_argument(
      "--duration-after-bulk",
      type=int,
      default=15,
      help="Seconds to continue running BAU after bulk load & index build finish",
  )
  args = parser.parse_args()

  if args.preset == "local-smoke":
    bau_conns = args.bau_conns or 100
    bau_procs = args.bau_procs or 5
    threads_per_proc = args.threads_per_proc or 10
    target_bau_ops = args.target_bau_ops or 500.0
    bulk_docs = args.bulk_docs if args.bulk_docs is not None else 100000
    bulk_workers = args.bulk_workers or 4
    bulk_target_rate = (
        args.bulk_target_rate if args.bulk_target_rate is not None else 500.0
    )
    seed_docs = min(args.seed_docs, 10000)
  else:
    # Tuned for 16-vCPU e2-highcpu-16 GCE VMs:
    # 16 BAU processes x 62-63 pooled TLS sockets = exact 1,000 live open conns,
    # driven by 24 active threads/process (384 total threads -> 76.8ms interval
    # budget per thread for rock-steady 5,000 BAU ops/sec).
    bau_conns = args.bau_conns or 1000
    bau_procs = args.bau_procs or 16
    threads_per_proc = args.threads_per_proc or 24
    target_bau_ops = args.target_bau_ops or 5000.0
    bulk_docs = args.bulk_docs if args.bulk_docs is not None else 6000000
    bulk_workers = args.bulk_workers or 16
    bulk_target_rate = (
        args.bulk_target_rate if args.bulk_target_rate is not None else 4000.0
    )
    seed_docs = args.seed_docs

  disp_ratio = min(1.0, max(0.0, float(args.disposition_write_ratio)))

  if not args.project and shutil.which("gcloud"):
    try:
      p_proc = subprocess.run(
          ["gcloud", "config", "get-value", "project"],
          capture_output=True,
          text=True,
          timeout=8.0,
          check=False,
      )
      cfg_proj = p_proc.stdout.strip()
      if p_proc.returncode == 0 and cfg_proj and cfg_proj != "(unset)":
        args.project = cfg_proj
    except Exception:
      pass

  uri, database_id = resolve_or_refresh_mongodb_uri(
      os.environ.get("MONGODB_URI"), args.database, args.project
  )

  preflight_verify_connection(uri, database_id, args.project)

  vm_zone = detect_gce_vm_zone()
  db_region = parse_db_region_from_uri(uri)
  if (
      vm_zone != "non-gce"
      and db_region != "unknown"
      and not vm_zone.startswith(db_region)
  ):
    print(
        f"[Latency Advisory] GCE VM zone '{vm_zone}' is outside database "
        f"region '{db_region}' — cross-region network RTT will add latency to "
        "every read and write!",
        file=sys.stderr,
    )

  soft_fd, hard_fd = raise_nofile_limit(65535)
  needed_fds = (
      (bulk_workers * 4 + 64)
      if args.role == "bulk-worker"
      else (bau_conns + (bulk_workers * 4) + 128)
  )
  if soft_fd < needed_fds:
    print(
        f"WARNING: OS open file limit (ulimit -n = {soft_fd}, hard = {hard_fd}) "
        f"is below recommended {needed_fds}. Run `ulimit -n 65535` first!",
        file=sys.stderr,
    )

  # Dedicated VM 2 Bulk-Worker Mode
  if args.role == "bulk-worker":
    run_dedicated_bulk_worker_vm(
        args=args,
        uri=uri,
        database_id=database_id,
        bulk_docs=bulk_docs,
        bulk_workers=bulk_workers,
        bulk_target_rate=bulk_target_rate,
    )
    return

  # VM 1 (bau-primary) or Single-Host (all-in-one) Mode
  is_two_vm_primary = args.role == "bau-primary"
  hostname = socket.gethostname()
  session_id = uuid.uuid4().hex[:8]
  campaign_date = f"27-10-{session_id[:6]}"

  initial_max_seq = ensure_operational_indexes_and_seed(
      uri,
      database_id,
      args.collection,
      seed_docs,
      seed_workers=min(max(bulk_workers, 4), 12),
      rebuild_online_index=not args.no_rebuild_online_index,
  )
  b_offset = max(args.bulk_start_offset, initial_max_seq)

  # Shared state across BAU (& local Bulk if all-in-one) worker processes
  start_epoch_shared = mp.Value("d", 0.0)
  max_seq_shared = mp.Value("q", initial_max_seq)
  bulk_start_offset_shared = mp.Value("q", b_offset)
  campaign_date_shared = mp.Array("c", 64)
  campaign_date_shared.value = campaign_date.encode("utf-8")
  bau_conns_shared = mp.Value("i", 0)
  bulk_conns_shared = mp.Value("i", 0)
  ready_procs_shared = mp.Value("i", 0)
  ready_bulk_shared = mp.Value("i", 0)
  bulk_loaded_shared = mp.Value("q", 0)
  bulk_errors_shared = mp.Value("q", 0)
  start_gate = mp.Event()
  stop_event = mp.Event()
  metrics_q = mp.Queue(maxsize=8192)

  # Continuous background drainer keeps the 64 KB Linux OS kernel pipe <2 KB at
  # all times so worker _feeder threads never block or alias 5-second windows.
  acc_lock = threading.Lock()
  acc_reads = array("f")
  acc_aggs = array("f")
  acc_writes = array("f")
  acc_counts = [0, 0, 0]  # [timeouts, errors, over_30s]
  acc_err_sample = [""]

  def _metrics_drain_loop():
    while not stop_event.is_set():
      try:
        pt_b, q_b, ag_b, wr_b, to_c, er_c, s30_c, err_s = metrics_q.get(
            timeout=0.25
        )
      except queue.Empty:
        continue
      except (EOFError, OSError):
        break
      with acc_lock:
        if pt_b:
          acc_reads.frombytes(pt_b)
        if q_b:
          acc_reads.frombytes(q_b)
        if ag_b:
          acc_aggs.frombytes(ag_b)
        if wr_b:
          acc_writes.frombytes(wr_b)
        acc_counts[0] += to_c
        acc_counts[1] += er_c
        acc_counts[2] += s30_c
        if err_s and not acc_err_sample[0]:
          acc_err_sample[0] = err_s

  drainer_thread = threading.Thread(target=_metrics_drain_loop, daemon=True)
  drainer_thread.start()

  base_c, rem_c = divmod(bau_conns, bau_procs)
  proc_conn_counts = [
      base_c + (1 if pid < rem_c else 0) for pid in range(bau_procs)
  ]
  proc_thread_counts = [
      min(threads_per_proc, p_conns) if p_conns > 0 else 0
      for p_conns in proc_conn_counts
  ]
  actual_bau_conns = sum(proc_conn_counts)
  total_bau_threads = sum(proc_thread_counts)

  print(
      f"\n[Firestore Benchmark {BENCHMARK_VERSION} | Warmup | {hostname} ({vm_zone})] "
      f"Establishing {actual_bau_conns:,} BAU TLS connections across "
      f"{bau_procs} worker processes ({min(proc_conn_counts)}-"
      f"{max(proc_conn_counts)} conns/proc, {threads_per_proc} active "
      f"threads/proc = {total_bau_threads} disjoint call-owner threads)..."
  )
  bau_processes: list[mp.Process] = []
  thread_offset_cursor = 0
  for pid, p_conns in enumerate(proc_conn_counts):
    if p_conns <= 0:
      continue
    p_threads = proc_thread_counts[pid]
    p_target_ops = target_bau_ops * (p_conns / actual_bau_conns)
    p = mp.Process(
        target=bau_process_main,
        args=(
            pid,
            uri,
            database_id,
            args.collection,
            p_conns,
            p_threads,
            thread_offset_cursor,
            total_bau_threads,
            p_target_ops,
            args.ramp_seconds,
            disp_ratio,
            initial_max_seq,
            campaign_date_shared,
            start_epoch_shared,
            max_seq_shared,
            bau_conns_shared,
            ready_procs_shared,
            start_gate,
            stop_event,
            metrics_q,
        ),
    )
    p.start()
    bau_processes.append(p)
    thread_offset_cursor += p_threads

  t_warm = time.perf_counter()
  while (
      ready_procs_shared.value < len(bau_processes)
      and (time.perf_counter() - t_warm) < 35.0
  ):
    time.sleep(0.25)
  print(
      f"[Warmup | {hostname}] Pool ready: {bau_conns_shared.value:,} live BAU "
      f"connections established (OS ulimit -n = {soft_fd})."
  )
  if bau_conns_shared.value < int(actual_bau_conns * 0.90):
    print(
        f"[Diagnostic] Only {bau_conns_shared.value}/{actual_bau_conns} sockets "
        f"completed handshake during warmup. Verify `ulimit -n 65535` and ensure "
        f"Subnet Private Google Access is enabled (to avoid 64-port Cloud NAT caps).",
        file=sys.stderr,
    )

  sync_client: MongoClient | None = None
  sync_coll = None
  ctrl_id = f"{args.run_id}:control"
  bulk_id = f"{args.run_id}:bulk"
  remote_bulk_host = "local"
  bulk_processes: list[mp.Process] = []

  if is_two_vm_primary:
    sync_client = MongoClient(
        uri, maxPoolSize=4, socketTimeoutMS=15000, connectTimeoutMS=15000
    )
    sync_coll = sync_client[database_id][SYNC_COLLECTION]
    sync_coll.update_one(
        {"_id": ctrl_id},
        {
            "$set": {
                "session_id": session_id,
                "state": "WAITING_FOR_BULK",
                "primary_host": hostname,
                "campaign_date": campaign_date,
                "bulk_start_offset": b_offset,
                "updated_epoch": time.time(),
            }
        },
        upsert=True,
    )
    print(
        f"[2-VM Sync] Waiting for VM 2 (--role bulk-worker --run-id "
        f"{args.run_id}) to report READY..."
    )
    t_sync = time.perf_counter()
    while True:
      if (time.perf_counter() - t_sync) > args.sync_timeout:
        print(
            f"ERROR: Timed out after {args.sync_timeout}s waiting for VM 2 "
            f"(--role bulk-worker --run-id {args.run_id}).",
            file=sys.stderr,
        )
        stop_event.set()
        start_gate.set()
        for p in bau_processes:
          p.join(timeout=3.0)
        sync_client.close()
        sys.exit(1)
      try:
        b_doc = sync_coll.find_one({"_id": bulk_id})
      except OperationFailure as ofe:
        if ofe.code == 391:
          new_uri = refresh_access_token_in_uri(uri)
          if new_uri != uri:
            uri = new_uri
            sync_client.close()
            sync_client = MongoClient(
                uri, maxPoolSize=4, socketTimeoutMS=15000, connectTimeoutMS=15000
            )
            sync_coll = sync_client[database_id][SYNC_COLLECTION]
            continue
        raise
      if (
          b_doc
          and b_doc.get("session_id") == session_id
          and b_doc.get("status") == "READY"
      ):
        bulk_docs = int(b_doc.get("target_docs", bulk_docs))
        bulk_workers = int(b_doc.get("workers", bulk_workers))
        bulk_target_rate = float(
            b_doc.get("bulk_target_rate", bulk_target_rate)
        )
        remote_bulk_host = str(b_doc.get("host", "vm2-bulk"))
        print(
            f"[2-VM Sync] Linked with VM 2 ({remote_bulk_host}): "
            f"{bulk_workers} bulk workers ready for {bulk_docs:,} docs!"
        )
        break
      time.sleep(0.4)
  else:
    # Single-host all-in-one mode: spawn local bulk worker processes
    bulk_processes = spawn_bulk_worker_pool(
        uri=uri,
        database_id=database_id,
        coll_name=args.collection,
        bulk_docs=bulk_docs,
        bulk_workers=bulk_workers,
        batch_size=args.bulk_batch_size,
        bulk_target_rate=bulk_target_rate,
        bulk_start_offset_shared=bulk_start_offset_shared,
        campaign_date_shared=campaign_date_shared,
        bulk_conns_shared=bulk_conns_shared,
        ready_bulk_shared=ready_bulk_shared,
        bulk_loaded_shared=bulk_loaded_shared,
        max_seq_shared=max_seq_shared,
        bulk_errors_shared=bulk_errors_shared,
        start_gate=start_gate,
        stop_event=stop_event,
    )

  # Start Online Index Build/Rebuild & Zero-Delay Consistency Coordinator
  idx_coord = OnlineIndexAndConsistencyCoordinator(
      uri=uri,
      database_id=database_id,
      coll_name=args.collection,
      project_id=args.project,
      trigger_delay_sec=args.index_trigger_delay,
      rebuild_if_exists=not args.no_rebuild_online_index,
      stop_event=stop_event,
  )
  idx_coord.start()

  # Release BAU workers (and VM 2 remote bulk workers or local bulk workers) simultaneously!
  start_epoch_shared.value = time.time()
  if is_two_vm_primary and sync_coll is not None:
    sync_coll.update_one(
        {"_id": ctrl_id},
        {"$set": {"state": "RUNNING", "updated_epoch": time.time()}},
    )
  start_gate.set()

  topo_label = (
      f"2-VM Isolated (BAU={hostname} [{vm_zone}] | Bulk={remote_bulk_host})"
      if is_two_vm_primary
      else f"Single-Host ({hostname} [{vm_zone}])"
  )
  bulk_rate_label = (
      f"paced @ {bulk_target_rate:,.0f}/s"
      if bulk_target_rate > 0
      else "unthrottled"
  )
  banner = "=" * 178
  print(f"\n{banner}")
  print(
      f" FIRESTORE ENTERPRISE (MONGODB COMPATIBILITY) — UNIFIED LIVE SCALE & CONSISTENCY TELEMETRY ({BENCHMARK_VERSION})\n"
      f" Infra: {topo_label} | DB: {database_id}.{args.collection} ({db_region})\n"
      f" Target: {actual_bau_conns:,} BAU Conns ({bau_procs} procs x {threads_per_proc} threads) @ {target_bau_ops:,.0f} BAU Ops/s "
      f"(Read/Write Target <15ms, SLA <<30,000ms, 0 Timeouts) | "
      f"Bulk Scale Load: {bulk_docs:,} 34-Field Docs ({bulk_workers} workers, {bulk_rate_label})"
  )
  print(banner)
  hdr1 = (
      f"{'Time':<8} | {'Conns':<9} | {'BAU Ops/s':<9} | {'Reads/s':<7} | "
      f"{'Writes/s':<8} | {'Read P50/P99':<13} | {'Write P50/P99':<13} | "
      f"{'Agg P50/P99':<13} | {'TO/Er/30s':<9} | {'Bulk Ingestion Load':<28} | "
      f"{'Online Index Build/Rebuild':<26} | {'0-Delay Index Consistency'}"
  )
  hdr2 = (
      f"{'(UTC)':<8} | {'BAU+Bulk':<9} | {'(5k Goal)':<9} | {'Pt+Q+Ag':<7} | "
      f"{'(State)':<8} | {'(ms) [<15ms]':<13} | {'(ms) [<15ms]':<13} | "
      f"{'(ms)':<13} | {'(SLA)':<9} | {'(Docs/s & Cumulative)':<28} | "
      f"{'{customerCRTId,...}':<26} | {'(Write->0ms IDXSCAN)'}"
  )
  print(hdr1)
  print(hdr2)
  print("-" * 178)

  cum_timeouts = 0
  cum_errors = 0
  cum_over_30s = 0
  cum_bau_ops = 0
  prev_bulk_docs = 0
  final_bulk_loaded = 0
  final_bulk_errors = 0
  t_prev = time.perf_counter()
  post_finish_start: float | None = None

  try:
    while not stop_event.is_set():
      time.sleep(5.0)
      t_now = time.perf_counter()
      dt = max(t_now - t_prev, 1e-6)
      t_prev = t_now

      # Drain any residual items still queued in metrics_q before snapshotting
      while True:
        try:
          pt_b, q_b, ag_b, wr_b, to_c, er_c, s30_c, err_s = (
              metrics_q.get_nowait()
          )
          with acc_lock:
            if pt_b:
              acc_reads.frombytes(pt_b)
            if q_b:
              acc_reads.frombytes(q_b)
            if ag_b:
              acc_aggs.frombytes(ag_b)
            if wr_b:
              acc_writes.frombytes(wr_b)
            acc_counts[0] += to_c
            acc_counts[1] += er_c
            acc_counts[2] += s30_c
            if err_s and not acc_err_sample[0]:
              acc_err_sample[0] = err_s
        except queue.Empty:
          break

      with acc_lock:
        win_reads = sorted(acc_reads)
        win_aggs = sorted(acc_aggs)
        win_writes = sorted(acc_writes)
        win_timeouts, win_errors, win_over_30s = acc_counts
        win_err_sample = acc_err_sample[0]
        del acc_reads[:]
        del acc_aggs[:]
        del acc_writes[:]
        acc_counts[0] = 0
        acc_counts[1] = 0
        acc_counts[2] = 0
        acc_err_sample[0] = ""

      n_reads_all = len(win_reads) + len(win_aggs)
      n_writes = len(win_writes)
      n_bau_total = n_reads_all + n_writes

      bau_ops_s = n_bau_total / dt
      reads_s = n_reads_all / dt
      writes_s = n_writes / dt

      cum_bau_ops += n_bau_total
      cum_timeouts += win_timeouts
      cum_errors += win_errors
      cum_over_30s += win_over_30s

      if is_two_vm_primary and sync_coll is not None:
        try:
          b_doc = sync_coll.find_one({"_id": bulk_id}) or {}
        except OperationFailure as ofe:
          if ofe.code == 391 and sync_client is not None:
            new_uri = refresh_access_token_in_uri(uri)
            if new_uri != uri:
              uri = new_uri
              sync_client.close()
              sync_client = MongoClient(
                  uri,
                  maxPoolSize=4,
                  socketTimeoutMS=15000,
                  connectTimeoutMS=15000,
              )
              sync_coll = sync_client[database_id][SYNC_COLLECTION]
              b_doc = sync_coll.find_one({"_id": bulk_id}) or {}
            else:
              raise
          else:
            raise
        cur_bulk = int(b_doc.get("bulk_loaded", final_bulk_loaded))
        k_conns = int(b_doc.get("bulk_conns", 0))
        remote_max_seq = int(b_doc.get("max_seq", 0))
        final_bulk_errors = int(b_doc.get("bulk_errors", 0))
        bulk_finished = bool(b_doc.get("done", False))
        if remote_max_seq > max_seq_shared.value:
          with max_seq_shared.get_lock():
            if remote_max_seq > max_seq_shared.value:
              max_seq_shared.value = remote_max_seq
      else:
        cur_bulk = bulk_loaded_shared.value
        k_conns = bulk_conns_shared.value
        final_bulk_errors = bulk_errors_shared.value
        bulk_finished = all(not bp.is_alive() for bp in bulk_processes)

      final_bulk_loaded = cur_bulk
      bulk_rate_s = max(0, cur_bulk - prev_bulk_docs) / dt
      prev_bulk_docs = cur_bulk
      bulk_pct = (cur_bulk / bulk_docs * 100.0) if bulk_docs > 0 else 100.0

      b_conns = bau_conns_shared.value
      conn_str = f"{b_conns}+{k_conns}"
      sla_str = f"{win_timeouts}/{win_errors}/{win_over_30s}"

      r_lat = f"{percentile(win_reads, 0.50):4.1f}/{percentile(win_reads, 0.99):4.1f}ms"
      w_lat = f"{percentile(win_writes, 0.50):4.1f}/{percentile(win_writes, 0.99):4.1f}ms"
      a_lat = f"{percentile(win_aggs, 0.50):4.1f}/{percentile(win_aggs, 0.99):4.1f}ms"

      if bulk_docs >= 1000000:
        prog_str = (
            f"{bulk_rate_s:6,.0f}/s ({cur_bulk / 1e6:5.2f}M/"
            f"{bulk_docs / 1e6:4.1f}M {bulk_pct:3.0f}%)"
        )
      else:
        prog_str = f"{bulk_rate_s:6,.0f}/s ({cur_bulk:,}/{bulk_docs:,})"

      idx_st, probe_st, _, tot_p_miss = idx_coord.snapshot_probe()

      ts_utc = datetime.now(timezone.utc).strftime("%H:%M:%S")
      print(
          f"{ts_utc:<8} | {conn_str:<9} | {bau_ops_s:7,.0f}/s | "
          f"{reads_s:5,.0f}/s | {writes_s:6,.0f}/s | {r_lat:<13} | "
          f"{w_lat:<13} | {a_lat:<13} | {sla_str:<9} | {prog_str:<28} | "
          f"{idx_st[:26]:<26} | {probe_st}"
      )
      if win_err_sample:
        print(f"  [BAU Error Sample] {win_err_sample}", file=sys.stderr)

      if bulk_finished and idx_coord.online_index_ready:
        if post_finish_start is None:
          post_finish_start = time.perf_counter()
          print("-" * 178)
          print(
              f"[Status] Bulk ingestion ({cur_bulk:,} docs) & Online Index "
              f"Build both COMPLETE! Observing steady-state BAU for "
              f"{args.duration_after_bulk}s (Timeouts={cum_timeouts}, "
              f"Errors={cum_errors}, >30s={cum_over_30s}, "
              f"IndexLagMisses={tot_p_miss})..."
          )
          print("-" * 178)
        elif (
            time.perf_counter() - post_finish_start
        ) >= args.duration_after_bulk:
          break

  except KeyboardInterrupt:
    print("\n[Interrupt] Stopping benchmark workers gracefully...")
  finally:
    stop_event.set()
    if is_two_vm_primary and sync_coll is not None:
      try:
        sync_coll.update_one(
            {"_id": ctrl_id},
            {"$set": {"state": "STOPPED", "updated_epoch": time.time()}},
        )
      except Exception:
        pass
    if sync_client is not None:
      sync_client.close()
    for p in bau_processes:
      p.join(timeout=5.0)
      if p.is_alive():
        p.terminate()
    for bp in bulk_processes:
      bp.join(timeout=5.0)
      if bp.is_alive():
        bp.terminate()
    drainer_thread.join(timeout=1.5)

  _, _, final_p_ok, final_p_miss = idx_coord.snapshot_probe()
  print(banner)
  print(
      f" FINAL SUMMARY | Total BAU Ops: {cum_bau_ops:,} | "
      f"Bulk Ingested: {final_bulk_loaded:,} docs | "
      f"Timeouts: {cum_timeouts} | >30s SLA Breaches: {cum_over_30s} | "
      f"BAU Errors: {cum_errors} | Bulk Errors: {final_bulk_errors} | "
      f"0-Delay Index Probes: {final_p_ok} PASS / {final_p_miss} Lag Miss"
  )
  print(banner)

  # Clean up any older demo candidate indexes after run exit so background
  # deletion GC never competes with live benchmark traffic.
  idx_coord.cleanup_previous_demo_indexes()


if __name__ == "__main__":
  main()
