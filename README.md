# Firestore Enterprise (MongoDB Compatibility) — Scale & Consistency Benchmark

End-to-end scale benchmark, index execution-plan verifier, and **0-ms secondary index consistency** validation suite for **Google Cloud Firestore Enterprise with MongoDB Compatibility**.

Simulates a high-concurrency outbound voicebot / contact-center dialer workload over a **34-field BSON call document (~1.16 KiB)** while simultaneously driving bulk campaign ingestion, online composite index builds, and zero-delay read-after-write index probes.

---

## Repository Contents

| Path | Description |
| :--- | :--- |
| `firestore_mongodb_benchmark.py` | Multi-process PyMongo scale & consistency benchmark (`all-in-one`, `bau-primary`, and `bulk-worker` roles) with real-time 5-second telemetry dashboard. |
| `scripts/mongo_connect.sh` | One-command `mongosh` launcher using URL-encoded 60-minute `gcloud` OAuth2 access tokens. |
| `scripts/live_validation_queries.js` | Interactive / batch `mongosh` validation suite: `COLLSCAN` vs. `IDXSCAN` `.explain("executionStats")` proofs, composite index verification, **0-ms secondary index consistency** proofs, document counts, and logical BSON sizing. |
| `scripts/check_storage_size.py` | Queries Cloud Monitoring (`data_and_index_storage_bytes`) to report total Firestore Enterprise **Data + Index** storage in GiB / GB. |
| `requirements.txt` | Python dependencies (`pymongo>=4.6.0,<5.0.0`). |

---

## Workload Architecture

`firestore_mongodb_benchmark.py` runs four concurrent workloads against a 34-field BSON document schema:

1. **Sustained BAU Operational Traffic (500 – 1,000+ Live TLS Connections, 2,500 – 5,000+ Ops/sec)**
   - **48% Point Lookups**: 70% full 34-field `_id` fetches + 30% index-covered `phone_number_input_date` lead-deduplication checks.
   - **20% Dialer Queue Fetches**: Index-covered `find().sort({ next_trigger_date: 1 }).limit(10)` with zero in-memory `SORT`.
   - **2% Live Campaign Aggregations**: Covered `$match` + `$group` status & connect-count rollups.
   - **30% Live Call-State Updates**: Disjoint per-thread active call ownership (`85%` mid-call utterance turns appending to `conversation_log` + `15%` terminal call-disposition updates transitioning composite index keys).
2. **Parallel Bulk Campaign Ingestion (1M – 10M Documents)**
   - Staggered `insert_many(..., ordered=False)` worker pool with configurable rate pacing (`--bulk-target-rate`) or unthrottled saturation (`--bulk-target-rate 0`).
3. **Concurrent Online Composite Index Build**
   - Triggers an online composite index build (`{ customerCRTId: 1, ..., inserted_on: -1 }`) mid-benchmark with zero write locks, polling Firestore LRO progress via REST and verifying 100% sentinel visibility immediately upon completion.
4. **Zero-Delay (`0.0 ms`) Secondary Index Consistency Probe**
   - Continuously inserts fresh document cohorts and—with **zero sleep (`0.0 ms`)**—queries them back via forced `.hint()` secondary index scans, tracking `PASS / Lag Miss` counts in real time.

---

## 1. Prerequisites & Setup

1. **Firestore Enterprise Database (MongoDB Compatibility)** created in the same region as your benchmark VM(s) (for example, `asia-south1` or `us-central1`):
   ```bash
   export PROJECT_ID="<YOUR_GCP_PROJECT_ID>"
   export DB_ID="voicebot-demo"
   export REGION="asia-south1"

   gcloud firestore databases create \
     --database="${DB_ID}" \
     --location="${REGION}" \
     --type=firestore-native \
     --edition=enterprise \
     --mongodb-compatible-data-access \
     --project="${PROJECT_ID}"
   ```
2. **Client VM Recommendations** (for 1,000+ connections & 5,000+ BAU ops/sec):
   - Two `e2-highcpu-16` GCE VMs in the **same region** as the database, with **Private Google Access** enabled on the subnet and `cloud-platform` OAuth scope.
   - Ensure the caller / VM service account has `roles/datastore.user` (data plane + index DDL) and `roles/monitoring.viewer` (optional, for `scripts/check_storage_size.py`).
3. **Install Python Virtual Environment & Raise File Descriptor Limit** (run on each VM):
   ```bash
   sudo apt-get update && sudo apt-get install -y python3-venv python3-pip
   python3 -m venv ~/bench-venv
   source ~/bench-venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt

   # Raise open socket limit in every new SSH session before running 500-1,000+ connections:
   ulimit -n 65535
   ```
   > **Tip:** After reconnecting a terminal session, always re-activate the virtual environment (`source ~/bench-venv/bin/activate`) and run `ulimit -n 65535` so `bson` / `pymongo` are on `PYTHONPATH`.

---

## 2. Connect via `mongosh` & Run Live Index / Consistency Proofs

Make the helper scripts executable once:
```bash
chmod +x scripts/mongo_connect.sh scripts/check_storage_size.py firestore_mongodb_benchmark.py
```

### Interactive `mongosh` Login
```bash
./scripts/mongo_connect.sh --project "${PROJECT_ID}" --database "${DB_ID}"
```

### Run End-to-End Plan & Zero-Delay Consistency Validation
Executes `COLLSCAN` $\rightarrow$ `IDXSCAN` `.explain("executionStats")` comparisons, composite index checks, and **3 live `0 ms` synchronous index consistency proofs** on a dedicated `calls_phase1_demo` collection:
```bash
./scripts/mongo_connect.sh \
  --project "${PROJECT_ID}" \
  --database "${DB_ID}" \
  --file scripts/live_validation_queries.js
```

---

## 3. Running the Scale Benchmark

The benchmark automatically constructs and refreshes 60-minute OAuth2 `MONGODB_URI` tokens via `gcloud` (or accepts an explicit `export MONGODB_URI="mongodb://..."` using SCRAM-SHA-256 or `PLAIN` auth).

### Option A: Quick Single-Machine Smoke Test (`100` Conns, `500` BAU Ops/s, `100k` Bulk Docs)
```bash
source ~/bench-venv/bin/activate
ulimit -n 65535

python3 firestore_mongodb_benchmark.py \
  --preset local-smoke \
  --project "${PROJECT_ID}" \
  --database "${DB_ID}" \
  --collection calls
```

### Option B: 2-VM Isolated Full-Scale Benchmark (`1,000` BAU Conns @ `5,000` Ops/s + `6M` Bulk Load)

Run **VM 1 (`bau-primary`)** and **VM 2 (`bulk-worker`)** with a matching `--run-id`. The two VMs automatically coordinate warmup and release simultaneously via Firestore.

**On VM 1 (BAU Primary + Online Index Coordinator + Live Telemetry Dashboard):**
```bash
source ~/bench-venv/bin/activate
ulimit -n 65535

python3 firestore_mongodb_benchmark.py \
  --role bau-primary \
  --run-id live-scale-run \
  --project "${PROJECT_ID}" \
  --database "${DB_ID}" \
  --collection calls \
  --bau-conns 1000 \
  --bau-procs 16 \
  --threads-per-proc 24 \
  --target-bau-ops 5000
```

**On VM 2 (Dedicated Parallel Bulk Ingestion Tier):**
```bash
source ~/bench-venv/bin/activate
ulimit -n 65535

python3 firestore_mongodb_benchmark.py \
  --role bulk-worker \
  --run-id live-scale-run \
  --project "${PROJECT_ID}" \
  --database "${DB_ID}" \
  --collection calls \
  --bulk-docs 6000000 \
  --bulk-workers 16 \
  --bulk-batch-size 100 \
  --bulk-target-rate 4000
```

### Option C: Custom 500-Connection / 1M-Doc Steady-State Profile (Skipping Online Index Rebuild)
To measure pure BAU + Bulk steady-state latency without triggering a mid-run online index build, set `--index-trigger-delay 9999999`:

- **VM 1 (`bau-primary`)**:
  ```bash
  python3 firestore_mongodb_benchmark.py \
    --role bau-primary \
    --run-id live-500conn-run \
    --project "${PROJECT_ID}" \
    --database "${DB_ID}" \
    --collection calls \
    --bau-conns 500 \
    --bau-procs 8 \
    --threads-per-proc 8 \
    --target-bau-ops 2500 \
    --index-trigger-delay 9999999
  ```
- **VM 2 (`bulk-worker`)**:
  ```bash
  python3 firestore_mongodb_benchmark.py \
    --role bulk-worker \
    --run-id live-500conn-run \
    --project "${PROJECT_ID}" \
    --database "${DB_ID}" \
    --collection calls \
    --bulk-docs 1000000 \
    --bulk-workers 8 \
    --bulk-batch-size 100 \
    --bulk-target-rate 2500
  ```

---

## 4. Useful `mongosh` & Storage Inspection Commands

Inside `mongosh` (`./scripts/mongo_connect.sh --project "${PROJECT_ID}" --database "${DB_ID}"`):

```javascript
// 1. List all indexes on the collection
db.calls.getIndexes()

// 2. Get exact total document count
db.calls.countDocuments()

// 3. Estimate logical BSON payload size in GiB / GB from a 10,000-doc sample
const totalDocs = db.calls.countDocuments();
const avgBytes = db.calls.aggregate([
  { $limit: 10000 },
  { $group: { _id: null, avg: { $avg: { $bsonSize: "$$ROOT" } } } }
]).toArray()[0].avg;
print(`Docs: ${totalDocs.toLocaleString()} | Avg: ${avgBytes.toFixed(1)} B | Logical Size: ${(totalDocs * avgBytes / 1024 ** 3).toFixed(2)} GiB`);
```

To check **total billed storage (Data + Indexes)** from Cloud Monitoring:
```bash
python3 scripts/check_storage_size.py --project "${PROJECT_ID}" --database "${DB_ID}"
```

---

## 5. Monitoring Performance in Google Cloud Console

Open **Google Cloud Console $\rightarrow$ Firestore $\rightarrow$ Databases $\rightarrow$ `<DB_ID>`** and inspect the left-hand observability tabs:

1. **Monitoring** (Primary server-side golden signals — zero client/network RTT):
   - **Active Connections**: Confirms live open MongoDB wire-protocol TLS sockets (e.g., `500` or `1,000` BAU + bulk worker pools).
   - **Document Reads / Writes / Deletes**: Tracks server-side read/write throughput across BAU + Bulk load.
   - **Latency (`p50` / `p99`)**: Displays server-side execution latency (typically `~1–5 ms` server-side vs. `~10–14 ms` client-side including TLS + VPC RTT).
   - **Database Storage Size**: Total stored bytes broken down by document data and index entries.
2. **Query insights**:
   - Identifies top query shapes by total execution time, read units, and execution count, confirming secondary index scans (`IDXSCAN`) with `1:1` keys-to-documents ratios.
3. **Usage** & **Usage insights**:
   - Tracks billed Read Units and Write Units over 1-minute / 1-hour windows.
