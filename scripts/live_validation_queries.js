// ============================================================================
// Firestore Enterprise (MongoDB Compatibility) — Live Validation & Proofs
// ============================================================================
// Run interactively inside `mongosh`, or execute end-to-end via:
//   ./scripts/mongo_connect.sh --project <PROJECT_ID> --database <DB_ID> \
//     --file scripts/live_validation_queries.js
// ============================================================================

print("\n=== [Phase I] 1. Seed 5,000 34-Field Call Documents (Zero Secondary Indexes) ===");
const demoCol = db.calls_phase1_demo;
demoCol.drop();

const baseTime = new Date("2026-10-14T10:00:00Z");
const seedBatch = [];
for (let i = 0; i < 5000; i++) {
  const phone = 9845010000 + i;
  const dedupKey = `${phone}--14-10-2026`;
  const answered = i % 3 === 0 ? 1 : 0;
  seedBatch.push({
    phone_number: Long.fromNumber(phone),
    full_name: `Customer_${i}`,
    language: ["hi-IN", "en-IN", "ta-IN", "te-IN"][i % 4],
    duplicate: 1,
    month: "October",
    last_triggered_date: new Date(baseTime.getTime() - (i % 120) * 60000),
    input_date: "14-10-2026",
    next_trigger_date: new Date(baseTime.getTime() + (i % 3600) * 1000),
    phone_number_input_date: dedupKey,
    sequence_number: 1,
    answered_seq: answered,
    no_answer_seq: answered ? 0 : 1,
    call_status: ["ANSWERED", "NOT_CALLED", "NO_ANSWER"][i % 3],
    setupTime: 20 + (i % 25),
    sts: answered ? "Answered" : "NotAnswered",
    trigger_call: false,
    flow_id: `voicebot-flow-oct14-c0-${(i % 20) + 1}`,
    STAGE_CODE: ["INITIAL", "FOLLOWUP", "REMINDER", "FINAL"][i % 4],
    stage: "initial",
    call_comp_flag: "no",
    call_sequence_mapping: {
      "13/10/2026": {
        calls_triggered: 1,
        answered: answered,
        not_answered: answered ? 0 : 1,
        max_trigger: 3
      }
    },
    conversation_log: [],
    conversation_id: "",
    call_analysis: 0,
    fallback_count: 0,
    fallback_failure: false,
    repeat_count: 0,
    repeat_failure: false,
    tracker_log: [],
    fallback_conv: [],
    customerCRTId: `${7000000000000000000n + BigInt(i % 500)}`,
    active_call: 1,
    max_trigger: 3,
    ringingTime: i % 15,
    inserted_by: "voicebot-scheduler@example.com",
    inserted_from: "dashboard",
    inserted_on: new Date(baseTime.getTime() - (i % 3600) * 1000),
    triggered_from: "vbcalltrigger",
    sender_id: `${7000000000000000000n + BigInt(i % 500)}`,
    stop_calling: false,
    total_connects: answered
  });
}
demoCol.insertMany(seedBatch, { ordered: false });
print("Seeded documents:", demoCol.countDocuments());
print("Initial indexes (only _id_):");
printjson(demoCol.getIndexes());

print("\n=== [Phase I] 2. Unindexed Point Read -> Full Collection Scan (COLLSCAN) ===");
const collscanPlan = demoCol.find({
  phone_number_input_date: "9845012345--14-10-2026"
}).explain("executionStats");
printjson(collscanPlan.executionStats);

print("\n=== [Phase I] 3. Create Secondary Index -> Single-Key Index Scan (IDXSCAN) ===");
demoCol.createIndex({ phone_number_input_date: 1 }, { name: "phone_number_input_date_1" });
const idxscanPlan = demoCol.find({
  phone_number_input_date: "9845012345--14-10-2026"
}).explain("executionStats");
printjson(idxscanPlan.executionStats);

print("\n=== [Phase II] 4. Create Operational Composite Indexes & Verify Plans ===");
demoCol.createIndex(
  { flow_id: 1, stop_calling: 1, call_status: 1, next_trigger_date: 1 },
  { name: "flow_id_1_stop_calling_1_call_status_1_next_trigger_date_1" }
);
demoCol.createIndex(
  { flow_id: 1, STAGE_CODE: 1, call_status: 1, total_connects: 1 },
  { name: "flow_id_1_STAGE_CODE_1_call_status_1_total_connects_1" }
);

print("Dialer Queue Fetch executionStats (IDXSCAN, zero in-memory SORT):");
printjson(
  demoCol.find({
    flow_id: "voicebot-flow-oct14-c0-1",
    stop_calling: false,
    call_status: "NOT_CALLED",
    next_trigger_date: { $lte: new Date("2026-10-14T12:00:00Z") }
  }).sort({ next_trigger_date: 1 }).limit(10).explain("executionStats").executionStats
);

print("Covered Campaign $group Aggregation executionStats:");
printjson(
  demoCol.aggregate([
    { $match: { flow_id: "voicebot-flow-oct14-c0-1", STAGE_CODE: "INITIAL" } },
    {
      $group: {
        _id: "$call_status",
        total_calls: { $sum: 1 },
        connects: { $sum: "$total_connects" }
      }
    }
  ]).explain("executionStats")
);

print("\n=== [Phase III] 5. Zero-Delay (0 ms) Synchronous Index Consistency Proofs ===");
// Proof A: Insert -> 0ms Delay Forced Secondary Index Scan
const tag = "LIVE-" + Date.now();
const key = "9876543210--" + tag;
const flow = "voicebot-realtime-" + tag;
const t0 = Date.now();
const ins = demoCol.insertOne({
  phone_number_input_date: key,
  flow_id: flow,
  stop_calling: false,
  call_status: "NOT_CALLED",
  STAGE_CODE: "INIT",
  total_connects: 0,
  next_trigger_date: new Date()
});
const t1 = Date.now();
const hit = demoCol
  .find({ phone_number_input_date: key }, { _id: 1 })
  .hint("phone_number_input_date_1")
  .toArray();
print(
  `[Proof A] Insert: ${t1 - t0}ms | 0ms-Delay Index Scan: ${Date.now() - t1}ms | ` +
  `Found in Index: ${hit.length}/1 (_id=${hit[0]?._id})`
);

// Proof B: Atomic Update State Transition Across Composite Indexes (0 Stale Reads)
demoCol.updateOne(
  { _id: ins.insertedId },
  { $set: { call_status: "ANSWERED" }, $inc: { total_connects: 1 } }
);
const staleHits = demoCol
  .find({
    flow_id: flow,
    stop_calling: false,
    call_status: "NOT_CALLED",
    next_trigger_date: { $lte: new Date(Date.now() + 60000) }
  })
  .hint("flow_id_1_stop_calling_1_call_status_1_next_trigger_date_1")
  .toArray();
const freshAgg = demoCol
  .aggregate([
    { $match: { flow_id: flow, STAGE_CODE: "INIT", call_status: "ANSWERED" } },
    { $group: { _id: "$call_status", connects: { $sum: "$total_connects" } } }
  ])
  .toArray();
print(
  `[Proof B] Stale 'NOT_CALLED' Index Hits: ${staleHits.length} (expected 0) | ` +
  `Fresh 'ANSWERED' Composite Index Connects: ${freshAgg[0]?.connects ?? 0} (expected 1)`
);

// Proof C: 50-Iteration Back-to-Back Insert -> 0ms Forced Index Scan
let hits50 = 0;
const burstStart = Date.now();
for (let i = 0; i < 50; i++) {
  const k = `989900${String(i).padStart(4, "0")}--BURST-${burstStart}-${i}`;
  demoCol.insertOne({
    phone_number_input_date: k,
    flow_id: flow,
    stop_calling: false,
    call_status: "NOT_CALLED",
    STAGE_CODE: "INIT",
    total_connects: 0,
    next_trigger_date: new Date()
  });
  const found = demoCol
    .find({ phone_number_input_date: k }, { _id: 1 })
    .hint("phone_number_input_date_1")
    .toArray();
  if (found.length === 1) hits50++;
}
print(
  `[Proof C] Tight-Loop 0ms Index Consistency: ${hits50}/50 immediate hits ` +
  `(${Date.now() - burstStart}ms total)`
);

print("\n=== [Phase IV] 6. Collection Count, Indexes & Logical BSON Size Utility ===");
const targetColName = db.getCollectionNames().includes("calls") ? "calls" : "calls_phase1_demo";
const targetCol = db.getCollection(targetColName);
const totalDocs = targetCol.countDocuments();
const avgObj = targetCol.aggregate([
  { $limit: 10000 },
  { $group: { _id: null, avgBytes: { $avg: { $bsonSize: "$$ROOT" } } } }
]).toArray()[0];
const avgBytes = avgObj ? avgObj.avgBytes : 0;
const totalGiB = (totalDocs * avgBytes) / Math.pow(1024, 3);
const totalGB = (totalDocs * avgBytes) / 1e9;

print(`Collection '${targetColName}':`);
print(`  Total Documents : ${totalDocs.toLocaleString()}`);
print(`  Avg BSON Size   : ${avgBytes.toFixed(1)} bytes/doc`);
print(`  Est Logical Size: ${totalGiB.toFixed(3)} GiB (${totalGB.toFixed(3)} GB)`);
print(`  Active Indexes  : ${targetCol.getIndexes().length}`);
printjson(targetCol.getIndexes());
