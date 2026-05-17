package com.example.multietl.pipelines.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.multietl.parser.LogParser;
import com.example.multietl.parser.LogRecord;
import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.base.QueryType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoPipeline implements Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(MongoPipeline.class);

    private final String uri;
    private final String dbName;

    private MongoClient client;
    private MongoDatabase db;
    private MongoCollection<Document> rawColl;
    private MongoCollection<Document> parsedColl;
    private String runId;

    // FIX (days-mode batch bug): separate fields for records-per-chunk vs days-per-batch
    // previously a single batchSize was shared by both modes, causing assignBatchIdsByDate
    // to compute (i / 1000) = 0 for all dates, collapsing everything into batchId=1.
    private int batchSize    = 1; // records per chunk  — set by BatchManager via startRun()
    private int daysBatchSize = 1; // days per batch     — set via setDaysBatchSize()

    private String batchMode = "records";

    private final LogParser parser = new LogParser();
    private long malformedCount = 0;
    private long processed = 0;
    private long rawLoaded = 0;
    private int ingestChunks = 0;

    // FIX (Bug 5): only populate dateStats when we are actually in days mode,
    // avoiding unnecessary work on every record in records mode.
    private final Map<String, long[]> dateStats = new HashMap<>();

    private List<Map<String, Object>> batchSummaries = new ArrayList<>();

    public MongoPipeline(String uri, String dbName) {
        this.uri = uri;
        this.dbName = dbName;
    }

    @Override
    public void setBatchMode(String batchMode) {
        this.batchMode = batchMode;
    }

    @Override
    public void setDaysBatchSize(
            int daysBatchSize
    ) {
        this.daysBatchSize =
            Math.max(
                1,
                daysBatchSize
            );
    }

    @Override
    public void startRun(String runId, int batchSize) {
        this.runId = runId;
        this.batchSize = Math.max(1, batchSize);

        client = MongoClients.create(uri);
        db = client.getDatabase(dbName);

        rawColl  = db.getCollection("raw_logs");
        parsedColl = db.getCollection("parsed_logs");

        parsedColl.createIndex(new Document("log_date",      1));
        parsedColl.createIndex(new Document("resource_path", 1));
        parsedColl.createIndex(new Document("status_code",   1));
        // FIX (Bug 6): index ingest_chunk_id so countDocuments() in buildBatchSummaries
        // (records mode) doesn't do a full collection scan for each chunk.
        parsedColl.createIndex(new Document("ingest_chunk_id", 1));

        logger.info(
            "MongoPipeline started run {} with batchSize={} daysBatchSize={} mode={}",
            runId, this.batchSize, this.daysBatchSize, batchMode
        );
    }

    @Override
    public void processBatch(List<String> rawLines, int chunkId) {
        ingestChunks++;
        rawLoaded += rawLines.size();

        List<Document> rawDocs = new ArrayList<>(rawLines.size());
        for (String line : rawLines) {
            rawDocs.add(new Document("run_id", runId)
                .append("ingest_chunk_id", chunkId)
                .append("raw_line", line));
        }
        if (!rawDocs.isEmpty()) {
            rawColl.insertMany(rawDocs);
        }

        List<Document> parsedDocs = new ArrayList<>(rawLines.size());
        for (String line : rawLines) {
            LogParser.ParseResult r = parser.parse(line);
            LogRecord record = r.record;

            Document d = new Document();
            d.append("host",          record.getHost());
            d.append("timestamp",     record.getTimestamp() == null ? null : record.getTimestamp().toString());
            d.append("log_date",      record.getLogDate());
            d.append("log_hour",      record.getLogHour());
            d.append("method",        record.getMethod());
            d.append("resource_path", record.getResourcePath());
            d.append("protocol",      record.getProtocol());
            d.append("status_code",   record.getStatusCode());
            d.append("bytes",         record.getBytes());
            d.append("malformed",     record.isMalformed());
            d.append("raw_line",      record.getRawLine());
            d.append("run_id",        runId);
            d.append("ingest_chunk_id", chunkId);

            // FIX (Bug 1): in records mode, set batch_id = chunkId immediately so the
            // field is correct in the document without any follow-up update pass.
            // In days mode, batch_id starts at 0 and is overwritten by assignBatchIdsByDate()
            // during finalizeRun() once all dates are known.
            d.append("batch_id", "records".equalsIgnoreCase(batchMode) ? chunkId : 0);

            parsedDocs.add(d);

            if (!r.success) {
                malformedCount++;
            } else {
                processed++;
            }

            // Always populate dateStats regardless of batchMode.
            // A batchMode guard here is unsafe because BatchManager may call setBatchMode()
            // *after* processBatch(), meaning the guard would evaluate against the default
            // "records" value and skip all date tracking — leaving dateStats empty when
            // assignBatchIdsByDate() runs in finalizeRun(), which collapses everything into
            // batch_id=0. The overhead of maintaining this map is negligible.
            String logDate = record.getLogDate();
            if (logDate != null) {
                long[] stats = dateStats.computeIfAbsent(logDate, k -> new long[]{0L, 0L});
                stats[0]++;
                if (!r.success) stats[1]++;
            }
        }

        if (!parsedDocs.isEmpty()) {
            parsedColl.insertMany(parsedDocs);
        }
        logger.info(
            "Processed ingest chunk {}: inserted {} parsed docs (malformed so far={})",
            chunkId, parsedDocs.size(), malformedCount
        );
    }

    @Override
    public Map<String, List<Map<String, Object>>> finalizeRun(QueryPlan plan) {
        if ("days".equalsIgnoreCase(batchMode)) {
            assignBatchIdsByDate();
        }

        batchSummaries = buildBatchSummaries();

        Map<String, List<Map<String, Object>>> results = new LinkedHashMap<>();

        if (plan.isSplitByMonth()) {
            List<String> months = extractMonths();
            for (String month : months) {
                if (plan.getQueries().contains(QueryType.DAILY_TRAFFIC_SUMMARY)) {
                    results.put(scopedKey(QueryType.DAILY_TRAFFIC_SUMMARY, month), runQuery1(month));
                }
                if (plan.getQueries().contains(QueryType.TOP_RESOURCES)) {
                    results.put(scopedKey(QueryType.TOP_RESOURCES, month), runQuery2(month));
                }
                if (plan.getQueries().contains(QueryType.HOURLY_ERROR_ANALYSIS)) {
                    results.put(scopedKey(QueryType.HOURLY_ERROR_ANALYSIS, month), runQuery3(month));
                }
            }
        } else {
            if (plan.getQueries().contains(QueryType.DAILY_TRAFFIC_SUMMARY)) {
                results.put(QueryType.DAILY_TRAFFIC_SUMMARY.getKey(), runQuery1(null));
            }
            if (plan.getQueries().contains(QueryType.TOP_RESOURCES)) {
                results.put(QueryType.TOP_RESOURCES.getKey(), runQuery2(null));
            }
            if (plan.getQueries().contains(QueryType.HOURLY_ERROR_ANALYSIS)) {
                results.put(QueryType.HOURLY_ERROR_ANALYSIS.getKey(), runQuery3(null));
            }
        }

        logger.info("Finalized run {}: processed={}, malformed={}", runId, processed, malformedCount);
        return results;
    }

    @Override
    public void shutdown() {
        if (client != null) client.close();
        dateStats.clear();
        batchSummaries = new ArrayList<>();
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> m = new HashMap<>();
        m.put("processed",     processed);
        m.put("malformed",     malformedCount);
        m.put("total_records", processed + malformedCount);
        m.put("total_batches", computeBatchCount());
        m.put("raw_loaded",    rawLoaded);
        m.put("ingest_chunks", ingestChunks);
        return m;
    }

    @Override
    public List<Map<String, Object>> getBatchSummaries() {
        return batchSummaries;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private int computeBatchCount() {
        if ("records".equalsIgnoreCase(batchMode)) {
            return ingestChunks;
        }
        int dateCount = dateStats.size();
        if (dateCount == 0) return 0;
        // FIX (days-mode batch bug): use daysBatchSize, not batchSize.
        return (int) Math.ceil((double) dateCount / daysBatchSize);
    }

    private void assignBatchIdsByDate() {
        List<String> dates = new ArrayList<>(dateStats.keySet());
        Collections.sort(dates);
        for (int i = 0; i < dates.size(); i++) {
            String date = dates.get(i);
            // FIX (days-mode batch bug): use daysBatchSize, not batchSize,
            // so e.g. daysBatchSize=7 gives batchId groups of 7 days each
            // instead of (i / 1000) = 0 which collapsed all dates into batch 1.
            int batchId = (i / daysBatchSize) + 1;
            parsedColl.updateMany(
                new Document("run_id", runId).append("log_date", date),
                new Document("$set", new Document("batch_id", batchId))
            );
        }
    }

    private List<Map<String, Object>> buildBatchSummaries() {

        // RECORDS MODE
        if ("records".equalsIgnoreCase(batchMode)) {
            List<Map<String, Object>> summaries = new ArrayList<>();

            for (int chunkId = 1; chunkId <= ingestChunks; chunkId++) {
                long recordsInChunk = parsedColl.countDocuments(
                    new Document("run_id", runId).append("ingest_chunk_id", chunkId)
                );
                long malformedInChunk = parsedColl.countDocuments(
                    new Document("run_id", runId)
                        .append("ingest_chunk_id", chunkId)
                        .append("malformed", true)
                );

                Map<String, Object> m = new LinkedHashMap<>();
                m.put("batch_id",         chunkId);
                // FIX (Bug 2, carried over): report actual records, not the configured batchSize.
                // The last chunk is almost always smaller than batchSize.
                m.put("batch_records",    recordsInChunk);
                m.put("batch_start_date", null);
                m.put("batch_end_date",   null);
                m.put("records_total",    recordsInChunk);
                m.put("malformed_records", malformedInChunk);
                summaries.add(m);
            }
            return summaries;
        }

        // DAYS MODE

        // Build per-batch malformed counts from MongoDB — the source of truth.
        // dateStats[1] cannot be used here because malformed records have a null
        // log_date and are never inserted into dateStats, causing every batch to
        // report malformed_records=0 even when the run-level total is non-zero.
        // A single aggregation is cheaper than N per-batch countDocuments calls.
        Map<Integer, Long> malformedPerBatch = new HashMap<>();
        parsedColl.aggregate(Arrays.asList(
            new Document("$match",  new Document("run_id", runId).append("malformed", true)),
            new Document("$group",  new Document("_id", "$batch_id")
                .append("count", new Document("$sum", 1)))
        )).into(new ArrayList<>()).forEach(d ->
            malformedPerBatch.put(d.getInteger("_id"), ((Number) d.get("count")).longValue())
        );

        List<String> dates =
            new ArrayList<>(dateStats.keySet());

        Collections.sort(dates);

        Map<Integer, Map<String, Object>> summaries =
            new LinkedHashMap<>();

        for (int i = 0; i < dates.size(); i++) {

            String date   = dates.get(i);
            int  batchId  = (i / daysBatchSize) + 1;

            Map<String, Object> summary =
                summaries.computeIfAbsent(batchId, id -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("batch_id",         id);
                    m.put("batch_days",        daysBatchSize);
                    m.put("batch_start_date",  date);
                    m.put("batch_end_date",    date);
                    m.put("records_total",     0L);
                    m.put("malformed_records", malformedPerBatch.getOrDefault(id, 0L));
                    return m;
                });

            summary.put("batch_end_date", date);

            long[] stats = dateStats.get(date);
            summary.put("records_total",
                (Long) summary.get("records_total") + stats[0]);
        }

        return new ArrayList<>(summaries.values());
    }

    private List<String> extractMonths() {
        List<String> dates = new ArrayList<>(dateStats.keySet());
        Collections.sort(dates);
        LinkedHashSet<String> months = new LinkedHashSet<>();
        for (String date : dates) {
            if (date.length() >= 7) {
                months.add(date.substring(0, 7));
            }
        }
        return new ArrayList<>(months);
    }

    private List<Map<String, Object>> runQuery1(String month) {
        List<Map<String, Object>> q1 = new ArrayList<>();
        List<Document> agg1 = parsedColl.aggregate(Arrays.asList(
            new Document("$match", baseMatch(month)),
            new Document("$group", new Document("_id",
                    new Document("log_date",    "$log_date")
                        .append("status_code",  "$status_code"))
                .append("request_count", new Document("$sum", 1))
                .append("total_bytes",   new Document("$sum", "$bytes")))
        )).into(new ArrayList<>());

        for (Document d : agg1) {
            Document id = (Document) d.get("_id");
            Map<String, Object> row = new HashMap<>();
            // FIX (Bug 4): batch_id is not meaningful for aggregation query results;
            // removed the hardcoded 0 to avoid misleading downstream consumers.
            row.put("log_date",      id.getString("log_date"));
            row.put("status_code",   id.getInteger("status_code"));
            row.put("request_count", ((Number) d.get("request_count")).longValue());
            row.put("total_bytes",   d.get("total_bytes"));
            q1.add(row);
        }
        return q1;
    }

    private List<Map<String, Object>> runQuery2(String month) {
        List<Map<String, Object>> q2 = new ArrayList<>();
        List<Document> agg2 = parsedColl.aggregate(Arrays.asList(
            new Document("$match", baseMatch(month)),
            new Document("$group", new Document("_id", "$resource_path")
                .append("request_count",      new Document("$sum", 1))
                .append("total_bytes",         new Document("$sum", "$bytes"))
                .append("distinct_host_count", new Document("$addToSet", "$host"))),
            new Document("$project", new Document("request_count", 1)
                .append("total_bytes", 1)
                .append("distinct_host_count", new Document("$size", "$distinct_host_count"))),
            new Document("$sort",  new Document("request_count", -1)),
            new Document("$limit", 20)
        )).into(new ArrayList<>());

        for (Document d : agg2) {
            Map<String, Object> row = new HashMap<>();
            // FIX (Bug 4): removed hardcoded batch_id = 0
            row.put("resource_path",       d.getString("_id"));
            row.put("request_count",       d.get("request_count"));
            row.put("total_bytes",         d.get("total_bytes"));
            row.put("distinct_host_count", d.get("distinct_host_count"));
            q2.add(row);
        }
        return q2;
    }

    private List<Map<String, Object>> runQuery3(String month) {
        List<Map<String, Object>> q3 = new ArrayList<>();
        List<Document> agg3 = parsedColl.aggregate(Arrays.asList(
            new Document("$match", baseMatch(month)),
            new Document("$group", new Document("_id",
                    new Document("log_date", "$log_date").append("log_hour", "$log_hour"))
                .append("total_request_count", new Document("$sum", 1))
                .append("error_request_count", new Document("$sum", new Document("$cond", Arrays.asList(
                    new Document("$and", Arrays.asList(
                        new Document("$gte", Arrays.asList("$status_code", 400)),
                        new Document("$lte", Arrays.asList("$status_code", 599))
                    )), 1, 0
                ))))
                .append("error_hosts", new Document("$addToSet", new Document("$cond", Arrays.asList(
                    new Document("$and", Arrays.asList(
                        new Document("$gte", Arrays.asList("$status_code", 400)),
                        new Document("$lte", Arrays.asList("$status_code", 599))
                    )), "$host", null
                ))))
            ),
            new Document("$project", new Document("log_date",  "$_id.log_date")
                .append("log_hour",             "$_id.log_hour")
                .append("error_request_count",  1)
                .append("total_request_count",  1)
                .append("distinct_error_hosts", new Document("$size",
                    new Document("$setDifference",
                        Arrays.asList("$error_hosts", java.util.Collections.singletonList(null)))))
                .append("error_rate", new Document("$cond", Arrays.asList(
                    new Document("$eq", Arrays.asList("$total_request_count", 0)),
                    0,
                    new Document("$divide", Arrays.asList("$error_request_count", "$total_request_count"))
                )))
            ),
            new Document("$sort", new Document("log_date", 1).append("log_hour", 1))
        )).into(new ArrayList<>());

        for (Document d : agg3) {
            Map<String, Object> row = new HashMap<>();
            // FIX (Bug 4): removed hardcoded batch_id = 0
            row.put("log_date",             d.getString("log_date"));
            row.put("log_hour",             d.getInteger("log_hour"));
            row.put("error_request_count",  d.get("error_request_count"));
            row.put("total_request_count",  d.get("total_request_count"));
            row.put("error_rate",           d.get("error_rate"));
            row.put("distinct_error_hosts", d.get("distinct_error_hosts"));
            q3.add(row);
        }
        return q3;
    }

    private Document baseMatch(String month) {
        Document match = new Document("run_id", runId).append("malformed", false);
        if (month != null && !month.isBlank()) {
            match.append("log_date", new Document("$regex", "^" + month));
        }
        return match;
    }

    private String scopedKey(QueryType type, String month) {
        return type.getKey() + "|month=" + month;
    }
}