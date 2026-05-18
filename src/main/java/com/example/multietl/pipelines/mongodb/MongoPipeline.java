package com.example.multietl.pipelines.mongodb;

import com.example.multietl.parser.LogParser;
import com.example.multietl.parser.LogRecord;
import com.example.multietl.pipelines.base.BatchConfig;
import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.base.QueryType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MongoPipeline implements Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(MongoPipeline.class);

    private final String uri;
    private final String dbName;

    private MongoClient client;
    private MongoDatabase db;
    private MongoCollection<Document> rawColl;
    private MongoCollection<Document> parsedColl;
    private String runId;
    private BatchConfig batchConfig = BatchConfig.days(1);

    private final LogParser parser = new LogParser();
    private long malformedCount = 0;
    private long processed = 0;
    private long rawLoaded = 0;
    private long recordSequence = 0;
    private int ingestChunks = 0;
    private final Map<String, long[]> dateStats = new HashMap<>();
    private final Map<Integer, long[]> dayNoDateBatchStats = new LinkedHashMap<>();
    private final Map<Integer, Map<String, Object>> recordBatchSummaries = new LinkedHashMap<>();
    private List<Map<String, Object>> batchSummaries = new ArrayList<>();

    public MongoPipeline(String uri, String dbName) {
        this.uri = uri;
        this.dbName = dbName;
    }

    @Override
    public void startRun(String runId, BatchConfig batchConfig) {
        this.runId = runId;
        this.batchConfig = batchConfig == null ? BatchConfig.days(1) : batchConfig;
        malformedCount = 0;
        processed = 0;
        rawLoaded = 0;
        recordSequence = 0;
        ingestChunks = 0;
        dateStats.clear();
        dayNoDateBatchStats.clear();
        recordBatchSummaries.clear();
        batchSummaries = new ArrayList<>();
        client = MongoClients.create(uri);
        db = client.getDatabase(dbName);
        rawColl = db.getCollection("raw_logs");
        parsedColl = db.getCollection("parsed_logs");
        // ensure indexes
        parsedColl.createIndex(new Document("run_id", 1).append("batch_id", 1));
        parsedColl.createIndex(new Document("log_date", 1));
        parsedColl.createIndex(new Document("resource_path", 1));
        parsedColl.createIndex(new Document("status_code", 1));
        logger.info("MongoPipeline started run {} with batchConfig={}", runId, this.batchConfig.describe());
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
            long currentSequence = ++recordSequence;
            int batchId = batchConfig.isRecords() ? batchIdForRecord(currentSequence) : 0;
            Document d = new Document();
            d.append("host", record.getHost());
            d.append("timestamp", record.getTimestamp() == null ? null : record.getTimestamp().toString());
            d.append("log_date", record.getLogDate());
            d.append("log_hour", record.getLogHour());
            d.append("method", record.getMethod());
            d.append("resource_path", record.getResourcePath());
            d.append("protocol", record.getProtocol());
            d.append("status_code", record.getStatusCode());
            d.append("bytes", record.getBytes());
            d.append("malformed", record.isMalformed());
            d.append("raw_line", record.getRawLine());
            d.append("run_id", runId);
            d.append("batch_id", batchId);
            d.append("record_seq", currentSequence);
            d.append("ingest_chunk_id", chunkId);
            parsedDocs.add(d);

            if (!r.success) {
                malformedCount++;
            } else {
                processed++;
            }

            String logDate = record.getLogDate();
            if (logDate != null) {
                long[] stats = dateStats.computeIfAbsent(logDate, k -> new long[] {0L, 0L});
                stats[0]++;
                if (!r.success) {
                    stats[1]++;
                }
            }
            if (batchConfig.isRecords()) {
                addRecordBatchSummary(batchId, record);
            }
        }
        if (!parsedDocs.isEmpty()) {
            parsedColl.insertMany(parsedDocs);
        }
        logger.info("Processed ingest chunk {}: inserted {} parsed docs (malformed so far={})", chunkId, parsedDocs.size(), malformedCount);
    }

    @Override
    public Map<String, List<Map<String, Object>>> finalizeRun(QueryPlan plan) {
        if (batchConfig.isDays()) {
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
        m.put("processed", processed);
        m.put("malformed", malformedCount);
        m.put("total_records", processed + malformedCount);
        m.put("total_batches", computeBatchCount());
        m.put("batch_mode", batchConfig.getModeKey());
        m.put("batch_size", batchConfig.getSize());
        m.put("raw_loaded", rawLoaded);
        m.put("ingest_chunks", ingestChunks);
        return m;
    }

    @Override
    public List<Map<String, Object>> getBatchSummaries() {
        return batchSummaries;
    }

    private int computeBatchCount() {
        if (batchConfig.isRecords()) {
            long totalRecords = processed + malformedCount;
            return totalRecords == 0 ? 0 : (int) Math.ceil((double) totalRecords / batchConfig.getSize());
        }
        int dateCount = dateStats.size();
        if (dateCount == 0) {
            return (processed + malformedCount) == 0 ? 0 : 1;
        }
        return (int) Math.ceil((double) dateCount / batchConfig.getSize());
    }

    private void assignBatchIdsByDate() {
        List<String> dates = new ArrayList<>(dateStats.keySet());
        Collections.sort(dates);
        Map<String, Integer> dateToBatch = new HashMap<>();
        for (int i = 0; i < dates.size(); i++) {
            String date = dates.get(i);
            int batchId = (i / batchConfig.getSize()) + 1;
            dateToBatch.put(date, batchId);
            parsedColl.updateMany(new Document("run_id", runId).append("log_date", date),
                new Document("$set", new Document("batch_id", batchId)));
        }

        dayNoDateBatchStats.clear();
        int fallbackBatchId = (processed + malformedCount) == 0 ? 0 : 1;
        int currentBatchId = fallbackBatchId;
        for (Document doc : parsedColl.find(new Document("run_id", runId))
            .projection(new Document("_id", 1).append("log_date", 1).append("malformed", 1))
            .sort(new Document("record_seq", 1))) {
            String logDate = doc.getString("log_date");
            if (logDate != null) {
                currentBatchId = dateToBatch.getOrDefault(logDate, currentBatchId);
                continue;
            }
            if (currentBatchId <= 0) {
                continue;
            }
            parsedColl.updateOne(new Document("_id", doc.get("_id")),
                new Document("$set", new Document("batch_id", currentBatchId)));
            long[] stats = dayNoDateBatchStats.computeIfAbsent(currentBatchId, id -> new long[] {0L, 0L});
            stats[0]++;
            if (Boolean.TRUE.equals(doc.getBoolean("malformed"))) {
                stats[1]++;
            }
        }
    }

    private List<Map<String, Object>> buildBatchSummaries() {
        if (batchConfig.isRecords()) {
            return new ArrayList<>(recordBatchSummaries.values());
        }
        List<String> dates = new ArrayList<>(dateStats.keySet());
        Collections.sort(dates);
        Map<Integer, Map<String, Object>> summaries = new LinkedHashMap<>();
        for (int i = 0; i < dates.size(); i++) {
            String date = dates.get(i);
            int batchId = (i / batchConfig.getSize()) + 1;
            Map<String, Object> summary = summaries.computeIfAbsent(batchId, id -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("batch_id", id);
                m.put("batch_start_date", date);
                m.put("batch_end_date", date);
                putBatchConfig(m);
                m.put("records_total", 0L);
                m.put("malformed_records", 0L);
                return m;
            });
            summary.put("batch_end_date", date);
            long[] stats = dateStats.getOrDefault(date, new long[] {0L, 0L});
            summary.put("records_total", (Long) summary.get("records_total") + stats[0]);
            summary.put("malformed_records", (Long) summary.get("malformed_records") + stats[1]);
        }
        for (Map.Entry<Integer, long[]> entry : dayNoDateBatchStats.entrySet()) {
            int batchId = entry.getKey();
            long[] stats = entry.getValue();
            Map<String, Object> summary = summaries.computeIfAbsent(batchId, id -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("batch_id", id);
                m.put("batch_start_date", null);
                m.put("batch_end_date", null);
                putBatchConfig(m);
                m.put("records_total", 0L);
                m.put("malformed_records", 0L);
                return m;
            });
            summary.put("records_total", (Long) summary.get("records_total") + stats[0]);
            summary.put("malformed_records", (Long) summary.get("malformed_records") + stats[1]);
        }
        return new ArrayList<>(summaries.values());
    }

    private void addRecordBatchSummary(int batchId, LogRecord record) {
        Map<String, Object> summary = recordBatchSummaries.computeIfAbsent(batchId, id -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("batch_id", id);
            m.put("batch_start_date", null);
            m.put("batch_end_date", null);
            putBatchConfig(m);
            m.put("records_total", 0L);
            m.put("malformed_records", 0L);
            return m;
        });
        summary.put("records_total", (Long) summary.get("records_total") + 1);
        if (record.isMalformed()) {
            summary.put("malformed_records", (Long) summary.get("malformed_records") + 1);
        }
        String logDate = record.getLogDate();
        if (logDate != null) {
            Object start = summary.get("batch_start_date");
            Object end = summary.get("batch_end_date");
            if (start == null || logDate.compareTo((String) start) < 0) {
                summary.put("batch_start_date", logDate);
            }
            if (end == null || logDate.compareTo((String) end) > 0) {
                summary.put("batch_end_date", logDate);
            }
        }
    }

    private int batchIdForRecord(long sequence) {
        return (int) (((sequence - 1) / batchConfig.getSize()) + 1);
    }

    private void putBatchConfig(Map<String, Object> m) {
        m.put("batch_mode", batchConfig.getModeKey());
        m.put("batch_size", batchConfig.getSize());
        m.put("batch_size_days", batchConfig.getBatchSizeDays());
        m.put("batch_size_records", batchConfig.getBatchSizeRecords());
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
            new Document("$group", new Document("_id", new Document("batch_id", "$batch_id")
                    .append("log_date", "$log_date")
                    .append("status_code", "$status_code"))
                .append("request_count", new Document("$sum", 1))
                .append("total_bytes", new Document("$sum", "$bytes"))),
            new Document("$sort", new Document("_id.batch_id", 1).append("_id.log_date", 1).append("_id.status_code", 1))
        )).into(new ArrayList<>());
        for (Document d : agg1) {
            Document id = (Document) d.get("_id");
            Map<String, Object> row = new HashMap<>();
            row.put("batch_id", id.getInteger("batch_id", 0));
            row.put("log_date", id.getString("log_date"));
            row.put("status_code", id.getInteger("status_code"));
            row.put("request_count", ((Number) d.get("request_count")).longValue());
            row.put("total_bytes", d.get("total_bytes"));
            q1.add(row);
        }
        return q1;
    }

    private List<Map<String, Object>> runQuery2(String month) {
        List<Map<String, Object>> q2 = new ArrayList<>();
        List<Document> agg2 = parsedColl.aggregate(Arrays.asList(
            new Document("$match", baseMatch(month)),
            new Document("$group", new Document("_id", new Document("batch_id", "$batch_id")
                    .append("resource_path", "$resource_path"))
                .append("request_count", new Document("$sum", 1))
                .append("total_bytes", new Document("$sum", "$bytes"))
                .append("distinct_host_count", new Document("$addToSet", "$host"))),
            new Document("$project", new Document("request_count", 1)
                .append("total_bytes", 1)
                .append("distinct_host_count", new Document("$size", "$distinct_host_count")))
        )).into(new ArrayList<>());
        for (Document d : agg2) {
            Document id = (Document) d.get("_id");
            Map<String, Object> row = new HashMap<>();
            row.put("batch_id", id.getInteger("batch_id", 0));
            row.put("resource_path", id.getString("resource_path"));
            row.put("request_count", d.get("request_count"));
            row.put("total_bytes", d.get("total_bytes"));
            row.put("distinct_host_count", d.get("distinct_host_count"));
            q2.add(row);
        }
        return topResourcesPerBatch(q2);
    }

    private List<Map<String, Object>> runQuery3(String month) {
        List<Map<String, Object>> q3 = new ArrayList<>();
        List<Document> agg3 = parsedColl.aggregate(Arrays.asList(
            new Document("$match", baseMatch(month)),
            new Document("$group", new Document("_id", new Document("batch_id", "$batch_id")
                    .append("log_date", "$log_date")
                    .append("log_hour", "$log_hour"))
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
            new Document("$project", new Document("batch_id", "$_id.batch_id")
                .append("log_date", "$_id.log_date")
                .append("log_hour", "$_id.log_hour")
                .append("error_request_count", 1)
                .append("total_request_count", 1)
                .append("distinct_error_hosts", new Document("$size", new Document("$setDifference", Arrays.asList("$error_hosts", java.util.Collections.singletonList(null)))))
                .append("error_rate", new Document("$cond", Arrays.asList(
                    new Document("$eq", Arrays.asList("$total_request_count", 0)),
                    0,
                    new Document("$divide", Arrays.asList("$error_request_count", "$total_request_count"))
                )))
            ),
            new Document("$sort", new Document("batch_id", 1).append("log_date", 1).append("log_hour", 1))
        )).into(new ArrayList<>());
        for (Document d : agg3) {
            Document id = (Document) d.get("_id");
            Map<String, Object> row = new HashMap<>();
            row.put("batch_id", id.getInteger("batch_id", 0));
            row.put("log_date", d.getString("log_date"));
            row.put("log_hour", d.getInteger("log_hour"));
            row.put("error_request_count", d.get("error_request_count"));
            row.put("total_request_count", d.get("total_request_count"));
            row.put("error_rate", d.get("error_rate"));
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

    private List<Map<String, Object>> topResourcesPerBatch(List<Map<String, Object>> rows) {
        rows.sort((a, b) -> {
            int batchA = ((Number) a.get("batch_id")).intValue();
            int batchB = ((Number) b.get("batch_id")).intValue();
            int batchCompare = Integer.compare(batchA, batchB);
            if (batchCompare != 0) return batchCompare;
            long countA = ((Number) a.get("request_count")).longValue();
            long countB = ((Number) b.get("request_count")).longValue();
            int countCompare = Long.compare(countB, countA);
            if (countCompare != 0) return countCompare;
            return String.valueOf(a.get("resource_path")).compareTo(String.valueOf(b.get("resource_path")));
        });

        Map<Integer, Integer> rowsPerBatch = new HashMap<>();
        List<Map<String, Object>> topRows = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            int batchId = ((Number) row.get("batch_id")).intValue();
            int currentCount = rowsPerBatch.getOrDefault(batchId, 0);
            if (currentCount < 20) {
                topRows.add(row);
                rowsPerBatch.put(batchId, currentCount + 1);
            }
        }
        return topRows;
    }
}
