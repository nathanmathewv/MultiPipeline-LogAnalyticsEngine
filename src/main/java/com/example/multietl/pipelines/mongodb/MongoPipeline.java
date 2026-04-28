package com.example.multietl.pipelines.mongodb;

import com.example.multietl.parser.LogParser;
import com.example.multietl.parser.LogRecord;
import com.example.multietl.pipelines.base.Pipeline;
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
    private MongoCollection<Document> parsedColl;
    private String runId;

    private final LogParser parser = new LogParser();
    private long malformedCount = 0;
    private long processed = 0;
    private int batchCount = 0;

    public MongoPipeline(String uri, String dbName) {
        this.uri = uri;
        this.dbName = dbName;
    }

    @Override
    public void startRun(String runId) {
        this.runId = runId;
        client = MongoClients.create(uri);
        db = client.getDatabase(dbName);
        parsedColl = db.getCollection("parsed_logs");
        // ensure indexes
        parsedColl.createIndex(new Document("log_date", 1));
        parsedColl.createIndex(new Document("resource_path", 1));
        parsedColl.createIndex(new Document("status_code", 1));
        logger.info("MongoPipeline started run {}", runId);
    }

    @Override
    public void processBatch(List<String> rawLines, int batchId) {
        batchCount++;
        List<Document> docs = new ArrayList<>();
        for (String line : rawLines) {
            LogParser.ParseResult r = parser.parse(line);
            if (!r.success) {
                malformedCount++;
                docs.add(r.record.toDocument(runId, batchId));
            } else {
                docs.add(r.record.toDocument(runId, batchId));
                processed++;
            }
        }
        if (!docs.isEmpty()) {
            parsedColl.insertMany(docs);
        }
        logger.info("Processed batch {}: inserted {} docs (malformed so far={})", batchId, docs.size(), malformedCount);
    }

    @Override
    public Map<String, List<Map<String, Object>>> finalizeRun() {
        // Run aggregations for three queries.
        Map<String, List<Map<String, Object>>> results = new HashMap<>();

        // Query 1: Daily Traffic Summary (log_date, status_code)
        List<Map<String, Object>> q1 = new ArrayList<>();
        List<Document> agg1 = parsedColl.aggregate(Arrays.asList(
            new Document("$match", new Document("malformed", false)),
            new Document("$group", new Document("_id", new Document("log_date", "$log_date").append("status_code", "$status_code"))
                .append("request_count", new Document("$sum", 1))
                .append("total_bytes", new Document("$sum", "$bytes")))
        )).into(new ArrayList<>());
        for (Document d : agg1) {
            Document id = (Document) d.get("_id");
            Map<String, Object> row = new HashMap<>();
            row.put("batch_id", 0);
            row.put("log_date", id.getString("log_date"));
            row.put("status_code", id.getInteger("status_code"));
            row.put("request_count", d.getLong("request_count"));
            row.put("total_bytes", d.get("total_bytes"));
            q1.add(row);
        }
        results.put("daily_traffic_summary", q1);

        // Query 2: Top 20 Requested Resources
        List<Map<String, Object>> q2 = new ArrayList<>();
        List<Document> agg2 = parsedColl.aggregate(Arrays.asList(
            new Document("$match", new Document("malformed", false)),
            new Document("$group", new Document("_id", "$resource_path")
                .append("request_count", new Document("$sum", 1))
                .append("total_bytes", new Document("$sum", "$bytes"))
                .append("distinct_host_count", new Document("$addToSet", "$host"))),
            new Document("$project", new Document("request_count", 1)
                .append("total_bytes", 1)
                .append("distinct_host_count", new Document("$size", "$distinct_host_count"))),
            new Document("$sort", new Document("request_count", -1)),
            new Document("$limit", 20)
        )).into(new ArrayList<>());
        for (Document d : agg2) {
            Map<String, Object> row = new HashMap<>();
            row.put("batch_id", 0);
            row.put("resource_path", d.getString("_id"));
            row.put("request_count", d.get("request_count"));
            row.put("total_bytes", d.get("total_bytes"));
            row.put("distinct_host_count", d.get("distinct_host_count"));
            q2.add(row);
        }
        results.put("top_resources", q2);

        // Query 3: Hourly Error Analysis
        List<Map<String, Object>> q3 = new ArrayList<>();
        List<Document> agg3 = parsedColl.aggregate(Arrays.asList(
            new Document("$match", new Document("malformed", false)),
            new Document("$group", new Document("_id", new Document("log_date", "$log_date").append("log_hour", "$log_hour"))
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
            new Document("$project", new Document("log_date", "$_id.log_date")
                .append("log_hour", "$_id.log_hour")
                .append("error_request_count", 1)
                .append("total_request_count", 1)
                .append("distinct_error_hosts", new Document("$size", new Document("$setDifference", Arrays.asList("$error_hosts", Arrays.asList(null)))))
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
            row.put("batch_id", 0);
            row.put("log_date", d.getString("log_date"));
            row.put("log_hour", d.getInteger("log_hour"));
            row.put("error_request_count", d.get("error_request_count"));
            row.put("total_request_count", d.get("total_request_count"));
            row.put("error_rate", d.get("error_rate"));
            row.put("distinct_error_hosts", d.get("distinct_error_hosts"));
            q3.add(row);
        }
        results.put("hourly_error_analysis", q3);

        logger.info("Finalized run {}: processed={}, malformed={}", runId, processed, malformedCount);
        return results;
    }

    @Override
    public void shutdown() {
        if (client != null) client.close();
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> m = new HashMap<>();
        m.put("processed", processed);
        m.put("malformed", malformedCount);
        m.put("total_records", processed + malformedCount);
        m.put("total_batches", batchCount);
        return m;
    }
}
