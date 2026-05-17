package com.example.multietl.pipelines.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.multietl.parser.LogParser;
import com.example.multietl.parser.LogRecord;
import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.base.QueryType;

/**
 * Common in-memory pipeline implementation used by Pig, MapReduce, Hive, and other non-MongoDB backends.
 * Handles log parsing, aggregation, and query computation in memory.
 */
public class CommonPipeline implements Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(CommonPipeline.class);

    private final LogParser parser = new LogParser();
    private String runId;
    private long malformedCount = 0;
    private long processed = 0;
    private long rawLoaded = 0;
    private int ingestChunks = 0;
    private int batchSize = 1;
    private final List<String> rawLines = new ArrayList<>();
    private final List<LogRecord> allRecords = new ArrayList<>();
    private List<Map<String, Object>> batchSummaries = new ArrayList<>();

    @Override
    public void startRun(String runId, int batchSize) {
        this.runId = runId;
        this.batchSize = Math.max(1, batchSize);
        logger.info("CommonPipeline started run {} with batchSize={}", runId, this.batchSize);
    }

    @Override
    public void processBatch(List<String> rawLines, int chunkId) throws Exception {
        ingestChunks++;
        int batchProcessed = 0;
        int batchMalformed = 0;
        String firstDate = null;
        String lastDate = null;

        this.rawLines.addAll(rawLines);
        rawLoaded += rawLines.size();

        for (String line : rawLines) {
            LogParser.ParseResult r = parser.parse(line);
            allRecords.add(r.record);
            if (!r.success) {
                malformedCount++;
                batchMalformed++;
            } else {
                processed++;
                batchProcessed++;
            }
            String logDate = r.record.getLogDate();
            if (logDate != null) {
                if (firstDate == null || logDate.compareTo(firstDate) < 0) firstDate = logDate;
                if (lastDate == null || logDate.compareTo(lastDate) > 0) lastDate = logDate;
            }
        }

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("batch_id", chunkId);
        summary.put("batch_start_date", firstDate);
        summary.put("batch_end_date", lastDate);
        summary.put("batch_records", batchSize);
        summary.put("records_total", (long) rawLines.size());
        summary.put("malformed_records", (long) batchMalformed);
        batchSummaries.add(summary);

        logger.info("Processed ingest chunk {}: {} good records, {} malformed", chunkId, batchProcessed, batchMalformed);
    }

    @Override
    public Map<String, List<Map<String, Object>>> finalizeRun(QueryPlan plan) throws Exception {
        Map<String, List<Map<String, Object>>> results = new LinkedHashMap<>();

        // Filter out malformed records for aggregations
        List<LogRecord> goodRecords = allRecords.stream()
            .filter(r -> !r.isMalformed())
            .collect(Collectors.toList());

        if (plan.isSplitByMonth()) {
            Map<String, List<LogRecord>> byMonth = groupByMonth(goodRecords);
            for (Map.Entry<String, List<LogRecord>> entry : byMonth.entrySet()) {
                String month = entry.getKey();
                List<LogRecord> monthRecords = entry.getValue();
                if (plan.getQueries().contains(QueryType.DAILY_TRAFFIC_SUMMARY)) {
                    results.put(scopedKey(QueryType.DAILY_TRAFFIC_SUMMARY, month), computeQuery1(monthRecords));
                }
                if (plan.getQueries().contains(QueryType.TOP_RESOURCES)) {
                    results.put(scopedKey(QueryType.TOP_RESOURCES, month), computeQuery2(monthRecords));
                }
                if (plan.getQueries().contains(QueryType.HOURLY_ERROR_ANALYSIS)) {
                    results.put(scopedKey(QueryType.HOURLY_ERROR_ANALYSIS, month), computeQuery3(monthRecords));
                }
            }
        } else {
            if (plan.getQueries().contains(QueryType.DAILY_TRAFFIC_SUMMARY)) {
                results.put(QueryType.DAILY_TRAFFIC_SUMMARY.getKey(), computeQuery1(goodRecords));
            }
            if (plan.getQueries().contains(QueryType.TOP_RESOURCES)) {
                results.put(QueryType.TOP_RESOURCES.getKey(), computeQuery2(goodRecords));
            }
            if (plan.getQueries().contains(QueryType.HOURLY_ERROR_ANALYSIS)) {
                results.put(QueryType.HOURLY_ERROR_ANALYSIS.getKey(), computeQuery3(goodRecords));
            }
        }

        return results;
    }

    /**
     * Query 1: Group by log_date and status_code, count requests and sum bytes.
     */
    private List<Map<String, Object>> computeQuery1(List<LogRecord> records) {
        Map<String, Map<String, Object>> agg = new LinkedHashMap<>();
        
        for (LogRecord r : records) {
            String key = r.getLogDate() + "|" + r.getStatusCode();
            Map<String, Object> row = agg.computeIfAbsent(key, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("batch_id", 0);
                m.put("log_date", r.getLogDate());
                m.put("status_code", r.getStatusCode());
                m.put("request_count", 0L);
                m.put("total_bytes", 0L);
                return m;
            });
            row.put("request_count", (Long) row.get("request_count") + 1);
            row.put("total_bytes", (Long) row.get("total_bytes") + r.getBytes());
        }
        
        return new ArrayList<>(agg.values());
    }

    /**
     * Query 2: Group by resource_path, count requests, sum bytes, count distinct hosts, then sort by count desc and limit 20.
     */
    private List<Map<String, Object>> computeQuery2(List<LogRecord> records) {
        Map<String, Map<String, Object>> agg = new LinkedHashMap<>();
        
        for (LogRecord r : records) {
            String resource = r.getResourcePath();
            Map<String, Object> row = agg.computeIfAbsent(resource, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("batch_id", 0);
                m.put("resource_path", resource);
                m.put("request_count", 0L);
                m.put("total_bytes", 0L);
                m.put("distinct_hosts", new HashSet<String>());
                return m;
            });
            row.put("request_count", (Long) row.get("request_count") + 1);
            row.put("total_bytes", (Long) row.get("total_bytes") + r.getBytes());
            @SuppressWarnings("unchecked")
            Set<String> hosts = (Set<String>) row.get("distinct_hosts");
            hosts.add(r.getHost());
        }
        
        // Convert and limit to top 20
        return agg.values().stream()
            .map(row -> {
                @SuppressWarnings("unchecked")
                Set<String> hosts = (Set<String>) row.remove("distinct_hosts");
                row.put("distinct_host_count", (long) hosts.size());
                return row;
            })
            .sorted((a, b) -> Long.compare((Long) b.get("request_count"), (Long) a.get("request_count")))
            .limit(20)
            .collect(Collectors.toList());
    }

    /**
     * Query 3: Group by log_date and log_hour, compute error metrics including distinct error hosts.
     */
    private List<Map<String, Object>> computeQuery3(List<LogRecord> records) {
        Map<String, Map<String, Object>> agg = new LinkedHashMap<>();
        
        for (LogRecord r : records) {
            String key = r.getLogDate() + "|" + r.getLogHour();
            Map<String, Object> row = agg.computeIfAbsent(key, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("batch_id", 0);
                m.put("log_date", r.getLogDate());
                m.put("log_hour", r.getLogHour());
                m.put("total_request_count", 0L);
                m.put("error_request_count", 0L);
                m.put("error_hosts", new HashSet<String>());
                return m;
            });
            
            row.put("total_request_count", (Long) row.get("total_request_count") + 1);
            
            boolean isError = r.getStatusCode() >= 400 && r.getStatusCode() <= 599;
            if (isError) {
                row.put("error_request_count", (Long) row.get("error_request_count") + 1);
                @SuppressWarnings("unchecked")
                Set<String> errorHosts = (Set<String>) row.get("error_hosts");
                errorHosts.add(r.getHost());
            }
        }
        
        // Compute error_rate and distinct_error_hosts, then sort
        return agg.values().stream()
            .map(row -> {
                long totalRequests = (Long) row.get("total_request_count");
                long errorRequests = (Long) row.get("error_request_count");
                double errorRate = totalRequests == 0 ? 0.0 : (double) errorRequests / totalRequests;
                row.put("error_rate", errorRate);
                
                @SuppressWarnings("unchecked")
                Set<String> errorHosts = (Set<String>) row.remove("error_hosts");
                row.put("distinct_error_hosts", (long) errorHosts.size());
                
                return row;
            })
            .sorted((a, b) -> {
                String dateA = (String) a.get("log_date");
                String dateB = (String) b.get("log_date");
                int dateCompare = dateA.compareTo(dateB);
                if (dateCompare != 0) return dateCompare;
                int hourA = (Integer) a.get("log_hour");
                int hourB = (Integer) b.get("log_hour");
                return Integer.compare(hourA, hourB);
            })
            .collect(Collectors.toList());
    }

    @Override
    public void shutdown() {
        allRecords.clear();
        rawLines.clear();
        batchSummaries = new ArrayList<>();
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> m = new HashMap<>();
        m.put("processed", processed);
        m.put("malformed", malformedCount);
        m.put("total_records", processed + malformedCount);
        m.put("total_batches", ingestChunks);
        m.put("raw_loaded", rawLoaded);
        m.put("ingest_chunks", ingestChunks);
        return m;
    }

    @Override
    public List<Map<String, Object>> getBatchSummaries() {
        return batchSummaries;
    }

    private Map<String, List<LogRecord>> groupByMonth(List<LogRecord> records) {
        Map<String, List<LogRecord>> byMonth = new LinkedHashMap<>();
        for (LogRecord r : records) {
            String logDate = r.getLogDate();
            if (logDate == null || logDate.length() < 7) {
                continue;
            }
            String month = logDate.substring(0, 7);
            byMonth.computeIfAbsent(month, k -> new ArrayList<>()).add(r);
        }
        return byMonth;
    }

    private String scopedKey(QueryType type, String month) {
        return type.getKey() + "|month=" + month;
    }
}
