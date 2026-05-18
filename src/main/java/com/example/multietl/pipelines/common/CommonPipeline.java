package com.example.multietl.pipelines.common;

import com.example.multietl.parser.LogParser;
import com.example.multietl.parser.LogRecord;
import com.example.multietl.pipelines.base.BatchConfig;
import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.base.QueryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Common in-memory pipeline implementation used by the local Hive adapter and other non-MongoDB backends.
 * Handles log parsing, aggregation, and query computation in memory.
 */
public class CommonPipeline implements Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(CommonPipeline.class);

    private final LogParser parser = new LogParser();
    private String runId;
    private long malformedCount = 0;
    private long processed = 0;
    private long rawLoaded = 0;
    private long recordSequence = 0;
    private int ingestChunks = 0;
    private BatchConfig batchConfig = BatchConfig.days(1);
    private final List<String> rawLines = new ArrayList<>();
    private final List<BatchRecord> allRecords = new ArrayList<>();
    private final Map<String, long[]> dateStats = new HashMap<>();
    private List<Map<String, Object>> batchSummaries = new ArrayList<>();

    @Override
    public void startRun(String runId, BatchConfig batchConfig) {
        this.runId = runId;
        this.batchConfig = batchConfig == null ? BatchConfig.days(1) : batchConfig;
        malformedCount = 0;
        processed = 0;
        rawLoaded = 0;
        recordSequence = 0;
        ingestChunks = 0;
        rawLines.clear();
        allRecords.clear();
        dateStats.clear();
        batchSummaries = new ArrayList<>();
        logger.info("CommonPipeline started run {} with batchConfig={}", runId, this.batchConfig.describe());
    }

    @Override
    public void processBatch(List<String> rawLines, int chunkId) throws Exception {
        ingestChunks++;
        int batchProcessed = 0;
        int batchMalformed = 0;

        this.rawLines.addAll(rawLines);
        rawLoaded += rawLines.size();

        for (String line : rawLines) {
            LogParser.ParseResult r = parser.parse(line);
            long currentSequence = ++recordSequence;
            int batchId = batchConfig.isRecords() ? batchIdForRecord(currentSequence) : 0;
            allRecords.add(new BatchRecord(r.record, batchId, currentSequence));
            if (!r.success) {
                malformedCount++;
                batchMalformed++;
            } else {
                processed++;
                batchProcessed++;
            }
            String logDate = r.record.getLogDate();
            if (logDate != null) {
                long[] stats = dateStats.computeIfAbsent(logDate, k -> new long[] {0L, 0L});
                stats[0]++;
                if (!r.success) {
                    stats[1]++;
                }
            }
        }
        logger.info("Processed ingest chunk {}: {} good records, {} malformed", chunkId, batchProcessed, batchMalformed);
    }

    @Override
    public Map<String, List<Map<String, Object>>> finalizeRun(QueryPlan plan) throws Exception {
        Map<String, List<Map<String, Object>>> results = new LinkedHashMap<>();
        if (batchConfig.isDays()) {
            assignDayBatchIds();
        }

        // Filter out malformed records for aggregations
        List<BatchRecord> goodRecords = allRecords.stream()
            .filter(r -> !r.record.isMalformed())
            .collect(Collectors.toList());

        batchSummaries = buildBatchSummaries();

        if (plan.isSplitByMonth()) {
            Map<String, List<BatchRecord>> byMonth = groupByMonth(goodRecords);
            for (Map.Entry<String, List<BatchRecord>> entry : byMonth.entrySet()) {
                String month = entry.getKey();
                List<BatchRecord> monthRecords = entry.getValue();
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
    private List<Map<String, Object>> computeQuery1(List<BatchRecord> records) {
        Map<String, Map<String, Object>> agg = new LinkedHashMap<>();

        for (BatchRecord br : records) {
            LogRecord r = br.record;
            String key = br.batchId + "|" + r.getLogDate() + "|" + r.getStatusCode();
            Map<String, Object> row = agg.computeIfAbsent(key, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("batch_id", br.batchId);
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
    private List<Map<String, Object>> computeQuery2(List<BatchRecord> records) {
        Map<String, Map<String, Object>> agg = new LinkedHashMap<>();

        for (BatchRecord br : records) {
            LogRecord r = br.record;
            String resource = r.getResourcePath();
            String key = br.batchId + "|" + resource;
            Map<String, Object> row = agg.computeIfAbsent(key, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("batch_id", br.batchId);
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
        
        List<Map<String, Object>> rows = agg.values().stream()
            .map(row -> {
                @SuppressWarnings("unchecked")
                Set<String> hosts = (Set<String>) row.remove("distinct_hosts");
                row.put("distinct_host_count", (long) hosts.size());
                return row;
            })
            .sorted((a, b) -> {
                int batchA = ((Number) a.get("batch_id")).intValue();
                int batchB = ((Number) b.get("batch_id")).intValue();
                int batchCompare = Integer.compare(batchA, batchB);
                if (batchCompare != 0) return batchCompare;
                int countCompare = Long.compare((Long) b.get("request_count"), (Long) a.get("request_count"));
                if (countCompare != 0) return countCompare;
                return String.valueOf(a.get("resource_path")).compareTo(String.valueOf(b.get("resource_path")));
            })
            .collect(Collectors.toList());

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

    /**
     * Query 3: Group by log_date and log_hour, compute error metrics including distinct error hosts.
     */
    private List<Map<String, Object>> computeQuery3(List<BatchRecord> records) {
        Map<String, Map<String, Object>> agg = new LinkedHashMap<>();

        for (BatchRecord br : records) {
            LogRecord r = br.record;
            String key = br.batchId + "|" + r.getLogDate() + "|" + r.getLogHour();
            Map<String, Object> row = agg.computeIfAbsent(key, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("batch_id", br.batchId);
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
                int batchA = ((Number) a.get("batch_id")).intValue();
                int batchB = ((Number) b.get("batch_id")).intValue();
                int batchCompare = Integer.compare(batchA, batchB);
                if (batchCompare != 0) return batchCompare;
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

    private List<Map<String, Object>> buildBatchSummaries() {
        if (batchConfig.isRecords()) {
            return buildRecordBatchSummaries();
        }
        return buildRecordBatchSummaries();
    }

    private List<Map<String, Object>> buildRecordBatchSummaries() {
        Map<Integer, Map<String, Object>> summaries = new LinkedHashMap<>();
        for (BatchRecord br : allRecords) {
            Map<String, Object> summary = summaries.computeIfAbsent(br.batchId, id -> {
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
            if (br.record.isMalformed()) {
                summary.put("malformed_records", (Long) summary.get("malformed_records") + 1);
            }
            String logDate = br.record.getLogDate();
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
        return new ArrayList<>(summaries.values());
    }

    private void assignDayBatchIds() {
        List<String> dates = new ArrayList<>(dateStats.keySet());
        Collections.sort(dates);
        Map<String, Integer> dateToBatch = new HashMap<>();
        for (int i = 0; i < dates.size(); i++) {
            dateToBatch.put(dates.get(i), (i / batchConfig.getSize()) + 1);
        }
        int fallbackBatchId = allRecords.isEmpty() ? 0 : 1;
        int currentBatchId = fallbackBatchId;
        for (BatchRecord br : allRecords) {
            String logDate = br.record.getLogDate();
            if (logDate != null) {
                currentBatchId = dateToBatch.getOrDefault(logDate, currentBatchId);
                br.batchId = currentBatchId;
            } else {
                br.batchId = currentBatchId;
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

    private Map<String, List<BatchRecord>> groupByMonth(List<BatchRecord> records) {
        Map<String, List<BatchRecord>> byMonth = new LinkedHashMap<>();
        for (BatchRecord br : records) {
            LogRecord r = br.record;
            String logDate = r.getLogDate();
            if (logDate == null || logDate.length() < 7) {
                continue;
            }
            String month = logDate.substring(0, 7);
            byMonth.computeIfAbsent(month, k -> new ArrayList<>()).add(br);
        }
        return byMonth;
    }

    private String scopedKey(QueryType type, String month) {
        return type.getKey() + "|month=" + month;
    }

    private static class BatchRecord {
        private final LogRecord record;
        private final long sequence;
        private int batchId;

        private BatchRecord(LogRecord record, int batchId, long sequence) {
            this.record = record;
            this.batchId = batchId;
            this.sequence = sequence;
        }
    }
}
