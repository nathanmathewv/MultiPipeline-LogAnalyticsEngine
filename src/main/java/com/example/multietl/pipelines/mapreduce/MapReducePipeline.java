package com.example.multietl.pipelines.mapreduce;

import com.example.multietl.pipelines.base.BatchConfig;
import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.base.QueryType;
import com.example.multietl.pipelines.mapreduce.jobs.BatchMetadataJob;
import com.example.multietl.pipelines.mapreduce.jobs.DailyTrafficSummaryJob;
import com.example.multietl.pipelines.mapreduce.jobs.DayBatchAssignmentJob;
import com.example.multietl.pipelines.mapreduce.jobs.HourlyErrorAnalysisJob;
import com.example.multietl.pipelines.mapreduce.jobs.MalformedSummaryJob;
import com.example.multietl.pipelines.mapreduce.jobs.MapReduceLogRecord;
import com.example.multietl.pipelines.mapreduce.jobs.ParseLogsJob;
import com.example.multietl.pipelines.mapreduce.jobs.SequencedLogLine;
import com.example.multietl.pipelines.mapreduce.jobs.TopResourcesJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * MapReduce pipeline implementation.
 *
 * This local implementation runs explicit MapReduce-style jobs in process:
 * mapper output is grouped by key and then reduced. That keeps the CLI runnable
 * without a Hadoop cluster while preserving the same job boundaries expected in
 * a Hadoop deployment: parse/clean, batch assignment, metadata, and one job per
 * analytical query.
 */
public class MapReducePipeline implements Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(MapReducePipeline.class);

    private String runId;
    private BatchConfig batchConfig = BatchConfig.days(1);
    private final List<MapReduceLogRecord> records = new ArrayList<>();
    private List<Map<String, Object>> batchSummaries = new ArrayList<>();
    private long rawLoaded = 0;
    private long processed = 0;
    private long malformed = 0;
    private long recordSequence = 0;
    private int ingestChunks = 0;

    @Override
    public void startRun(String runId, BatchConfig batchConfig) {
        this.runId = runId;
        this.batchConfig = batchConfig == null ? BatchConfig.days(1) : batchConfig;
        records.clear();
        batchSummaries = new ArrayList<>();
        rawLoaded = 0;
        processed = 0;
        malformed = 0;
        recordSequence = 0;
        ingestChunks = 0;
        logger.info("MapReducePipeline started run {} with batchConfig={}", runId, this.batchConfig.describe());
    }

    @Override
    public void processBatch(List<String> rawLines, int chunkId) {
        ingestChunks++;
        rawLoaded += rawLines.size();

        List<SequencedLogLine> sequencedLines = new ArrayList<>(rawLines.size());
        for (String rawLine : rawLines) {
            sequencedLines.add(new SequencedLogLine(++recordSequence, rawLine, chunkId));
        }

        List<MapReduceLogRecord> parsedChunk = new ParseLogsJob(runId, batchConfig).run(sequencedLines);
        for (MapReduceLogRecord parsed : parsedChunk) {
            records.add(parsed);
            if (parsed.getRecord().isMalformed()) {
                malformed++;
            } else {
                processed++;
            }
        }
        logger.info("MapReduce parse job finished ingest chunk {}: {} records", chunkId, parsedChunk.size());
    }

    @Override
    public Map<String, List<Map<String, Object>>> finalizeRun(QueryPlan plan) {
        if (batchConfig.isDays()) {
            new DayBatchAssignmentJob(batchConfig).run(records);
        }

        List<MalformedSummaryJob.Summary> malformedSummaries = new MalformedSummaryJob().run(records);
        if (!malformedSummaries.isEmpty()) {
            MalformedSummaryJob.Summary summary = malformedSummaries.get(0);
            malformed = summary.malformedRecords();
            processed = summary.totalRecords() - summary.malformedRecords();
        }

        batchSummaries = new BatchMetadataJob(batchConfig).run(records);
        List<MapReduceLogRecord> goodRecords = records.stream()
            .filter(r -> !r.getRecord().isMalformed() && r.getBatchId() > 0)
            .toList();

        Map<String, List<Map<String, Object>>> results = new LinkedHashMap<>();
        if (plan.isSplitByMonth()) {
            Map<String, List<MapReduceLogRecord>> byMonth = groupByMonth(goodRecords);
            for (Map.Entry<String, List<MapReduceLogRecord>> entry : byMonth.entrySet()) {
                addRequestedQueryResults(results, plan, entry.getValue(), entry.getKey());
            }
        } else {
            addRequestedQueryResults(results, plan, goodRecords, null);
        }

        logger.info("MapReduce run {} finalized: records={}, malformed={}, batches={}",
            runId, processed + malformed, malformed, batchSummaries.size());
        return results;
    }

    @Override
    public void shutdown() {
        records.clear();
        batchSummaries = new ArrayList<>();
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("processed", processed);
        m.put("malformed", malformed);
        m.put("total_records", processed + malformed);
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

    private void addRequestedQueryResults(Map<String, List<Map<String, Object>>> results,
                                          QueryPlan plan,
                                          List<MapReduceLogRecord> scopedRecords,
                                          String month) {
        if (plan.getQueries().contains(QueryType.DAILY_TRAFFIC_SUMMARY)) {
            results.put(resultKey(QueryType.DAILY_TRAFFIC_SUMMARY, month), new DailyTrafficSummaryJob().run(scopedRecords));
        }
        if (plan.getQueries().contains(QueryType.TOP_RESOURCES)) {
            results.put(resultKey(QueryType.TOP_RESOURCES, month), new TopResourcesJob().run(scopedRecords));
        }
        if (plan.getQueries().contains(QueryType.HOURLY_ERROR_ANALYSIS)) {
            results.put(resultKey(QueryType.HOURLY_ERROR_ANALYSIS, month), new HourlyErrorAnalysisJob().run(scopedRecords));
        }
    }

    private Map<String, List<MapReduceLogRecord>> groupByMonth(List<MapReduceLogRecord> scopedRecords) {
        Map<String, List<MapReduceLogRecord>> byMonth = new LinkedHashMap<>();
        for (MapReduceLogRecord record : scopedRecords) {
            String logDate = record.getRecord().getLogDate();
            if (logDate != null && logDate.length() >= 7) {
                String month = logDate.substring(0, 7);
                byMonth.computeIfAbsent(month, k -> new ArrayList<>()).add(record);
            }
        }
        return byMonth;
    }

    private int computeBatchCount() {
        if (!batchSummaries.isEmpty()) {
            return batchSummaries.size();
        }
        if (batchConfig.isRecords()) {
            long total = processed + malformed;
            return total == 0 ? 0 : (int) Math.ceil((double) total / batchConfig.getSize());
        }

        LinkedHashSet<String> dates = new LinkedHashSet<>();
        for (MapReduceLogRecord record : records) {
            String logDate = record.getRecord().getLogDate();
            if (logDate != null) {
                dates.add(logDate);
            }
        }
        return dates.isEmpty() ? (records.isEmpty() ? 0 : 1) : (int) Math.ceil((double) dates.size() / batchConfig.getSize());
    }

    private String resultKey(QueryType type, String month) {
        if (month == null) {
            return type.getKey();
        }
        return type.getKey() + "|month=" + month;
    }
}
