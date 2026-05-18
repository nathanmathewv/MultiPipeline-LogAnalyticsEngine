package com.example.multietl.pipelines.common;

import com.example.multietl.pipelines.base.BatchConfig;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.base.QueryType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class CommonPipelineBatchingTest {
    private static final List<String> LINES = List.of(
        "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /a HTTP/1.0\" 200 100",
        "unicomp6.unicomp.net - - [01/Jul/1995:00:01:02 -0400] \"GET /b HTTP/1.0\" 404 -",
        "burger.letters.com - - [02/Jul/1995:01:02:03 -0400] \"GET /a HTTP/1.0\" 200 25",
        "this is not a valid nasa log line",
        "d104.aa.net - - [03/Jul/1995:02:03:04 -0400] \"GET /c HTTP/1.0\" 500 75"
    );
    private static final List<String> ALL_BAD_LINES = List.of(
        "this is not a valid nasa log line",
        "still not a valid nasa log line"
    );

    @Test
    void batchesByRecordCountInsidePipeline() throws Exception {
        CommonPipeline pipeline = new CommonPipeline();
        pipeline.startRun("records-run", BatchConfig.records(2));
        pipeline.processBatch(LINES, 1);

        Map<String, List<Map<String, Object>>> results =
            pipeline.finalizeRun(QueryPlan.single(QueryType.DAILY_TRAFFIC_SUMMARY, false));

        assertEquals(3, pipeline.getMetrics().get("total_batches"));
        assertEquals("records", pipeline.getMetrics().get("batch_mode"));

        List<Map<String, Object>> summaries = pipeline.getBatchSummaries();
        assertEquals(3, summaries.size());
        assertEquals(2L, summaries.get(0).get("records_total"));
        assertEquals(1L, summaries.get(1).get("malformed_records"));
        assertEquals(1L, summaries.get(2).get("records_total"));
        assertEquals(2, summaries.get(0).get("batch_size_records"));

        Set<Integer> batchIds = results.get(QueryType.DAILY_TRAFFIC_SUMMARY.getKey()).stream()
            .map(row -> ((Number) row.get("batch_id")).intValue())
            .collect(Collectors.toSet());
        assertEquals(Set.of(1, 2, 3), batchIds);
    }

    @Test
    void batchesBySortedLogDatesInsidePipeline() throws Exception {
        CommonPipeline pipeline = new CommonPipeline();
        pipeline.startRun("days-run", BatchConfig.days(2));
        pipeline.processBatch(LINES, 1);

        Map<String, List<Map<String, Object>>> results =
            pipeline.finalizeRun(QueryPlan.single(QueryType.DAILY_TRAFFIC_SUMMARY, false));

        assertEquals(2, pipeline.getMetrics().get("total_batches"));
        assertEquals("days", pipeline.getMetrics().get("batch_mode"));

        List<Map<String, Object>> summaries = pipeline.getBatchSummaries();
        assertEquals(2, summaries.size());
        assertEquals("1995-07-01", summaries.get(0).get("batch_start_date"));
        assertEquals("1995-07-02", summaries.get(0).get("batch_end_date"));
        assertEquals("1995-07-03", summaries.get(1).get("batch_start_date"));
        assertEquals(4L, summaries.get(0).get("records_total"));
        assertEquals(1L, summaries.get(0).get("malformed_records"));
        assertEquals(1L, summaries.get(1).get("records_total"));
        assertEquals(0L, summaries.get(1).get("malformed_records"));
        assertEquals(2, summaries.get(0).get("batch_size_days"));

        Set<Integer> batchIds = results.get(QueryType.DAILY_TRAFFIC_SUMMARY.getKey()).stream()
            .map(row -> ((Number) row.get("batch_id")).intValue())
            .collect(Collectors.toSet());
        assertEquals(Set.of(1, 2), batchIds);
    }

    @Test
    void assignsAllNoDateMalformedRowsToFirstDayBatch() throws Exception {
        CommonPipeline pipeline = new CommonPipeline();
        pipeline.startRun("all-bad-days", BatchConfig.days(2));
        pipeline.processBatch(ALL_BAD_LINES, 1);

        pipeline.finalizeRun(QueryPlan.single(QueryType.DAILY_TRAFFIC_SUMMARY, false));

        assertEquals(1, pipeline.getMetrics().get("total_batches"));
        assertEquals(2L, pipeline.getMetrics().get("total_records"));
        List<Map<String, Object>> summaries = pipeline.getBatchSummaries();
        assertEquals(1, summaries.size());
        assertEquals(1, summaries.get(0).get("batch_id"));
        assertEquals(2L, summaries.get(0).get("records_total"));
        assertEquals(2L, summaries.get(0).get("malformed_records"));
    }
}
