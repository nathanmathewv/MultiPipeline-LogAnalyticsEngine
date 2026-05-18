package com.example.multietl.pipelines.mapreduce;

import com.example.multietl.pipelines.base.BatchConfig;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.base.QueryType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MapReducePipelineJobsTest {
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
    void runsRecordBatchingThroughMapReduceJobs() throws Exception {
        MapReducePipeline pipeline = new MapReducePipeline();
        pipeline.startRun("mr-records", BatchConfig.records(2));
        pipeline.processBatch(LINES.subList(0, 3), 1);
        pipeline.processBatch(LINES.subList(3, LINES.size()), 2);

        Map<String, List<Map<String, Object>>> results =
            pipeline.finalizeRun(QueryPlan.single(QueryType.DAILY_TRAFFIC_SUMMARY, false));

        assertEquals(3, pipeline.getMetrics().get("total_batches"));
        assertEquals("records", pipeline.getMetrics().get("batch_mode"));
        assertEquals(3, pipeline.getBatchSummaries().size());

        Set<Integer> batchIds = results.get(QueryType.DAILY_TRAFFIC_SUMMARY.getKey()).stream()
            .map(row -> ((Number) row.get("batch_id")).intValue())
            .collect(Collectors.toSet());
        assertEquals(Set.of(1, 2, 3), batchIds);
    }

    @Test
    void runsDayBatchingThroughMapReduceJobs() throws Exception {
        MapReducePipeline pipeline = new MapReducePipeline();
        pipeline.startRun("mr-days", BatchConfig.days(2));
        pipeline.processBatch(LINES, 1);

        Map<String, List<Map<String, Object>>> results =
            pipeline.finalizeRun(QueryPlan.single(QueryType.HOURLY_ERROR_ANALYSIS, false));

        assertEquals(2, pipeline.getMetrics().get("total_batches"));
        assertEquals("days", pipeline.getMetrics().get("batch_mode"));
        assertEquals(2, pipeline.getBatchSummaries().size());
        assertEquals("1995-07-01", pipeline.getBatchSummaries().get(0).get("batch_start_date"));
        assertEquals("1995-07-02", pipeline.getBatchSummaries().get(0).get("batch_end_date"));
        assertEquals(4L, pipeline.getBatchSummaries().get(0).get("records_total"));
        assertEquals(1L, pipeline.getBatchSummaries().get(0).get("malformed_records"));
        assertEquals(1L, pipeline.getBatchSummaries().get(1).get("records_total"));
        assertEquals(0L, pipeline.getBatchSummaries().get(1).get("malformed_records"));

        Set<Integer> batchIds = results.get(QueryType.HOURLY_ERROR_ANALYSIS.getKey()).stream()
            .map(row -> ((Number) row.get("batch_id")).intValue())
            .collect(Collectors.toSet());
        assertEquals(Set.of(1, 2), batchIds);
    }

    @Test
    void assignsAllNoDateMalformedRowsToFirstDayBatch() throws Exception {
        MapReducePipeline pipeline = new MapReducePipeline();
        pipeline.startRun("mr-all-bad-days", BatchConfig.days(2));
        pipeline.processBatch(ALL_BAD_LINES, 1);

        pipeline.finalizeRun(QueryPlan.single(QueryType.DAILY_TRAFFIC_SUMMARY, false));

        assertEquals(1, pipeline.getMetrics().get("total_batches"));
        assertEquals(2L, pipeline.getMetrics().get("total_records"));
        assertEquals(1, pipeline.getBatchSummaries().size());
        assertEquals(1, pipeline.getBatchSummaries().get(0).get("batch_id"));
        assertEquals(2L, pipeline.getBatchSummaries().get(0).get("records_total"));
        assertEquals(2L, pipeline.getBatchSummaries().get(0).get("malformed_records"));
    }
}
