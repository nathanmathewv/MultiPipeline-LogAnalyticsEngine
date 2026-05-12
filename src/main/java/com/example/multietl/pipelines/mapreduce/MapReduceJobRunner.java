package com.example.multietl.pipelines.mapreduce;

import com.example.multietl.pipelines.base.QueryType;
import com.example.multietl.pipelines.mapreduce.jobs.DailyTrafficJob;
import com.example.multietl.pipelines.mapreduce.jobs.HourlyErrorJob;
import com.example.multietl.pipelines.mapreduce.jobs.MalformedSummaryJob;
import com.example.multietl.pipelines.mapreduce.jobs.TopResourcesJob;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class MapReduceJobRunner {
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: MapReduceJobRunner <runId> <inputDir> <outputDir> <queryNames|all> <splitByMonth>");
            System.exit(2);
        }

        String runId = args[0];
        String inputDir = absolutePath(args[1]);
        String outputDir = absolutePath(args[2]);
        Set<String> queries = parseQueries(args[3]);
        boolean splitByMonth = Boolean.parseBoolean(args[4]);

        Configuration conf = new Configuration();
        conf.set("multietl.run.id", runId);
        conf.setBoolean("multietl.split.by.month", splitByMonth);
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true);

        boolean ok = MalformedSummaryJob.run(conf, inputDir, outputDir + "/malformed_summary");

        if (queries.contains(QueryType.DAILY_TRAFFIC_SUMMARY.getKey())) {
            ok &= DailyTrafficJob.run(conf, inputDir, outputDir + "/q1");
        }
        if (queries.contains(QueryType.TOP_RESOURCES.getKey())) {
            ok &= TopResourcesJob.run(conf, inputDir, outputDir + "/q2", outputDir + "/q2_intermediate", splitByMonth);
        }
        if (queries.contains(QueryType.HOURLY_ERROR_ANALYSIS.getKey())) {
            ok &= HourlyErrorJob.run(conf, inputDir, outputDir + "/q3");
        }

        if (!ok) {
            throw new IllegalStateException("One or more Dockerized MapReduce jobs failed");
        }
    }

    private static Set<String> parseQueries(String raw) {
        Set<String> all = new HashSet<>();
        for (QueryType type : QueryType.values()) {
            all.add(type.getKey());
        }
        if (raw == null || raw.isBlank() || "all".equalsIgnoreCase(raw)) {
            return all;
        }

        Set<String> selected = new HashSet<>();
        Arrays.stream(raw.split(","))
            .map(s -> s.trim().toLowerCase(Locale.ROOT))
            .filter(s -> !s.isBlank())
            .forEach(selected::add);
        return selected;
    }

    private static String absolutePath(String path) {
        return new File(path).getAbsolutePath();
    }
}
