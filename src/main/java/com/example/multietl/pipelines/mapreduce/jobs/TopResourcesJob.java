package com.example.multietl.pipelines.mapreduce.jobs;

import com.example.multietl.parser.LogParser;
import com.example.multietl.parser.LogRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TopResourcesJob {
    private static final String SPLIT_BY_MONTH = "multietl.split.by.month";

    public static boolean run(Configuration baseConf, String inputDir, String outputDir, String tempDir, boolean splitByMonth) throws Exception {
        Configuration aggregateConf = new Configuration(baseConf);
        aggregateConf.setBoolean(SPLIT_BY_MONTH, splitByMonth);

        Job aggregateJob = Job.getInstance(aggregateConf, "multietl-top-resources-aggregate");
        aggregateJob.setJarByClass(TopResourcesJob.class);
        aggregateJob.setMapperClass(ResourceAggregateMapper.class);
        aggregateJob.setReducerClass(ResourceAggregateReducer.class);
        aggregateJob.setMapOutputKeyClass(Text.class);
        aggregateJob.setMapOutputValueClass(Text.class);
        aggregateJob.setOutputKeyClass(Text.class);
        aggregateJob.setOutputValueClass(Text.class);

        Path temp = new Path(tempDir);
        FileSystem fs = temp.getFileSystem(aggregateConf);
        fs.delete(temp, true);
        fs.delete(new Path(outputDir), true);
        FileInputFormat.addInputPath(aggregateJob, new Path(inputDir));
        FileOutputFormat.setOutputPath(aggregateJob, temp);
        if (!aggregateJob.waitForCompletion(true)) {
            return false;
        }

        Configuration topConf = new Configuration(baseConf);
        topConf.setBoolean(SPLIT_BY_MONTH, splitByMonth);

        Job topJob = Job.getInstance(topConf, "multietl-top-resources-limit");
        topJob.setJarByClass(TopResourcesJob.class);
        topJob.setMapperClass(Top20Mapper.class);
        topJob.setReducerClass(Top20Reducer.class);
        topJob.setNumReduceTasks(1);
        topJob.setMapOutputKeyClass(Text.class);
        topJob.setMapOutputValueClass(Text.class);
        topJob.setOutputKeyClass(Text.class);
        topJob.setOutputValueClass(Text.class);

        Path output = new Path(outputDir);
        fs.delete(output, true);
        FileInputFormat.addInputPath(topJob, temp);
        FileOutputFormat.setOutputPath(topJob, output);
        boolean ok = topJob.waitForCompletion(true);
        fs.delete(temp, true);
        return ok;
    }

    public static class ResourceAggregateMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final LogParser parser = new LogParser();
        private final Text outKey = new Text();
        private final Text outValue = new Text();
        private boolean splitByMonth;

        @Override
        protected void setup(Context context) {
            splitByMonth = context.getConfiguration().getBoolean(SPLIT_BY_MONTH, true);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            LogParser.ParseResult parsed = parser.parse(value.toString());
            if (!parsed.success) {
                return;
            }

            LogRecord record = parsed.record;
            String logMonth = record.getLogDate().length() >= 7 ? record.getLogDate().substring(0, 7) : "";
            String safeResource = encode(record.getResourcePath());
            outKey.set(splitByMonth ? logMonth + "\t" + safeResource : safeResource);
            outValue.set("1\t" + record.getBytes() + "\t" + record.getHost());
            context.write(outKey, outValue);
        }
    }

    public static class ResourceAggregateReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long requestCount = 0L;
            long totalBytes = 0L;
            Set<String> hosts = new HashSet<>();

            for (Text value : values) {
                String[] parts = value.toString().split("\\t", -1);
                requestCount += Long.parseLong(parts[0]);
                totalBytes += Long.parseLong(parts[1]);
                if (parts.length > 2 && !parts[2].isBlank()) {
                    hosts.add(parts[2]);
                }
            }

            outValue.set(requestCount + "\t" + totalBytes + "\t" + hosts.size());
            context.write(key, outValue);
        }
    }

    public static class Top20Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();
        private boolean splitByMonth;

        @Override
        protected void setup(Context context) {
            splitByMonth = context.getConfiguration().getBoolean(SPLIT_BY_MONTH, true);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\t", -1);
            if (splitByMonth) {
                if (parts.length < 5) {
                    return;
                }
                outKey.set(parts[0]);
                outValue.set(parts[1] + "\t" + parts[2] + "\t" + parts[3] + "\t" + parts[4]);
            } else {
                if (parts.length < 4) {
                    return;
                }
                outKey.set("all");
                outValue.set(parts[0] + "\t" + parts[1] + "\t" + parts[2] + "\t" + parts[3]);
            }
            context.write(outKey, outValue);
        }
    }

    public static class Top20Reducer extends Reducer<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();
        private boolean splitByMonth;

        @Override
        protected void setup(Context context) {
            splitByMonth = context.getConfiguration().getBoolean(SPLIT_BY_MONTH, true);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<ResourceRow> rows = new ArrayList<>();
            for (Text value : values) {
                String[] parts = value.toString().split("\\t", -1);
                if (parts.length < 4) {
                    continue;
                }
                rows.add(new ResourceRow(parts[0], Long.parseLong(parts[1]), Long.parseLong(parts[2]), Long.parseLong(parts[3])));
            }

            rows.sort(Comparator
                .comparingLong(ResourceRow::requestCount).reversed()
                .thenComparing(ResourceRow::resourcePath));

            int limit = Math.min(20, rows.size());
            for (int i = 0; i < limit; i++) {
                ResourceRow row = rows.get(i);
                if (splitByMonth) {
                    outKey.set(key.toString() + "\t" + decode(row.resourcePath()));
                } else {
                    outKey.set(decode(row.resourcePath()));
                }
                outValue.set(row.requestCount() + "\t" + row.totalBytes() + "\t" + row.distinctHostCount());
                context.write(outKey, outValue);
            }
        }
    }

    private record ResourceRow(String resourcePath, long requestCount, long totalBytes, long distinctHostCount) {
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding()
            .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
