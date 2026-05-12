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
import java.util.HashSet;
import java.util.Set;

public class HourlyErrorJob {
    public static boolean run(Configuration baseConf, String inputDir, String outputDir) throws Exception {
        Configuration conf = new Configuration(baseConf);
        Job job = Job.getInstance(conf, "multietl-hourly-error");
        job.setJarByClass(HourlyErrorJob.class);
        job.setMapperClass(HourlyErrorMapper.class);
        job.setReducerClass(HourlyErrorReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path output = new Path(outputDir);
        FileSystem fs = output.getFileSystem(conf);
        fs.delete(output, true);
        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, output);
        return job.waitForCompletion(true);
    }

    public static class HourlyErrorMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final LogParser parser = new LogParser();
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            LogParser.ParseResult parsed = parser.parse(value.toString());
            if (!parsed.success) {
                return;
            }
            LogRecord record = parsed.record;
            boolean isError = record.getStatusCode() >= 400 && record.getStatusCode() <= 599;
            outKey.set(record.getLogDate() + "\t" + record.getLogHour());
            outValue.set("1\t" + (isError ? "1" : "0") + "\t" + (isError ? record.getHost() : ""));
            context.write(outKey, outValue);
        }
    }

    public static class HourlyErrorReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalRequests = 0L;
            long errorRequests = 0L;
            Set<String> errorHosts = new HashSet<>();

            for (Text value : values) {
                String[] parts = value.toString().split("\\t", -1);
                totalRequests += Long.parseLong(parts[0]);
                long errors = Long.parseLong(parts[1]);
                errorRequests += errors;
                if (errors > 0 && parts.length > 2 && !parts[2].isBlank()) {
                    errorHosts.add(parts[2]);
                }
            }

            double errorRate = totalRequests == 0L ? 0.0 : (double) errorRequests / totalRequests;
            outValue.set(errorRequests + "\t" + totalRequests + "\t" + errorRate + "\t" + errorHosts.size());
            context.write(key, outValue);
        }
    }
}
