package com.example.multietl.pipelines.mapreduce.jobs;

import com.example.multietl.parser.LogParser;
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

public class MalformedSummaryJob {
    public static boolean run(Configuration baseConf, String inputDir, String outputDir) throws Exception {
        Configuration conf = new Configuration(baseConf);
        Job job = Job.getInstance(conf, "multietl-malformed-summary");
        job.setJarByClass(MalformedSummaryJob.class);
        job.setMapperClass(SummaryMapper.class);
        job.setReducerClass(SummaryReducer.class);
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

    public static class SummaryMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final LogParser parser = new LogParser();
        private final Text outKey = new Text("summary");
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            LogParser.ParseResult parsed = parser.parse(value.toString());
            outValue.set("1\t" + (parsed.success ? "0" : "1"));
            context.write(outKey, outValue);
        }
    }

    public static class SummaryReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long total = 0L;
            long malformed = 0L;
            for (Text value : values) {
                String[] parts = value.toString().split("\\t", -1);
                total += Long.parseLong(parts[0]);
                malformed += Long.parseLong(parts[1]);
            }
            outKey.set(Long.toString(total));
            outValue.set(Long.toString(malformed));
            context.write(outKey, outValue);
        }
    }
}
