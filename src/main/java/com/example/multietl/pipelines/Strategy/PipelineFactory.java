package com.example.multietl.pipelines.Strategy;

import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.mongodb.MongoPipeline;
import com.example.multietl.pipelines.pig.PigPipeline;
import com.example.multietl.pipelines.mapreduce.MapReducePipeline;
import com.example.multietl.pipelines.hive.HivePipeline;

public class PipelineFactory {
    public static Pipeline create(String name, String uriOrConfig) {
        switch (name.toLowerCase()) {
            case "mongodb":
                System.out.println("Creating MongoPipeline with URI/Config: " + uriOrConfig);
                return new MongoPipeline(uriOrConfig != null ? uriOrConfig : "mongodb://root:secret@localhost:27017/admin", "web_logs");
            case "pig":
                return new PigPipeline();
            case "mapreduce":
                return new MapReducePipeline();
            case "hive":
                return new HivePipeline();
            default:
                throw new IllegalArgumentException("Unknown pipeline: " + name);
        }
    }
}
