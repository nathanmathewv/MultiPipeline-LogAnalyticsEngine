package com.example.multietl.pipelines.Strategy;

import com.example.multietl.config.AppConfig;
import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.hive.HivePipeline;
import com.example.multietl.pipelines.mapreduce.MapReducePipeline;
import com.example.multietl.pipelines.mongodb.MongoPipeline;
import com.example.multietl.pipelines.pig.PigPipeline;

public class PipelineFactory {
    public static Pipeline create(String name, AppConfig config) {
        switch (name.toLowerCase()) {
            case "mongodb":
                String mongoUri = config != null ? config.getMongoUri() : "mongodb://root:secret@localhost:27017/admin";
                String mongoDb = config != null ? config.getMongoDb() : "web_logs";
                System.out.println("Creating MongoPipeline with URI: " + mongoUri);
                return new MongoPipeline(mongoUri, mongoDb);
            case "pig":
                return new PigPipeline(config);
            case "mapreduce":
                return new MapReducePipeline(config);
            case "hive":
                return new HivePipeline(config);
            default:
                throw new IllegalArgumentException("Unknown pipeline: " + name);
        }
    }
}
