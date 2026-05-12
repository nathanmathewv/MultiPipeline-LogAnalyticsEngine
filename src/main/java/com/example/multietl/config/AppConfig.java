package com.example.multietl.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class AppConfig {
    private final Map<String, Object> root;

    public AppConfig(Path configPath) throws Exception {
        Yaml yaml = new Yaml();
        try (InputStream in = Files.newInputStream(configPath)) {
            root = yaml.load(in);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getSection(String key) {
        return (Map<String, Object>) root.getOrDefault(key, Map.of());
    }

    public int getBatchSize() {
        Map<String, Object> app = getSection("app");
        return ((Number) app.getOrDefault("batch_size", 1000)).intValue();
    }

    public int getIngestChunkSize() {
        Map<String, Object> app = getSection("app");
        return ((Number) app.getOrDefault("ingest_chunk_size", getBatchSize())).intValue();
    }

    public int getBatchSizeDays() {
        Map<String, Object> app = getSection("app");
        return ((Number) app.getOrDefault("batch_size_days", 1)).intValue();
    }

    public String getBatchMode() {
        Map<String, Object> app = getSection("app");
        return (String) app.getOrDefault("batch_mode", "records");
    }

    public String getDataDir() {
        Map<String, Object> app = getSection("app");
        return (String) app.getOrDefault("data_dir", "data/raw");
    }

    public String getMongoUri() {
        Map<String, Object> mongo = getSection("mongodb");
        return (String) mongo.getOrDefault("uri", "mongodb://localhost:27017");
    }

    public String getMongoDb() {
        Map<String, Object> mongo = getSection("mongodb");
        return (String) mongo.getOrDefault("database", "web_logs");
    }

    public String getJdbcUrl() {
        Map<String, Object> jdbc = getSection("jdbc");
        return (String) jdbc.getOrDefault("url", "jdbc:postgresql://localhost:5432/etl_results");
    }

    public String getJdbcUser() {
        Map<String, Object> jdbc = getSection("jdbc");
        return (String) jdbc.getOrDefault("user", "etl");
    }

    public String getJdbcPassword() {
        Map<String, Object> jdbc = getSection("jdbc");
        return (String) jdbc.getOrDefault("password", "secret");
    }

    public String getPigImage() {
        Map<String, Object> pig = getSection("pig");
        return (String) pig.getOrDefault("image", "multietl-pig:latest");
    }

    public String getPigScriptPath() {
        Map<String, Object> pig = getSection("pig");
        return (String) pig.getOrDefault("script", "app/pig/etl.pig");
    }

    public String getMapReduceImage() {
        Map<String, Object> mapreduce = getSection("mapreduce");
        return (String) mapreduce.getOrDefault("image", "multietl-mapreduce:latest");
    }

    public String getMapReduceWorkDir() {
        Map<String, Object> mapreduce = getSection("mapreduce");
        return (String) mapreduce.getOrDefault("work_dir", "results/mapreduce");
    }

    public String getMapReduceDockerfile() {
        Map<String, Object> mapreduce = getSection("mapreduce");
        return (String) mapreduce.getOrDefault("dockerfile", "app/mapreduce/Dockerfile");
    }

    public String getMapReduceScriptPath() {
        Map<String, Object> mapreduce = getSection("mapreduce");
        return (String) mapreduce.getOrDefault("script", "/usr/local/bin/run-mapreduce.sh");
    }
}
