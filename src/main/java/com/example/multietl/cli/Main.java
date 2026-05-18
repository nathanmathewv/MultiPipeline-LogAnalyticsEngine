package com.example.multietl.cli;

import com.example.multietl.EtlApplication;
import com.example.multietl.config.AppConfig;
import com.example.multietl.pipelines.base.BatchConfig;
import com.example.multietl.pipelines.base.BatchMode;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.base.QueryType;
import com.example.multietl.util.DatasetFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            // If no arguments provided, start interactive CLI
            if (args.length == 0) {
                runInteractiveCli();
                return;
            }

            if (args.length >= 1 && args[0].equalsIgnoreCase("server")) {
                logger.info("Starting ETL REST Service on http://localhost:8080");
                SpringApplication.run(EtlApplication.class, args);
                return;
            }

            // CLI mode for backward compatibility
            if (args.length < 2) {
                printUsage();
                return;
            }

            String pipelineName = args[0];
            String inputArg = args[1];

            AppConfig config = new AppConfig(Path.of("app/config/config.yaml"));
            BatchConfig batchConfig = parseBatchConfig(args, config);
            int ingestChunkSize = config.getIngestChunkSize();
            QueryPlan queryPlan = QueryPlan.all(true);
            List<Path> inputFiles = DatasetFiles.resolveInputFiles(inputArg, config.getDataDir());

            if (inputFiles.isEmpty()) {
                System.err.println("Error: Input file(s) not found for: " + inputArg);
                return;
            }

            logger.info("Starting ETL with pipeline: {}, inputs: {}, batch: {}", pipelineName, inputFiles, batchConfig.describe());

            var dbLoader = new com.example.multietl.loader.DbLoader(config.getJdbcUrl(), config.getJdbcUser(), config.getJdbcPassword());
            var controller = new com.example.multietl.orchestrator.Controller(dbLoader, config);
            controller.run(pipelineName, inputFiles, ingestChunkSize, batchConfig, queryPlan, null);
            
            logger.info("ETL completed successfully");
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void runInteractiveCli() throws Exception {
        AppConfig config = new AppConfig(Path.of("app/config/config.yaml"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String pipelineName = promptPipeline(reader);
        QueryPlan queryPlan = promptQueryPlan(reader, true);
        BatchConfig batchConfig = promptBatchConfig(reader, config);

        List<Path> inputFiles = resolveDefaultDatasets(config.getDataDir());
        if (inputFiles.isEmpty()) {
            System.err.println("No dataset files found in " + config.getDataDir());
            return;
        }

        int ingestChunkSize = config.getIngestChunkSize();
        logger.info("Starting ETL with pipeline: {}, inputs: {}, batch: {}", pipelineName, inputFiles, batchConfig.describe());

        var dbLoader = new com.example.multietl.loader.DbLoader(config.getJdbcUrl(), config.getJdbcUser(), config.getJdbcPassword());
        var controller = new com.example.multietl.orchestrator.Controller(dbLoader, config);

        ProgressPrinter progress = new ProgressPrinter(Duration.ofSeconds(5));
        Consumer<Map<String, Object>> metricsCallback = progress::maybePrint;

        controller.run(pipelineName, inputFiles, ingestChunkSize, batchConfig, queryPlan, metricsCallback);
        logger.info("ETL completed successfully");
    }

    private static String promptPipeline(BufferedReader reader) throws Exception {
        System.out.println("Select pipeline:");
        System.out.println("  1) mongodb");
        System.out.println("  2) pig (dockerized)");
        System.out.println("  3) mapreduce");
        System.out.println("  4) hive");
        while (true) {
            System.out.print("Enter choice [1-4]: ");
            String line = reader.readLine();
            if (line == null) return "mongodb";
            line = line.trim();
            if ("1".equals(line)) return "mongodb";
            if ("2".equals(line)) return "pig";
            if ("3".equals(line)) return "mapreduce";
            if ("4".equals(line)) return "hive";
            System.out.println("Invalid selection. Try again.");
        }
    }

    private static QueryPlan promptQueryPlan(BufferedReader reader, boolean splitByMonth) throws Exception {
        System.out.println("Select query:");
        System.out.println("  1) Daily Traffic Summary");
        System.out.println("  2) Top Requested Resources");
        System.out.println("  3) Hourly Error Analysis");
        System.out.println("  4) All Queries");
        while (true) {
            System.out.print("Enter choice [1-4]: ");
            String line = reader.readLine();
            if (line == null) return QueryPlan.all(splitByMonth);
            line = line.trim();
            switch (line) {
                case "1":
                    return QueryPlan.single(QueryType.DAILY_TRAFFIC_SUMMARY, splitByMonth);
                case "2":
                    return QueryPlan.single(QueryType.TOP_RESOURCES, splitByMonth);
                case "3":
                    return QueryPlan.single(QueryType.HOURLY_ERROR_ANALYSIS, splitByMonth);
                case "4":
                    return QueryPlan.all(splitByMonth);
                default:
                    System.out.println("Invalid selection. Try again.");
            }
        }
    }

    private static BatchConfig promptBatchConfig(BufferedReader reader, AppConfig config) throws Exception {
        BatchMode mode = promptBatchMode(reader, BatchMode.from(config.getBatchMode()));
        int defaultSize = mode == BatchMode.DAYS ? config.getBatchSizeDays() : config.getBatchSize();
        int size = promptBatchSize(reader, mode, defaultSize);
        return new BatchConfig(mode, size);
    }

    private static BatchMode promptBatchMode(BufferedReader reader, BatchMode defaultMode) throws Exception {
        while (true) {
            System.out.print("Enter batch mode: 1) days  2) records [default " + defaultMode.getKey() + "]: ");
            String line = reader.readLine();
            if (line == null || line.isBlank()) return defaultMode;
            line = line.trim().toLowerCase();
            if ("1".equals(line) || "day".equals(line) || "days".equals(line)) return BatchMode.DAYS;
            if ("2".equals(line) || "record".equals(line) || "records".equals(line)) return BatchMode.RECORDS;
            System.out.println("Invalid batch mode. Try days or records.");
        }
    }

    private static int promptBatchSize(BufferedReader reader, BatchMode mode, int defaultSize) throws Exception {
        String unit = mode == BatchMode.DAYS ? "days" : "records";
        while (true) {
            System.out.print("Enter batch size (" + unit + ") [default " + defaultSize + "]: ");
            String line = reader.readLine();
            if (line == null || line.isBlank()) return defaultSize;
            try {
                int size = Integer.parseInt(line.trim());
                if (size <= 0) {
                    System.out.println("Batch size must be > 0.");
                    continue;
                }
                return size;
            } catch (NumberFormatException e) {
                System.out.println("Invalid number. Try again.");
            }
        }
    }

    private static BatchConfig parseBatchConfig(String[] args, AppConfig config) {
        if (args.length < 3) {
            BatchMode defaultMode = BatchMode.from(config.getBatchMode());
            int defaultSize = defaultMode == BatchMode.DAYS ? config.getBatchSizeDays() : config.getBatchSize();
            return new BatchConfig(defaultMode, defaultSize);
        }

        if (isInteger(args[2])) {
            return BatchConfig.days(Integer.parseInt(args[2]));
        }

        BatchMode mode = BatchMode.from(args[2]);
        int defaultSize = mode == BatchMode.DAYS ? config.getBatchSizeDays() : config.getBatchSize();
        int size = args.length >= 4 ? Integer.parseInt(args[3]) : defaultSize;
        return new BatchConfig(mode, size);
    }

    private static boolean isInteger(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        try {
            Integer.parseInt(value.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static List<Path> resolveDefaultDatasets(String dataDir) {
        return DatasetFiles.resolveDefaultDatasets(dataDir);
    }

    private static class ProgressPrinter {
        private final long minIntervalMs;
        private long lastPrint = 0;

        private ProgressPrinter(Duration minInterval) {
            this.minIntervalMs = minInterval.toMillis();
        }

        public void maybePrint(Map<String, Object> metrics) {
            long now = System.currentTimeMillis();
            if (now - lastPrint < minIntervalMs) {
                return;
            }
            lastPrint = now;
            Object rawLoaded = metrics.getOrDefault("raw_loaded", 0L);
            Object processed = metrics.getOrDefault("processed", 0L);
            Object malformed = metrics.getOrDefault("malformed", 0L);
            Object totalBatches = metrics.getOrDefault("total_batches", 0);
            Object batchMode = metrics.getOrDefault("batch_mode", "");
            System.out.println("Progress: raw_loaded=" + rawLoaded
                    + " processed=" + processed
                    + " malformed=" + malformed
                    + " batch_mode=" + batchMode
                    + " total_batches=" + totalBatches);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -cp \"target/classes;target/dependency/*\" com.example.multietl.cli.Main");
        System.out.println("  Starts interactive CLI");
        System.out.println("\nOR (Server mode):");
        System.out.println("  java -cp \"target/classes;target/dependency/*\" com.example.multietl.cli.Main server");
        System.out.println("  Server will start on http://localhost:8080");
        System.out.println("\nOR (CLI mode):");
        System.out.println("  java -cp \"target/classes;target/dependency/*\" com.example.multietl.cli.Main <pipeline> <input-file> [batch_mode] [batch_size]");
        System.out.println("  Legacy form also works: <pipeline> <input-file> [batch_size_days]");
        System.out.println("\nAvailable pipelines: mongodb, pig, mapreduce, hive");
        System.out.println("\nExample:");
        System.out.println("  java -cp \"target/classes;target/dependency/*\" com.example.multietl.cli.Main mongodb data/raw/sample.log days 2");
        System.out.println("  java -cp \"target/classes;target/dependency/*\" com.example.multietl.cli.Main mongodb all days 2");
        System.out.println("  java -cp \"target/classes;target/dependency/*\" com.example.multietl.cli.Main mongodb data/raw/sample.log records 50000");
    }
}
