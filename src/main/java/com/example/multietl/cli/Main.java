package com.example.multietl.cli;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;

import com.example.multietl.EtlApplication;
import com.example.multietl.config.AppConfig;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.base.QueryType;

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
            Path inputFile = Path.of(args[1]);
            
            AppConfig config = new AppConfig(Path.of("app/config/config.yaml"));
            int batchSizeRecords = args.length >= 3 ? Integer.parseInt(args[2]) : config.getBatchSize();
            int ingestChunkSize =
                    "records".equalsIgnoreCase(config.getBatchMode())
                        ? batchSizeRecords
                        : config.getIngestChunkSize();

            QueryPlan queryPlan = QueryPlan.all(false);
            
            if (!inputFile.toFile().exists()) {
                System.err.println("Error: Input file not found: " + inputFile);
                return;
            }

            if ("records".equalsIgnoreCase(config.getBatchMode())) {
                logger.info(
                    "Starting ETL with pipeline: {}, input: {}, batch mode: {}, batch size: {}",
                    pipelineName,
                    inputFile,
                    config.getBatchMode(),
                    batchSizeRecords
                );
            } else {
                logger.info(
                    "Starting ETL with pipeline: {}, input: {}, batch mode: {}, batch days: {}",
                    pipelineName,
                    inputFile,
                    config.getBatchMode(),
                    config.getBatchDays()
                );
            }
            
            var dbLoader = new com.example.multietl.loader.DbLoader(config.getJdbcUrl(), config.getJdbcUser(), config.getJdbcPassword());
            var controller = new com.example.multietl.orchestrator.Controller(dbLoader, config);
            controller.run(pipelineName, List.of(inputFile), ingestChunkSize, batchSizeRecords, queryPlan, null);
            
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
        QueryPlan queryPlan = promptQueryPlan(reader, false);
        String batchMode = promptBatchMode(reader);

        int batchSizeRecords = config.getBatchSize();
        int batchDays = config.getBatchDays();

        if ("records".equalsIgnoreCase(batchMode)) {
            batchSizeRecords =
                promptBatchSizeRecords(reader, config.getBatchSize());
        }
        else if ("days".equalsIgnoreCase(batchMode)) {
            batchDays =
                promptBatchDays(reader, config.getBatchDays());
        }

        config.setBatchMode(batchMode);
        config.setBatchDays(batchDays);

        List<Path> inputFiles = resolveDefaultDatasets(config.getDataDir());
        if (inputFiles.isEmpty()) {
            System.err.println("No dataset files found in " + config.getDataDir());
            return;
        }

        int ingestChunkSize =
                    "records".equalsIgnoreCase(config.getBatchMode())
                        ? batchSizeRecords
                        : config.getIngestChunkSize();
        if ("records".equalsIgnoreCase(config.getBatchMode())) {
            logger.info(
                "Starting ETL with pipeline: {}, inputs: {}, batch mode: {}, batch size: {}",
                pipelineName,
                inputFiles,
                config.getBatchMode(),
                batchSizeRecords
            );
        } else {
            logger.info(
                "Starting ETL with pipeline: {}, inputs: {}, batch mode: {}, batch days: {}",
                pipelineName,
                inputFiles,
                config.getBatchMode(),
                config.getBatchDays()
            );
        }

        var dbLoader = new com.example.multietl.loader.DbLoader(config.getJdbcUrl(), config.getJdbcUser(), config.getJdbcPassword());
        var controller = new com.example.multietl.orchestrator.Controller(dbLoader, config);

        ProgressPrinter progress = new ProgressPrinter(Duration.ofSeconds(5));
        Consumer<Map<String, Object>> metricsCallback = progress::maybePrint;

        controller.run(pipelineName, inputFiles, ingestChunkSize, batchSizeRecords, queryPlan, metricsCallback);
        logger.info("ETL completed successfully");
    }

    private static String promptPipeline(BufferedReader reader) throws Exception {
        System.out.println("Select pipeline:");
        System.out.println("  1) mongodb");
        System.out.println("  2) pig (dockerized)");
        System.out.println("  3) mapreduce");
        System.out.println("  4) hive (dockerized)");

        while (true) {
            System.out.print("Enter choice [1-4]: ");

            String line = reader.readLine();

            if (line == null) {
                return "mongodb";
            }

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

    private static String promptBatchMode(BufferedReader reader) throws Exception {
        System.out.println("Select batch mode:");
        System.out.println("  1) records");
        System.out.println("  2) days");

        while (true) {
            System.out.print("Enter choice [1-2]: ");

            String line = reader.readLine();

            if (line == null) {
                return "records";
            }

            line = line.trim();

            if ("1".equals(line)) {
                return "records";
            }

            if ("2".equals(line)) {
                return "days";
            }

            System.out.println("Invalid selection. Try again.");
        }
    }

    private static int promptBatchSizeRecords(BufferedReader reader, int defaultRecords) throws Exception {
        while (true) {
            System.out.print("Enter batch size (records) [default " + defaultRecords + "]: ");
            String line = reader.readLine();
            if (line == null || line.isBlank()) return defaultRecords;
            try {
                int records = Integer.parseInt(line.trim());
                if (records <= 0) {
                    System.out.println("Batch size must be > 0.");
                    continue;
                }
                return records;
            } catch (NumberFormatException e) {
                System.out.println("Invalid number. Try again.");
            }
        }
    }

    private static int promptBatchDays(
        BufferedReader reader,
        int defaultDays
    ) throws Exception {

        while (true) {

            System.out.print(
                "Enter batch window (days) [default "
                    + defaultDays + "]: "
            );

            String line = reader.readLine();

            if (line == null || line.isBlank()) {
                return defaultDays;
            }

            try {
                int days = Integer.parseInt(line.trim());

                if (days <= 0) {
                    System.out.println("Days must be > 0.");
                    continue;
                }

                return days;

            } catch (NumberFormatException e) {
                System.out.println("Invalid number. Try again.");
            }
        }
    }

    private static List<Path> resolveDefaultDatasets(String dataDir) {
        List<Path> files = new ArrayList<>();
        addFirstExisting(files, dataDir, "NASA_access_log_Jul95", "NASA_access_log_Jul95.log", "NASA_access_log_Jul95.gz");
        addFirstExisting(files, dataDir, "NASA_access_log_Aug95", "NASA_access_log_Aug95.log", "NASA_access_log_Aug95.gz");
        return files;
    }

    private static void addFirstExisting(List<Path> files, String dataDir, String... candidates) {
        for (String candidate : candidates) {
            Path path = Path.of(dataDir, candidate);
            if (path.toFile().exists()) {
                files.add(path);
                return;
            }
        }
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
            System.out.println("Progress: raw_loaded=" + rawLoaded
                    + " processed=" + processed
                    + " malformed=" + malformed
                    + " batches=" + totalBatches);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -cp target/classes:target/lib/* com.example.multietl.cli.Main");
        System.out.println("  Starts interactive CLI");
        System.out.println("\nOR (Server mode):");
        System.out.println("  java -cp target/classes:target/lib/* com.example.multietl.cli.Main server");
        System.out.println("  Server will start on http://localhost:8080");
        System.out.println("\nOR (CLI mode):");
        System.out.println("  java -cp target/classes:target/lib/* com.example.multietl.cli.Main <pipeline> <input-file> [batch_size]");
        System.out.println("\nAvailable pipelines: mongodb, pig, mapreduce, hive");
        System.out.println("\nExample:");
        System.out.println("  java -cp target/classes:target/lib/* com.example.multietl.cli.Main mongodb data/raw/sample.log 1000");
    }
}