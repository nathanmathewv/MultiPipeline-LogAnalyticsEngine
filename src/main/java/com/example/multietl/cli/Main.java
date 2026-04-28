package com.example.multietl.cli;

import com.example.multietl.EtlApplication;
import com.example.multietl.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;

import java.nio.file.Path;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            // If no arguments provided, start REST server
            if (args.length == 0) {
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
            
            // Load config
            AppConfig config = new AppConfig(Path.of("app/config/config.yaml"));
            int batchSize = args.length >= 3 ? Integer.parseInt(args[2]) : config.getBatchSize();
            
            if (!inputFile.toFile().exists()) {
                System.err.println("Error: Input file not found: " + inputFile);
                return;
            }

            logger.info("Starting ETL with pipeline: {}, input: {}, batch size: {}", pipelineName, inputFile, batchSize);
            
            var dbLoader = new com.example.multietl.loader.DbLoader(config.getJdbcUrl(), config.getJdbcUser(), config.getJdbcPassword());
            var controller = new com.example.multietl.orchestrator.Controller(dbLoader);
            controller.run(pipelineName, inputFile, batchSize, null);
            
            logger.info("ETL completed successfully");
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -cp target/classes:target/lib/* com.example.multietl.cli.Main");
        System.out.println("\nOR (Server mode):");
        System.out.println("  java -cp target/classes:target/lib/* com.example.multietl.cli.Main");
        System.out.println("  Server will start on http://localhost:8080");
        System.out.println("\nOR (CLI mode):");
        System.out.println("  java -cp target/classes:target/lib/* com.example.multietl.cli.Main <pipeline> <input-file> [batch_size]");
        System.out.println("\nAvailable pipelines: mongodb, pig, mapreduce, hive");
        System.out.println("\nExample:");
        System.out.println("  java -cp target/classes:target/lib/* com.example.multietl.cli.Main mongodb data/raw/sample.log 1000");
    }
}
