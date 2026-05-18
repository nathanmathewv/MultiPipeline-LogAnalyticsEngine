# Architecture

## Overview
The Multi-Pipeline ETL Framework is a modular Java-based system that allows executing the same ETL workflow using MongoDB, Pig, MapReduce, and Hive pipeline implementations. The architecture follows SOLID principles and uses enterprise design patterns.

## Components

### 1. Parser (`app/parser/`)
**Purpose**: Parse NASA HTTP web server logs into a canonical data model.

- `LogRecord.java`: DTO representing a single parsed log entry with fields: host, timestamp, log_date, log_hour, method, resource_path, protocol, status_code, bytes, malformed flag.
- `LogParser.java`: Manual string parsing (avoids complex regex escaping) to extract fields from NASA log format. Returns `ParseResult` object indicating success or malformed with reason.

**Design Decision**: Manual parsing is more maintainable than complex regex patterns and provides better error diagnostics.

### 2. Pipeline Abstraction (`app/pipelines/base/`)
**Purpose**: Define a common interface for all pipeline implementations.

- `Pipeline.java`: Interface specifying methods:
  - `startRun(String runId, BatchConfig batchConfig)`: Initialize pipeline for a run with day or record batching
  - `processBatch(List<String> rawLines, int chunkId)`: Process an ingest chunk of raw logs
  - `finalizeRun()`: Run aggregations and return results
  - `getMetrics()`: Expose run metrics (processed, malformed, total_batches)
  - `shutdown()`: Cleanup resources

**Design Pattern**: Strategy Pattern allows switching pipeline implementations at runtime.

### 3. Pipeline Implementations (`app/pipelines/`)

#### MongoDB (`mongodb/`)
**Status**: Fully implemented.

- `MongoPipeline.java`: 
  - Parses logs and stores parsed documents in MongoDB
  - Creates indexes on log_date, resource_path, status_code
  - Implements batching by tagging documents with run_id and batch_id
  - Runs three aggregation pipelines for queries
  - Tracks malformed records

- `queries.py` (embedded in MongoPipeline):
  - **Query 1**: Daily Traffic Summary (group by log_date, status_code)
  - **Query 2**: Top 20 Resources (group by resource_path, sorted by request count)
  - **Query 3**: Hourly Error Analysis (group by log_date, log_hour, compute error rates)

#### Pig, MapReduce, Hive (`pig/`, `mapreduce/`, `hive/`)
**Status**: Implemented through their pipeline adapters.

- Pig executes parsing, batch assignment, malformed summaries, and the three aggregations through `app/pig/etl.pig`.
- Hive has a matching HiveQL workflow in `app/hive/etl.hql` for Hive runtimes; the CLI keeps a local adapter for development machines without Hive installed.
- MapReduce runs explicit local MapReduce-style jobs under `src/main/java/com/example/multietl/pipelines/mapreduce/jobs`.

### 4. Orchestrator (`app/orchestrator/`)
**Purpose**: Control execution flow, manage batching, and coordinate persistence.

- `Controller.java`: 
  - Generates run_id
  - Streams input files to the selected pipeline in ingest chunks
  - Times execution
  - Calls pipeline methods in sequence
  - Computes avg_batch_size
  - Persists metadata and results via DbLoader
  - Invokes Reporter to print summary

- `BatchManager.java`:
  - Splits file into fixed-size batches
  - Assigns batch_id starting from 1

**Design Pattern**: Orchestrator Pattern (Controller) manages workflow orchestration.

### 5. Data Persistence (`app/loader/`)
**Purpose**: Store run metadata and ETL results in PostgreSQL.

- `DbLoader.java`:
  - JDBC-based loader using parameterized queries (prevents SQL injection)
  - `insertRunMetadata`: Stores run_id, pipeline_name, batch_mode, batch_size, mode-specific batch size, avg_batch_size, total_records, malformed_records, total_batches, runtime_ms
  - `insertEtlResults`: Stores query results with mapping k1 (first key), k2 (second key), m1-m4 (metrics)
  - `queryRunMetadata`, `queryEtlResults`: Fetch data for reporting

- `schema.sql`:
  - `run_metadata`: One row per ETL run with aggregated statistics
  - `etl_results`: Multiple rows per run (one per _result_, query and key combination)

### 6. Reporting (`app/reporting/`)
**Purpose**: Display run results and metrics.

- `Reporter.java`:
  - Reads from PostgreSQL
  - Formats output with query-specific column handling
  - Groups results by query name
  - Displays run metadata and all query results

### 7. Configuration (`app/config/`)
**Purpose**: Load external configuration.

- `config.yaml`: YAML file specifying:
  - MongoDB URI and database name
  - PostgreSQL JDBC URL, user, password
  - Default batch size and data directory

- `AppConfig.java`: YAML parser using SnakeYAML to load config at runtime.

### 8. CLI Interface (`app/cli/`)
**Purpose**: User-facing entry point.

- `Main.java`:
  - Loads configuration
  - Validates input file
  - Creates DbLoader with DB credentials
  - Creates Controller and runs orchestration
  - Handles errors and logs events

## Design Patterns

1. **Strategy Pattern**: Pipeline interface + factory-based selection
2. **Adapter Pattern**: Each pipeline wraps its respective tech (MongoDB, Pig, MR, Hive)
3. **Orchestrator Pattern**: Controller manages execution flow
4. **Factory Pattern**: PipelineFactory creates pipeline instances by name
5. **DTO Pattern**: LogRecord, ParseResult encapsulate data
6. **Repository Pattern**: DbLoader provides data access layer

## Batching Strategy

- **Pipeline-level batching**: Orchestrator streams ingest chunks; each selected pipeline assigns analytical `batch_id` values.
- **Day mode**: Unique log dates are sorted and grouped by the configured number of days.
- **Record mode**: Raw records are assigned to consecutive batches by configured record count.
- **Batch persistence**: Pipeline outputs and MongoDB documents are tagged with run_id and batch_id
- **Compute avg_batch_size**: Total records / number of batches

## Query Semantics (Uniform Across All Pipelines)

### Query 1: Daily Traffic Summary
```
SELECT log_date, status_code, COUNT(*) as request_count, SUM(bytes) as total_bytes
GROUP BY log_date, status_code
```

### Query 2: Top 20 Requested Resources
```
SELECT resource_path, COUNT(*) as request_count, SUM(bytes) as total_bytes, COUNT(DISTINCT host) as distinct_host_count
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 20
```

### Query 3: Hourly Error Analysis
```
SELECT log_date, log_hour, 
  SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) as error_request_count,
  COUNT(*) as total_request_count,
  SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)::float / COUNT(*) as error_rate,
  COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END) as distinct_error_hosts
GROUP BY log_date, log_hour
```

## Error Handling

- **Malformed records**: Tracked separately; parser marks with malformed=true and reason
- **Compilation errors**: Maven enforces interface contracts
- **Database errors**: DbLoader throws SQLException; Controller logs and fails gracefully
- **File not found**: Main CLI validates input file before execution

## Extensibility

- **New pipelines**: Implement Pipeline interface, add to PipelineFactory
- **New queries**: Add to the pipeline implementations and the shared reporting formatter
- **Configuration**: Update config.yaml and AppConfig getters
- **Reporting**: Add query-specific formatting in Reporter.formatRow()

## Deployment

- **Java version**: JDK 17+
- **Build tool**: Maven
- **Database**: PostgreSQL 15
- **NoSQL store**: MongoDB 6.0
- **Docker**: Use docker-compose.yml to provision services

## Performance Considerations

- **Indexing**: MongoDB creates indexes on frequently queried fields
- **Batch size**: Configurable; larger batches reduce query overhead but increase memory
- **Connection pooling**: JDBC driver handles by default
- **Bulk writes**: MongoPipeline uses insertMany for efficiency

