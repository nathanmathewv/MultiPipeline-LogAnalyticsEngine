# Multi-Pipeline ETL and Reporting Framework

## Overview

This project is a Java-based ETL and reporting framework for web server log analytics. It supports MongoDB, Pig, MapReduce, and Hive execution backends through one CLI/API interface and one relational reporting layer.

## Requirements

- Java 17 or later
- Maven 3.8 or later
- Docker and Docker Compose

## Dataset

The project uses the NASA HTTP Web Server Logs from the Internet Traffic Archive:

- https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
- https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
- https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz

Place the official raw files in `data/raw/`. Both compressed `.gz` and decompressed files are supported; no manual preprocessing is required.

```bash
mkdir -p data/raw
cp /path/to/NASA_access_log_Jul95.gz data/raw/
cp /path/to/NASA_access_log_Aug95.gz data/raw/
```

On Windows, you can copy the two files from Downloads with:

```powershell
.\scripts\load-dataset.ps1
```

Use only the official raw log files. Decompression is allowed, but is no longer necessary; no other preprocessing should be done outside the pipeline.

The subset command in the utility script is intended for development tests only. For official runs and comparisons, use the full official datasets without any manual preprocessing.

## Pipeline Status

| Pipeline | Status |
|----------|--------|
| MongoDB  | Implemented |
| Pig      | Implemented with Dockerized Pig script |
| MapReduce| Implemented with explicit local MapReduce-style Java jobs |
| Hive     | Implemented with Dockerized Hive running `app/hive/etl.hql` |

## Query Set

1. Daily Traffic Summary: log date, status code, request count, total bytes
2. Top Requested Resources: resource path, request count, total bytes, distinct host count
3. Hourly Error Analysis: log date, log hour, error request count, total request count, error rate, distinct error hosts

## Configuration

Edit [app/config/config.yaml](app/config/config.yaml):

```yaml
app:
  data_dir: data/raw
  batch_mode: days
  batch_size: 1000
  batch_size_days: 1

mongodb:
  uri: mongodb://localhost:27017
  database: web_logs

jdbc:
  url: jdbc:postgresql://localhost:5432/etl_results
  user: etl
  password: secret

pig:
  image: multietl-pig:latest
  script: app/pig/etl.pig

hive:
  image: multietl-hive:latest
  script: app/hive/etl.hql
```

## End-to-End Workflow

1. Start services:
   ```bash
   docker-compose up -d
   docker-compose ps
   ```

2. Place the dataset in `data/raw/`:
   ```powershell
   .\scripts\load-dataset.ps1
   ```

3. Build the project:
   ```bash
   mvn clean install -DskipTests
   ```

4. Initialize the database schema:
   ```bash
   docker exec etl_postgres psql -U etl -d etl_results -c "$(cat app/loader/schema.sql)"
   ```

5. Start the REST service:
   ```bash
   java -jar target/multi-pipeline-etl-*.jar
   ```

6. Run API smoke checks:
   ```bash
   scripts/api-smoke-test.sh
   ```

7. Submit a job:
   ```bash
   curl -X POST "http://localhost:8080/api/etl/run" \
     -G \
     --data-urlencode "pipeline=mongodb" \
     --data-urlencode "file=NASA_access_log_Jul95.gz" \
     --data-urlencode "batchMode=records" \
     --data-urlencode "batchSize=50000"
   ```

8. Check job status:
   ```bash
   curl http://localhost:8080/api/etl/status/{jobId}
   ```

9. Verify results in PostgreSQL:
   ```bash
   docker exec etl_postgres psql -U etl -d etl_results -c \
     "SELECT run_id, pipeline_name, total_records, runtime_ms FROM run_metadata ORDER BY created_at DESC LIMIT 1;"
   ```

## REST API

- `GET /api/etl/dataset/files` - List available log files
- `GET /api/etl/dataset/stats` - Get file size and line count
- `POST /api/etl/run` - Submit a job (async)
- `GET /api/etl/status/{jobId}` - Get job status

## CLI Mode

```bash
java -cp "target/classes;target/dependency/*" com.example.multietl.cli.Main
java -cp "target/classes;target/dependency/*" com.example.multietl.cli.Main mongodb data/raw/sample.log days 2
java -cp "target/classes;target/dependency/*" com.example.multietl.cli.Main mongodb data/raw/sample.log records 50000
```

## Testing

Unit tests:

```bash
mvn test
```

REST API smoke tests:

```bash
chmod +x scripts/api-smoke-test.sh
scripts/api-smoke-test.sh
```

The smoke test script requires the REST service to be running on `http://localhost:8080` and will echo the HTTP responses and pass or fail status.

## Project Structure

```
app/                     Configuration, schema, and utilities
src/main/java/           Source code
src/test/java/           Unit tests
data/raw/                Dataset location
docker-compose.yml       Local PostgreSQL and MongoDB
```
