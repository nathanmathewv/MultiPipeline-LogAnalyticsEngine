# NASA HTTP Web Server Logs Dataset

This directory contains the NASA HTTP web server access logs used for ETL testing and analysis.
Use only the official raw logs from the Internet Traffic Archive. Decompression is allowed, but do not preprocess the data outside the pipeline.

## File Placement

Large dataset files are not included in the repository. Place the official logs in this directory:

```bash
cp ~/Downloads/NASA_access_log_Jul95 .
cp ~/Downloads/NASA_access_log_Aug95 .
```

## Files

| File | Status | Records | Size | Notes |
|------|--------|---------|------|-------|
| sample.log | Included | 26 | 1 KB | Small sample for testing |
| NASA_access_log_Jul95 | Add manually | 1,891,714 | 196 MB | July 1995 data |
| NASA_access_log_Aug95 | Add manually | 1,569,898 | 160 MB | August 1995 data |
| NASA_access_log_Jul95.gz | Add manually | 1,891,714 | 20 MB | July compressed |
| NASA_access_log_Aug95.gz | Add manually | 1,569,898 | 16 MB | August compressed |

## Log Format

Each line follows the NASA Common Log Format:

```
host - - [timestamp] "method resource protocol" status_code bytes
```

Example:

```
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
```

## Dataset Statistics

| File | Lines | Uncompressed | Date Range | Period |
|------|-------|--------------|------------|--------|
| Jul95 | 1.89M | 196 MB | 1995-07-01 to 1995-07-31 | 31 days |
| Aug95 | 1.57M | 160 MB | 1995-08-01 to 1995-08-28 | 28 days |
| Total | 3.46M | 356 MB | July to August 1995 | 59 days |

## REST API Examples

```bash
curl http://localhost:8080/api/etl/dataset/files | jq
curl http://localhost:8080/api/etl/dataset/stats | jq
```
