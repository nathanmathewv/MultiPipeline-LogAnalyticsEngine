#!/bin/bash

# ETL Dataset Management Script using REST API

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$PROJECT_ROOT/data/raw"
SERVER_URL="${ETL_SERVER_URL:-http://localhost:8080}"

printf "\n====================================\n"
printf "ETL Dataset Utility (REST API Client)\n"
printf "====================================\n"
printf "Server: $SERVER_URL\n"
printf "Dataset Dir: $DATA_DIR\n\n"

# Check if server is running
function check_server() {
  if ! curl -s "$SERVER_URL/api/etl/dataset/files" > /dev/null 2>&1; then
    echo "Error: ETL server not running at $SERVER_URL"
    echo ""
    echo "To start the server, run in a separate terminal:"
    echo "  cd $PROJECT_ROOT"
    echo "  mvn clean install -DskipTests"
    echo "  java -jar target/multi-pipeline-etl-*.jar"
    echo ""
    echo "Or start it in the background:"
    echo "  mvn clean install -DskipTests && java -jar target/multi-pipeline-etl-*.jar &"
    exit 1
  fi
}

function show_files() {
  echo "Fetching available log files from server..."
  echo ""
  curl -s "$SERVER_URL/api/etl/dataset/files" | jq '.files[] | "\(.name) (\(.sizeReadable))"' 2>/dev/null || echo "Could not retrieve files"
  echo ""
}

function create_subset() {
  local input=$1
  local lines=$2
  local output=$3
  
  if [ ! -f "$DATA_DIR/$input" ]; then
    echo "Error: $input not found in $DATA_DIR"
    exit 1
  fi
  
  echo "Creating $output with first $lines lines from $input..."
  head -n "$lines" "$DATA_DIR/$input" > "$DATA_DIR/$output"
  local count=$(wc -l < "$DATA_DIR/$output")
  echo "Created $output: $count lines"
}

function show_stats() {
  echo "Fetching dataset statistics from server..."
  echo ""
  check_server
  curl -s "$SERVER_URL/api/etl/dataset/stats" | jq '.stats[] | "- \(.name): \(.sizeReadable) (\(.lines) lines)"' 2>/dev/null || echo "Could not retrieve stats"
  echo ""
}

function submit_job() {
  local pipeline=$1
  local file=$2
  local batch_size=${3:-10000}
  
  check_server
  
  if [ ! -f "$DATA_DIR/$file" ]; then
    echo "Error: $file not found in $DATA_DIR"
    exit 1
  fi
  
  echo "Submitting job: pipeline=$pipeline, file=$file, batch_size=$batch_size"
  echo ""
  
  # Submit the job
  response=$(curl -s -X POST "$SERVER_URL/api/etl/run" \
    -G \
    --data-urlencode "pipeline=$pipeline" \
    --data-urlencode "file=$file" \
    --data-urlencode "batchSize=$batch_size")
  
  echo "$response" | jq '.' 2>/dev/null || echo "$response"
  
  jobId=$(echo "$response" | jq -r '.jobId' 2>/dev/null)
  if [ "$jobId" != "null" ] && [ ! -z "$jobId" ]; then
    echo ""
    echo "Job submitted with ID: $jobId"
    echo "Check status with: ./etl-dataset.sh status $jobId"
  fi
}

function check_job_status() {
  local jobId=$1
  
  if [ -z "$jobId" ]; then
    echo "Error: Job ID required"
    exit 1
  fi
  
  check_server
  
  echo "Checking status of job: $jobId"
  echo ""
  
  curl -s "$SERVER_URL/api/etl/status/$jobId" | jq '.' 2>/dev/null || echo "Could not retrieve job status"
  echo ""
}

function list_methods() {
  echo "Available commands:"
  echo ""
  echo "  stats                                - Show dataset statistics"
  echo "  files                                - List available files"
  echo "  status <jobId>                       - Check job status"
  echo "  subset <input> <lines> <output>     - Create a subset of lines (local)"
  echo "  test-small                           - Submit ETL job on sample.log"
  echo "  test-medium                          - Create and submit ETL on 100k lines"
  echo "  test-large                           - Submit ETL on full July dataset"
  echo "  run <pipeline> <file> [batch_size]   - Submit ETL job manually"
  echo ""
  echo "Environment variables:"
  echo "  ETL_SERVER_URL - Override server URL (default: http://localhost:8080)"
  echo ""
}

# Main logic
case "${1:-help}" in
  stats)
    show_stats
    ;;
  files)
    show_files
    ;;
  status)
    if [ -z "$2" ]; then
      echo "Usage: status <jobId>"
      exit 1
    fi
    check_job_status "$2"
    ;;
  subset)
    if [ $# -lt 4 ]; then
      echo "Usage: subset <input> <lines> <output>"
      exit 1
    fi
    create_subset "$2" "$3" "$4"
    ;;
  test-small)
    echo "Submitting quick test on sample.log..."
    submit_job mongodb sample.log 5
    ;;
  test-medium)
    create_subset NASA_access_log_Jul95 100000 medium_100k.log
    echo ""
    echo "Submitting ETL on medium dataset..."
    submit_job mongodb medium_100k.log 10000
    ;;
  test-large)
    echo "Submitting ETL on full July dataset (1.89M lines)..."
    echo "This will take 1-2 minutes..."
    submit_job mongodb NASA_access_log_Jul95 50000
    ;;
  run)
    if [ $# -lt 3 ]; then
      echo "Usage: run <pipeline> <file> [batch_size]"
      exit 1
    fi
    submit_job "$2" "$3" "${4:-10000}"
    ;;
  help|--help|-h)
    list_methods
    ;;
  *)
    echo "Unknown command: $1"
    echo ""
    list_methods
    exit 1
    ;;
esac

