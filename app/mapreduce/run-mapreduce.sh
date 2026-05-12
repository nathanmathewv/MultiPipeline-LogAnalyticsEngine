#!/usr/bin/env bash
set -euo pipefail

cd /workspace

if [ "$#" -lt 3 ]; then
  echo "Usage: run-mapreduce.sh <runId> <inputDir> <outputDir> [queryNames] [splitByMonth]" >&2
  echo "Example: run-mapreduce.sh run-123 results/mapreduce/run-123/input results/mapreduce/run-123/output all true" >&2
  exit 2
fi

RUN_ID="$1"
INPUT_DIR="$2"
OUTPUT_DIR="$3"
QUERY_NAMES="${4:-all}"
SPLIT_BY_MONTH="${5:-true}"

mkdir -p "$OUTPUT_DIR"

# Compile inside Docker so Windows does not need Hadoop installed locally.
mvn -q -DskipTests package dependency:copy-dependencies

java -cp "target/classes:target/dependency/*" \
  com.example.multietl.pipelines.mapreduce.MapReduceJobRunner \
  "$RUN_ID" \
  "$INPUT_DIR" \
  "$OUTPUT_DIR" \
  "$QUERY_NAMES" \
  "$SPLIT_BY_MONTH"
