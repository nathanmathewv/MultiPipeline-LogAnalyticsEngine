#!/usr/bin/env bash

# REST API smoke tests for the ETL service.
# Expected usage:
#   ETL_SERVER_URL=http://localhost:8080 scripts/api-smoke-test.sh

set -euo pipefail

SERVER_URL="${ETL_SERVER_URL:-http://localhost:8080}"
TEST_FILE="${ETL_TEST_FILE:-sample.log}"
TEST_BATCH_SIZE="${ETL_TEST_BATCH_SIZE:-5}"

function echo_step() {
  echo ""
  echo "==> $1"
}

function fail() {
  echo "FAIL: $1"
  exit 1
}

function http_get() {
  local url="$1"
  local response
  response=$(curl -s -w "HTTP_STATUS:%{http_code}" "$url")
  local body="${response%HTTP_STATUS:*}"
  local status="${response##*HTTP_STATUS:}"
  echo "$status"$'\n'"$body"
}

function http_post() {
  local url="$1"
  shift
  local response
  response=$(curl -s -X POST -G -w "HTTP_STATUS:%{http_code}" "$url" "$@")
  local body="${response%HTTP_STATUS:*}"
  local status="${response##*HTTP_STATUS:}"
  echo "$status"$'\n'"$body"
}

function assert_contains() {
  local body="$1"
  local needle="$2"
  if ! echo "$body" | grep -q "$needle"; then
    fail "Expected response to contain: $needle"
  fi
}

echo "ETL API Smoke Tests"
echo "Server: $SERVER_URL"
echo "Test file: $TEST_FILE"
echo "Batch size: $TEST_BATCH_SIZE"

# 1) List files
echo_step "GET /api/etl/dataset/files"
read -r status body < <(http_get "$SERVER_URL/api/etl/dataset/files")
if [ "$status" != "200" ]; then
  fail "Expected HTTP 200, got $status"
fi
echo "$body"
assert_contains "$body" "files"

# 2) Dataset stats
echo_step "GET /api/etl/dataset/stats"
read -r status body < <(http_get "$SERVER_URL/api/etl/dataset/stats")
if [ "$status" != "200" ]; then
  fail "Expected HTTP 200, got $status"
fi
echo "$body"
assert_contains "$body" "stats"

# 3) Submit job
echo_step "POST /api/etl/run"
read -r status body < <(http_post "$SERVER_URL/api/etl/run" \
  --data-urlencode "pipeline=mongodb" \
  --data-urlencode "file=$TEST_FILE" \
  --data-urlencode "batchSize=$TEST_BATCH_SIZE")
if [ "$status" != "200" ]; then
  fail "Expected HTTP 200, got $status"
fi
echo "$body"
assert_contains "$body" "jobId"
assert_contains "$body" "status"

job_id=$(echo "$body" | sed -n 's/.*"jobId"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
if [ -z "$job_id" ]; then
  fail "Failed to extract jobId"
fi

echo "Extracted jobId: $job_id"

# 4) Check status
echo_step "GET /api/etl/status/{jobId}"
read -r status body < <(http_get "$SERVER_URL/api/etl/status/$job_id")
if [ "$status" != "200" ]; then
  fail "Expected HTTP 200, got $status"
fi
echo "$body"
assert_contains "$body" "status"
assert_contains "$body" "pipeline"

echo ""
echo "PASS: All REST API checks completed successfully"
