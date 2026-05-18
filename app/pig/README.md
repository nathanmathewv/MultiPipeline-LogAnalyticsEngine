# Pig Pipeline (Docker)

This project runs Apache Pig inside a Docker container.

## Build image

```bash
docker build -t multietl-pig:latest -f app/pig/Dockerfile .
```

## Run manually (optional)

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace \
  multietl-pig:latest \
  pig -x local -f /workspace/app/pig/etl.pig \
  -param INPUT=/workspace/data/raw/NASA_access_log_Jul95 \
  -param OUTPUT=/workspace/results/pig/manual_test \
  -param BATCH_MODE=days \
  -param BATCH_SIZE=1
```

Outputs go to `results/pig/<run_id>/output` when invoked via the Java CLI.
