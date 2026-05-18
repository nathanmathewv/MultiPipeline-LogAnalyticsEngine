# Hive Pipeline

`etl.hql` is the HiveQL version of the ETL workflow. It loads raw NASA log files, parses records, assigns `batch_id` inside Hive, writes malformed and batch summaries, and writes the three query outputs as tab-delimited result folders.

The Java CLI runs this script through the `multietl-hive:latest` Docker image. Example manual run:

```bash
docker run --rm -v "$PWD:/workspace" -w /tmp multietl-hive:latest hive \
  --hiveconf RUN_ID=manual_hive_run \
  --hiveconf INPUT=/workspace/data/raw/NASA_access_log_Jul95.gz \
  --hiveconf OUTPUT=/workspace/results/hive/manual_test \
  --hiveconf BATCH_MODE=records \
  --hiveconf BATCH_SIZE=50000 \
  -f /workspace/app/hive/etl.hql
```

Use `BATCH_MODE=days` with `BATCH_SIZE=<number_of_days>` for date-window batching.
