# MapReduce Pipeline Jobs

The `mapreduce` CLI option now runs explicit MapReduce-style Java jobs in process. Each job follows the same shape as Hadoop MapReduce: map records to intermediate keys, group/shuffle by key, then reduce grouped values.

Jobs implemented:

- `ParseLogsJob`: parses raw NASA log lines and marks malformed records.
- `DayBatchAssignmentJob`: assigns `batch_id` by sorted log dates when `BATCH_MODE=days`.
- `MalformedSummaryJob`: counts total and malformed records.
- `BatchMetadataJob`: emits batch date ranges, record counts, and malformed counts.
- `DailyTrafficSummaryJob`: query 1.
- `TopResourcesJob`: query 2.
- `HourlyErrorAnalysisJob`: query 3.

The jobs live under:

```text
src/main/java/com/example/multietl/pipelines/mapreduce/jobs
```

This local runner keeps the project demonstrable without a Hadoop cluster. The job boundaries map directly to a Hadoop deployment, where these classes can be replaced with Hadoop `Mapper` and `Reducer` wrappers using the same keys and value contracts.
