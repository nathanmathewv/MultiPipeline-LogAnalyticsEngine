# Multi-Pipeline Log Analytics Engine - Demo Script

**Team Size:** 3 Members (Speaker 1, Speaker 2, Speaker 3)
**Duration:** ~5-7 minutes
**Prerequisites:** 
- Have Docker Desktop running (with `etl_postgres` and `etl_mongo` up).
- Ensure the project is compiled (`mvn clean install -DskipTests`).
- Have a terminal open in the project root (`c:\6th Sem\NoSQL\project\MultiPipeline-LogAnalyticsEngine`).
- Have a database client (like DBeaver, pgAdmin, or a terminal) open and connected to the PostgreSQL database (`etl_results`). *Note: The requirements mention MySQL, but our project uses PostgreSQL for the relational reporting layer. You can simply introduce it as our "relational SQL reporting layer".*

---

## Part 1: Introduction & Architecture (Speaker 1)
**Goal:** Introduce the project, the pipelines, and how the reporting layer unifies them.

**Speaker 1:**
"Hello everyone! Today we are demonstrating our Multi-Pipeline Log Analytics Engine. The goal of this project was to build a system that can process massive web server logs using different execution backends—specifically MongoDB, Hadoop MapReduce, Pig, and Hive—while ensuring the analytical results are completely identical across all of them.

Instead of writing separate, disjointed scripts, we built a unified Java orchestrator. This orchestrator handles file reading, batching, and routing the data to the selected pipeline. Once a pipeline finishes processing the data and executing our analytical queries, the results are loaded into a common relational SQL database (PostgreSQL in our case) for standardized reporting."

*(Speaker 1 shares screen showing the architecture or the code IDE)*

"To show you this in action, we'll use our Command-Line Interface to trigger the pipelines. I'll now pass it over to [Speaker 2], who will demonstrate the pipeline selection and batch processing behavior."

---

## Part 2: Command-Line Pipeline Selection & Batch Processing (Speaker 2)
**Goal:** Show the CLI, run a pipeline (e.g., MongoDB), explain batching, and run a second pipeline (e.g., Pig) to prove interchangeable logic.

**Speaker 2:**
"Thanks! Let's jump into the terminal. Our system is fully controllable via the command line. We pass the pipeline name, the input file, the batching mode, and the batch size. 

First, let's run the **MongoDB pipeline** in 'records' mode, processing the log file in chunks of 50,000 records."

*(Speaker 2 types and runs the following command)*
```bash
java -cp "target/classes;target/dependency/*" com.example.multietl.cli.Main mongodb data/raw/NASA_access_log_Jul95.gz records 50000
```

**Speaker 2:**
"As it runs, you can see the terminal output logging the batch processing behavior. The orchestrator is streaming the raw file and the pipeline is internally segmenting it into chunks of 50,000 records. This prevents memory overflows and allows us to process files much larger than our available RAM.

Behind the scenes, this pipeline is parsing the logs, identifying malformed records, and executing our predefined analytical queries—like Daily Traffic Summaries and Hourly Error Analysis.

Now, just to show how modular this is, we can seamlessly switch to the **Pig pipeline** using the exact same CLI structure, just by changing the first argument."

*(Speaker 2 types and runs the Pig command)*
```bash
java -cp "target/classes;target/dependency/*" com.example.multietl.cli.Main pig data/raw/NASA_access_log_Jul95.gz records 50000
```

**Speaker 2:**
"Here, the Java orchestrator is doing the same batching, but it writes chunked files to disk and spins up a Docker container to execute our Apache Pig scripts on the data. The query logic is completely preserved, just executed in a different paradigm.

Next, [Speaker 3] will show you how these results are captured and loaded into our SQL reporting layer."

---

## Part 3: SQL Loading, Table Structure, and Reporting (Speaker 3)
**Goal:** Show the Postgres database, the table schemas, and the final analytical outputs.

**Speaker 3:**
"Thank you. Once any pipeline finishes, our `PostgresLoader` takes over. It reads the output from the pipeline and performs bulk inserts into our SQL database. By using a single relational database at the end of the ETL process, we have a common reporting layer regardless of whether the data was processed in a NoSQL document store like Mongo or a MapReduce cluster.

Let's look at the database."

*(Speaker 3 switches to DBeaver / pgAdmin or terminal connected to Postgres)*

**Speaker 3:**
"Here is the structure of our tables in the SQL database. 
- We have a `run_metadata` table that stores the pipeline name, runtime, total records, and malformed counts.
- We have `batch_summaries` that proves our chunking strategy worked, logging exactly how many records were in each batch.
- And we have our query result tables: `query_daily_traffic`, `query_top_resources`, and `query_hourly_errors`."

*(Speaker 3 runs a SELECT query on run_metadata)*
```sql
SELECT run_id, pipeline_name, total_records, malformed_records, runtime_ms 
FROM run_metadata ORDER BY created_at DESC LIMIT 5;
```

**Speaker 3:**
"As you can see here, both the MongoDB and Pig runs we just executed successfully logged their runtime statistics. More importantly, notice that the `total_records` and `malformed_records` are **exactly identical** across both pipelines. This proves that our parsing rules and data integrity checks are perfectly synchronized."

*(Speaker 3 runs a SELECT query on one of the analytical tables)*
```sql
SELECT * FROM query_daily_traffic 
WHERE run_id = (SELECT run_id FROM run_metadata WHERE pipeline_name = 'mongodb' ORDER BY created_at DESC LIMIT 1)
LIMIT 10;
```

**Speaker 3:**
"And here is the output of our Daily Traffic query. It aggregates total bytes and request counts per status code per day. If we ran this exact same SQL query filtering for the 'pig' run ID instead, the analytical results would match row-for-row. 

This concludes our demonstration. We successfully showed our command-line orchestrator, scalable batch processing, seamless pipeline switching, and a unified SQL reporting layer. Thank you!"
