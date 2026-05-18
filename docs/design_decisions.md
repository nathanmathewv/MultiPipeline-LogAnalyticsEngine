# Design Decisions

## 1. Language: Java
**Rationale**: 
- Enterprise-grade type safety and mature ecosystem
- MongoDB Java driver provides excellent support
- JDBC abstraction allows easy database swaps
- Maven standard for dependency management

## 2. Manual String Parsing vs Regex
**Decision**: Manual byte-oriented parsing in LogParser.java  
**Rationale**:
- Avoids fragile regex escaping issues in Java source files
- Provides granular error diagnostics (which part failed)
- More readable and maintainable
- Performance adequate for 30,000+ log lines

## 3. Batching in Pipelines
**Decision**: The orchestrator streams raw ingest chunks, while each pipeline assigns analytical batches by day count or record count
**Rationale**:
- Keeps loading, cleaning, batching, and query execution inside the selected pipeline boundary
- Consistent batch semantics across all implementations through `BatchConfig`
- Easier to test and debug
- Allows either date-window batches or fixed-record batches in the same CLI/API workflow
## 4. Shared Canonical Parser for Java Pipelines
**Decision**: MongoDB, MapReduce jobs, and the local Hive adapter use the same `LogParser.java`; Pig and Hive scripts perform equivalent parsing in `app/pig/etl.pig` and `app/hive/etl.hql`
**Rationale**:
- Eliminates duplicate parsing logic in Java adapters
- Ensures consistency across Java-backed pipelines
- Keeps Pig parsing inside Pig for demonstrations that require pipeline-owned loading and cleaning
- Easier to update parsing rules in one place

## 5. Pipeline Interface as Strategy Pattern
**Decision**: Abstract Pipeline with concrete implementations, PipelineFactory for selection  
**Rationale**:
- Allows runtime pipeline switching
- Follows SOLID Dependency Inversion
- Clean separation of concerns
- Easy to add new pipelines

## 6. Batch Metadata Columns
**Decision**: Store `batch_mode`, generic `batch_size`, and mode-specific `batch_size_days` / `batch_size_records` columns
**Rationale**:
- Keeps the rubric-required batch size explicit
- Makes day and record batching distinguishable in reports
- Avoids overloading a day-count field with record-count values

## 7. PostgreSQL for Results, MongoDB for Raw Logs
**Decision**: Separate stores for different data types  
**Rationale**:
- PostgreSQL: ACID guarantees for run metadata + results
- MongoDB: Schema-flexible for log docs with run_id, batch_id annotations
- Mirrors real architectures (transactional DB + document store)

## 8. Generic k1, k2, m1-m4 Columns in etl_results
**Decision**: One schema accommodates all three queries  
**Rationale**:
- Query 1: k1=log_date, k2=status_code, m1=request_count, m2=total_bytes
- Query 2: k1=resource_path, m1=request_count, m2=total_bytes, m3=distinct_host_count
- Query 3: k1=log_date, k2=log_hour, m1=error_request_count, m2=total_request_count, m4=error_rate
- Avoids N schemas, reduces reporting complexity

## 9. YAML Configuration Over Hardcoding
**Decision**: config.yaml + AppConfig loader  
**Rationale**:
- Non-developers can modify DB credentials
- Supports multiple environments (dev/test/prod)
- Standard practice in Java
- Avoids recompilation for config changes

## 10. Structured Logging with SLF4J
**Decision**: SLF4J facade + Logback implementation  
**Rationale**:
- Standard in Java ecosystem
- Easily swappable backends
- Hierarchical logging levels
- Integrates with DevOps tooling (ELK, Splunk)

## 11. Malformed Record Tracking
**Decision**: Flag as malformed, store reason, count separately  
**Rationale**:
- Preserves original line for debugging
- Counts reported in metadata
- Allows later audit of parse errors
- Non-fatal (doesn't fail run)

## 12. Run ID as UUID
**Decision**: Generate UUID for each run  
**Rationale**:
- Globally unique, no collisions
- Can run in parallel safely
- Humans can use as reference

## 13. Cascading Metrics Calculation
**Decision**: Orchestrator computes avg_batch_size after finalizeRun  
**Rationale**:
- Avoids pipeline needing to know total record count upfront
- Simple formula: total_records / total_batches
- Accurate reflection of actual batching

## 14. Reporter Formats Output by Query Name
**Decision**: Query-specific formatting in Reporter.formatRow()  
**Rationale**:
- Human-readable output for each query type
- Reporter handles interpretation of k1, k2, m1-m4
- Easy to extend with new query formats

## 15. Dockerfile for Services
**Decision**: Use docker-compose.yml with postgres + mongo images  
**Rationale**:
- No native installation needed
- Reproduces production environment
- Data volumes persist between runs
- Health checks ensure readiness

## 16. Checked Exceptions in DbLoader
**Decision**: Throw SQLException; let Controller catch  
**Rationale**:
- Follows JDBC standard
- Caller decides error handling strategy
- Clearer error context propagation

## 17. Aggregation Pipeline Over MapReduce for MongoDB
**Decision**: MongoDB aggregation pipeline (not map-reduce)  
**Rationale**:
- Faster, more efficient
- Modern MongoDB best practice
- Cleaner syntax
- Better debugging support

## 18. Batch Size Configurable, Default 1000
**Decision**: config.yaml default + CLI override  
**Rationale**:
- Balance between memory and I/O efficiency
- Not too small (excessive pipeline calls)
- Not too large (memory pressure)
- CLI allows ad hoc tuning

## 19. No Database Connection Pooling Explicit
**Decision**: Rely on JDBC driver default behavior  
**Rationale**:
- PostgreSQL JDBC handles simple pooling
- Avoids adding HikariCP dependency
- Sufficient for typical ETL volume
- Can add if needed later

## 20. Error Messages and Logging for Observability
**Decision**: Log pipeline events, parse errors, query counts  
**Rationale**:
- Operator visibility into run progress
- Easier troubleshooting
- Standard DevOps practice

## Tradeoffs

- **Simplicity vs Flexibility**: Chose modular design so Pig/MR/Hive can share a common reporting contract while still preserving pipeline-specific execution boundaries
- **Schema Flexibility vs Strictness**: Single etl_results table supports 3+ queries; could create query-specific tables
- **Manual Parse vs Regex**: More code but more maintainable
- **Synchronous Orchestration**: Simple; streaming could improve for very large files

