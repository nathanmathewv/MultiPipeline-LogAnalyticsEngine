# Setup Instructions

## Local Development Setup

### 1. Prerequisites
- Java 17 or higher
- Maven 3.8+
- Docker and Docker Compose
- Git

### 2. Clone and Navigate
```bash
git clone <repository>
cd MultiPipeline-LogAnalyticsEngine
```

### 3. Start Docker Services
```bash
docker-compose up -d
```

This starts:
- PostgreSQL 15 (port 5432): etl/secret
- MongoDB 6.0 (port 27017): no auth by default

Health check:
```bash
docker-compose ps
# All services should be "healthy" after ~15 seconds
```

### 4. Build Project
```bash
mvn clean install -DskipTests
```

Dependencies will be downloaded (~150MB first time).

### 5. Initialize Database Schema
PostgreSQL schema is auto-loaded via docker-entrypoint-initdb.d. Verify:
```bash
docker exec etl_postgres psql -U etl -d etl_results -c "\dt"
```

Expected output:
```
              List of relations
 Schema |      Name      | Type  | Owner
--------+----------------+-------+-------
 public | etl_results    | table | etl
 public | run_metadata   | table | etl
```

### 6. Run Sample ETL
```bash
java -cp target/classes:target/lib/* com.example.multietl.cli.Main mongodb data/raw/sample.log 5
```

Expected runtime: 1-3 seconds

### 7. Verify Results
Check PostgreSQL:
```bash
docker exec etl_postgres psql -U etl -d etl_results -c "SELECT run_id, pipeline_name FROM run_metadata ORDER BY created_at DESC LIMIT 1;"
```

Check MongoDB:
```bash
docker exec etl_mongo mongosh --eval "use web_logs; db.parsed_logs.count()"
```

## Clean Up
```bash
docker-compose down
docker volume rm multipipe... (optional, removes data)
```

## IDE Setup (IntelliJ IDEA)

1. Open project as Maven project
2. Right-click pom.xml → Maven → Reimport
3. Set JDK to 17+: File → Project Structure → SDK
4. Run → Edit Configurations → Add Maven:
   - Working directory: project root
   - Command: `clean test`

## IDE Setup (VS Code)

1. Install Extension Pack for Java
2. Open project folder
3. Maven automatically detects pom.xml
4. View → Command Palette → Maven: Generate from archetype (optional)

## Troubleshooting

### Docker containers not starting
```bash
docker-compose logs
docker-compose down && docker-compose up -d --remove-orphans
```

### "Connection refused"
Wait 15-20 seconds for health checks. Check logs:
```bash
docker-compose logs postgres
docker-compose logs mongodb
```

### Compilation errors
```bash
mvn clean install
rm -rf ~/.m2/repository/com/example
mvn clean -U install
```

### Test failures
```bash
mvn clean test -e
```

### Port conflicts (5432 or 27017 in use)
Edit docker-compose.yml ports section or kill existing services:
```bash
lsof -i :5432
kill -9 <PID>
```

## Development Workflow

1. **Make code changes**
2. **Compile**: `mvn compile`
3. **Test**: `mvn test`
4. **Run**: `java -cp target/classes:target/lib/* com.example.multietl.cli.Main [args]`
5. **Check results**:
   - PostgreSQL: `docker exec etl_postgres psql -U etl -d etl_results -c "SELECT * FROM run_metadata;"`
   - MongoDB: `docker exec etl_mongo mongosh`
6. **Package**: `mvn package` (creates JAR in target/)

## Performance Testing

### Large file (1M lines)
```bash
# Generate test file
seq 1 1000000 | awk '{print "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /resource"NR" HTTP/1.0\" 200 1024"}' > data/raw/large.log

# Run with larger batch
java -cp target/classes:target/lib/* com.example.multietl.cli.Main mongodb data/raw/large.log 10000
```

## Next Steps

1. Read [architecture.md](docs/architecture.md) for component overview
2. Read [design_decisions.md](docs/design_decisions.md) for design choices
3. Explore MongoDB queries in `MongoPipeline.java`
4. Implement a custom pipeline by extending `Pipeline` interface
5. Add more sample logs in `data/raw/`

