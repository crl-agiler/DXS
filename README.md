# DXS DPU Streaming Pipeline

Apache Flink streaming pipeline — DXS CDC events → Dremio Iceberg + Palantir CSV delta.

## Project Structure

```
dxs-dpu-pipeline/
├── pom.xml
├── src/
│   ├── main/
│   │   ├── avro/                          # Avro .avsc schema files (place here for code gen)
│   │   ├── java/eu/unicredit/dpu/pipeline/
│   │   │   ├── DxsPipelineJob.java         # Main entry point
│   │   │   ├── config/
│   │   │   │   ├── ConfigLoader.java        # YAML config loader
│   │   │   │   ├── PipelineConfig.java      # Root config POJO
│   │   │   │   ├── EnvironmentConfig.java   # Per-env config
│   │   │   │   ├── KafkaConfig.java
│   │   │   │   ├── SchemaRegistryConfig.java
│   │   │   │   └── FlinkConfig.java
│   │   │   ├── kafka/
│   │   │   │   ├── KafkaPropertiesBuilder.java   # Builds Kafka consumer Properties
│   │   │   │   └── DxsKafkaSourceFactory.java    # Builds Flink KafkaSource
│   │   │   └── schema/
│   │   │       └── SchemaRegistryClientFactory.java
│   │   └── resources/
│   │       ├── pipeline-config.yaml         # Environment configurations
│   │       └── logback.xml
│   └── test/
│       └── java/eu/unicredit/dpu/pipeline/
│           └── ConfigLoaderTest.java
```

## Build

```bash
mvn clean package -DskipTests
```

The Maven Shade plugin produces a fat JAR at:
```
target/dxs-dpu-pipeline-1.0.0-SNAPSHOT.jar
```

## Configuration

The pipeline reads `pipeline-config.yaml` from the classpath by default.
Override with:

| Method | Example |
|--------|---------|
| CLI arg | `--config /opt/config/pipeline-config.yaml` |
| Env var | `PIPELINE_CONFIG_PATH=/opt/config/pipeline-config.yaml` |

Active environment is set via `PIPELINE_ENV` env var (default: `sit`):

```bash
export PIPELINE_ENV=uat
```

## Required Environment Variables

| Variable | Description |
|----------|-------------|
| `PIPELINE_ENV` | Target environment: `sit`, `uat`, `ppd`, `prd` |
| `KAFKA_USERNAME` | SASL username for CKF Kafka cluster |
| `KAFKA_PASSWORD` | SASL password for CKF Kafka cluster |
| `SCHEMA_REGISTRY_USERNAME` | Schema Registry basic-auth user _(optional)_ |
| `SCHEMA_REGISTRY_PASSWORD` | Schema Registry basic-auth password _(optional)_ |

> ⚠️ Never commit credentials to the repository. Inject them via CI/CD secrets or a secrets manager.

## Run locally (SIT)

```bash
export PIPELINE_ENV=sit
export KAFKA_USERNAME=your_user
export KAFKA_PASSWORD=your_password

java -jar target/dxs-dpu-pipeline-1.0.0-SNAPSHOT.jar
```

## Submit to Flink cluster (PRD)

```bash
flink run \
  -p 12 \
  target/dxs-dpu-pipeline-1.0.0-SNAPSHOT.jar \
  --config /opt/config/pipeline-config.yaml
```

## Kafka Topics per Environment

| Env | Topic | Partitions | Consumer Group ID |
|-----|-------|-----------|-------------------|
| SIT | DXSUIWWDPUSIT | 3 | DXS01DXSUIWWDPUSIT |
| UAT | DXSUIWWDPUUAT | 3 | DXS01DXSUIWWDPUUAT |
| PPD | DXSUIWWDPUPPD | 3 | DXS01DXSUIWWDPUPPD |
| PRD | DXSUIWWDPUPRD | 12 | DXS01DXSUIWWDPUPRD |

## Schema Registry Endpoints

| Env | URL |
|-----|-----|
| SIT/DEV | http://ckfdevlsr01.internal.unicreditgroup.eu:8081 |
| UAT | http://ckfqalsr01.internal.unicreditgroup.eu:8081 |
| PPD | http://ckfqalsr01.internal.unicreditgroup.eu:8081 |
| PRD | http://ckfprdlsr01.internal.unicreditgroup.eu:8081 |

## Next Steps (TODO)

- [ ] Add Avro `.avsc` schema file for `DossierTraceinfoEvent` to `src/main/avro/`
- [ ] Implement `DossierTransformFunction` (flatten nested arrays, OUTCOME mapping)
- [ ] Implement `DedupAndHistoryLogFunction` (keyed state RocksDB, history log builder)
- [ ] Implement `IcebergDossierSink` (upsert by uuid to GCS-backed Iceberg)
- [ ] Implement `CsvDeltaSink` (delta accumulator for 7:30 AM export)
- [ ] Confirm batch/streaming cutover offset with DXS team
- [ ] Confirm Kafka exactly-once guarantee with CKF team
