package eu.unicredit.document.dxstraceinfo.it;

import com.datarocks.schemaregistry.test.junit5.SharedSchemaRegistryTestResource;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import eu.unicredit.document.dxstraceinfo.DXSApplication;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.config.MockConfigAppRetriever;
import eu.unicredit.document.dxstraceinfo.credentials.MockCredentialsRetriever;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.instancio.junit.InstancioExtension;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@ExtendWith(InstancioExtension.class)
@Slf4j
class FlinkDSXTraceInfoIT {

  private static final String TEST_TOPIC = "DXSUIWWDPUSIT";
  private static final String AVRO_SUBJECT =
          "DossierTraceinfoEvent-value";

  private static final String AVRO_SCHEMA_PATH =
          "src/test/resources/avro/DossierTraceInfoEvent.avsc";

  private static final int VALID_EVENT_COUNT = 10;
  private static final int INVALID_EVENT_COUNT = 3;

  private static final int TOPIC_PARTITIONS = 1;
  private static final short TOPIC_REPLICATION_FACTOR = 1;

  private static final long CHECKPOINT_INTERVAL_MS = 5_000L;
  private static final long CHECKPOINT_TIMEOUT_MS = 120_000L;

  private static final Duration JOB_OPERATION_TIMEOUT =
          Duration.ofSeconds(30);

  static final TableIdentifier DOSSIER_TABLE =
          TableIdentifier.of("dxs-traceinfo-dossier");

  static final TableIdentifier DOCUMENTS_GROUP_TABLE =
          TableIdentifier.of("dxs-traceinfo-documents-group");

  static final TableIdentifier DOCUMENT_TABLE =
          TableIdentifier.of("dxs-traceinfo-document");

  static final TableIdentifier SIGNER_TABLE =
          TableIdentifier.of("dxs-traceinfo-signer");

  static final TableIdentifier HISTORY_LOG_TABLE =
          TableIdentifier.of("dxs-traceinfo-history-log");

  static final TableIdentifier DISCARD_LOG_TABLE =
          TableIdentifier.of("dxs-traceinfo-discard-log");

  private static final Map<TableIdentifier, Schema> TABLES =
          Map.of(
                  DOSSIER_TABLE,
                  IcebergTestSchemas.dossier(),
                  DOCUMENTS_GROUP_TABLE,
                  IcebergTestSchemas.documentsGroup(),
                  DOCUMENT_TABLE,
                  IcebergTestSchemas.document(),
                  SIGNER_TABLE,
                  IcebergTestSchemas.signer(),
                  HISTORY_LOG_TABLE,
                  IcebergTestSchemas.historyLog(),
                  DISCARD_LOG_TABLE,
                  IcebergTestSchemas.discardLog());

  @RegisterExtension
  @Order(1)
  static final SharedKafkaTestResource sharedKafka =
          new SharedKafkaTestResource()
                  .withBrokers(1)
                  .withBrokerProperty(
                          "transaction.max.timeout.ms",
                          Integer.toString(2 * 60 * 60 * 1_000))
                  .withBrokerProperty(
                          "offsets.topic.num.partitions",
                          "1")
                  .withBrokerProperty(
                          "offsets.topic.replication.factor",
                          "1")
                  .withBrokerProperty(
                          "max.message.bytes",
                          "10485880")
                  .withBrokerProperty(
                          "replica.fetch.max.bytes",
                          "10485880");

  @RegisterExtension
  @Order(2)
  static final SharedSchemaRegistryTestResource schemaRegistry =
          new SharedSchemaRegistryTestResource()
                  .withBootstrapServers(
                          sharedKafka::getKafkaConnectString)
                  .withProperty(
                          SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG,
                          5_000);

  static final MiniClusterWithClientResource flinkCluster =
          new MiniClusterWithClientResource(
                  new MiniClusterResourceConfiguration.Builder()
                          .setNumberSlotsPerTaskManager(4)
                          .setNumberTaskManagers(1)
                          .setConfiguration(
                                  Configuration.fromMap(
                                          Map.of(
                                                  RestOptions.PORT.key(),
                                                  "18081")))
                          .build());

  @TempDir(cleanup = CleanupMode.NEVER)
  static Path catalogDir;

  @TempDir(cleanup = CleanupMode.NEVER)
  static Path checkpointDir;

  private static org.apache.avro.Schema avroSchema;
  private static int schemaId;
  private static Catalog catalog;
  private static CatalogLoader catalogLoader;

  @BeforeAll
  static void beforeAll() throws Exception {
    loadAvroSchema();
    createKafkaTopic();
    registerAvroSchema();
    initializeCatalog();

    flinkCluster.before();
  }

  @AfterAll
  static void afterAll() {
    try {
      flinkCluster.after();
    } finally {
      catalog = null;
      catalogLoader = null;

      log.info(
              "Temporary test directories retained: "
                      + "catalog={}, checkpoints={}",
              catalogDir,
              checkpointDir);
    }
  }

  @Test
  void run() throws Exception {
    DossierTestData testData =
            DossierTestData.generate(
                    VALID_EVENT_COUNT,
                    INVALID_EVENT_COUNT);

    DossierKafkaPublisher publisher =
            new DossierKafkaPublisher(
                    sharedKafka.getKafkaConnectString(),
                    TEST_TOPIC,
                    schemaId);

    publisher.publish(testData.getAllEvents());

    Weld weld =
            new Weld()
                    .enableDiscovery()
                    .addBeanClass(MockCredentialsRetriever.class)
                    .addBeanClass(FlinkTestConfiguration.class)
                    .addBeanClass(TestConfiguration.class)
                    .addAlternative(MockCredentialsRetriever.class)
                    .addAlternative(FlinkTestConfiguration.class)
                    .addAlternative(TestConfiguration.class);

    StreamExecutionEnvironment environment =
            StreamExecutionEnvironment.getExecutionEnvironment();
    try (WeldContainer container = weld.initialize()) {
      DXSApplication application =
              container
                      .select(DXSApplication.class)
                      .get();

      application.execute(environment);

      ConfigApp configApp =
              container
                      .select(ConfigApp.class)
                      .get();

      JobClient jobClient =
              environment.executeAsync(
                      configApp
                              .getFlinkConfig()
                              .getJobName());

      IcebergTestVerifier verifier =
              new IcebergTestVerifier(
                      catalog,
                      Duration.ofMinutes(2),
                      Duration.ofSeconds(5),
                      JOB_OPERATION_TIMEOUT);

      try {
        verifier.awaitExpectedRecords(
                jobClient,
                testData.getExpectedRecordCounts());

        verifier.assertBusinessTables(
                testData.getExpectedRecordCounts());

        verifier.assertDiscardTable(
                testData.getInvalidEvents());

        verifier.dumpTables(TABLES.keySet());
      } finally {
        cancelJobIfRunning(jobClient);
      }
    }
  }

  private static void loadAvroSchema() throws IOException {
    Path schemaPath = Path.of(AVRO_SCHEMA_PATH);

    avroSchema =
            new org.apache.avro.Schema.Parser()
                    .parse(Files.readString(schemaPath));

    log.info(
            "Loaded Avro schema {} from {}",
            avroSchema.getFullName(),
            schemaPath);
  }

  private static void createKafkaTopic() {
    sharedKafka
            .getKafkaTestUtils()
            .createTopic(
                    TEST_TOPIC,
                    TOPIC_PARTITIONS,
                    TOPIC_REPLICATION_FACTOR);

    log.info("Created Kafka topic {}", TEST_TOPIC);
  }

  private static void registerAvroSchema()
          throws Exception {

    schemaRegistry
            .schemaRegistryTestUtils()
            .schemaRegistryClient()
            .register(
                    AVRO_SUBJECT,
                    avroSchema);

    schemaId =
            schemaRegistry
                    .schemaRegistryTestUtils()
                    .schemaRegistryClient()
                    .getId(
                            AVRO_SUBJECT,
                            avroSchema);

    log.info(
            "Registered Avro schema subject={} id={}",
            AVRO_SUBJECT,
            schemaId);
  }

  private static void initializeCatalog()
          throws IOException {

    Files.createDirectories(catalogDir);

    catalogLoader =
            CatalogLoader.hadoop(
                    "hadoop",
                    new org.apache.hadoop.conf.Configuration(),
                    Collections.singletonMap(
                            "warehouse",
                            warehouseLocation()));

    catalog = catalogLoader.loadCatalog();

    TABLES.forEach(
            (identifier, schema) -> {
              catalog.createTable(identifier, schema);

              log.info(
                      "Created Iceberg table {}",
                      identifier);
            });
  }

  private void cancelJobIfRunning(
          JobClient jobClient) {

    try {
      JobStatus status =
              jobClient
                      .getJobStatus()
                      .get(
                              JOB_OPERATION_TIMEOUT.toSeconds(),
                              TimeUnit.SECONDS);

      if (status.isGloballyTerminalState()) {
        log.info(
                "Flink job {} already terminated with status {}",
                jobClient.getJobID(),
                status);

        return;
      }

      log.info(
              "Cancelling Flink job {} with status {}",
              jobClient.getJobID(),
              status);

      jobClient
              .cancel()
              .get(
                      JOB_OPERATION_TIMEOUT.toSeconds(),
                      TimeUnit.SECONDS);

    } catch (Exception exception) {
      log.warn(
              "Unable to cancel Flink test job {} cleanly",
              jobClient.getJobID(),
              exception);
    }
  }

  @Alternative
  public static class TestConfiguration {

    @Produces
    @Singleton
    public CatalogLoader catalogLoader() {
      return Objects.requireNonNull(
              FlinkDSXTraceInfoIT.catalogLoader,
              "catalogLoader has not been initialized");
    }

    @Produces
    @Singleton
    public ConfigApp configApp() {
      return MockConfigAppRetriever.builder()
              .schemaRegistryUrl(
                      schemaRegistry.schemaRegistryUrl())
              .kafkaBootstrapServers(
                      sharedKafka.getKafkaConnectString())
              .catalogLocation(
                      warehouseLocation())
              .baseConfigPath(
                      "configs/config.yaml")
              .envConfigPath(
                      "configs/test/config.yaml")
              .bucket("")
              .build()
              .getConfig();
    }
  }

  private static String warehouseLocation() {
    return catalogDir
            .toAbsolutePath()
            .normalize()
            .toString();
  }
}