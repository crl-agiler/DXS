package eu.unicredit.dpu.pipeline;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import eu.unicredit.dpu.pipeline.config.ConfigLoader;
import eu.unicredit.dpu.pipeline.config.EnvironmentConfig;
import eu.unicredit.dpu.pipeline.config.KafkaConfig;
import eu.unicredit.dpu.pipeline.config.PipelineConfig;
import eu.unicredit.dpu.pipeline.config.SchemaRegistryConfig;
import eu.unicredit.dpu.pipeline.kafka.KafkaPropertiesBuilder;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test — DXS DPU Streaming Pipeline.
 *
 * <p>Infrastructure (no Docker, no Scala internals):
 * <ul>
 *   <li>{@link SharedKafkaTestResource} (kafka-junit5) — embedded Kafka broker,
 *       in-process, pure Java API, started/stopped automatically via
 *       {@code @RegisterExtension}</li>
 *   <li>{@link WireMockServer} — mock Confluent Schema Registry over HTTP</li>
 *   <li>{@link MiniClusterWithClientResource} — in-process Flink cluster</li>
 * </ul>
 *
 * <p>Credentials: the embedded broker uses {@code PLAINTEXT}, so the SASL
 * branch of {@link KafkaPropertiesBuilder} is naturally skipped. Where a
 * test needs to exercise the SASL_SSL / missing-credentials code path
 * (see {@link #shouldThrowWhenKafkaCredentialsAreMissing()}), an
 * {@link EnvironmentConfig} is built by hand instead of going through
 * {@link #loadTestConfig()} (which always points to the embedded PLAINTEXT
 * broker) — this avoids ever needing to mock {@code System.class}, which
 * Mockito forbids.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DxsPipelineIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(DxsPipelineIntegrationTest.class);

    // ── Constants ─────────────────────────────────────────────────────────────
    private static final String TEST_TOPIC       = "DXSUIWWDPUSIT";
    private static final int    TOPIC_PARTITIONS = 3;
    private static final int    EVENTS_TO_SEND   = 5;
    private static final int    SCHEMA_ID        = 1;

    // ── Embedded Kafka (kafka-junit5) ─────────────────────────────────────────
    // Starts/stops automatically for this test class. No ZooKeeper management,
    // no kafka.server.KafkaServer$ — pure Java API.
    @RegisterExtension
    static final SharedKafkaTestResource sharedKafka = new SharedKafkaTestResource()
            .withBrokers(1);

    // ── Other infrastructure ──────────────────────────────────────────────────
    private static WireMockServer mockSchemaRegistry;
    private static MiniClusterWithClientResource flinkCluster;
    private static int schemaRegistryPort;

    // ── Avro schema ───────────────────────────────────────────────────────────
    private static Schema DOSSIER_SCHEMA;

    // ── Sink output ───────────────────────────────────────────────────────────
    static final List<GenericRecord> RECEIVED_RECORDS = new CopyOnWriteArrayList<>();
    static final AtomicInteger       RECEIVED_COUNT   = new AtomicInteger(0);

    // ─────────────────────────────────────────────────────────────────────────
    // SETUP / TEARDOWN
    // ─────────────────────────────────────────────────────────────────────────

    @BeforeAll
    static void startInfrastructure() throws Exception {

        // 1. Simplified Avro schema matching DossierTraceinfoEvent's top-level fields
        DOSSIER_SCHEMA = SchemaBuilder.record("DossierTraceinfoEvent")
                .namespace("eu.unicredit.dxs")
                .fields()
                .requiredString("uuid")
                .requiredString("eventType")
                .requiredLong("eventTimestamp")
                .requiredLong("dossierId")
                .requiredString("status")
                .optionalString("subStatus")
                .requiredString("applicationCode")
                .requiredString("bankCode")
                .endRecord();

        // 2. Create test topic — sharedKafka broker is already up via @RegisterExtension
        sharedKafka.getKafkaTestUtils()
                .createTopic(TEST_TOPIC, TOPIC_PARTITIONS, (short) 1);
        LOG.info("Topic '{}' created — brokers: {}", TEST_TOPIC, sharedKafka.getKafkaConnectString());

        // 3. Mock Schema Registry (WireMock)
        schemaRegistryPort = findFreePort();
        mockSchemaRegistry = new WireMockServer(
                WireMockConfiguration.wireMockConfig().port(schemaRegistryPort));
        mockSchemaRegistry.start();
        stubSchemaRegistry(mockSchemaRegistry, DOSSIER_SCHEMA, SCHEMA_ID);
        LOG.info("Mock Schema Registry on port {}", schemaRegistryPort);

        // 4. Flink MiniCluster
        flinkCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(TOPIC_PARTITIONS)
                        .setNumberTaskManagers(1)
                        .build());
        flinkCluster.before();
        LOG.info("Flink MiniCluster started — {} slots", TOPIC_PARTITIONS);
    }

    @AfterAll
    static void stopInfrastructure() {
        try { if (flinkCluster       != null) flinkCluster.after();      } catch (Exception e) { LOG.warn("Flink stop", e); }
        try { if (mockSchemaRegistry != null) mockSchemaRegistry.stop(); } catch (Exception e) { LOG.warn("WireMock stop", e); }
        // sharedKafka stopped automatically by @RegisterExtension
    }

    @BeforeEach
    void resetCollectors() {
        RECEIVED_RECORDS.clear();
        RECEIVED_COUNT.set(0);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // TEST CASES
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Publishes N Avro events, consumes them via the Flink KafkaSource,
     * and verifies field values end to end.
     */
    @Test
    @Order(1)
    void shouldConsumeEventsFromKafkaAndDeserialiseAvro() throws Exception {

        List<GenericRecord> published = publishTestEvents(EVENTS_TO_SEND);
        LOG.info("Published {} events to '{}'", published.size(), TEST_TOPIC);

        StreamExecutionEnvironment env    = buildTestFlinkEnv();
        KafkaSource<GenericRecord> source = buildTestKafkaSource();

        DataStream<GenericRecord> stream = env.fromSource(
                source,
                WatermarkStrategy
                        .<GenericRecord>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((record, ts) -> (Long) record.get("eventTimestamp")),
                "Test DXS Kafka Source");

        // Bounded source — stops after reading all messages currently in the topic
        List<GenericRecord> results = new ArrayList<>();
        try (CloseableIterator<GenericRecord> it = stream.executeAndCollect("integration-test")) {
            it.forEachRemaining(results::add);
        }

        RECEIVED_RECORDS.addAll(results);
        RECEIVED_COUNT.addAndGet(results.size());

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(RECEIVED_COUNT.get()).isEqualTo(EVENTS_TO_SEND));

        assertThat(RECEIVED_RECORDS).hasSize(EVENTS_TO_SEND);

        for (int i = 0; i < EVENTS_TO_SEND; i++) {
            GenericRecord received = RECEIVED_RECORDS.get(i);
            GenericRecord expected = published.get(i);
            assertThat(received.get("uuid").toString()).isEqualTo(expected.get("uuid").toString());
            assertThat(received.get("dossierId")).isEqualTo(expected.get("dossierId"));
            assertThat(received.get("status").toString()).isEqualTo(expected.get("status").toString());
            assertThat(received.get("applicationCode").toString()).isEqualTo(expected.get("applicationCode").toString());
            assertThat(received.get("bankCode").toString()).isEqualTo(expected.get("bankCode").toString());
        }

        LOG.info("All {} events received and validated.", EVENTS_TO_SEND);
    }

    /**
     * Verifies event_type sequence: first event INSERT, subsequent events UPDATE.
     */
    @Test
    @Order(2)
    void shouldHaveCorrectEventTypeSequence() throws Exception {
        List<GenericRecord> events = publishTestEvents(3);

        assertThat(events.get(0).get("eventType").toString()).isEqualTo("INSERT");
        assertThat(events.get(1).get("eventType").toString()).isEqualTo("UPDATE");
        assertThat(events.get(2).get("eventType").toString()).isEqualTo("UPDATE");
    }

    /**
     * Verifies that KafkaPropertiesBuilder throws when SASL credentials are
     * required (security.protocol = SASL_SSL) but absent from the EnvProvider.
     *
     * <p>Built by hand instead of via {@link #loadTestConfig()}, which always
     * points every environment at the embedded PLAINTEXT broker — under
     * PLAINTEXT the SASL branch is skipped entirely, so this test would
     * never throw if it reused that helper.
     */
    @Test
    @Order(3)
    void shouldThrowWhenKafkaCredentialsAreMissing() {
        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.setBootstrapServers("localhost:9092");
        kafkaConfig.setTopic(TEST_TOPIC);
        kafkaConfig.setConsumerGroupId("DXS01" + TEST_TOPIC);
        kafkaConfig.setSecurityProtocol("SASL_SSL"); // forces the SASL branch
        kafkaConfig.setSaslMechanism("SCRAM-SHA-512");

        SchemaRegistryConfig srConfig = new SchemaRegistryConfig();
        srConfig.setUrl("http://localhost:8081");

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setKafka(kafkaConfig);
        envConfig.setSchemaRegistry(srConfig);

        // EnvProvider that resolves no credentials at all
        KafkaPropertiesBuilder builder = new KafkaPropertiesBuilder(envConfig, name -> null);

        Assertions.assertThrows(
                IllegalStateException.class,
                builder::buildConsumerProperties,
                "Expected IllegalStateException when KAFKA_USERNAME is missing");
    }

    /**
     * Verifies ConfigLoader resolves all 4 environments and SIT fields are correct.
     */
    @Test
    @Order(4)
    void shouldLoadConfigAndResolveAllEnvironments() {
        PipelineConfig config = loadTestConfig();

        assertThat(config.getEnvironments()).containsKeys("sit", "uat", "ppd", "prd");

        EnvironmentConfig sit = config.getEnvironments().get("sit");
        assertThat(sit.getKafka().getTopic()).isEqualTo("DXSUIWWDPUSIT");
        assertThat(sit.getKafka().getConsumerGroupId()).isEqualTo("DXS01DXSUIWWDPUSIT");
        assertThat(sit.getKafka().getPartitions()).isEqualTo(3);
        assertThat(sit.getFlink().getParallelism()).isEqualTo(3);
    }

    /**
     * Verifies Flink parallelism equals Kafka partition count for every environment.
     */
    @Test
    @Order(5)
    void flinkParallelismShouldMatchPartitionCount() {
        PipelineConfig config = loadTestConfig();
        config.getEnvironments().forEach((env, cfg) ->
                assertThat(cfg.getFlink().getParallelism())
                        .as("parallelism == partitions for env '%s'", env)
                        .isEqualTo(cfg.getKafka().getPartitions()));
    }

    /**
     * Smoke test — mock Schema Registry responds on /subjects and /schemas/ids/{id}.
     */
    @Test
    @Order(6)
    void shouldRespondCorrectlyOnSchemaRegistryEndpoints() throws Exception {
        String base = "http://localhost:" + schemaRegistryPort;

        java.net.HttpURLConnection subjects =
                (java.net.HttpURLConnection) new java.net.URL(base + "/subjects").openConnection();
        assertThat(subjects.getResponseCode()).isEqualTo(200);

        java.net.HttpURLConnection schema =
                (java.net.HttpURLConnection) new java.net.URL(base + "/schemas/ids/" + SCHEMA_ID).openConnection();
        assertThat(schema.getResponseCode()).isEqualTo(200);

        LOG.info("Mock Schema Registry verified on port {}", schemaRegistryPort);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HELPERS — Flink
    // ─────────────────────────────────────────────────────────────────────────

    private StreamExecutionEnvironment buildTestFlinkEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(TOPIC_PARTITIONS);
        env.disableOperatorChaining(); // easier debugging
        return env;
    }

    private KafkaSource<GenericRecord> buildTestKafkaSource() {
        String bootstrapServers  = sharedKafka.getKafkaConnectString();
        String schemaRegistryUrl = "http://localhost:" + schemaRegistryPort;

        Properties props = new Properties();
        props.setProperty("bootstrap.servers",  bootstrapServers);
        props.setProperty("group.id",           "test-consumer-group");
        props.setProperty("auto.offset.reset",  "earliest");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("security.protocol",  "PLAINTEXT");

        return KafkaSource.<GenericRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(TEST_TOPIC)
                .setGroupId("test-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forGeneric(DOSSIER_SCHEMA, schemaRegistryUrl))
                .setProperties(props)
                // Bounded — stops after consuming all messages present at start
                .setBounded(OffsetsInitializer.latest())
                .build();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HELPERS — Avro publishing
    // ─────────────────────────────────────────────────────────────────────────

    private List<GenericRecord> publishTestEvents(int count) throws Exception {
        String bootstrapServers = sharedKafka.getKafkaConnectString();

        Properties producerProps = new Properties();
        producerProps.setProperty(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.setProperty(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        producerProps.setProperty(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        producerProps.setProperty(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");

        List<GenericRecord> published = new ArrayList<>();

        try (org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> producer =
                     new org.apache.kafka.clients.producer.KafkaProducer<>(producerProps)) {
            for (int i = 0; i < count; i++) {
                GenericRecord record = buildDossierEvent(i);
                byte[] avroBytes    = serializeWithSchemaId(record, DOSSIER_SCHEMA, SCHEMA_ID);
                String partitionKey = String.valueOf(1000000L + i);

                producer.send(
                        new org.apache.kafka.clients.producer.ProducerRecord<>(TEST_TOPIC, partitionKey, avroBytes)
                ).get(5, TimeUnit.SECONDS);

                published.add(record);
                LOG.debug("Published event {} — dossierId={}, eventType={}",
                        i, record.get("dossierId"), record.get("eventType"));
            }
            producer.flush();
        }
        return published;
    }

    private GenericRecord buildDossierEvent(int index) {
        GenericRecord record = new GenericData.Record(DOSSIER_SCHEMA);
        record.put("uuid",            UUID.randomUUID().toString());
        record.put("eventType",       index == 0 ? "INSERT" : "UPDATE");
        record.put("eventTimestamp",  Instant.now().toEpochMilli());
        record.put("dossierId",       1000000L + index);
        record.put("status",          "PROCESSING");
        record.put("subStatus",       null);
        record.put("applicationCode", "ELD");
        record.put("bankCode",        "02008");
        return record;
    }

    /**
     * Confluent wire format: [0x00 magic byte][4 bytes schema ID big-endian][Avro binary payload]
     */
    private byte[] serializeWithSchemaId(GenericRecord record, Schema schema, int schemaId) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x00);
        out.write((schemaId >> 24) & 0xFF);
        out.write((schemaId >> 16) & 0xFF);
        out.write((schemaId >>  8) & 0xFF);
        out.write( schemaId        & 0xFF);
        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HELPERS — Schema Registry stubs
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Stubs WireMock to respond like a real Confluent Schema Registry.
     *
     * <p>Uses {@code urlPathEqualTo} (not {@code urlEqualTo}) because the
     * Confluent 7.6 client appends query parameters to the schema-by-id
     * lookup (e.g. {@code ?fetchMaxId=false&subject=}) — an exact-match
     * stub would never match the real request URL.
     */
    private static void stubSchemaRegistry(WireMockServer server, Schema schema, int schemaId) {
        String schemaJson = schema.toString().replace("\"", "\\\"");

        server.stubFor(get(urlPathEqualTo("/subjects"))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"DossierTraceinfoEvent-value\"]")));

        server.stubFor(get(urlPathEqualTo("/schemas/ids/" + schemaId))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                        .withBody("{\"schema\":\"" + schemaJson + "\"}")));

        server.stubFor(get(urlPathEqualTo("/subjects/DossierTraceinfoEvent-value/versions/latest"))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                        .withBody("{\"subject\":\"DossierTraceinfoEvent-value\"," +
                                "\"version\":1,\"id\":" + schemaId + "," +
                                "\"schema\":\"" + schemaJson + "\"}")));

        server.stubFor(get(urlPathMatching("/subjects/DossierTraceinfoEvent-value/versions/.*"))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                        .withBody("{\"subject\":\"DossierTraceinfoEvent-value\"," +
                                "\"version\":1,\"id\":" + schemaId + "," +
                                "\"schema\":\"" + schemaJson + "\"}")));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HELPERS — Config
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Loads the real YAML config and overrides every environment to point
     * at the embedded PLAINTEXT broker — convenient for structural tests
     * (env count, topic names, parallelism), but NOT suitable for testing
     * the SASL/credentials code path (see {@link #shouldThrowWhenKafkaCredentialsAreMissing()}).
     */
    private PipelineConfig loadTestConfig() {
        try {
            PipelineConfig config = new ConfigLoader().load();
            String bootstrapServers = sharedKafka.getKafkaConnectString();
            config.getEnvironments().forEach((env, envConfig) -> {
                envConfig.getKafka().setBootstrapServers(bootstrapServers);
                envConfig.getKafka().setSecurityProtocol("PLAINTEXT");
                envConfig.getSchemaRegistry().setUrl("http://localhost:" + schemaRegistryPort);
            });
            return config;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load test config", e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HELPERS — Free port
    // ─────────────────────────────────────────────────────────────────────────

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
