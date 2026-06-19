package eu.unicredit.dpu.pipeline.config;

import lombok.Data;

/**
 * Kafka connection and consumer configuration for a single environment.
 */
@Data
public class KafkaConfig {

    /** Comma-separated list of broker addresses. */
    private String bootstrapServers;

    /** Target Kafka topic name. */
    private String topic;

    /** Consumer group ID assigned by CKF for this environment. */
    private String consumerGroupId;

    /** LDAP group name for access control. */
    private String ldapGroup;

    /** Number of partitions configured on the topic. */
    private int partitions;

    /**
     * Offset reset strategy when no committed offset exists.
     * Values: earliest | latest
     */
    private String autoOffsetReset = "earliest";

    /** Session timeout in milliseconds. */
    private int sessionTimeoutMs = 30000;

    /** Heartbeat interval in milliseconds. */
    private int heartbeatIntervalMs = 10000;

    /** Maximum number of records returned per poll. */
    private int maxPollRecords = 500;

    /**
     * Security protocol for broker communication.
     * Expected: SASL_SSL for UniCredit CKF cluster.
     */
    private String securityProtocol = "SASL_SSL";

    /**
     * SASL mechanism.
     * Expected: SCRAM-SHA-512 for UniCredit CKF cluster.
     */
    private String saslMechanism = "SCRAM-SHA-512";
}
