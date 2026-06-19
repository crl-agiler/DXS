package eu.unicredit.dpu.pipeline.config;

import lombok.Data;

/**
 * Apache Flink job tuning configuration for a single environment.
 */
@Data
public class FlinkConfig {

    /** Job-level parallelism — must equal the number of Kafka partitions. */
    private int parallelism;

    /** Checkpoint interval in milliseconds. */
    private long checkpointIntervalMs = 30000L;

    /** Maximum time allowed for a single checkpoint to complete. */
    private long checkpointTimeoutMs = 60000L;

    /** Minimum pause between two consecutive checkpoint triggers. */
    private long minPauseBetweenCheckpointsMs = 10000L;

    /**
     * State backend type.
     * Values: rocksdb | hashmap
     * RocksDB is required for keyed state at production scale.
     */
    private String stateBackend = "rocksdb";

    /** GCS path where checkpoint data is written (e.g. gs://bucket/checkpoints). */
    private String checkpointDir;
}
