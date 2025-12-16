package com.uid2.optout.delta;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;

/**
 * Metrics counters for delta production operations.
 * 
 * Tracks:
 * - Number of delta files produced
 * - Number of opt-out entries processed
 * - Number of dropped request files produced
 * - Number of dropped requests processed
 */
public class DeltaProductionMetrics {
    
    private final Counter deltasProduced;
    private final Counter entriesProcessed;
    private final Counter droppedRequestFilesProduced;
    private final Counter droppedRequestsProcessed;

    public DeltaProductionMetrics() {
        this.deltasProduced = Counter
            .builder("uid2_optout_sqs_delta_produced_total")
            .description("counter for how many optout delta files are produced from SQS")
            .register(Metrics.globalRegistry);

        this.entriesProcessed = Counter
            .builder("uid2_optout_sqs_entries_processed_total")
            .description("counter for how many optout entries are processed from SQS")
            .register(Metrics.globalRegistry);

        this.droppedRequestFilesProduced = Counter
            .builder("uid2_optout_sqs_dropped_request_files_produced_total")
            .description("counter for how many optout dropped request files are produced from SQS")
            .register(Metrics.globalRegistry);

        this.droppedRequestsProcessed = Counter
            .builder("uid2_optout_sqs_dropped_requests_processed_total")
            .description("counter for how many optout dropped requests are processed from SQS")
            .register(Metrics.globalRegistry);
    }

    /**
     * Record that a delta file was produced with the given number of entries.
     */
    public void recordDeltaProduced(int entryCount) {
        deltasProduced.increment();
        entriesProcessed.increment(entryCount);
    }

    /**
     * Record that a dropped requests file was produced with the given number of entries.
     */
    public void recordDroppedRequestsProduced(int requestCount) {
        droppedRequestFilesProduced.increment();
        droppedRequestsProcessed.increment(requestCount);
    }
}

