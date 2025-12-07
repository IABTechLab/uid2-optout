package com.uid2.optout.vertx;

import com.uid2.optout.delta.DeltaProductionJobStatus;
import com.uid2.optout.delta.StopReason;

import io.vertx.core.json.JsonObject;

/**
 * Immutable result containing statistics from a delta production job.
 * 
 * <p>This class holds production counts and the stop reason, with methods for JSON serialization.
 * Use {@link Builder} to accumulate statistics during production, then call {@link Builder#build()}
 * to create the immutable result.</p>
 * 
 * <p>Note: Job duration is tracked by {@link DeltaProductionJobStatus}, not this class.</p>
 */
public class DeltaProductionResult {
    private final int deltasProduced;
    private final int entriesProcessed;
    private final int droppedRequestFilesProduced;
    private final int droppedRequestsProcessed;
    private final StopReason stopReason;

    /**
     * Private constructor. Use {@link #builder()} to create instances.
     */
    private DeltaProductionResult(int deltasProduced, int entriesProcessed, 
                                  int droppedRequestFilesProduced, int droppedRequestsProcessed, 
                                  StopReason stopReason) {
        this.deltasProduced = deltasProduced;
        this.entriesProcessed = entriesProcessed;
        this.droppedRequestFilesProduced = droppedRequestFilesProduced;
        this.droppedRequestsProcessed = droppedRequestsProcessed;
        this.stopReason = stopReason;
    }

    /**
     * Creates a new Builder for accumulating production statistics.
     */
    public static Builder builder() {
        return new Builder();
    }

    // ==================== Getters ====================

    public int getDeltasProduced() {
        return deltasProduced;
    }

    public int getEntriesProcessed() {
        return entriesProcessed;
    }

    public int getDroppedRequestFilesProduced() {
        return droppedRequestFilesProduced;
    }

    public int getDroppedRequestsProcessed() {
        return droppedRequestsProcessed;
    }

    public StopReason getStopReason() {
        return stopReason;
    }

    // ==================== JSON Serialization ====================

    public JsonObject toJson() {
        return new JsonObject()
            .put("deltas_produced", deltasProduced)
            .put("entries_processed", entriesProcessed)
            .put("dropped_request_files_produced", droppedRequestFilesProduced)
            .put("dropped_requests_processed", droppedRequestsProcessed)
            .put("stop_reason", stopReason.name());
    }

    public JsonObject toJsonWithStatus(String status) {
        return toJson().put("status", status);
    }

    @Override
    public String toString() {
        return String.format(
            "DeltaProductionResult{deltasProduced=%d, entriesProcessed=%d, " +
            "droppedRequestFilesProduced=%d, droppedRequestsProcessed=%d, stopReason=%s}",
            deltasProduced, entriesProcessed, droppedRequestFilesProduced, 
            droppedRequestsProcessed, stopReason);
    }

    // ==================== Builder ====================

    /**
     * Mutable builder for accumulating production statistics.
     * 
     * <p>Use this builder to track stats during delta production jobs,
     * then call {@link #build()} to create the immutable result.</p>
     */
    public static class Builder {
        private int deltasProduced;
        private int entriesProcessed;
        private int droppedRequestFilesProduced;
        private int droppedRequestsProcessed;
        private StopReason stopReason = StopReason.NONE;

        public Builder incrementDeltasProduced() {
            deltasProduced++;
            return this;
        }

        public Builder incrementEntriesProcessed(int count) {
            entriesProcessed += count;
            return this;
        }

        public Builder incrementDroppedRequestFilesProduced() {
            droppedRequestFilesProduced++;
            return this;
        }

        public Builder incrementDroppedRequestsProcessed(int count) {
            droppedRequestsProcessed += count;
            return this;
        }

        public Builder stopReason(StopReason reason) {
            this.stopReason = reason;
            return this;
        }

        /**
         * Builds the DeltaProductionResult with the accumulated statistics.
         */
        public DeltaProductionResult build() {
            return new DeltaProductionResult(
                    deltasProduced, 
                    entriesProcessed, 
                    droppedRequestFilesProduced, 
                    droppedRequestsProcessed, 
                    stopReason);
        }
    }
}
