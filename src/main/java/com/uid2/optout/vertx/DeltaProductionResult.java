package com.uid2.optout.vertx;

import io.vertx.core.json.JsonObject;

/**
 * Result object containing statistics from delta production.
 */
public class DeltaProductionResult {
    private final int deltasProduced;
    private final int entriesProcessed;
    private final int droppedRequestFilesProduced;
    private final int droppedRequestsProcessed;

    public DeltaProductionResult(int deltasProduced, int entriesProcessed, int droppedRequestFilesProduced, int droppedRequestsProcessed) {
        this.deltasProduced = deltasProduced;
        this.entriesProcessed = entriesProcessed;
        this.droppedRequestFilesProduced = droppedRequestFilesProduced;
        this.droppedRequestsProcessed = droppedRequestsProcessed;
    }

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

    public JsonObject encodeSuccessResult() {
        return new JsonObject()
            .put("status", "success")
            .put("deltas_produced", deltasProduced)
            .put("entries_processed", entriesProcessed)
            .put("dropped_request_files_produced", droppedRequestFilesProduced)
            .put("dropped_requests_processed", droppedRequestsProcessed);
    }

    public static JsonObject createSkippedResult(String reason) {
        return new JsonObject()
            .put("status", "skipped")
            .put("reason", reason)
            .put("deltas_produced", 0)
            .put("entries_processed", 0)
            .put("dropped_request_files_produced", 0)
            .put("dropped_requests_processed", 0);
    }
}

