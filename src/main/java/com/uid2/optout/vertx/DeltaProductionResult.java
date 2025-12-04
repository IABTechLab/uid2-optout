package com.uid2.optout.vertx;

import io.vertx.core.json.JsonObject;

/**
 * Data class containing statistics from delta production.
 * 
 * This class holds the counts and provides JSON encoding methods.
 * API response status is determined by the caller based on these statistics.
 */
public class DeltaProductionResult {
    private final int deltasProduced;
    private final int entriesProcessed;
    private final int droppedRequestFilesProduced;
    private final int droppedRequestsProcessed;
    
    /* 
     * indicates that there are still messages in the queue, however,
     * not enough time has elapsed to produce a delta file.
     * We produce in batches of (5 minutes) 
     */
    private final boolean stoppedDueToRecentMessages;

    public DeltaProductionResult(int deltasProduced, int entriesProcessed, int droppedRequestFilesProduced, int droppedRequestsProcessed, boolean stoppedDueToRecentMessages) {
        this.deltasProduced = deltasProduced;
        this.entriesProcessed = entriesProcessed;
        this.droppedRequestFilesProduced = droppedRequestFilesProduced;
        this.droppedRequestsProcessed = droppedRequestsProcessed;
        this.stoppedDueToRecentMessages = stoppedDueToRecentMessages;
    }

    public int getDeltasProduced() {
        return deltasProduced;
    }

    public int getEntriesProcessed() {
        return entriesProcessed;
    }

    public boolean stoppedDueToRecentMessages() {
        return stoppedDueToRecentMessages;
    }

    public int getDroppedRequestFilesProduced() {
        return droppedRequestFilesProduced;
    }

    public int getDroppedRequestsProcessed() {
        return droppedRequestsProcessed;
    }

    /**
     * Convert to JSON with just the production counts.
     */
    public JsonObject toJson() {
        return new JsonObject()
            .put("deltas_produced", deltasProduced)
            .put("entries_processed", entriesProcessed)
            .put("dropped_request_files_produced", droppedRequestFilesProduced)
            .put("dropped_requests_processed", droppedRequestsProcessed);
    }

    /**
     * Convert to JSON with status and counts.
     */
    public JsonObject toJsonWithStatus(String status) {
        return toJson().put("status", status);
    }

    /**
     * Convert to JSON with status, reason/error, and counts.
     */
    public JsonObject toJsonWithStatus(String status, String reasonKey, String reasonValue) {
        return toJsonWithStatus(status).put(reasonKey, reasonValue);
    }
}
