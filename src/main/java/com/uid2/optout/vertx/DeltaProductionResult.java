package com.uid2.optout.vertx;

/**
 * Result object containing statistics from delta production.
 */
public class DeltaProductionResult {
    private final int deltasProduced;
    private final int entriesProcessed;

    /* 
     * indicates that there are still messages in the queue, however,
     * not enough time has elapsed to produce a delta file.
     * We produce in batches of (5 minutes) 
     */
    private final boolean stoppedDueToMessagesTooRecent;

    public DeltaProductionResult(int deltasProduced, int entriesProcessed, boolean stoppedDueToMessagesTooRecent) {
        this.deltasProduced = deltasProduced;
        this.entriesProcessed = entriesProcessed;
        this.stoppedDueToMessagesTooRecent = stoppedDueToMessagesTooRecent;
    }

    public int getDeltasProduced() {
        return deltasProduced;
    }

    public int getEntriesProcessed() {
        return entriesProcessed;
    }

    public boolean stoppedDueToMessagesTooRecent() {
        return stoppedDueToMessagesTooRecent;
    }
}

