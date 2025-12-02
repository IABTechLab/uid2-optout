package com.uid2.optout.vertx;

/**
 * Result object containing statistics from delta production.
 */
public class DeltaProductionResult {
    private final int deltasProduced;
    private final int entriesProcessed;
    private final boolean stoppedDueToRecentMessages;

    public DeltaProductionResult(int deltasProduced, int entriesProcessed, boolean stoppedDueToRecentMessages) {
        this.deltasProduced = deltasProduced;
        this.entriesProcessed = entriesProcessed;
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
}

