package com.uid2.optout.vertx;

/**
 * Result object containing statistics from delta production.
 */
public class DeltaProductionResult {
    private final int deltasProduced;
    private final int entriesProcessed;

    public DeltaProductionResult(int deltasProduced, int entriesProcessed) {
        this.deltasProduced = deltasProduced;
        this.entriesProcessed = entriesProcessed;
    }

    public int getDeltasProduced() {
        return deltasProduced;
    }

    public int getEntriesProcessed() {
        return entriesProcessed;
    }
}

