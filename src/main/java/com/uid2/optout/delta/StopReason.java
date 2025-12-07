package com.uid2.optout.delta;

/**
 * Represents why delta production stopped.
 * Used across all layers (batch, window, orchestrator) for consistent stop reason tracking.
 */
public enum StopReason {
    
    /**
     * Processing completed normally with work done, or still in progress.
     */
    NONE,
    
    /**
     * No messages available in the SQS queue.
     */
    QUEUE_EMPTY,
    
    /**
     * Messages exist in the queue but are too recent (less than deltaWindowSeconds old).
     */
    MESSAGES_TOO_RECENT,
    
    /**
     * Hit the maximum messages per window limit.
     */
    MESSAGE_LIMIT_EXCEEDED,
    
    /**
     * Pre-existing manual override was set (checked at job start).
     */
    MANUAL_OVERRIDE_ACTIVE,
    
    /**
     * Circuit breaker triggered during processing (traffic spike detected).
     */
    CIRCUIT_BREAKER_TRIGGERED
}

