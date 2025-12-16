package com.uid2.optout.vertx;

import io.vertx.core.json.JsonObject;
import java.time.Instant;

/**
 * Represents the status and result of an async delta production job on a pod.
 * 
 * This class tracks the lifecycle of a delta production job including its state
 * (running, completed, failed), timing information, and result or error details.
 * 
 */
public class DeltaProduceJobStatus {
    private final Instant startTime;
    private volatile JobState state;
    private volatile JsonObject result;
    private volatile String errorMessage;
    private volatile Instant endTime;

    public enum JobState {
        RUNNING,
        COMPLETED,
        FAILED
    }

    public DeltaProduceJobStatus() {
        this.startTime = Instant.now();
        this.state = JobState.RUNNING;
    }

    /**
     * Mark the job as completed with the given result.
     * @param result The result details as a JsonObject
     */
    public void complete(JsonObject result) {
        this.result = result;
        this.state = JobState.COMPLETED;
        this.endTime = Instant.now();
    }

    /**
     * Mark the job as failed with the given error message.
     * @param errorMessage Description of the failure
     */
    public void fail(String errorMessage) {
        this.errorMessage = errorMessage;
        this.state = JobState.FAILED;
        this.endTime = Instant.now();
    }

    /**
     * Get the current state of the job.
     * @return The job state
     */
    public JobState getState() {
        return state;
    }

    /**
     * Convert the job status to a JSON representation for API responses.
     * @return JsonObject with state, timing, and result/error information
     */
    public JsonObject toJson() {
        JsonObject json = new JsonObject()
                .put("state", state.name().toLowerCase())
                .put("start_time", startTime.toString());

        if (endTime != null) {
            json.put("end_time", endTime.toString());
            long durationSeconds = endTime.getEpochSecond() - startTime.getEpochSecond();
            json.put("duration_seconds", durationSeconds);
        }

        if (state == JobState.COMPLETED && result != null) {
            json.put("result", result);
        }

        if (state == JobState.FAILED && errorMessage != null) {
            json.put("error", errorMessage);
        }

        return json;
    }
}

