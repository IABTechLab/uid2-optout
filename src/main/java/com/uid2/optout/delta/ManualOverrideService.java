package com.uid2.optout.delta;

import com.uid2.shared.Utils;
import com.uid2.shared.cloud.ICloudStorage;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Service for managing manual override status in S3.
 * 
 * The manual override allows operators to force DELAYED_PROCESSING status,
 * which stops delta production until manually cleared.
 * 
 * Override file format:
 * <pre>
 * {"manual_override": "DELAYED_PROCESSING"}
 * </pre>
 */
public class ManualOverrideService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManualOverrideService.class);
    
    private static final String OVERRIDE_KEY = "manual_override";
    private static final String DELAYED_PROCESSING_VALUE = "DELAYED_PROCESSING";
    
    private final ICloudStorage cloudStorage;
    private final String overrideS3Path;
    
    /**
     * Create a ManualOverrideService.
     * 
     * @param cloudStorage Cloud storage client for reading/writing override file
     * @param overrideS3Path S3 path where the override file is stored
     */
    public ManualOverrideService(ICloudStorage cloudStorage, String overrideS3Path) {
        this.cloudStorage = cloudStorage;
        this.overrideS3Path = overrideS3Path;
    }
    
    /**
     * Check if DELAYED_PROCESSING override is currently set.
     * 
     * @return true if manual override is set to DELAYED_PROCESSING
     */
    public boolean isDelayedProcessing() {
        return DELAYED_PROCESSING_VALUE.equals(getOverrideValue());
    }

    /**
     * Set the manual override to DELAYED_PROCESSING.
     * This will stop delta production until manually cleared.
     * 
     * @return true if override was set successfully
     */
    public boolean setDelayedProcessing() {
        try {
            JsonObject config = new JsonObject().put(OVERRIDE_KEY, DELAYED_PROCESSING_VALUE);
            cloudStorage.upload(new ByteArrayInputStream(config.encode().getBytes()), overrideS3Path);
            LOGGER.info("set manual override to DELAYED_PROCESSING: {}", overrideS3Path);
            return true;
        } catch (Exception e) {
            LOGGER.error("manual_override_error: failed to set override at {}", overrideS3Path, e);
            return false;
        }
    }
    
    /**
     * Get the current manual override value
     */
    private String getOverrideValue() {
        try {
            InputStream inputStream = cloudStorage.download(overrideS3Path);
            JsonObject configJson = Utils.toJsonObject(inputStream);
            return configJson.getString(OVERRIDE_KEY, "");
        } catch (Exception e) {
            LOGGER.error("manual_override_error: no manual override file found at {}", overrideS3Path);
            return "";
        }
    }
}

