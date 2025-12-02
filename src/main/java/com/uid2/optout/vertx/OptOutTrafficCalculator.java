package com.uid2.optout.vertx;

import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.optout.OptOutCollection;
import com.uid2.shared.optout.OptOutEntry;
import com.uid2.shared.optout.OptOutUtils;
import com.uid2.optout.Const;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import java.nio.charset.StandardCharsets;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.InputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Calculates opt-out traffic patterns to determine DEFAULT or DELAYED_PROCESSING status.
 * 
 * Compares recent ~24h traffic (sumCurrent) against a configurable baseline (baselineTraffic) of expected traffic in 24 hours.
 * The baseline is multiplied by (thresholdMultiplier) to determine the threshold.
 * sumCurrent excludes records in allowlist ranges (surge windows determined by engineers).
 * 
 * Returns DELAYED_PROCESSING if sumCurrent >= thresholdMultiplier * baselineTraffic, indicating abnormal traffic spike.
 */
public class OptOutTrafficCalculator {
    private static final Logger LOGGER = LoggerFactory.getLogger(OptOutTrafficCalculator.class);
    
    private static final int HOURS_24 = 24 * 3600;  // 24 hours in seconds
    
    private final Map<String, FileRecordCache> deltaFileCache = new ConcurrentHashMap<>();
    private final ICloudStorage cloudStorage;
    private final String s3DeltaPrefix;  // (e.g. "optout-v2/delta/")
    private final String trafficCalcConfigPath;
    private int baselineTraffic;
    private int thresholdMultiplier;
    private int evaluationWindowSeconds;
    private List<List<Long>> allowlistRanges;
    
    public enum TrafficStatus {
        DELAYED_PROCESSING,
        DEFAULT
    }
    
    /**
     * Cache entry for a delta file containing all record timestamps.
     * 
     * Memory usage: ~8 bytes per timestamp (long)
     * 1GB of memory can store ~130 million timestamps (1024^3)/8
     */
    private static class FileRecordCache {
        final List<Long> timestamps;  // All non-sentinel record timestamps
        final long newestTimestamp;    // evict delta from cache based on oldest record timestamp
        
        FileRecordCache(List<Long> timestamps) {
            this.timestamps = timestamps;
            this.newestTimestamp = timestamps.isEmpty() ? 0 : Collections.max(timestamps);
        }
    }

    /**
     * Exception thrown by malformed traffic calculator config
     */
    public static class MalformedTrafficCalcConfigException extends Exception {
        public MalformedTrafficCalcConfigException(String message) {
            super(message);
        }
    }

    /**
     * Constructor for OptOutTrafficCalculator
     * 
     * @param cloudStorage Cloud storage for reading delta files
     * @param s3DeltaPrefix S3 prefix for delta files
     * @param trafficCalcConfigS3Path S3 path for traffic calc config
     */
    public OptOutTrafficCalculator(ICloudStorage cloudStorage, String s3DeltaPrefix, String trafficCalcConfigPath) throws MalformedTrafficCalcConfigException {
        this.cloudStorage = cloudStorage;
        this.s3DeltaPrefix = s3DeltaPrefix;
        this.trafficCalcConfigPath = trafficCalcConfigPath;
        reloadTrafficCalcConfig();  // Load ConfigMap
        
        LOGGER.info("OptOutTrafficCalculator initialized: s3DeltaPrefix={}, threshold={}x", 
                   s3DeltaPrefix, thresholdMultiplier);
    }
    
    /**
     * Reload traffic calc config from ConfigMap.
     * Expected format:
     * {
     *   "traffic_calc_evaluation_window_seconds": 86400,
     *   "traffic_calc_baseline_traffic": 100,
     *   "traffic_calc_threshold_multiplier": 5,
     *   "traffic_calc_allowlist_ranges": [
     *     [startTimestamp1, endTimestamp1],
     *     [startTimestamp2, endTimestamp2]
     *   ],
     * }
     * 
     * Can be called periodically to pick up config changes without restarting.
     */
    public void reloadTrafficCalcConfig() throws MalformedTrafficCalcConfigException {
        LOGGER.info("Loading traffic calc config from ConfigMap");
        try (InputStream is = Files.newInputStream(Paths.get(trafficCalcConfigPath))) {
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            JsonObject trafficCalcConfig = new JsonObject(content);

            // Validate required fields exist
            if (!trafficCalcConfig.containsKey(Const.Config.OptOutTrafficCalcEvaluationWindowSecondsProp)) {
                throw new MalformedTrafficCalcConfigException("Missing required field: traffic_calc_evaluation_window_seconds");
            }
            if (!trafficCalcConfig.containsKey(Const.Config.OptOutTrafficCalcBaselineTrafficProp)) {
                throw new MalformedTrafficCalcConfigException("Missing required field: traffic_calc_baseline_traffic");
            }
            if (!trafficCalcConfig.containsKey(Const.Config.OptOutTrafficCalcThresholdMultiplierProp)) {
                throw new MalformedTrafficCalcConfigException("Missing required field: traffic_calc_threshold_multiplier");
            }
            if (!trafficCalcConfig.containsKey(Const.Config.OptOutTrafficCalcAllowlistRangesProp)) {
                throw new MalformedTrafficCalcConfigException("Missing required field: traffic_calc_allowlist_ranges");
            }

            this.evaluationWindowSeconds = trafficCalcConfig.getInteger(Const.Config.OptOutTrafficCalcEvaluationWindowSecondsProp);
            this.baselineTraffic = trafficCalcConfig.getInteger(Const.Config.OptOutTrafficCalcBaselineTrafficProp);
            this.thresholdMultiplier = trafficCalcConfig.getInteger(Const.Config.OptOutTrafficCalcThresholdMultiplierProp);
            
            List<List<Long>> ranges = parseAllowlistRanges(trafficCalcConfig);
            this.allowlistRanges = ranges;
            
            LOGGER.info("Successfully loaded traffic calc config from ConfigMap: evaluationWindowSeconds={}, baselineTraffic={}, thresholdMultiplier={}, allowlistRanges={}",
                       this.evaluationWindowSeconds, this.baselineTraffic, this.thresholdMultiplier, ranges.size());
            
        } catch (MalformedTrafficCalcConfigException e) {
            LOGGER.warn("Failed to load traffic calc config. Config is malformed: {}", trafficCalcConfigPath, e);
            throw e;
        } catch (Exception e) {
            LOGGER.warn("Failed to load traffic calc config. Config is malformed or missing: {}", trafficCalcConfigPath, e);
            throw new MalformedTrafficCalcConfigException("Failed to load traffic calc config: " + e.getMessage());
        }
    }
    
    /**
     * Parse allowlist ranges from JSON config
     */
    List<List<Long>> parseAllowlistRanges(JsonObject config) throws MalformedTrafficCalcConfigException {
        List<List<Long>> ranges = new ArrayList<>();
        
        try {
            var rangesArray = config.getJsonArray(Const.Config.OptOutTrafficCalcAllowlistRangesProp);
            if (rangesArray != null) {
                for (int i = 0; i < rangesArray.size(); i++) {
                    var rangeArray = rangesArray.getJsonArray(i);
                    if (rangeArray != null && rangeArray.size() >= 2) {
                        long start = rangeArray.getLong(0);
                        long end = rangeArray.getLong(1);
                        
                        if(start >= end) {
                            LOGGER.error("Invalid allowlist range: start must be less than end: [{}, {}]", start, end);
                            throw new MalformedTrafficCalcConfigException("Invalid allowlist range at index " + i + ": start must be less than end");
                        }

                        if (end - start > 86400) {
                            LOGGER.error("Invalid allowlist range: range must be less than 24 hours: [{}, {}]", start, end);
                            throw new MalformedTrafficCalcConfigException("Invalid allowlist range at index " + i + ": range must be less than 24 hours");
                        }
                        
                        List<Long> range = Arrays.asList(start, end);
                        ranges.add(range);
                        LOGGER.info("Loaded allowlist range: [{}, {}]", start, end);
                    }
                }
            }
            
            ranges.sort(Comparator.comparing(range -> range.get(0)));
            
        } catch (Exception e) {
            LOGGER.error("Failed to parse allowlist ranges", e);
            throw new MalformedTrafficCalcConfigException("Failed to parse allowlist ranges: " + e.getMessage());
        }
        
        return ranges;
    }
    
    /**
     * Calculate traffic status based on delta files and SQS queue messages.
     * 
     * Uses the newest delta file timestamp to anchor the 24-hour delta traffic window,
     * and the oldest queue timestamp to anchor the 5-minute queue window.
     * 
     * @param sqsMessages List of SQS messages
     * @return TrafficStatus (DELAYED_PROCESSING or DEFAULT)
     */
    public TrafficStatus calculateStatus(List<Message> sqsMessages) {
        
        try {
            // Get list of delta files from S3 (sorted newest to oldest)
            List<String> deltaS3Paths = listDeltaFiles();
            
            if (deltaS3Paths.isEmpty()) {
                LOGGER.warn("No delta files found in S3 with prefix: {}", s3DeltaPrefix);
                return TrafficStatus.DEFAULT;
            }
            
            // Find newest delta file timestamp for delta traffic window
            long newestDeltaTs = findNewestDeltaTimestamp(deltaS3Paths);
            LOGGER.info("Traffic calculation: newestDeltaTs={}", newestDeltaTs);
            
            // Find oldest SQS queue message timestamp for queue window
            long oldestQueueTs = findOldestQueueTimestamp(sqsMessages);
            LOGGER.info("Traffic calculation: oldestQueueTs={}", oldestQueueTs);
            
            // Define start time of the delta evaluation window [newestDeltaTs - 24h, newestDeltaTs]
            long deltaWindowStart = newestDeltaTs - this.evaluationWindowSeconds - getAllowlistDuration(newestDeltaTs, newestDeltaTs - this.evaluationWindowSeconds);
            
            // Evict old cache entries (older than delta window start)
            evictOldCacheEntries(deltaWindowStart);
            
            // Process delta files and count records in [deltaWindowStart, newestDeltaTs]
            int sum = 0;
            
            for (String s3Path : deltaS3Paths) {
                List<Long> timestamps = getTimestampsFromFile(s3Path);
                
                boolean shouldStop = false;
                for (long ts : timestamps) {
                    // Stop condition: record is older than our window
                    if (ts < deltaWindowStart) {
                        LOGGER.debug("Stopping delta file processing at timestamp {} (older than window start {})", ts, deltaWindowStart);
                        break;
                    }
                    
                    // skip records in allowlisted ranges
                    if (isInAllowlist(ts)) {
                        continue;
                    }
                    
                    // increment sum if record is in delta window
                    if (ts >= deltaWindowStart) {
                        sum++;
                    }
                    
                }
                
                if (shouldStop) {
                    break;
                }
            }
            
            // Count SQS messages in [oldestQueueTs, oldestQueueTs + 5m]
            if (sqsMessages != null && !sqsMessages.isEmpty()) {
                int sqsCount = countSqsMessages(sqsMessages, oldestQueueTs);
                sum += sqsCount;
            }
            
            // Determine status
            TrafficStatus status = determineStatus(sum, this.baselineTraffic);
            
            LOGGER.info("Traffic calculation complete: sum={}, baselineTraffic={}, thresholdMultiplier={}, status={}", 
                       sum, this.baselineTraffic, this.thresholdMultiplier, status);
            
            return status;
            
        } catch (Exception e) {
            LOGGER.error("Error calculating traffic status", e);
            return TrafficStatus.DEFAULT;
        }
    }
    
    /**
     * Find the newest timestamp from delta files.
     * Reads the newest delta file and returns its maximum timestamp.
     */
    private long findNewestDeltaTimestamp(List<String> deltaS3Paths) throws IOException {
        if (deltaS3Paths == null || deltaS3Paths.isEmpty()) {
            return System.currentTimeMillis() / 1000;
        }
        
        // Delta files are sorted (ISO 8601 format, lexicographically sortable) so first file is newest
        String newestDeltaPath = deltaS3Paths.get(0);
        List<Long> timestamps = getTimestampsFromFile(newestDeltaPath);
        
        if (timestamps.isEmpty()) {
            LOGGER.warn("Newest delta file has no timestamps: {}", newestDeltaPath);
            return System.currentTimeMillis() / 1000;
        }
        
        long newestTs = Collections.max(timestamps);
        LOGGER.debug("Found newest delta timestamp {} from file {}", newestTs, newestDeltaPath);
        return newestTs;
    }
    
    /**
     * List all delta files from S3, sorted newest to oldest
     */
    private List<String> listDeltaFiles() {
        try {
            // List all objects with the delta prefix
            List<String> allFiles = cloudStorage.list(s3DeltaPrefix);
            
            // Filter to only .dat delta files and sort newest to oldest
            return allFiles.stream()
                .filter(OptOutUtils::isDeltaFile)
                .sorted(OptOutUtils.DeltaFilenameComparatorDescending)
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            LOGGER.error("Failed to list delta files from S3 with prefix: {}", s3DeltaPrefix, e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Get timestamps from a delta file (S3 path), using cache if available
     */
    private List<Long> getTimestampsFromFile(String s3Path) throws IOException {
        // Extract filename from S3 path for cache key
        String filename = s3Path.substring(s3Path.lastIndexOf('/') + 1);
        
        // Check cache first
        FileRecordCache cached = deltaFileCache.get(filename);
        if (cached != null) {
            LOGGER.debug("Using cached timestamps for file: {}", filename);
            return cached.timestamps;
        }
        
        // Cache miss - download from S3
        LOGGER.debug("Downloading and reading timestamps from S3: {}", s3Path);
        List<Long> timestamps = readTimestampsFromS3(s3Path);
        
        // Store in cache
        deltaFileCache.put(filename, new FileRecordCache(timestamps));
        
        return timestamps;
    }
    
    /**
     * Read all non-sentinel record timestamps from a delta file in S3
     */
    private List<Long> readTimestampsFromS3(String s3Path) throws IOException {
        try (InputStream is = cloudStorage.download(s3Path)) {
            byte[] data = is.readAllBytes();
            OptOutCollection collection = new OptOutCollection(data);
            
            List<Long> timestamps = new ArrayList<>();
            for (int i = 0; i < collection.size(); i++) {
                OptOutEntry entry = collection.get(i);
                
                // Skip sentinel entries
                if (entry.isSpecialHash()) {
                    continue;
                }
                
                timestamps.add(entry.timestamp);
            }
            
            return timestamps;
        } catch (Exception e) {
            LOGGER.error("Failed to read delta file from S3: {}", s3Path, e);
            throw new IOException("Failed to read delta file from S3: " + s3Path, e);
        }
    }

    /**
     * Calculate total duration of allowlist ranges that overlap with the given time window.
     */
    long getAllowlistDuration(long t, long windowStart) {
        long totalDuration = 0;
        for (List<Long> range : this.allowlistRanges) {
            long start = range.get(0);
            long end = range.get(1);
            
            // Clip range to window boundaries
            if (start < windowStart) {
                start = windowStart;
            }
            if (end > t) {
                end = t;
            }
            
            // Only add duration if there's actual overlap (start < end)
            if (start < end) {
                totalDuration += end - start;
            }
        }
        return totalDuration;
    }
    
    /**
     * Find the oldest SQS queue message timestamp
     */
    private long findOldestQueueTimestamp(List<Message> sqsMessages) throws IOException {
        long oldest = System.currentTimeMillis() / 1000;
        
        if (sqsMessages != null && !sqsMessages.isEmpty()) {
            for (Message msg : sqsMessages) {
                Long ts = extractTimestampFromMessage(msg);
                if (ts != null && ts < oldest) {
                    oldest = ts;
                }
            }
        }
        
        return oldest;
    }
    
    /**
     * Extract timestamp from SQS message (from SentTimestamp attribute)
     */
    private Long extractTimestampFromMessage(Message msg) {
        // Get SentTimestamp attribute (milliseconds)
        String sentTimestamp = msg.attributes().get(MessageSystemAttributeName.SENT_TIMESTAMP);
        if (sentTimestamp != null) {
            try {
                return Long.parseLong(sentTimestamp) / 1000;  // Convert ms to seconds
            } catch (NumberFormatException e) {
                LOGGER.debug("Invalid SentTimestamp: {}", sentTimestamp);
            }
        }
        
        // Fallback: use current time
        return System.currentTimeMillis() / 1000;
    }
    
    /**
     * Count SQS messages from oldestQueueTs to oldestQueueTs + 5 minutes
     */
    private int countSqsMessages(List<Message> sqsMessages, long oldestQueueTs) {
        
        int count = 0;
        long windowEnd = oldestQueueTs + 5 * 60;
        
        for (Message msg : sqsMessages) {
            Long ts = extractTimestampFromMessage(msg);

            if (ts < oldestQueueTs || ts > windowEnd) {
                continue;
            }
            
            if (isInAllowlist(ts)) {
                continue;
            }
            count++;
            
        }
        
        LOGGER.info("SQS messages: {} in window [oldestQueueTs={}, oldestQueueTs+5m={}]", count, oldestQueueTs, windowEnd);
        return count;
    }
    
    /**
     * Check if a timestamp falls within any allowlist range
     */
    boolean isInAllowlist(long timestamp) {
        if (allowlistRanges == null || allowlistRanges.isEmpty()) {
            return false;
        }
        
        for (List<Long> range : allowlistRanges) {
            if (range.size() < 2) {
                continue;
            }
            
            long start = range.get(0);
            long end = range.get(1);
            
            if (timestamp >= start && timestamp <= end) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Evict cache entries with data older than the cutoff timestamp
     */
    private void evictOldCacheEntries(long cutoffTimestamp) {
        int beforeSize = deltaFileCache.size();
        
        deltaFileCache.entrySet().removeIf(entry -> 
            entry.getValue().newestTimestamp < cutoffTimestamp
        );
        
        int afterSize = deltaFileCache.size();
        if (beforeSize != afterSize) {
            LOGGER.info("Evicted {} old cache entries (before={}, after={})", 
                       beforeSize - afterSize, beforeSize, afterSize);
        }
    }
    
    /**
     * Determine traffic status based on current vs past counts
     */
    TrafficStatus determineStatus(int sumCurrent, int baselineTraffic) {
        if (baselineTraffic == 0 || thresholdMultiplier == 0) {
            // Avoid division by zero - if no baseline traffic, return DEFAULT status
            LOGGER.warn("baselineTraffic is 0 or thresholdMultiplier is 0 returning DEFAULT status.");
            return TrafficStatus.DEFAULT;
        }
        
        if (sumCurrent >= thresholdMultiplier * baselineTraffic) {
            LOGGER.warn("DELAYED_PROCESSING threshold breached: sumCurrent={} >= {}×baselineTraffic={}", 
                       sumCurrent, thresholdMultiplier, baselineTraffic);
            return TrafficStatus.DELAYED_PROCESSING;
        }
        
        LOGGER.info("Traffic within normal range: sumCurrent={} < {}×baselineTraffic={}", 
                   sumCurrent, thresholdMultiplier, baselineTraffic);
        return TrafficStatus.DEFAULT;
    }
    
    /**
     * Get the traffic threshold (baseline × multiplier).
     * Used for early termination
     */
    public int getThreshold() {
        return this.baselineTraffic * this.thresholdMultiplier;
    }

    /**
     * Get cache statistics for monitoring
     */
    public Map<String, Object> getCacheStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("cached_files", deltaFileCache.size());
        
        int totalTimestamps = deltaFileCache.values().stream()
            .mapToInt(cache -> cache.timestamps.size())
            .sum();
        stats.put("total_cached_timestamps", totalTimestamps);
        
        return stats;
    }
    
}
