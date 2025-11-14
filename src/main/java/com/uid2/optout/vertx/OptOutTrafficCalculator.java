package com.uid2.optout.vertx;

import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.optout.OptOutCollection;
import com.uid2.shared.optout.OptOutEntry;
import com.uid2.shared.optout.OptOutUtils;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import java.nio.charset.StandardCharsets;

import java.io.InputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Calculates opt-out traffic patterns to determine DEFAULT or DELAYED_PROCESSING status.
 * 
 * Compares recent ~24h traffic (sumCurrent) against previous ~24h baseline (sumPast).
 * Both sums exclude records in whitelist ranges (surge windows determined by engineers).
 * 
 * Returns DELAYED_PROCESSING if sumCurrent >= 5 × sumPast, indicating abnormal traffic spike.
 */
public class OptOutTrafficCalculator {
    private static final Logger LOGGER = LoggerFactory.getLogger(OptOutTrafficCalculator.class);
    
    private static final int HOURS_24 = 24 * 3600;  // 24 hours in seconds
    private static final int DEFAULT_THRESHOLD_MULTIPLIER = 5;
    
    private final Map<String, FileRecordCache> deltaFileCache = new ConcurrentHashMap<>();
    private final int thresholdMultiplier;
    private final ICloudStorage cloudStorage;
    private final String s3DeltaPrefix;  // (e.g. "optout-v2/delta/")
    private final String whitelistS3Path;  // (e.g. "optout-breaker/traffic-filter-config.json")
    private List<List<Long>> whitelistRanges;
    
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
     * Constructor for OptOutTrafficCalculator
     * 
     * @param config JsonObject containing configuration
     * @param cloudStorage Cloud storage for reading delta files and whitelist from S3
     * @param cloudSync Cloud sync for path conversion
     */
    public OptOutTrafficCalculator(JsonObject config, ICloudStorage cloudStorage, String s3DeltaPrefix, String whitelistS3Path) {
        this.cloudStorage = cloudStorage;
        this.thresholdMultiplier = config.getInteger("traffic_calc_threshold_multiplier", DEFAULT_THRESHOLD_MULTIPLIER);
        this.s3DeltaPrefix = s3DeltaPrefix;
        this.whitelistS3Path = whitelistS3Path;
        
        // Initial whitelist load
        this.whitelistRanges = Collections.emptyList();  // Start empty
        reloadWhitelist();  // Load from S3
        
        LOGGER.info("OptOutTrafficCalculator initialized: s3DeltaPrefix={}, whitelistPath={}, threshold={}x", 
                   s3DeltaPrefix, whitelistS3Path, thresholdMultiplier);
    }
    
    /**
     * Reload whitelist ranges from S3.
     * Expected format:
     * {
     *   "traffic_calc_whitelist_ranges": [
     *     [startTimestamp1, endTimestamp1],
     *     [startTimestamp2, endTimestamp2]
     *   ]
     * }
     * 
     * Can be called periodically to pick up config changes without restarting.
     */
    public void reloadWhitelist() {
        LOGGER.info("Reloading whitelist from S3: {}", whitelistS3Path);
        try (InputStream is = cloudStorage.download(whitelistS3Path)) {
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            JsonObject whitelistConfig = new JsonObject(content);
            
            List<List<Long>> ranges = parseWhitelistRanges(whitelistConfig);
            this.whitelistRanges = ranges;
            
            LOGGER.info("Successfully loaded {} whitelist ranges from S3", ranges.size());
            
        } catch (Exception e) {
            LOGGER.warn("No whitelist found at: {}", whitelistS3Path, e);
            this.whitelistRanges = Collections.emptyList();
        }
    }
    
    /**
     * Parse whitelist ranges from JSON config
     */
    List<List<Long>> parseWhitelistRanges(JsonObject config) {
        List<List<Long>> ranges = new ArrayList<>();
        
        try {
            if (config.containsKey("traffic_calc_whitelist_ranges")) {
                var rangesArray = config.getJsonArray("traffic_calc_whitelist_ranges");
                if (rangesArray != null) {
                    for (int i = 0; i < rangesArray.size(); i++) {
                        var rangeArray = rangesArray.getJsonArray(i);
                        if (rangeArray != null && rangeArray.size() >= 2) {
                            long val1 = rangeArray.getLong(0);
                            long val2 = rangeArray.getLong(1);
                            
                            // Ensure start <= end (correct misordered ranges)
                            long start = Math.min(val1, val2);
                            long end = Math.max(val1, val2);
                            
                            List<Long> range = Arrays.asList(start, end);
                            ranges.add(range);
                            LOGGER.info("Loaded whitelist range: [{}, {}]", start, end);
                        }
                    }
                }
            }
            
            // Sort ranges by start time for efficient lookups
            ranges.sort(Comparator.comparing(range -> range.get(0)));
            
        } catch (Exception e) {
            LOGGER.error("Failed to parse whitelist ranges", e);
        }
        
        return ranges;
    }
    
    /**
     * Calculate traffic status based on delta files and SQS queue messages.
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
            
            // Find t = oldest SQS queue message timestamp
            long t = findOldestQueueTimestamp(sqsMessages);
            LOGGER.info("Traffic calculation starting with t={} (oldest SQS message)", t);
            
            // Define time windows
            long currentWindowStart = t - (HOURS_24-300) - getWhitelistDuration(t, t - (HOURS_24-300)); // for range [t-23h55m, t+5m]
            long pastWindowStart = currentWindowStart - HOURS_24 - getWhitelistDuration(currentWindowStart, currentWindowStart - HOURS_24); // for range [t-47h55m, t-23h55m]
            
            // Evict old cache entries (older than past window start)
            evictOldCacheEntries(pastWindowStart);
            
            // Process delta files and count
            int sumCurrent = 0;
            int sumPast = 0;
            
            for (String s3Path : deltaS3Paths) {
                List<Long> timestamps = getTimestampsFromFile(s3Path);
                
                boolean shouldStop = false;
                for (long ts : timestamps) {
                    // Stop condition: record is older than our 48h window
                    if (ts < pastWindowStart) {
                        LOGGER.debug("Stopping delta file processing at timestamp {} (older than t-48h)", ts);
                        break;
                    }
                    
                    // skip records in whitelisted ranges
                    if (isInWhitelist(ts)) {
                        continue;
                    }
                    
                    // Count for sumCurrent: [t-24h, t]
                    if (ts >= currentWindowStart && ts <= t) {
                        sumCurrent++;
                    }
                    
                    // Count for sumPast: [t-48h, t-24h]
                    if (ts >= pastWindowStart && ts < currentWindowStart) {
                        sumPast++;
                    }
                }
                
                if (shouldStop) {
                    break;
                }
            }
            
            // Count SQS messages in [t, t+5m]
            if (sqsMessages != null && !sqsMessages.isEmpty()) {
                int sqsCount = countSqsMessages(
                    sqsMessages, t);
                sumCurrent += sqsCount;
            }
            
            // Determine status
            TrafficStatus status = determineStatus(sumCurrent, sumPast);
            
            LOGGER.info("Traffic calculation complete: sumCurrent={}, sumPast={}, status={}", 
                       sumCurrent, sumPast, status);
            
            return status;
            
        } catch (Exception e) {
            LOGGER.error("Error calculating traffic status", e);
            return TrafficStatus.DEFAULT;
        }
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
     * Calculate total duration of whitelist ranges that overlap with the given time window.
     */
    long getWhitelistDuration(long t, long windowStart) {
        long totalDuration = 0;
        for (List<Long> range : this.whitelistRanges) {
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
     * Count SQS messages from t to t+5 minutes
     */
    private int countSqsMessages(List<Message> sqsMessages, long t) {
        
        int count = 0;
        
        for (Message msg : sqsMessages) {
            Long ts = extractTimestampFromMessage(msg);

            if (ts < t || ts > t + 5 * 60) {
                continue;
            }
            
            if (isInWhitelist(ts)) {
                continue;
            }
            count++;
            
        }
        
        LOGGER.info("SQS messages: {} in window [t={}, t+5(minutes)={}]", count, t, t + 5 * 60);
        return count;
    }
    
    /**
     * Check if a timestamp falls within any whitelist range
     */
    boolean isInWhitelist(long timestamp) {
        if (whitelistRanges == null || whitelistRanges.isEmpty()) {
            return false;
        }
        
        for (List<Long> range : whitelistRanges) {
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
    TrafficStatus determineStatus(int sumCurrent, int sumPast) {
        if (sumPast == 0) {
            // Avoid division by zero - if no baseline traffic, return DEFAULT status
            LOGGER.warn("sumPast is 0, cannot detect thresholdcrossing. Returning DEFAULT status.");
            return TrafficStatus.DEFAULT;
        }
        
        if (sumCurrent >= thresholdMultiplier * sumPast) {
            LOGGER.warn("DELAYED_PROCESSING threshold breached: sumCurrent={} >= {}×sumPast={}", 
                       sumCurrent, thresholdMultiplier, sumPast);
            return TrafficStatus.DELAYED_PROCESSING;
        }
        
        LOGGER.info("Traffic within normal range: sumCurrent={} < {}×sumPast={}", 
                   sumCurrent, thresholdMultiplier, sumPast);
        return TrafficStatus.DEFAULT;
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
    
    /**
     * Clear the cache (for testing or manual reset)
     */
    public void clearCache() {
        int size = deltaFileCache.size();
        deltaFileCache.clear();
        LOGGER.info("Cleared cache ({} entries)", size);
    }
}
