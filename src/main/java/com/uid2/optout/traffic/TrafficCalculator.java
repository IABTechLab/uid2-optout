package com.uid2.optout.traffic;

import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.optout.OptOutCollection;
import com.uid2.shared.optout.OptOutEntry;
import com.uid2.shared.optout.OptOutUtils;
import com.uid2.optout.Const;
import com.uid2.optout.sqs.SqsMessageOperations.QueueAttributes;
import com.uid2.optout.sqs.SqsParsedMessage;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class TrafficCalculator {
    private static final Logger LOGGER = LoggerFactory.getLogger(TrafficCalculator.class);
    
    private final Map<String, FileRecordCache> deltaFileCache = new ConcurrentHashMap<>();
    private final ICloudStorage cloudStorage;
    private final String s3DeltaPrefix;
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
        final List<Long> timestamps;  // all non-sentinel record timestamps
        final long newestTimestamp;    // evict delta from cache based on newest record timestamp
        
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
     * Constructor for TrafficCalculator
     * 
     * @param cloudStorage Cloud storage for reading delta files
     * @param s3DeltaPrefix S3 prefix for delta files
     * @param trafficCalcConfigPath mount path for traffic calc config
     */
    public TrafficCalculator(ICloudStorage cloudStorage, String s3DeltaPrefix, String trafficCalcConfigPath) throws MalformedTrafficCalcConfigException {
        this.cloudStorage = cloudStorage;
        this.s3DeltaPrefix = s3DeltaPrefix;
        this.trafficCalcConfigPath = trafficCalcConfigPath;
        reloadTrafficCalcConfig();
        
        LOGGER.info("initialized: s3DeltaPrefix={}, threshold={}x", 
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
        LOGGER.info("loading traffic calc config from configmap");
        try (InputStream is = Files.newInputStream(Paths.get(trafficCalcConfigPath))) {
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            JsonObject trafficCalcConfig = new JsonObject(content);

            // Validate required fields exist
            if (!trafficCalcConfig.containsKey(Const.Config.OptOutTrafficCalcEvaluationWindowSecondsProp)) {
                throw new MalformedTrafficCalcConfigException("missing required field: traffic_calc_evaluation_window_seconds");
            }
            if (!trafficCalcConfig.containsKey(Const.Config.OptOutTrafficCalcBaselineTrafficProp)) {
                throw new MalformedTrafficCalcConfigException("missing required field: traffic_calc_baseline_traffic");
            }
            if (!trafficCalcConfig.containsKey(Const.Config.OptOutTrafficCalcThresholdMultiplierProp)) {
                throw new MalformedTrafficCalcConfigException("missing required field: traffic_calc_threshold_multiplier");
            }
            if (!trafficCalcConfig.containsKey(Const.Config.OptOutTrafficCalcAllowlistRangesProp)) {
                throw new MalformedTrafficCalcConfigException("missing required field: traffic_calc_allowlist_ranges");
            }

            this.evaluationWindowSeconds = trafficCalcConfig.getInteger(Const.Config.OptOutTrafficCalcEvaluationWindowSecondsProp);
            this.baselineTraffic = trafficCalcConfig.getInteger(Const.Config.OptOutTrafficCalcBaselineTrafficProp);
            this.thresholdMultiplier = trafficCalcConfig.getInteger(Const.Config.OptOutTrafficCalcThresholdMultiplierProp);
            
            List<List<Long>> ranges = parseAllowlistRanges(trafficCalcConfig);
            this.allowlistRanges = ranges;
            
            LOGGER.info("loaded traffic calc config: evaluationWindowSeconds={}, baselineTraffic={}, thresholdMultiplier={}, allowlistRanges={}",
                       this.evaluationWindowSeconds, this.baselineTraffic, this.thresholdMultiplier, ranges.size());
            
        } catch (MalformedTrafficCalcConfigException e) {
            LOGGER.error("circuit_breaker_config_error: config is malformed, configPath={}", trafficCalcConfigPath, e);
            throw e;
        } catch (Exception e) {
            LOGGER.error("circuit_breaker_config_error: config is malformed or missing, configPath={}", trafficCalcConfigPath, e);
            throw new MalformedTrafficCalcConfigException("failed to load traffic calc config: " + e.getMessage());
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
                            LOGGER.error("circuit_breaker_config_error: allowlist range start must be less than end, range=[{}, {}]", start, end);
                            throw new MalformedTrafficCalcConfigException("invalid allowlist range at index " + i + ": start must be less than end");
                        }

                        if (end - start > 86400) {
                            LOGGER.error("circuit_breaker_config_error: allowlist range must be less than 24 hours, range=[{}, {}]", start, end);
                            throw new MalformedTrafficCalcConfigException("invalid allowlist range at index " + i + ": range must be less than 24 hours");
                        }
                        
                        List<Long> range = Arrays.asList(start, end);
                        ranges.add(range);
                        LOGGER.info("loaded allowlist range: [{}, {}]", start, end);
                    }
                }
            }
            
            ranges.sort(Comparator.comparing(range -> range.get(0)));
            
            // validate that there are no overlapping ranges
            for (int i = 0; i < ranges.size() - 1; i++) {
                long currentEnd = ranges.get(i).get(1);
                long nextStart = ranges.get(i + 1).get(0);
                if (currentEnd >= nextStart) {
                    LOGGER.error("circuit_breaker_config_error: overlapping allowlist ranges, range=[{}, {}] overlaps with range=[{}, {}]",
                            ranges.get(i).get(0), currentEnd, nextStart, ranges.get(i + 1).get(1));
                    throw new MalformedTrafficCalcConfigException(
                            "overlapping allowlist ranges detected at indices " + i + " and " + (i + 1));
                }
            }
            
        } catch (MalformedTrafficCalcConfigException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.error("circuit_breaker_config_error: failed to parse allowlist ranges", e);
            throw new MalformedTrafficCalcConfigException("failed to parse allowlist ranges: " + e.getMessage());
        }
        
        return ranges;
    }
    
    /**
     * Calculate traffic status based on delta files and SQS queue messages.
     * 
     * Uses the newest delta file timestamp to anchor the 24-hour delta traffic window,
     * and the oldest queue timestamp to anchor the 5-minute queue window.
     * 
     * Counts:
     * - Delta file records (with allowlist filtering)
     * - SQS messages passed in (with allowlist filtering)
     * - Invisible messages from other consumers (from queue attributes, avoiding double count)
     * 
     * @param sqsMessages List of parsed SQS messages this consumer has read (non-denylisted)
     * @param queueAttributes Queue attributes including invisible message count (can be null)
     * @param denylistedCount Number of denylisted messages read by this consumer
     * @param filteredAsTooRecentCount Number of messages filtered as "too recent" by window reader
     * @return TrafficStatus (DELAYED_PROCESSING or DEFAULT)
     */
    public TrafficStatus calculateStatus(List<SqsParsedMessage> sqsMessages, QueueAttributes queueAttributes, int denylistedCount, int filteredAsTooRecentCount) {
        
        try {
            // get list of delta files from s3 (sorted newest to oldest)
            List<String> deltaS3Paths = listDeltaFiles();
            
            if (deltaS3Paths.isEmpty()) {
                LOGGER.error("s3_error: no delta files found in s3 at prefix={}", s3DeltaPrefix);
                throw new RuntimeException("no delta files found in s3 at prefix=" + s3DeltaPrefix);
            }
            
            // find newest delta file timestamp for delta traffic window
            long newestDeltaTs = findNewestDeltaTimestamp(deltaS3Paths);
            LOGGER.info("traffic calculation: newestDeltaTs={}", newestDeltaTs);
            
            // find oldest sqs queue message timestamp for queue window
            long oldestQueueTs = findOldestQueueTimestamp(sqsMessages);
            LOGGER.info("traffic calculation: oldestQueueTs={}", oldestQueueTs);
            
            // define start time of the delta evaluation window
            // we need evaluationWindowSeconds of non-allowlisted time, so we iteratively extend
            // the window to account for any allowlist ranges in the extended portion
            long deltaWindowStart = calculateWindowStartWithAllowlist(newestDeltaTs, this.evaluationWindowSeconds);
            
            // evict old cache entries (older than delta window start)
            evictOldCacheEntries(deltaWindowStart);
            
            // process delta files and count records in [deltaWindowStart, newestDeltaTs]
            // files are sorted newest to oldest, records within files are sorted newest to oldest
            // stop when the newest record in a file is older than the window
            int totalRecords = 0;
            int deltaRecordsCount = 0;
            int deltaAllowlistedCount = 0;
            int filesProcessed = 0;
            int cacheHits = 0;
            int cacheMisses = 0;
            
            for (String s3Path : deltaS3Paths) {
                boolean wasCached = isCached(s3Path);
                if (wasCached) {
                    cacheHits++;
                } else {
                    cacheMisses++;
                }
                
                List<Long> timestamps = getTimestampsFromFile(s3Path);
                filesProcessed++;
                
                // check newest record in file - if older than window, stop processing remaining files
                long newestRecordTs = timestamps.get(0);
                if (newestRecordTs < deltaWindowStart) {
                    break;
                }
                
                for (long ts : timestamps) {
                    // stop condition: record is older than our window
                    if (ts < deltaWindowStart) {
                        break;
                    }
                    
                    // skip records that are in allowlisted ranges
                    if (isInAllowlist(ts)) {
                        deltaAllowlistedCount++;
                        continue;
                    }
                    
                    // increment sum if record is within the delta window
                    if (ts >= deltaWindowStart) {
                        deltaRecordsCount++;
                        totalRecords++;
                    }
                }
            }
            
            LOGGER.info("delta files: processed={}, deltaRecords={}, allowlisted={}, cache hits={}, misses={}, cacheSize={}", 
                       filesProcessed, deltaRecordsCount, deltaAllowlistedCount, cacheHits, cacheMisses, deltaFileCache.size());
            
            // count sqs messages in [oldestQueueTs, oldestQueueTs + 5m] with allowlist filtering
            int sqsCount = 0;
            if (sqsMessages != null && !sqsMessages.isEmpty()) {
                sqsCount = countSqsMessages(sqsMessages, oldestQueueTs);
                totalRecords += sqsCount;
            }
            
            // add invisible messages being processed by other consumers
            // (notVisible count includes our messages, so subtract what we've read to avoid double counting)
            // ourMessages = delta messages + denylisted messages + filtered as "too recent" messages
            int otherConsumersMessages = 0;
            if (queueAttributes != null) {
                int totalInvisible = queueAttributes.getApproximateNumberOfMessagesNotVisible();
                int ourMessages = (sqsMessages != null ? sqsMessages.size() : 0) + denylistedCount + filteredAsTooRecentCount;
                otherConsumersMessages = Math.max(0, totalInvisible - ourMessages);
                totalRecords += otherConsumersMessages;
                LOGGER.info("traffic calculation: adding {} invisible messages from other consumers (totalInvisible={}, ourMessages={})",
                           otherConsumersMessages, totalInvisible, ourMessages);
            }
            
            // determine status
            TrafficStatus status = determineStatus(totalRecords, this.baselineTraffic);
            
            LOGGER.info("traffic calculation complete: sum={} (deltaRecords={} + sqsMessages={} + otherConsumers={}), baselineTraffic={}, thresholdMultiplier={}, status={}", 
                       totalRecords, deltaRecordsCount, sqsCount, otherConsumersMessages, this.baselineTraffic, this.thresholdMultiplier, status);
            
            return status;
            
        } catch (Exception e) {
            LOGGER.error("delta_job_failed: error calculating traffic status", e);
            throw new RuntimeException("error calculating traffic status", e);
        }
    }
    
    /**
     * find the newest timestamp from delta files.
     * reads the newest delta file and returns its maximum timestamp.
     */
    private long findNewestDeltaTimestamp(List<String> deltaS3Paths) throws IOException {
        if (deltaS3Paths == null || deltaS3Paths.isEmpty()) {
            return System.currentTimeMillis() / 1000;
        }
        
        // delta files are sorted (ISO 8601 format, lexicographically sortable) so first file is newest
        String newestDeltaPath = deltaS3Paths.get(0);
        List<Long> timestamps = getTimestampsFromFile(newestDeltaPath);
        
        if (timestamps.isEmpty()) {
            LOGGER.error("s3_error: newest delta file has no timestamps, path={}", newestDeltaPath);
            return System.currentTimeMillis() / 1000;
        }
        
        long newestTs = Collections.max(timestamps);
        LOGGER.info("found newest delta timestamp {} from file {}", newestTs, newestDeltaPath);
        return newestTs;
    }
    
    /**
     * List all delta files from S3, sorted newest to oldest
     */
    private List<String> listDeltaFiles() {
        try {
            // list all objects with the delta prefix
            List<String> allFiles = cloudStorage.list(s3DeltaPrefix);
            
            // filter to only .dat delta files and sort newest to oldest
            List<String> deltaFiles = allFiles.stream()
                .filter(OptOutUtils::isDeltaFile)
                .sorted(OptOutUtils.DeltaFilenameComparatorDescending)
                .collect(Collectors.toList());
            
            LOGGER.info("listed {} delta files from s3 (prefix={})", deltaFiles.size(), s3DeltaPrefix);
            return deltaFiles;
                
        } catch (Exception e) {
            LOGGER.error("s3_error: failed to list delta files at prefix={}", s3DeltaPrefix, e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Check if a delta file is already cached
     */
    private boolean isCached(String s3Path) {
        String filename = s3Path.substring(s3Path.lastIndexOf('/') + 1);
        return deltaFileCache.containsKey(filename);
    }
    
    /**
     * Get timestamps from a delta file (S3 path), using cache if available
     */
    private List<Long> getTimestampsFromFile(String s3Path) throws IOException {
        // extract filename from s3 path for cache key
        String filename = s3Path.substring(s3Path.lastIndexOf('/') + 1);
        
        // check cache first
        FileRecordCache cached = deltaFileCache.get(filename);
        if (cached != null) {
            return cached.timestamps;
        }
        
        // cache miss - download from s3
        List<Long> timestamps = readTimestampsFromS3(s3Path);
        
        // store in cache
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
                
                // skip sentinel entries
                if (entry.isSpecialHash()) {
                    continue;
                }
                
                timestamps.add(entry.timestamp);
            }
            
            return timestamps;
        } catch (Exception e) {
            LOGGER.error("s3_error: failed to read delta file at path={}", s3Path, e);
            throw new IOException("failed to read delta file from s3: " + s3Path, e);
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
            
            // clip range to window boundaries
            if (start < windowStart) {
                start = windowStart;
            }
            if (end > t) {
                end = t;
            }
            
            // only add duration if there's actual overlap (start < end)
            if (start < end) {
                totalDuration += end - start;
            }
        }
        return totalDuration;
    }
    
    /**
     * Calculate the window start time that provides evaluationWindowSeconds of non-allowlisted time.
     * Iteratively extends the window to account for allowlist ranges that may fall in extended portions.
     */
    long calculateWindowStartWithAllowlist(long newestDeltaTs, int evaluationWindowSeconds) {
        long allowlistDuration = getAllowlistDuration(newestDeltaTs, newestDeltaTs - evaluationWindowSeconds);
        
        // each iteration discovers at least one new allowlist range, so max iterations = number of ranges
        int maxIterations = this.allowlistRanges.size() + 1;
        
        for (int i = 0; i < maxIterations && allowlistDuration > 0; i++) {
            long newWindowStart = newestDeltaTs - evaluationWindowSeconds - allowlistDuration;
            long newAllowlistDuration = getAllowlistDuration(newestDeltaTs, newWindowStart);
            
            if (newAllowlistDuration == allowlistDuration) {
                // no new allowlist time in extended portion, we've converged
                break;
            }
            
            allowlistDuration = newAllowlistDuration;
        }
        
        return newestDeltaTs - evaluationWindowSeconds - allowlistDuration;
    }
    
    /**
     * Find the oldest SQS queue message timestamp
     */
    private long findOldestQueueTimestamp(List<SqsParsedMessage> sqsMessages) throws IOException {
        long oldest = System.currentTimeMillis() / 1000;
        
        if (sqsMessages != null && !sqsMessages.isEmpty()) {
            for (SqsParsedMessage msg : sqsMessages) {
                long ts = msg.timestamp();
                if (ts < oldest) {
                    oldest = ts;
                }
            }
        }
        
        return oldest;
    }
    
    /**
     * Count non-allowlisted SQS messages from oldestQueueTs to oldestQueueTs + 5 minutes
     */
    private int countSqsMessages(List<SqsParsedMessage> sqsMessages, long oldestQueueTs) {

        int count = 0;
        int allowlistedCount = 0;
        long windowEnd = oldestQueueTs + 5 * 60;

        for (SqsParsedMessage msg : sqsMessages) {
            long ts = msg.timestamp();

            if (ts < oldestQueueTs || ts > windowEnd) {
                continue;
            }

            if (isInAllowlist(ts)) {
                allowlistedCount++;
                continue;
            }
            count++;

        }

        LOGGER.info("sqs messages: {} in window, {} allowlisted [oldestQueueTs={}, oldestQueueTs+5m={}]", count, allowlistedCount, oldestQueueTs, windowEnd);
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
            LOGGER.info("evicted {} old cache entries (before={}, after={})", 
                       beforeSize - afterSize, beforeSize, afterSize);
        }
    }
    
    /**
     * Determine traffic status based on current vs baseline traffic.
     * Logs warnings at 50%, 75%, and 90% of the circuit breaker threshold.
     */
    TrafficStatus determineStatus(int sumCurrent, int baselineTraffic) {
        if (baselineTraffic == 0 || thresholdMultiplier == 0) {
            LOGGER.error("circuit_breaker_config_error: baselineTraffic is 0 or thresholdMultiplier is 0");
            throw new RuntimeException("invalid circuit breaker config: baselineTraffic=" + baselineTraffic + ", thresholdMultiplier=" + thresholdMultiplier);
        }
        
        int threshold = thresholdMultiplier * baselineTraffic;
        double thresholdPercent = (double) sumCurrent / threshold * 100;
        
        // log warnings at increasing thresholds before circuit breaker triggers
        if (thresholdPercent >= 90.0) {
            LOGGER.warn("high_message_volume: 90% of threshold reached, sumCurrent={}, threshold={} ({}x{}), thresholdPercent={}%", 
                       sumCurrent, threshold, thresholdMultiplier, baselineTraffic, String.format("%.1f", thresholdPercent));
        } else if (thresholdPercent >= 75.0) {
            LOGGER.warn("high_message_volume: 75% of threshold reached, sumCurrent={}, threshold={} ({}x{}), thresholdPercent={}%", 
                       sumCurrent, threshold, thresholdMultiplier, baselineTraffic, String.format("%.1f", thresholdPercent));
        } else if (thresholdPercent >= 50.0) {
            LOGGER.warn("high_message_volume: 50% of threshold reached, sumCurrent={}, threshold={} ({}x{}), thresholdPercent={}%", 
                       sumCurrent, threshold, thresholdMultiplier, baselineTraffic, String.format("%.1f", thresholdPercent));
        }
        
        if (sumCurrent >= threshold) {
            LOGGER.error("circuit_breaker_triggered: traffic threshold breached, sumCurrent={}, threshold={} ({}x{})", 
                       sumCurrent, threshold, thresholdMultiplier, baselineTraffic);
            return TrafficStatus.DELAYED_PROCESSING;
        }
        
        LOGGER.info("traffic within normal range: sumCurrent={}, threshold={} ({}x{}), thresholdPercent={}%", 
                   sumCurrent, threshold, thresholdMultiplier, baselineTraffic, String.format("%.1f", thresholdPercent));
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
    
}