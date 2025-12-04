package com.uid2.optout.vertx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

public class OptOutTrafficFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(OptOutTrafficFilter.class);

    private final String trafficFilterConfigPath;
    List<TrafficFilterRule> filterRules;

    /**
     * Traffic filter rule defining a time range and a list of IP addresses to exclude
     */
    private static class TrafficFilterRule {
        private final List<Long> range;
        private final List<String> ipAddresses;

        TrafficFilterRule(List<Long> range, List<String> ipAddresses) {
            this.range = range;
            this.ipAddresses = ipAddresses;
        }

        public long getRangeStart() {
            return range.get(0);
        }
        public long getRangeEnd() {
            return range.get(1);
        }
        public List<String> getIpAddresses() {
            return ipAddresses;
        }
    }

    public static class MalformedTrafficFilterConfigException extends Exception {
        public MalformedTrafficFilterConfigException(String message) {
            super(message);
        }
    }

    /**
     * Constructor for OptOutTrafficFilter
     * 
     * @param trafficFilterConfigPath S3 path for traffic filter config
     * @throws MalformedTrafficFilterConfigException if the traffic filter config is invalid
     */
    public OptOutTrafficFilter(String trafficFilterConfigPath) throws MalformedTrafficFilterConfigException {
        this.trafficFilterConfigPath = trafficFilterConfigPath;
        // Initial filter rules load
        this.filterRules = Collections.emptyList(); // start empty
        reloadTrafficFilterConfig(); // load ConfigMap

        LOGGER.info("OptOutTrafficFilter initialized: filterRules={}", 
        filterRules.size());
    }

    /**
     * Reload traffic filter config from ConfigMap.
     * Expected format:
	 * {
	 *   "denylist_requests": [
	 *     {range: [startTimestamp, endTimestamp], IPs: ["ip1"]},
	 *     {range: [startTimestamp, endTimestamp], IPs: ["ip1", "ip2"]},
	 *     {range: [startTimestamp, endTimestamp], IPs: ["ip1", "ip3"]},
	 *   ]
	 * }
     * 
     * Can be called periodically to pick up config changes without restarting.
     */
    public void reloadTrafficFilterConfig() throws MalformedTrafficFilterConfigException {
        LOGGER.info("Loading traffic filter config from ConfigMap");
        try (InputStream is = Files.newInputStream(Paths.get(trafficFilterConfigPath))) {
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            JsonObject filterConfigJson = new JsonObject(content);

            this.filterRules = parseFilterRules(filterConfigJson);

            LOGGER.info("Successfully loaded traffic filter config from ConfigMap: filterRules={}",
                       filterRules.size());

        } catch (Exception e) {
            LOGGER.warn("No traffic filter config found at: {}", trafficFilterConfigPath, e);
            throw new MalformedTrafficFilterConfigException(e.getMessage());
        } 
    }

    /**
     * Parse request filtering rules from JSON config
     */
    List<TrafficFilterRule> parseFilterRules(JsonObject config) throws MalformedTrafficFilterConfigException {
        List<TrafficFilterRule> rules = new ArrayList<>();
        try {
            JsonArray denylistRequests = config.getJsonArray("denylist_requests");
            if (denylistRequests == null) {
                LOGGER.error("Invalid traffic filter config: denylist_requests is null");
                throw new MalformedTrafficFilterConfigException("Invalid traffic filter config: denylist_requests is null");
            }
            for (int i = 0; i < denylistRequests.size(); i++) {
                JsonObject ruleJson = denylistRequests.getJsonObject(i);

                // parse range
                var rangeJson = ruleJson.getJsonArray("range");
                List<Long> range = new ArrayList<>();
                if (rangeJson != null && rangeJson.size() == 2) {
                    long start = rangeJson.getLong(0);
                    long end = rangeJson.getLong(1);

                    if (start >= end) {
                        LOGGER.error("Invalid traffic filter rule: range start must be less than end: {}", ruleJson.encode());
                        throw new MalformedTrafficFilterConfigException("Invalid traffic filter rule: range start must be less than end");
                    }
                    range.add(start);
                    range.add(end);
                }

                // parse IPs
                var ipAddressesJson = ruleJson.getJsonArray("IPs");
                List<String> ipAddresses = new ArrayList<>();
                if (ipAddressesJson != null) {
                    for (int j = 0; j < ipAddressesJson.size(); j++) {
                        ipAddresses.add(ipAddressesJson.getString(j));
                    }
                }

                // log error and throw exception if rule is invalid
                if (range.size() != 2 || ipAddresses.size() == 0 || range.get(1) - range.get(0) > 86400) { // range must be 24 hours or less
                    LOGGER.error("Invalid traffic filter rule, range must be 24 hours or less: {}", ruleJson.encode());
                    throw new MalformedTrafficFilterConfigException("Invalid traffic filter rule, range must be 24 hours or less");
                }

                TrafficFilterRule rule = new TrafficFilterRule(range, ipAddresses);

                LOGGER.info("Loaded traffic filter rule: range=[{}, {}], IPs={}", rule.getRangeStart(), rule.getRangeEnd(), rule.getIpAddresses());
                rules.add(rule);
            }
            return rules;
        } catch (Exception e) {
            LOGGER.error("Failed to parse traffic filter rules: config={}, error={}", config.encode(), e.getMessage());
            throw new MalformedTrafficFilterConfigException(e.getMessage());
        }
    }

    public boolean isDenylisted(SqsParsedMessage message) {
        long timestamp = message.getTimestamp();
        String clientIp = message.getClientIp();

        if (clientIp == null || clientIp.isEmpty()) {
            LOGGER.error("Request does not contain client IP, timestamp={}", timestamp);
            return false;
        }

        for (TrafficFilterRule rule : filterRules) {
            if(timestamp >= rule.getRangeStart() && timestamp <= rule.getRangeEnd()) {
                if(rule.getIpAddresses().contains(clientIp)) {
                    return true;
                }
            };
        }
        return false;
    }

}