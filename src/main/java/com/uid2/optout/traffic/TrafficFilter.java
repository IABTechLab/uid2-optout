package com.uid2.optout.traffic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uid2.optout.sqs.SqsParsedMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

public class TrafficFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TrafficFilter.class);

    private final String trafficFilterConfigPath;
    List<TrafficFilterRule> filterRules;

    /**
     * Traffic filter rule defining a time range and a list of IP addresses to exclude
     */
    private static class TrafficFilterRule {
        private final long rangeStart;
        private final long rangeEnd;
        private final Set<String> ipAddresses;

        TrafficFilterRule(long rangeStart, long rangeEnd, Set<String> ipAddresses) {
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
            this.ipAddresses = ipAddresses;
        }

        public long getRangeStart() {
            return rangeStart;
        }
        public long getRangeEnd() {
            return rangeEnd;
        }
        public Set<String> getIpAddresses() {
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
    public TrafficFilter(String trafficFilterConfigPath) throws MalformedTrafficFilterConfigException {
        this.trafficFilterConfigPath = trafficFilterConfigPath;
        // Initial filter rules load
        this.filterRules = Collections.emptyList(); // start empty
        reloadTrafficFilterConfig(); // load ConfigMap

        LOGGER.info("initialized: filterRules={}", filterRules.size());
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
        LOGGER.info("loading traffic filter config");
        try (InputStream is = Files.newInputStream(Paths.get(trafficFilterConfigPath))) {
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            JsonObject filterConfigJson = new JsonObject(content);

            this.filterRules = parseFilterRules(filterConfigJson);

            LOGGER.info("loaded traffic filter config: filterRules={}", filterRules.size());

        } catch (Exception e) {
            LOGGER.error("circuit_breaker_config_error: no traffic filter config found at {}", trafficFilterConfigPath, e);
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
                LOGGER.error("circuit_breaker_config_error: denylist_requests is null");
                throw new MalformedTrafficFilterConfigException("invalid traffic filter config: denylist_requests is null");
            }
            for (int i = 0; i < denylistRequests.size(); i++) {
                JsonObject ruleJson = denylistRequests.getJsonObject(i);

                // parse and validate range
                var rangeJson = ruleJson.getJsonArray("range");
                if (rangeJson == null || rangeJson.size() != 2) {
                    LOGGER.error("circuit_breaker_config_error: rule range is not 2 elements, rule={}", ruleJson.encode());
                    throw new MalformedTrafficFilterConfigException("invalid traffic filter rule: range is not 2 elements");
                }

                long start = rangeJson.getLong(0);
                long end = rangeJson.getLong(1);

                if (start >= end) {
                    LOGGER.error("circuit_breaker_config_error: rule range start must be less than end, rule={}", ruleJson.encode());
                    throw new MalformedTrafficFilterConfigException("invalid traffic filter rule: range start must be less than end");
                }

                if (end - start > 86400) {
                    LOGGER.error("circuit_breaker_config_error: rule range must be 24 hours or less, rule={}", ruleJson.encode());
                    throw new MalformedTrafficFilterConfigException("invalid traffic filter rule: range must be 24 hours or less");
                }

                // parse IPs using stream
                var ipAddressesJson = ruleJson.getJsonArray("IPs");
                if (ipAddressesJson == null || ipAddressesJson.isEmpty()) {
                    LOGGER.error("circuit_breaker_config_error: rule IPs is empty, rule={}", ruleJson.encode());
                    throw new MalformedTrafficFilterConfigException("invalid traffic filter rule: IPs is empty");
                }

                Set<String> ipAddresses = ipAddressesJson.stream()
                        .map(Object::toString)
                        .collect(Collectors.toSet());

                TrafficFilterRule rule = new TrafficFilterRule(start, end, ipAddresses);

                LOGGER.info("loaded traffic filter rule: range=[{}, {}], IPs={}", rule.getRangeStart(), rule.getRangeEnd(), rule.getIpAddresses());
                rules.add(rule);
            }
            return rules;
        } catch (Exception e) {
            LOGGER.error("circuit_breaker_config_error: failed to parse rules, config={}, error={}", config.encode(), e.getMessage());
            throw new MalformedTrafficFilterConfigException(e.getMessage());
        }
    }

    public boolean isDenylisted(SqsParsedMessage message) {
        long timestamp = message.timestamp();
        String clientIp = message.clientIp();

        if (clientIp == null || clientIp.isEmpty()) {
            LOGGER.error("sqs_error: request does not contain client ip, messageId={}", message.originalMessage().messageId());
            return false;
        }

        for (TrafficFilterRule rule : filterRules) {
            if(timestamp >= rule.getRangeStart() && timestamp <= rule.getRangeEnd()) {
                if(rule.getIpAddresses().contains(clientIp)) {
                    return true;
                }
            }
        }
        return false;
    }

}