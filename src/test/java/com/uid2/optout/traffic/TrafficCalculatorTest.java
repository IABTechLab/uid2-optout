package com.uid2.optout.traffic;

import com.uid2.shared.cloud.CloudStorageException;
import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.optout.OptOutCollection;
import com.uid2.shared.optout.OptOutEntry;
import com.uid2.optout.sqs.SqsMessageOperations;
import com.uid2.optout.sqs.SqsParsedMessage;
import com.uid2.optout.Const;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import com.uid2.optout.traffic.TrafficCalculator;
import com.uid2.optout.traffic.TrafficCalculator.MalformedTrafficCalcConfigException;
import java.io.ByteArrayInputStream;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TrafficCalculatorTest {

    @Mock
    private ICloudStorage cloudStorage;

    private static final String S3_DELTA_PREFIX = "optout-v2/delta/";
    private static final String TRAFFIC_CONFIG_PATH = "./traffic-config.json";
    private static final int BASELINE_TRAFFIC = 100;
    private static final int THRESHOLD_MULTIPLIER = 5;
    private static final int EVALUATION_WINDOW_SECONDS = 24 * 3600;

    @BeforeEach
    void setUp() {
        // default config
        JsonObject config = new JsonObject();
        config.put(Const.Config.OptOutTrafficCalcBaselineTrafficProp, BASELINE_TRAFFIC);
        config.put(Const.Config.OptOutTrafficCalcThresholdMultiplierProp, THRESHOLD_MULTIPLIER);
        config.put(Const.Config.OptOutTrafficCalcEvaluationWindowSecondsProp, EVALUATION_WINDOW_SECONDS);
        config.put(Const.Config.OptOutTrafficCalcAllowlistRangesProp, new JsonArray());
        try {
            createTrafficConfigFile(config.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tearDown() {
        if (Files.exists(Path.of(TRAFFIC_CONFIG_PATH))) {
            try {
                Files.delete(Path.of(TRAFFIC_CONFIG_PATH));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void createTrafficConfigFile(String content) {
        try {
            Path configPath = Path.of(TRAFFIC_CONFIG_PATH);
            Files.writeString(configPath, content);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Helper to create config by merging partial JSON with defaults
     */
    private void createConfigFromPartialJson(String partialJson) {
        JsonObject partial = new JsonObject(partialJson);
        JsonObject config = new JsonObject();
        
        // Set defaults
        if (!partial.containsKey(Const.Config.OptOutTrafficCalcBaselineTrafficProp)) {
            config.put(Const.Config.OptOutTrafficCalcBaselineTrafficProp, BASELINE_TRAFFIC);
        }
        if (!partial.containsKey(Const.Config.OptOutTrafficCalcThresholdMultiplierProp)) {
            config.put(Const.Config.OptOutTrafficCalcThresholdMultiplierProp, THRESHOLD_MULTIPLIER);
        }
        if (!partial.containsKey(Const.Config.OptOutTrafficCalcEvaluationWindowSecondsProp)) {
            config.put(Const.Config.OptOutTrafficCalcEvaluationWindowSecondsProp, EVALUATION_WINDOW_SECONDS);
        }
        if (!partial.containsKey(Const.Config.OptOutTrafficCalcAllowlistRangesProp)) {
            config.put(Const.Config.OptOutTrafficCalcAllowlistRangesProp, new JsonArray());
        }
        
        // Merge in partial config (overrides defaults)
        partial.forEach(entry -> config.put(entry.getKey(), entry.getValue()));
        
        createTrafficConfigFile(config.toString());
    }

    /**
     * Helper to create config with custom threshold
     */
    private void createConfigWithThreshold(int threshold) {
        createConfigFromPartialJson("{\"" + Const.Config.OptOutTrafficCalcThresholdMultiplierProp + "\": " + threshold + "}");
    }

    // ============================================================================
    // SECTION 1: Constructor & Initialization Tests
    // ============================================================================

    @Test
    void testConstructor_defaultThreshold() throws Exception {
        // Setup - default threshold of 5
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - DEFAULT when below threshold, DELAYED_PROCESSING when above threshold
        TrafficCalculator.TrafficStatus status = calculator.determineStatus(10, 3);
        assertEquals(TrafficCalculator.TrafficStatus.DEFAULT, status); // 10 < 5*3

        status = calculator.determineStatus(15, 3);
        assertEquals(TrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status); // 15 >= 5*3
    }

    @Test
    void testConstructor_customThreshold() throws Exception {
        // Setup - custom threshold of 10
        createConfigWithThreshold(10);
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - DEFAULT when below threshold, DELAYED_PROCESSING when above threshold
        TrafficCalculator.TrafficStatus status = calculator.determineStatus(49, 5);
        assertEquals(TrafficCalculator.TrafficStatus.DEFAULT, status); // 49 < 10*5
        status = calculator.determineStatus(50, 5);
        assertEquals(TrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status); // 50 >= 10*5
    }

    @Test
    void testConstructor_trafficCalcConfigLoadFailure() throws Exception {
        // Setup - traffic calc config load failure
        createTrafficConfigFile("Invalid JSON");
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            new TrafficCalculator(
                cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);
        });

        // Create valid config to test reload failure
        createConfigFromPartialJson("{}");
        TrafficCalculator calculator = new TrafficCalculator(
                cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        createTrafficConfigFile("Invalid JSON");
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            calculator.reloadTrafficCalcConfig();
        });
    }

    // ============================================================================
    // SECTION 2: parseTrafficCalcConfigRanges()
    // ============================================================================

    @Test
    void testParseTrafficCalcConfigRanges_emptyConfig() throws Exception {
        // Setup - no config
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);
        JsonObject emptyConfig = new JsonObject();

        // Act
        List<List<Long>> ranges = calculator.parseAllowlistRanges(emptyConfig);

        // Assert - empty ranges
        assertTrue(ranges.isEmpty());
    }

    @Test
    void testParseTrafficCalcConfigRanges_singleRange() throws Exception {
        // Setup - single range
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(1000L).add(2000L));
        configWithRanges.put("traffic_calc_allowlist_ranges", ranges);

        // Act
        List<List<Long>> result = calculator.parseAllowlistRanges(configWithRanges);

        // Assert - single range
        assertEquals(1, result.size());
        assertEquals(1000L, result.get(0).get(0));
        assertEquals(2000L, result.get(0).get(1));
    }

    @Test
    void testParseTrafficCalcConfigRanges_multipleRanges() throws Exception {
        // Setup - multiple ranges
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(1000L).add(2000L))
            .add(new JsonArray().add(3000L).add(4000L))
            .add(new JsonArray().add(5000L).add(6000L));
        configWithRanges.put("traffic_calc_allowlist_ranges", ranges);

        // Act
        List<List<Long>> result = calculator.parseAllowlistRanges(configWithRanges);

        // Assert - multiple ranges
        assertEquals(3, result.size());
        assertEquals(1000L, result.get(0).get(0));
        assertEquals(3000L, result.get(1).get(0));
        assertEquals(5000L, result.get(2).get(0));
    }

    @Test
    void testParseTrafficCalcConfigRanges_misorderedRange() throws Exception {
        // Setup - range with end < start is malformed
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(2000L).add(1000L)); // End before start
        configWithRanges.put("traffic_calc_allowlist_ranges", ranges);

        // Act
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            calculator.parseAllowlistRanges(configWithRanges);
        });
    }

    @Test
    void testParseTrafficCalcConfigRanges_rangeTooLong() throws Exception {
        // Setup - range longer than 24 hours is malformed
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(2000L).add(200000L)); // Longer than 24 hours
        configWithRanges.put("traffic_calc_allowlist_ranges", ranges);

        // Act
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            calculator.parseAllowlistRanges(configWithRanges);
        });
    }

    @Test
    void testParseTrafficCalcConfigRanges_sortsByStartTime() throws Exception {
        // Setup - ranges added out of order
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(5000L).add(6000L))
            .add(new JsonArray().add(1000L).add(2000L))
            .add(new JsonArray().add(3000L).add(4000L));
        configWithRanges.put("traffic_calc_allowlist_ranges", ranges);

        // Act
        List<List<Long>> result = calculator.parseAllowlistRanges(configWithRanges);

        // Assert - should be sorted by start time
        assertEquals(3, result.size());
        assertEquals(1000L, result.get(0).get(0));
        assertEquals(3000L, result.get(1).get(0));
        assertEquals(5000L, result.get(2).get(0));
    }

    @Test
    void testParseTrafficCalcConfigRanges_invalidRangeTooFewElements() throws Exception {
        // Setup - invalid range with only 1 element;
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(1000L)) // Only 1 element
            .add(new JsonArray().add(2000L).add(3000L)); // Valid
        configWithRanges.put("traffic_calc_allowlist_ranges", ranges);

        // Act
        List<List<Long>> result = calculator.parseAllowlistRanges(configWithRanges);

        // Assert - should skip invalid range
        assertEquals(1, result.size());
        assertEquals(2000L, result.get(0).get(0));
    }

    @Test
    void testParseTrafficCalcConfigRanges_nullArray() throws Exception {
        // Setup - null array
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        configWithRanges.put("traffic_calc_allowlist_ranges", (JsonArray) null);

        // Act
        List<List<Long>> result = calculator.parseAllowlistRanges(configWithRanges);

        // Assert - empty ranges
        assertTrue(result.isEmpty());
    }

    @Test
    void testParseTrafficCalcConfigRanges_overlappingRanges() throws Exception {
        // Setup - overlapping ranges
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(1000L).add(2000L))
            .add(new JsonArray().add(1500L).add(2500L)); // Overlaps with first range
        configWithRanges.put("traffic_calc_allowlist_ranges", ranges);

        // Act & Assert - should throw exception due to overlap
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            calculator.parseAllowlistRanges(configWithRanges);
        });
    }

    @Test
    void testParseTrafficCalcConfigRanges_adjacentRangesWithSameBoundary() throws Exception {
        // Setup - ranges where end of first equals start of second (touching but not overlapping semantically, but we treat as overlap)
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(1000L).add(2000L))
            .add(new JsonArray().add(2000L).add(3000L)); // Starts exactly where first ends
        configWithRanges.put("traffic_calc_allowlist_ranges", ranges);

        // Act & Assert - should throw exception because ranges touch at boundary
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            calculator.parseAllowlistRanges(configWithRanges);
        });
    }

    @Test
    void testParseTrafficCalcConfigRanges_nonOverlappingRanges() throws Exception {
        // Setup - ranges that don't overlap (with gap between them)
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(1000L).add(2000L))
            .add(new JsonArray().add(2001L).add(3000L)); // Starts after first ends
        configWithRanges.put("traffic_calc_allowlist_ranges", ranges);

        // Act
        List<List<Long>> result = calculator.parseAllowlistRanges(configWithRanges);

        // Assert - should succeed with 2 ranges
        assertEquals(2, result.size());
    }

    // ============================================================================
    // SECTION 3: isInTrafficCalcConfig()
    // ============================================================================

    @Test
    void testIsInTrafficCalcConfig_withinSingleRange() throws Exception {
        // Setup - load traffic calc config with single range [1000, 2000]
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - true when within range
        assertTrue(calculator.isInAllowlist(1500L));
    }

    @Test
    void testIsInTrafficCalcConfig_exactlyAtStart() throws Exception {
        // Setup - load traffic calc config with single range [1000, 2000]
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - true when exactly at start of range
        assertTrue(calculator.isInAllowlist(1000L));
    }

    @Test
    void testIsInTrafficCalcConfig_exactlyAtEnd() throws Exception {
        // Setup - load traffic calc config with single range [1000, 2000]
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - true when exactly at end of range
        assertTrue(calculator.isInAllowlist(2000L));
    }

    @Test
    void testIsInTrafficCalcConfig_beforeRange() throws Exception {
        // Setup - load traffic calc config with single range [1000, 2000]
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - false when before range
        assertFalse(calculator.isInAllowlist(999L));
    }

    @Test
    void testIsInTrafficCalcConfig_afterRange() throws Exception {
        // Setup - load traffic calc config with single range [1000, 2000]
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - false when after range
        assertFalse(calculator.isInAllowlist(2001L));
    }

    @Test
    void testIsInTrafficCalcConfig_betweenRanges() throws Exception {
        // Setup - load traffic calc config with two ranges [1000, 2000] and [3000, 4000]
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [1000, 2000],
                    [3000, 4000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - false when between ranges
        assertFalse(calculator.isInAllowlist(2500L));
    }

    @Test
    void testIsInTrafficCalcConfig_emptyRanges() throws Exception {
        // Setup uses default config from setUp() which has empty traffic calc config ranges
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - false when empty ranges
        assertFalse(calculator.isInAllowlist(1500L));
    }

    @Test
    void testIsInTrafficCalcConfig_nullRanges() throws Exception {
        // Setup - no traffic calc config ranges loaded (will fail and set empty)
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": null
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - false when null/empty ranges
        assertFalse(calculator.isInAllowlist(1500L));
    }

    @Test
    void testIsInTrafficCalcConfig_invalidRangeSize() throws Exception {
        // Setup - load traffic calc config with invalid range (only 1 element) and valid range
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [1000],
                    [2000, 3000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert
        assertFalse(calculator.isInAllowlist(1500L)); // Should not match invalid range
        assertTrue(calculator.isInAllowlist(2500L)); // Should match valid range
    }

    @Test
    void testIsInTrafficCalcConfig_multipleRanges() throws Exception {
        // Setup - load traffic calc config with multiple ranges
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [1000, 2000],
                    [3000, 4000],
                    [5000, 6000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert
        assertTrue(calculator.isInAllowlist(1500L)); // In first range
        assertTrue(calculator.isInAllowlist(3500L)); // In second range
        assertTrue(calculator.isInAllowlist(5500L)); // In third range
        assertFalse(calculator.isInAllowlist(2500L)); // Between first and second
    }

    // ============================================================================
    // SECTION 4: getTrafficCalcConfigDuration()
    // ============================================================================

    @Test
    void testGetTrafficCalcConfigDuration_noRanges() throws Exception {
        // Setup - no ranges
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert
        assertEquals(0L, calculator.getAllowlistDuration(10000L, 5000L)); // 0 duration when no ranges
    }

    @Test
    void testGetTrafficCalcConfigDuration_rangeFullyWithinWindow() throws Exception {
        // Setup - range fully within window
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [6000, 7000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window [5000, 10000], range [6000, 7000]
        long duration = calculator.getAllowlistDuration(10000L, 5000L);

        // Assert - full range duration
        assertEquals(1000L, duration);
    }

    @Test
    void testGetTrafficCalcConfigDuration_rangePartiallyOverlapsStart() throws Exception {
        // Setup - range partially overlaps start of window
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [3000, 7000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window [5000, 10000], range [3000, 7000]
        long duration = calculator.getAllowlistDuration(10000L, 5000L);

        // Assert - should clip to [5000, 7000] = 2000
        assertEquals(2000L, duration);
    }

    @Test
    void testGetTrafficCalcConfigDuration_rangePartiallyOverlapsEnd() throws Exception {
        // Setup - range partially overlaps end of window
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [8000, 12000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window [5000, 10000], range [8000, 12000]
        long duration = calculator.getAllowlistDuration(10000L, 5000L);

        // Assert - should clip to [8000, 10000] = 2000
        assertEquals(2000L, duration);
    }

    @Test
    void testGetTrafficCalcConfigDuration_rangeCompletelyOutsideWindow() throws Exception {
        // Setup - range completely outside window
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window [5000, 10000], range [1000, 2000]
        long duration = calculator.getAllowlistDuration(10000L, 5000L);

        // Assert - 0 duration when range completely outside window
        assertEquals(0L, duration);
    }

    @Test
    void testGetTrafficCalcConfigDuration_multipleRanges() throws Exception {
        // Setup - multiple ranges
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [6000, 7000],
                    [8000, 9000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window [5000, 10000], ranges [6000, 7000] and [8000, 9000]
        long duration = calculator.getAllowlistDuration(10000L, 5000L);

        // Assert - 1000 + 1000 = 2000
        assertEquals(2000L, duration);
    }

    @Test
    void testGetTrafficCalcConfigDuration_rangeSpansEntireWindow() throws Exception {
        // Setup - range spans entire window
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [3000, 12000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window [5000, 10000], range [3000, 12000]
        long duration = calculator.getAllowlistDuration(10000L, 5000L);

        // Assert - entire window is in traffic calc config ranges = 5000
        assertEquals(5000L, duration);
    }

    // ============================================================================
    // SECTION 4.5: calculateWindowStartWithAllowlist()
    // ============================================================================

    @Test
    void testCalculateWindowStartWithAllowlist_noAllowlist() throws Exception {
        // Setup - no allowlist ranges
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window should be [3, 8] with no extension
        long windowStart = calculator.calculateWindowStartWithAllowlist(8L, 5);

        // Assert - no allowlist, so window start is simply newestDeltaTs - evaluationWindowSeconds
        assertEquals(3L, windowStart);
    }

    @Test
    void testCalculateWindowStartWithAllowlist_allowlistInOriginalWindowOnly() throws Exception {
        // Setup - allowlist range only in original window, not in extended portion
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [6, 7]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - newestDeltaTs=8, evaluationWindow=5
        // Original window [3, 8] has [6,7] allowlisted (1 hour)
        // Extended portion [2, 3] has no allowlist
        // So window start should be 8 - 5 - 1 = 2
        long windowStart = calculator.calculateWindowStartWithAllowlist(8L, 5);

        assertEquals(2L, windowStart);
    }

    @Test
    void testCalculateWindowStartWithAllowlist_allowlistInExtendedPortion() throws Exception {
        // Setup - allowlist ranges in both original window AND extended portion
        // This is the user's example: evaluationWindow=5, newestDeltaTs=8, allowlist={[2,3], [6,7]}
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [2, 3],
                    [6, 7]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        // Original window [3, 8]: [6,7] allowlisted = 1 hour
        // First extension to [2, 8]: [2,3] and [6,7] allowlisted = 2 hours total
        // Second extension to [1, 8]: still [2,3] and [6,7] = 2 hours (no new allowlist)
        // Final: windowStart = 8 - 5 - 2 = 1
        long windowStart = calculator.calculateWindowStartWithAllowlist(8L, 5);

        assertEquals(1L, windowStart);
    }

    @Test
    void testCalculateWindowStartWithAllowlist_allowlistBeforeWindow() throws Exception {
        // Setup - allowlist range entirely before the initial window
        // This tests that we don't over-extend when allowlist is old
        // evaluationWindow=5, newestDeltaTs=20, allowlist=[10,13]
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [10, 13]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        // Initial window [15, 20]: no allowlist overlap, allowlistDuration = 0
        // No extension needed
        // Final: windowStart = 20 - 5 - 0 = 15
        long windowStart = calculator.calculateWindowStartWithAllowlist(20L, 5);

        // Verify: window [15, 20] has 5 hours, 0 allowlisted = 5 non-allowlisted
        assertEquals(15L, windowStart);
    }

    // ============================================================================
    // SECTION 5: determineStatus()
    // ============================================================================

    @Test
    void testDetermineStatus_belowThreshold() throws Exception {
        // Setup - below threshold
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - 10 < 5 * 3
        TrafficCalculator.TrafficStatus status = calculator.determineStatus(10, 3);

        // Assert - DEFAULT when below threshold
        assertEquals(TrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testDetermineStatus_atThreshold() throws Exception {
        // Setup - at threshold
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - 15 == 5 * 3
        TrafficCalculator.TrafficStatus status = calculator.determineStatus(15, 3);

        // Assert - DELAYED_PROCESSING when at threshold
        assertEquals(TrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

    @Test
    void testDetermineStatus_aboveThreshold() throws Exception {
        // Setup - above threshold
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - 20 > 5 * 3
        TrafficCalculator.TrafficStatus status = calculator.determineStatus(20, 3);

        // Assert - DELAYED_PROCESSING when above threshold
        assertEquals(TrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

    @Test
    void testDetermineStatus_sumPastZero() throws Exception {
        // Setup - sumPast is 0
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act & Assert - should throw exception for invalid config
        assertThrows(RuntimeException.class, () -> calculator.determineStatus(100, 0));
    }

    @Test
    void testDetermineStatus_bothZero() throws Exception {
        // Setup - both sumCurrent and sumPast are 0;
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act & Assert - should throw exception for invalid config
        assertThrows(RuntimeException.class, () -> calculator.determineStatus(0, 0));
    }

    @Test
    void testDetermineStatus_sumCurrentZero() throws Exception {
        // Setup - sumCurrent is 0
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - 0 < 5 * 10
        TrafficCalculator.TrafficStatus status = calculator.determineStatus(0, 10);

        // Assert - DEFAULT when sumCurrent is 0
        assertEquals(TrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @ParameterizedTest
    @CsvSource({
        "1, 1, 1, DELAYED_PROCESSING",  // threshold=1: 1 >= 1*1
        "2, 4, 2, DELAYED_PROCESSING",  // threshold=2: 4 >= 2*2
        "5, 10, 2, DELAYED_PROCESSING", // threshold=5: 10 >= 5*2
        "10, 100, 10, DELAYED_PROCESSING", // threshold=10: 100 >= 10*10
        "5, 24, 5, DEFAULT",            // threshold=5: 24 < 5*5
        "100, 1000, 11, DEFAULT"        // threshold=100: 1000 < 100*11
    })
    void testDetermineStatus_variousThresholds(int threshold, int sumCurrent, int sumPast, String expectedStatus) throws Exception {
        // Setup - various thresholds
        createConfigWithThreshold(threshold);
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        TrafficCalculator.TrafficStatus status = calculator.determineStatus(sumCurrent, sumPast);

        // Assert
        assertEquals(TrafficCalculator.TrafficStatus.valueOf(expectedStatus), status);
    }

    @Test
    void testDetermineStatus_largeNumbers() throws Exception {
        // Setup - test with large numbers
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act 
        TrafficCalculator.TrafficStatus status = calculator.determineStatus(1_000_000, 200_000);

        // Assert - 1M >= 5 * 200K = 1M
        assertEquals(TrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

    // ============================================================================
    // SECTION 6: S3 Config Reload Tests
    // ============================================================================

    @Test
    void testReloadTrafficCalcConfig_success() throws Exception {
        // Setup - initial traffic calc config
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [1000, 2000],
                    [3000, 4000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Change the traffic calc config to a new range
        String newTrafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [5000, 6000]
                ]
            }
            """;
        createConfigFromPartialJson(newTrafficCalcConfigJson);

        // Act - reload the traffic calc config
        calculator.reloadTrafficCalcConfig();

        // Assert - verify new traffic calc config is loaded
        assertTrue(calculator.isInAllowlist(5500L));
    }

    @Test
    void testReloadTrafficCalcConfig_failure() throws Exception {
        // Setup - initial traffic calc config
        String trafficCalcConfigJson = """
            {
                "traffic_calc_allowlist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createConfigFromPartialJson(trafficCalcConfigJson);

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Now make it fail
        createTrafficConfigFile("Invalid JSON");

        // Act - should not throw exception
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            calculator.reloadTrafficCalcConfig();
        });

    }

    @Test
    public void testReloadTrafficCalcConfig_failure_missingKeys() throws Exception {
        // Setup
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act & Assert missing threshold multiplier
        createTrafficConfigFile("{\"traffic_calc_evaluation_window_seconds\": 86400, \"traffic_calc_baseline_traffic\": 100, \"traffic_calc_allowlist_ranges\": [ [1000, 2000] ]}");
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            calculator.reloadTrafficCalcConfig();
        });

        // Act & Assert missing evaluation window seconds
        createTrafficConfigFile("{\"traffic_calc_threshold_multiplier\": 5, \"traffic_calc_baseline_traffic\": 100, \"traffic_calc_allowlist_ranges\": [ [1000, 2000] ]}");
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            calculator.reloadTrafficCalcConfig();
        });

        // Act & Assert missing baseline traffic
        createTrafficConfigFile("{\"traffic_calc_threshold_multiplier\": 5, \"traffic_calc_evaluation_window_seconds\": 86400, \"traffic_calc_allowlist_ranges\": [ [1000, 2000] ]}");
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            calculator.reloadTrafficCalcConfig();
        });

        // Act & Assert missing traffic calc config ranges
        createTrafficConfigFile("{\"traffic_calc_threshold_multiplier\": 5, \"traffic_calc_evaluation_window_seconds\": 86400, \"traffic_calc_baseline_traffic\": 100}");
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            calculator.reloadTrafficCalcConfig();
        });
    }

    @Test
    public void testReloadTrafficCalcConfig_failure_misorderedRanges() throws Exception {
        // Setup - misordered ranges
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);
        createConfigFromPartialJson("{\"traffic_calc_allowlist_ranges\": [ [2000, 1000] ]}");

        // Act & Assert 
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            calculator.reloadTrafficCalcConfig();
        });
    }

    @Test
    public void testReloadTrafficCalcConfig_failure_rangeTooLong() throws Exception {
        // Setup - range greater than 24 hours
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);
        createConfigFromPartialJson("{\"traffic_calc_allowlist_ranges\": [ [1000, 200000] ]}");

        // Act & Assert
        assertThrows(MalformedTrafficCalcConfigException.class, () -> {
            calculator.reloadTrafficCalcConfig();
        });
    }

    // ============================================================================
    // SECTION 7: Cache Management Tests (also tested in section 9)
    // ============================================================================

    @Test
    void testGetCacheStats_emptyCache() throws Exception {
        // Setup
        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        Map<String, Object> stats = calculator.getCacheStats();

        // Assert - should return empty stats
        assertEquals(0, stats.get("cached_files"));
        assertEquals(0, stats.get("total_cached_timestamps"));
    }

    // ============================================================================
    // SECTION 8: Helper Methods for Test Data Creation
    // ============================================================================

    /**
     * Create a mock SQS message with specified timestamp
     */
    private SqsParsedMessage createSqsMessage(long timestampSeconds) {
        return new SqsParsedMessage(
            Message.builder().build(), 
            new byte[32], 
            new byte[32], 
            timestampSeconds, 
            "test@test.test", 
            null, 
            "127.0.0.1", 
            "test-trace-id");
    }

    /**
     * Create delta file bytes with specified timestamps
     */
    private byte[] createDeltaFileBytes(List<Long> timestamps) throws Exception {
        // Create OptOutEntry objects using newTestEntry
        List<OptOutEntry> entries = new ArrayList<>();
        
        long idCounter = 1000; // Use incrementing IDs for test entries
        for (long timestamp : timestamps) {
            entries.add(OptOutEntry.newTestEntry(idCounter++, timestamp));
        }
        
        // Create OptOutCollection
        OptOutCollection collection = new OptOutCollection(entries.toArray(new OptOutEntry[0]));
        return collection.getStore();
    }


    // ============================================================================
    // SECTION 9: Tests for calculateStatus()
    // ============================================================================

    @Test
    void testCalculateStatus_noDeltaFiles() throws Exception {
        // Setup - no delta files
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Collections.emptyList());

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act & Assert - should throw exception when no delta files
        assertThrows(RuntimeException.class, () -> calculator.calculateStatus(Collections.emptyList(), null, 0, 0));
    }

    @Test
    void testCalculateStatus_normalTraffic() throws Exception {
        // Setup - setup time: current time
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        // Create delta files with timestamps distributed over 48 hours
        List<Long> timestamps = new ArrayList<>();
        
        // add 499 entries in current window
        for (int i = 0; i < 49; i++) {
            timestamps.add(t - 23*3600 + i * 60);
        }
        
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<SqsParsedMessage> sqsMessages = Arrays.asList(createSqsMessage(t));
        TrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages, null, 0, 0);

        // Assert - 100+1 < 5 * 50 = 250, so should be DEFAULT
        assertEquals(TrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testCalculateStatus_delayedProcessing() throws Exception {
        // Setup - create delta files with spike in current window
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        // Create delta files with spike in current window
        List<Long> timestamps = new ArrayList<>();
        
        // add 500 entries in current window
        for (int i = 0; i < 500; i++) {
            timestamps.add(t - 23*3600 + i * 60);
        }
        
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<SqsParsedMessage> sqsMessages = Arrays.asList(createSqsMessage(t));
        TrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages, null, 0, 0);

        // Assert - 100+1 >= 5 * 10 = 50, DELAYED_PROCESSING
        assertEquals(TrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

    @Test
    void testCalculateStatus_noSqsMessages() throws Exception {
        // Setup - create delta files with some entries
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        List<Long> timestamps = Arrays.asList(t - 3600, t - 7200); // Some entries
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - null SQS messages
        TrafficCalculator.TrafficStatus status = calculator.calculateStatus(null, null, 0, 0);

        // Assert - should still calculate based on delta files, DEFAULT
        assertEquals(TrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testCalculateStatus_emptySqsMessages() throws Exception {
        // Setup - create delta files with some entries
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        List<Long> timestamps = Arrays.asList(t - 3600);
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - empty SQS messages
        TrafficCalculator.TrafficStatus status = calculator.calculateStatus(Collections.emptyList(), null, 0, 0);

        // Assert - should still calculate based on delta files, DEFAULT
        assertEquals(TrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testCalculateStatus_multipleSqsMessages() throws Exception {
        // Setup - create delta files with some entries
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        List<Long> timestamps = new ArrayList<>();
        // add 470 entries in window
        for (int i = 0; i < 470; i++) {
            timestamps.add(t - 24*3600 + i * 60); 
        }
        
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Add 30 SQS entries in [t, t+5min]
        List<SqsParsedMessage> sqsMessages = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            sqsMessages.add(createSqsMessage(t - i * 10));
        }
        TrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages, null, 0, 0);

        // Assert - DELAYED_PROCESSING
        assertEquals(TrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

    @Test
    void testCalculateStatus_withTrafficCalcConfig() throws Exception {
        // Setup - create delta files with some entries
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        // Traffic calc config that covers part of window
        String trafficCalcConfigJson = String.format("""
            {
                "traffic_calc_allowlist_ranges": [
                    [%d, %d]
                ]
            }
            """, t - 12*3600, t - 6*3600);
        
        List<Long> timestamps = new ArrayList<>();
        
        // window - 600 entries (300 in traffic calc config range, 300 outside)
        for (int i = 0; i < 300; i++) {
            timestamps.add(t - 12*3600 + i);
        }
        for (int i = 0; i < 300; i++) {
            timestamps.add(t - 3600 + i);
        }
        
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        createConfigFromPartialJson(trafficCalcConfigJson);
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta-001_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta-001_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<SqsParsedMessage> sqsMessages = Arrays.asList(createSqsMessage(t));
        TrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages, null, 0, 0);

        // Assert - should filter out entries in traffic calc config ranges
        // Only 300 from window count (not in traffic calc config ranges) + 1 SQS = 301
        // 301 < 5*100, so DEFAULT
        assertEquals(TrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testCalculateStatus_cacheUtilization() throws Exception {
        // Setup - create delta files with some entries
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        List<Long> timestamps = Arrays.asList(t - 3600, t - 7200);
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - first call should populate cache
        List<SqsParsedMessage> sqsMessages = Arrays.asList(createSqsMessage(t));
        calculator.calculateStatus(sqsMessages, null, 0, 0);

        Map<String, Object> stats = calculator.getCacheStats();
        int cachedFiles = (Integer) stats.get("cached_files");

        // Second call should use cache (no additional S3 download)
        calculator.calculateStatus(sqsMessages, null, 0, 0);

        Map<String, Object> stats2 = calculator.getCacheStats();
        int cachedFiles2 = (Integer) stats2.get("cached_files");

        // Assert - cache should be populated and remain consistent
        assertEquals(1, cachedFiles);
        assertEquals(cachedFiles, cachedFiles2);
        
        // Verify S3 download was called only once per file
        verify(cloudStorage, times(1)).download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat");
    }

    @Test
    void testCalculateStatus_s3Exception() throws Exception {
        // Setup - S3 list error
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenThrow(new RuntimeException("S3 error"));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act & Assert - should throw exception on S3 error
        assertThrows(RuntimeException.class, () -> calculator.calculateStatus(Collections.emptyList(), null, 0, 0));
    }

    @Test
    void testCalculateStatus_deltaFileReadException() throws Exception {
        // Setup - S3 download error
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenThrow(new CloudStorageException("Failed to download"));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act & Assert - should throw exception on S3 download error
        assertThrows(RuntimeException.class, () -> calculator.calculateStatus(Collections.emptyList(), null, 0, 0));
    }

    @Test
    void testCalculateStatus_multipleDeltaFiles() throws Exception {
        // Setup - create delta files with some entries
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        // File 1 - recent entries
        List<Long> timestamps1 = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            timestamps1.add(t - 12*3600 + i * 1000);
        }
        byte[] deltaFileBytes1 = createDeltaFileBytes(timestamps1);
        
        // File 2 - older entries
        List<Long> timestamps2 = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            timestamps2.add(t - 36*3600 + i * 1000);
        }
        byte[] deltaFileBytes2 = createDeltaFileBytes(timestamps2);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList(
            "optout-v2/delta/optout-delta--01_2025-11-13T02.00.00Z_bbbbbbbb.dat",
            "optout-v2/delta/optout-delta--01_2025-11-13T01.00.00Z_aaaaaaaa.dat"
        ));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T02.00.00Z_bbbbbbbb.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes1));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T01.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes2));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<SqsParsedMessage> sqsMessages = Arrays.asList(createSqsMessage(t));
        TrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages, null, 0, 0);

        // Assert - DEFAULT
        assertEquals(TrafficCalculator.TrafficStatus.DEFAULT, status);
        
        // Verify cache has both files
        Map<String, Object> stats = calculator.getCacheStats();
        assertEquals(2, stats.get("cached_files"));
    }

    @Test
    void testCalculateStatus_windowBoundaryTimestamp() throws Exception {
        // Setup - create delta file with timestamps at window boundary
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        long currentWindowStart = t - 24*3600;
        List<Long> timestamps = new ArrayList<>();
        for (int i = 0; i < 250; i++) {
            timestamps.add(t);
        }
        for (int i = 0; i < 250; i++) {
            timestamps.add(currentWindowStart);
        }
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<SqsParsedMessage> sqsMessages = Arrays.asList(createSqsMessage(t));
        TrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages, null, 0, 0);

        // Assert - DEFAULT
        assertEquals(TrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

    @Test
    void testCalculateStatus_timestampsCached() throws Exception {
        // Setup - create delta files with some entries
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        List<Long> timestamps = Arrays.asList(t - 3600, t - 7200);
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<SqsParsedMessage> sqsMessages = Arrays.asList(createSqsMessage(t));
        TrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages, null, 0, 0);

        // Assert
        assertEquals(TrafficCalculator.TrafficStatus.DEFAULT, status);
        
        // Cache should contain the timestamps
        Map<String, Object> stats = calculator.getCacheStats();
        assertEquals(2, stats.get("total_cached_timestamps"));
    }

    // ============================================================================
    // SECTION 10: Tests for queue attributes (invisible messages from other consumers)
    // ============================================================================

    @Test
    void testCalculateStatus_delayedProcessingFromQueueAttributesOnly() throws Exception {
        // Setup - delta files with low traffic (10 records)
        // Threshold = 100 * 5 = 500
        // Queue attributes will have 600 invisible messages (other consumers processing)
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        List<Long> timestamps = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            timestamps.add(t - 3600 + i); // 10 entries from 1 hour ago
        }
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - 1 message read by us, but 600 invisible messages from other consumers
        List<SqsParsedMessage> sqsMessages = Arrays.asList(createSqsMessage(t));
        
        // QueueAttributes: 0 visible, 600 invisible (other consumers), 0 delayed
        // Since we read 1 message, otherConsumers = 600 - 1 = 599
        // Total = 10 (delta) + 1 (our message) + 599 (other consumers) = 610 >= 500 threshold
        SqsMessageOperations.QueueAttributes queueAttributes = 
            new SqsMessageOperations.QueueAttributes(0, 600, 0);
        
        TrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages, queueAttributes, 0, 0);

        // Assert - DELAYED_PROCESSING due to high invisible message count from other consumers
        assertEquals(TrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

    @Test
    void testCalculateStatus_delayedProcessingFromBothQueueAndMessages() throws Exception {
        // Setup - delta files with moderate traffic (100 records)
        // Threshold = 100 * 5 = 500
        // We'll have 200 messages + 250 invisible from other consumers = 550 > 500
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        List<Long> timestamps = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            timestamps.add(t - 3600 + i); // 100 entries from 1 hour ago
        }
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        TrafficCalculator calculator = new TrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - 200 messages read by us + 450 invisible (200 are ours + 250 from others)
        // Messages must be within 5-minute window to be counted, so use 1-second spacing
        List<SqsParsedMessage> sqsMessages = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            sqsMessages.add(createSqsMessage(t - i)); // 1 second apart, all within 5-minute window
        }
        
        // QueueAttributes: 0 visible, 450 invisible (200 ours + 250 others), 0 delayed
        // otherConsumers = 450 - 200 = 250
        // Total = 100 (delta) + 200 (our messages) + 250 (other consumers) = 550 >= 500 threshold
        SqsMessageOperations.QueueAttributes queueAttributes = 
            new SqsMessageOperations.QueueAttributes(0, 450, 0);
        
        TrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages, queueAttributes, 0, 0);

        // Assert - DELAYED_PROCESSING due to combined count exceeding threshold
        assertEquals(TrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

}