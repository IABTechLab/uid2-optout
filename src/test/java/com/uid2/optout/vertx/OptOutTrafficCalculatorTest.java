package com.uid2.optout.vertx;

import com.uid2.shared.cloud.CloudStorageException;
import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.optout.OptOutCollection;
import com.uid2.shared.optout.OptOutEntry;
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

import java.io.ByteArrayInputStream;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class OptOutTrafficCalculatorTest {

    @Mock
    private ICloudStorage cloudStorage;

    private static final String S3_DELTA_PREFIX = "optout-v2/delta/";
    private static final String TRAFFIC_CONFIG_PATH = "./traffic-config.json";

    @BeforeEach
    void setUp() {
        try {
            createTrafficConfigFile("{}");
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

    // ============================================================================
    // SECTION 1: Constructor & Initialization Tests
    // ============================================================================

    @Test
    void testConstructor_defaultThreshold() throws Exception {
        // Setup - default threshold of 5
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - DEFAULT when below threshold, DELAYED_PROCESSING when above threshold
        OptOutTrafficCalculator.TrafficStatus status = calculator.determineStatus(10, 3);
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status); // 10 < 5*3

        status = calculator.determineStatus(15, 3);
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status); // 15 >= 5*3
    }

    @Test
    void testConstructor_customThreshold() throws Exception {
        // Setup - custom threshold of 10
        createTrafficConfigFile("{\"traffic_calc_threshold_multiplier\": 10}");
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - DEFAULT when below threshold, DELAYED_PROCESSING when above threshold
        OptOutTrafficCalculator.TrafficStatus status = calculator.determineStatus(49, 5);
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status); // 49 < 10*5
        status = calculator.determineStatus(50, 5);
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status); // 50 >= 10*5
    }

    @Test
    void testConstructor_whitelistLoadFailure() throws Exception {
        // Setup - whitelist load failure
        createTrafficConfigFile("Invalid JSON");
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);
        calculator.reloadTrafficCalcConfig();

        // Assert - whitelist should be empty
        assertFalse(calculator.isInWhitelist(1000L));
    }

    // ============================================================================
    // SECTION 2: parseWhitelistRanges()
    // ============================================================================

    @Test
    void testParseWhitelistRanges_emptyConfig() throws Exception {
        // Setup - no config
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);
        JsonObject emptyConfig = new JsonObject();

        // Act
        List<List<Long>> ranges = calculator.parseWhitelistRanges(emptyConfig);

        // Assert - empty ranges
        assertTrue(ranges.isEmpty());
    }

    @Test
    void testParseWhitelistRanges_singleRange() throws Exception {
        // Setup - single range
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(1000L).add(2000L));
        configWithRanges.put("traffic_calc_whitelist_ranges", ranges);

        // Act
        List<List<Long>> result = calculator.parseWhitelistRanges(configWithRanges);

        // Assert - single range
        assertEquals(1, result.size());
        assertEquals(1000L, result.get(0).get(0));
        assertEquals(2000L, result.get(0).get(1));
    }

    @Test
    void testParseWhitelistRanges_multipleRanges() throws Exception {
        // Setup - multiple ranges
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(1000L).add(2000L))
            .add(new JsonArray().add(3000L).add(4000L))
            .add(new JsonArray().add(5000L).add(6000L));
        configWithRanges.put("traffic_calc_whitelist_ranges", ranges);

        // Act
        List<List<Long>> result = calculator.parseWhitelistRanges(configWithRanges);

        // Assert - multiple ranges
        assertEquals(3, result.size());
        assertEquals(1000L, result.get(0).get(0));
        assertEquals(3000L, result.get(1).get(0));
        assertEquals(5000L, result.get(2).get(0));
    }

    @Test
    void testParseWhitelistRanges_misorderedRange() throws Exception {
        // Setup - range with end < start should be corrected
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(2000L).add(1000L)); // End before start
        configWithRanges.put("traffic_calc_whitelist_ranges", ranges);

        // Act
        List<List<Long>> result = calculator.parseWhitelistRanges(configWithRanges);

        // Assert - should auto-correct to [1000, 2000]
        assertEquals(1, result.size());
        assertEquals(1000L, result.get(0).get(0));
        assertEquals(2000L, result.get(0).get(1));
    }

    @Test
    void testParseWhitelistRanges_sortsByStartTime() throws Exception {
        // Setup - ranges added out of order
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(5000L).add(6000L))
            .add(new JsonArray().add(1000L).add(2000L))
            .add(new JsonArray().add(3000L).add(4000L));
        configWithRanges.put("traffic_calc_whitelist_ranges", ranges);

        // Act
        List<List<Long>> result = calculator.parseWhitelistRanges(configWithRanges);

        // Assert - should be sorted by start time
        assertEquals(3, result.size());
        assertEquals(1000L, result.get(0).get(0));
        assertEquals(3000L, result.get(1).get(0));
        assertEquals(5000L, result.get(2).get(0));
    }

    @Test
    void testParseWhitelistRanges_invalidRangeTooFewElements() throws Exception {
        // Setup - invalid range with only 1 element;
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        JsonArray ranges = new JsonArray()
            .add(new JsonArray().add(1000L)) // Only 1 element
            .add(new JsonArray().add(2000L).add(3000L)); // Valid
        configWithRanges.put("traffic_calc_whitelist_ranges", ranges);

        // Act
        List<List<Long>> result = calculator.parseWhitelistRanges(configWithRanges);

        // Assert - should skip invalid range
        assertEquals(1, result.size());
        assertEquals(2000L, result.get(0).get(0));
    }

    @Test
    void testParseWhitelistRanges_nullArray() throws Exception {
        // Setup - null array
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        JsonObject configWithRanges = new JsonObject();
        configWithRanges.put("traffic_calc_whitelist_ranges", (JsonArray) null);

        // Act
        List<List<Long>> result = calculator.parseWhitelistRanges(configWithRanges);

        // Assert - empty ranges
        assertTrue(result.isEmpty());
    }

    // ============================================================================
    // SECTION 3: isInWhitelist()
    // ============================================================================

    @Test
    void testIsInWhitelist_withinSingleRange() throws Exception {
        // Setup - load whitelist with single range [1000, 2000]
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - true when within range
        assertTrue(calculator.isInWhitelist(1500L));
    }

    @Test
    void testIsInWhitelist_exactlyAtStart() throws Exception {
        // Setup - load whitelist with single range [1000, 2000]
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - true when exactly at start of range
        assertTrue(calculator.isInWhitelist(1000L));
    }

    @Test
    void testIsInWhitelist_exactlyAtEnd() throws Exception {
        // Setup - load whitelist with single range [1000, 2000]
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - true when exactly at end of range
        assertTrue(calculator.isInWhitelist(2000L));
    }

    @Test
    void testIsInWhitelist_beforeRange() throws Exception {
        // Setup - load whitelist with single range [1000, 2000]
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - false when before range
        assertFalse(calculator.isInWhitelist(999L));
    }

    @Test
    void testIsInWhitelist_afterRange() throws Exception {
        // Setup - load whitelist with single range [1000, 2000]
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - false when after range
        assertFalse(calculator.isInWhitelist(2001L));
    }

    @Test
    void testIsInWhitelist_betweenRanges() throws Exception {
        // Setup - load whitelist with two ranges [1000, 2000] and [3000, 4000]
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [1000, 2000],
                    [3000, 4000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - false when between ranges
        assertFalse(calculator.isInWhitelist(2500L));
    }

    @Test
    void testIsInWhitelist_emptyRanges() throws Exception {
        // Setup - no whitelist loaded (will fail and set empty)
        createTrafficConfigFile("Invalid JSON");

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - false when empty ranges
        assertFalse(calculator.isInWhitelist(1500L));
    }

    @Test
    void testIsInWhitelist_nullRanges() throws Exception {
        // Setup - no whitelist loaded (will fail and set empty)
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": null
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert - false when null/empty ranges
        assertFalse(calculator.isInWhitelist(1500L));
    }

    @Test
    void testIsInWhitelist_invalidRangeSize() throws Exception {
        // Setup - load whitelist with invalid range (only 1 element) and valid range
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [1000],
                    [2000, 3000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert
        assertFalse(calculator.isInWhitelist(1500L)); // Should not match invalid range
        assertTrue(calculator.isInWhitelist(2500L)); // Should match valid range
    }

    @Test
    void testIsInWhitelist_multipleRanges() throws Exception {
        // Setup - load whitelist with multiple ranges
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [1000, 2000],
                    [3000, 4000],
                    [5000, 6000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert
        assertTrue(calculator.isInWhitelist(1500L)); // In first range
        assertTrue(calculator.isInWhitelist(3500L)); // In second range
        assertTrue(calculator.isInWhitelist(5500L)); // In third range
        assertFalse(calculator.isInWhitelist(2500L)); // Between first and second
    }

    // ============================================================================
    // SECTION 4: getWhitelistDuration()
    // ============================================================================

    @Test
    void testGetWhitelistDuration_noRanges() throws Exception {
        // Setup - no ranges
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Assert
        assertEquals(0L, calculator.getWhitelistDuration(10000L, 5000L)); // 0 duration when no ranges
    }

    @Test
    void testGetWhitelistDuration_rangeFullyWithinWindow() throws Exception {
        // Setup - range fully within window
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [6000, 7000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window [5000, 10000], range [6000, 7000]
        long duration = calculator.getWhitelistDuration(10000L, 5000L);

        // Assert - full range duration
        assertEquals(1000L, duration);
    }

    @Test
    void testGetWhitelistDuration_rangePartiallyOverlapsStart() throws Exception {
        // Setup - range partially overlaps start of window
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [3000, 7000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window [5000, 10000], range [3000, 7000]
        long duration = calculator.getWhitelistDuration(10000L, 5000L);

        // Assert - should clip to [5000, 7000] = 2000
        assertEquals(2000L, duration);
    }

    @Test
    void testGetWhitelistDuration_rangePartiallyOverlapsEnd() throws Exception {
        // Setup - range partially overlaps end of window
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [8000, 12000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window [5000, 10000], range [8000, 12000]
        long duration = calculator.getWhitelistDuration(10000L, 5000L);

        // Assert - should clip to [8000, 10000] = 2000
        assertEquals(2000L, duration);
    }

    @Test
    void testGetWhitelistDuration_rangeCompletelyOutsideWindow() throws Exception {
        // Setup - range completely outside window
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window [5000, 10000], range [1000, 2000]
        long duration = calculator.getWhitelistDuration(10000L, 5000L);

        // Assert - 0 duration when range completely outside window
        assertEquals(0L, duration);
    }

    @Test
    void testGetWhitelistDuration_multipleRanges() throws Exception {
        // Setup - multiple ranges
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [6000, 7000],
                    [8000, 9000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window [5000, 10000], ranges [6000, 7000] and [8000, 9000]
        long duration = calculator.getWhitelistDuration(10000L, 5000L);

        // Assert - 1000 + 1000 = 2000
        assertEquals(2000L, duration);
    }

    @Test
    void testGetWhitelistDuration_rangeSpansEntireWindow() throws Exception {
        // Setup - range spans entire window
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [3000, 12000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - window [5000, 10000], range [3000, 12000]
        long duration = calculator.getWhitelistDuration(10000L, 5000L);

        // Assert - entire window is whitelisted = 5000
        assertEquals(5000L, duration);
    }

    // ============================================================================
    // SECTION 5: determineStatus()
    // ============================================================================

    @Test
    void testDetermineStatus_belowThreshold() throws Exception {
        // Setup - below threshold
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - 10 < 5 * 3
        OptOutTrafficCalculator.TrafficStatus status = calculator.determineStatus(10, 3);

        // Assert - DEFAULT when below threshold
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testDetermineStatus_atThreshold() throws Exception {
        // Setup - at threshold
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - 15 == 5 * 3
        OptOutTrafficCalculator.TrafficStatus status = calculator.determineStatus(15, 3);

        // Assert - DELAYED_PROCESSING when at threshold
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

    @Test
    void testDetermineStatus_aboveThreshold() throws Exception {
        // Setup - above threshold
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - 20 > 5 * 3
        OptOutTrafficCalculator.TrafficStatus status = calculator.determineStatus(20, 3);

        // Assert - DELAYED_PROCESSING when above threshold
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

    @Test
    void testDetermineStatus_sumPastZero() throws Exception {
        // Setup - sumPast is 0
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - should return DEFAULT to avoid crash
        OptOutTrafficCalculator.TrafficStatus status = calculator.determineStatus(100, 0);

        // Assert
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testDetermineStatus_bothZero() throws Exception {
        // Setup - both sumCurrent and sumPast are 0;
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - should return DEFAULT to avoid crash
        OptOutTrafficCalculator.TrafficStatus status = calculator.determineStatus(0, 0);

        // Assert
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testDetermineStatus_sumCurrentZero() throws Exception {
        // Setup - sumCurrent is 0
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - 0 < 5 * 10
        OptOutTrafficCalculator.TrafficStatus status = calculator.determineStatus(0, 10);

        // Assert - DEFAULT when sumCurrent is 0
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
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
        createTrafficConfigFile("{\"traffic_calc_threshold_multiplier\": " + threshold + "}");
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        OptOutTrafficCalculator.TrafficStatus status = calculator.determineStatus(sumCurrent, sumPast);

        // Assert
        assertEquals(OptOutTrafficCalculator.TrafficStatus.valueOf(expectedStatus), status);
    }

    @Test
    void testDetermineStatus_largeNumbers() throws Exception {
        // Setup - test with large numbers
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act 
        OptOutTrafficCalculator.TrafficStatus status = calculator.determineStatus(1_000_000, 200_000);

        // Assert - 1M >= 5 * 200K = 1M
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

    // ============================================================================
    // SECTION 6: S3 Config Reload Tests
    // ============================================================================

    @Test
    void testReloadWhitelist_success() throws Exception {
        // Setup - initial whitelist
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [1000, 2000],
                    [3000, 4000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Change the whitelist to a new range
        String newWhitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [5000, 6000]
                ]
            }
            """;
        createTrafficConfigFile(newWhitelistJson);

        // Act - reload the whitelist
        calculator.reloadTrafficCalcConfig();

        // Assert - verify new whitelist is loaded
        assertTrue(calculator.isInWhitelist(5500L));
    }

    @Test
    void testReloadWhitelist_failure() throws Exception {
        // Setup - initial whitelist
        String whitelistJson = """
            {
                "traffic_calc_whitelist_ranges": [
                    [1000, 2000]
                ]
            }
            """;
        createTrafficConfigFile(whitelistJson);

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Now make it fail
        createTrafficConfigFile("Invalid JSON");

        // Act - should not throw exception
        calculator.reloadTrafficCalcConfig();

        // Assert
        assertFalse(calculator.isInWhitelist(1500L));
    }

    // ============================================================================
    // SECTION 7: Cache Management Tests
    // ============================================================================

    @Test
    void testGetCacheStats_emptyCache() throws Exception {
        // Setup
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        Map<String, Object> stats = calculator.getCacheStats();

        // Assert - should return empty stats
        assertEquals(0, stats.get("cached_files"));
        assertEquals(0, stats.get("total_cached_timestamps"));
    }

    @Test
    void testClearCache() throws Exception {
        // Setup
        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        calculator.clearCache();

        // Assert - should return empty stats
        Map<String, Object> stats = calculator.getCacheStats();
        assertEquals(0, stats.get("cached_files"));
    }

    // ============================================================================
    // SECTION 8: Helper Methods for Test Data Creation
    // ============================================================================

    /**
     * Create a mock SQS message with specified timestamp
     */
    private Message createSqsMessage(long timestampSeconds) {
        Map<MessageSystemAttributeName, String> attributes = new HashMap<>();
        attributes.put(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(timestampSeconds * 1000));

        return Message.builder()
            .messageId("test-msg-" + timestampSeconds)
            .body("{\"test\": \"data\"}")
            .attributes(attributes)
            .build();
    }

    /**
     * Create a mock SQS message without timestamp
     */
    private Message createSqsMessageWithoutTimestamp() {
        return Message.builder()
            .messageId("test-msg-no-timestamp")
            .body("{\"test\": \"data\"}")
            .attributes(new HashMap<>())
            .build();
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

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(Collections.emptyList());

        // Assert - should return DEFAULT when no delta files
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testCalculateStatus_normalTraffic() throws Exception {
        // Setup - setup time: current time
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        // Create delta files with timestamps distributed over 48 hours
        List<Long> timestamps = new ArrayList<>();
        
        // Past window: t-47h to t-25h (add 50 entries)
        for (int i = 0; i < 50; i++) {
            timestamps.add(t - 47*3600 + i * 1000);
        }
        
        // Current window: t-23h to t-1h (add 100 entries - 2x past)
        for (int i = 0; i < 100; i++) {
            timestamps.add(t - 23*3600 + i * 1000);
        }
        
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<Message> sqsMessages = Arrays.asList(createSqsMessage(t));
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages);

        // Assert - 100+1 < 5 * 50 = 250, so should be DEFAULT
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testCalculateStatus_delayedProcessing() throws Exception {
        // Setup - create delta files with spike in current window
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        // Create delta files with spike in current window
        List<Long> timestamps = new ArrayList<>();
        
        // Past window: t-47h to t-25h (add 10 entries)
        for (int i = 0; i < 10; i++) {
            timestamps.add(t - 47*3600 + i * 1000);
        }
        
        // Current window: t-23h to t-1h (add 100 entries - 10x past!)
        for (int i = 0; i < 100; i++) {
            timestamps.add(t - 23*3600 + i * 1000);
        }
        
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<Message> sqsMessages = Arrays.asList(createSqsMessage(t));
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages);

        // Assert - 100+1 >= 5 * 10 = 50, DELAYED_PROCESSING
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
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

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - null SQS messages
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(null);

        // Assert - should still calculate based on delta files, DEFAULT
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
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

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - empty SQS messages
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(Collections.emptyList());

        // Assert - should still calculate based on delta files, DEFAULT
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testCalculateStatus_multipleSqsMessages() throws Exception {
        // Setup - create delta files with some entries
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        List<Long> timestamps = new ArrayList<>();
        // Past window - 10 entries
        for (int i = 0; i < 10; i++) {
            timestamps.add(t - 48*3600 + i * 1000);
        }
        // Current window - 20 entries
        for (int i = 0; i < 20; i++) {
            timestamps.add(t - 12*3600 + i * 1000);
        }
        
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - multiple SQS messages, oldest determines t
        // Add enough SQS messages to push total count over DELAYED_PROCESSING threshold
        List<Message> sqsMessages = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            sqsMessages.add(createSqsMessage(t - i * 10));
        }
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages);

        // Assert - DELAYED_PROCESSING
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

    @Test
    void testCalculateStatus_withWhitelist() throws Exception {
        // Setup - create delta files with some entries
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        // Whitelist that covers part of current window
        String whitelistJson = String.format("""
            {
                "traffic_calc_whitelist_ranges": [
                    [%d, %d]
                ]
            }
            """, t - 12*3600, t - 6*3600);
        
        List<Long> timestamps = new ArrayList<>();
        // Past window - 20 entries
        for (int i = 0; i < 20; i++) {
            timestamps.add(t - 48*3600 + i * 100);
        }
        
        // Current window - 100 entries (50 in whitelist range, 50 outside)
        for (int i = 0; i < 50; i++) {
            timestamps.add(t - 12*3600 + i * 100); // In whitelist
        }
        for (int i = 0; i < 50; i++) {
            timestamps.add(t - 3600 + i * 100); // Outside whitelist
        }
        
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        createTrafficConfigFile(whitelistJson);
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/delta-001.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<Message> sqsMessages = Arrays.asList(createSqsMessage(t));
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages);

        // Assert - should filter out whitelisted entries
        // Only ~50 from current window count (not whitelisted) + 1 SQS = 51
        // 51 < 5 * 20 = 100, so DEFAULT
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
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

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - first call should populate cache
        List<Message> sqsMessages = Arrays.asList(createSqsMessage(t));
        calculator.calculateStatus(sqsMessages);

        Map<String, Object> stats = calculator.getCacheStats();
        int cachedFiles = (Integer) stats.get("cached_files");

        // Second call should use cache (no additional S3 download)
        calculator.calculateStatus(sqsMessages);

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

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - should not throw exception
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(Collections.emptyList());

        // Assert - DEFAULT on error
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testCalculateStatus_deltaFileReadException() throws Exception {
        // Setup - S3 download error
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenThrow(new CloudStorageException("Failed to download"));

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - empty SQS messages
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(Collections.emptyList());

        // Assert - DEFAULT on error
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
    }

    @Test
    void testCalculateStatus_invalidSqsMessageTimestamp() throws Exception {
        // Setup - create delta files with some entries
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        
        List<Long> timestamps = Arrays.asList(t - 3600);
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act - SQS message without timestamp (should use current time)
        List<Message> sqsMessages = Arrays.asList(createSqsMessageWithoutTimestamp());
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages);

        // Assert - DEFAULT
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
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

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<Message> sqsMessages = Arrays.asList(createSqsMessage(t));
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages);

        // Assert - DEFAULT
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
        
        // Verify cache has both files
        Map<String, Object> stats = calculator.getCacheStats();
        assertEquals(2, stats.get("cached_files"));
    }

    @Test
    void testCalculateStatus_windowBoundaryTimestamps() throws Exception {
        // Setup - create delta files with some entries
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        long currentWindowStart = t - 24*3600 + 300; // t-23h55m
        long pastWindowStart = currentWindowStart - 24*3600; // t-47h55m
        
        List<Long> timestamps = Arrays.asList(
            t,
            currentWindowStart,
            pastWindowStart,
            t - 24*3600,
            t - 48*3600
        );
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<Message> sqsMessages = Arrays.asList(createSqsMessage(t));
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages);

        // Assert - DEFAULT
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
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

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<Message> sqsMessages = Arrays.asList(createSqsMessage(t));
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages);

        // Assert
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
        
        // Cache should contain the timestamps
        Map<String, Object> stats = calculator.getCacheStats();
        assertEquals(2, stats.get("total_cached_timestamps"));
    }

    @Test
    void testCalculateStatus_whitelistReducesPreviousWindowBaseline_customWindows() throws Exception {
        // Setup - test with custom 3-hour evaluation windows
        // Whitelist in previous window reduces baseline, causing DELAYED_PROCESSING
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        long threeHours = 3 * 3600; // 10800 seconds
        
        // Whitelist covering most of the PREVIOUS window (t-6h to t-4h)
        // This reduces the baseline count in the previous window
        String whitelistJson = String.format("""
            {
                "traffic_calc_current_evaluation_window_seconds": %d,
                "traffic_calc_previous_evaluation_window_seconds": %d,
                "traffic_calc_whitelist_ranges": [
                    [%d, %d]
                ]
            }
            """, threeHours, threeHours, t - 6*3600, t - 4*3600);
        
        List<Long> timestamps = new ArrayList<>();
        
        // Previous window (t-6h to t-3h): Add 100 entries
        // 80 of these will be whitelisted (between t-6h and t-4h)
        // Only 20 will count toward baseline
        for (int i = 0; i < 80; i++) {
            timestamps.add(t - 6*3600 + i); // Whitelisted entries in previous window
        }
        for (int i = 0; i < 20; i++) {
            timestamps.add(t - 4*3600 + i); // Non-whitelisted entries in previous window
        }
        
        // Current window (t-3h to t): Add 120 entries (none whitelisted)
        // This creates a spike: 120 + 1 SQS >= 5 * 20 = 100
        for (int i = 0; i < 120; i++) {
            timestamps.add(t - threeHours + i); // Current window entries
        }
        
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        createTrafficConfigFile(whitelistJson);
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<Message> sqsMessages = Arrays.asList(createSqsMessage(t));
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages);

        // Assert - DELAYED_PROCESSING
        // With 3-hour evaluation windows:
        // Previous window (t-6h to t-3h): 100 total entries, 80 whitelisted → 20 counted
        // Current window (t-3h to t): 120 entries (none whitelisted) + 1 SQS = 121
        // 121 >= 5 * 20 = 100, so DELAYED_PROCESSING
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DELAYED_PROCESSING, status);
    }

    @Test
    void testCalculateStatus_whitelistReducesCurrentWindowBaseline_customWindows() throws Exception {
        // Setup - test with custom 3-hour evaluation windows
        // Whitelist in previous window reduces baseline, causing DELAYED_PROCESSING
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;
        long threeHours = 3 * 3600; // 10800 seconds
        
        // Whitelist covering most of the CURRENT window (t-3h to t+5m)
        // This reduces the baseline count in the previous window
        String whitelistJson = String.format("""
            {
                "traffic_calc_current_evaluation_window_seconds": %d,
                "traffic_calc_previous_evaluation_window_seconds": %d,
                "traffic_calc_whitelist_ranges": [
                    [%d, %d]
                ]
            }
            """, threeHours, threeHours, t - 3*3600, t - 1*3600);
        
        List<Long> timestamps = new ArrayList<>();
        
        // Current window (t-3h to t+5m): Add 100 entries
        // 80 of these will be whitelisted (between t-3h and t-1h)
        // Only 20 will count toward baseline
        for (int i = 0; i < 80; i++) {
            timestamps.add(t - 3*3600 + i); // Whitelisted entries in current window
        }
        for (int i = 0; i < 20; i++) {
            timestamps.add(t - 1*3600 + i); // Non-whitelisted entries in current window
        }
        
        // Previous window (t-6h to t-3h): Add 10 entries (none whitelisted)
        for (int i = 0; i < 10; i++) {
            timestamps.add(t - 6*3600 + i); // Non-whitelisted entries in previous window
        }
        
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);
        
        createTrafficConfigFile(whitelistJson);
        when(cloudStorage.list(S3_DELTA_PREFIX)).thenReturn(Arrays.asList("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download("optout-v2/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
            .thenReturn(new ByteArrayInputStream(deltaFileBytes));

        OptOutTrafficCalculator calculator = new OptOutTrafficCalculator(
            cloudStorage, S3_DELTA_PREFIX, TRAFFIC_CONFIG_PATH);

        // Act
        List<Message> sqsMessages = Arrays.asList(createSqsMessage(t));
        OptOutTrafficCalculator.TrafficStatus status = calculator.calculateStatus(sqsMessages);

        // Assert - DEFAULT
        // With 3-hour evaluation windows:
        // Previous window (t-6h to t-3h): 10 entries (none whitelisted)
        // Current window (t-3h to t): 20 entries (non-whitelisted) + 1 SQS = 21
        // 21 < 5 * 10 = 50, so DEFAULT
        assertEquals(OptOutTrafficCalculator.TrafficStatus.DEFAULT, status);
    }
}
