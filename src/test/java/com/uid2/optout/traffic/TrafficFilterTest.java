package com.uid2.optout.traffic;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.uid2.optout.sqs.SqsParsedMessage;
import com.uid2.optout.traffic.TrafficFilter;

import software.amazon.awssdk.services.sqs.model.Message;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class TrafficFilterTest {
    
    private static final String TEST_CONFIG_PATH = "./traffic-config.json";
    
    @Before
    public void setUp() {
        try {
            Files.deleteIfExists(Path.of(TEST_CONFIG_PATH));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @After
    public void tearDown() {
        try {
            Files.deleteIfExists(Path.of(TEST_CONFIG_PATH));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Test
    public void testParseFilterRules_emptyRules() throws Exception {
        // Setup - empty denylist
        String config = """
                {
                    "denylist_requests": []
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        
        // Act & Assert - no rules
        TrafficFilter filter = new TrafficFilter(TEST_CONFIG_PATH);
        assertEquals(0, filter.filterRules.size());
    }
    
    @Test
    public void testParseFilterRules_singleRule() throws Exception {
        // Setup - config with one rule
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700003600],
                            "IPs": ["192.168.1.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        
        // Act & Assert - one rule
        TrafficFilter filter = new TrafficFilter(TEST_CONFIG_PATH);
        assertEquals(1, filter.filterRules.size());
    }
    
    @Test
    public void testParseFilterRules_multipleRules() throws Exception {
        // Setup - config with multiple rules
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700003600],
                            "IPs": ["192.168.1.1"]
                        },
                        {
                            "range": [1700010000, 1700013600],
                            "IPs": ["10.0.0.1", "10.0.0.2"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        
        // Act & Assert - two rules
        TrafficFilter filter = new TrafficFilter(TEST_CONFIG_PATH);
        assertEquals(2, filter.filterRules.size());
    }
    
    @Test(expected = TrafficFilter.MalformedTrafficFilterConfigException.class)
    public void testParseFilterRules_missingDenylistRequests() throws Exception {
        // Setup - config without denylist_requests field
        String config = """
                {
                    "other_field": "value"
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        
        // Act & Assert - throws exception
        new TrafficFilter(TEST_CONFIG_PATH);
    }
    
    @Test(expected = TrafficFilter.MalformedTrafficFilterConfigException.class)
    public void testParseFilterRules_invalidRange_startAfterEnd() throws Exception {
        // Setup - range where start > end
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700003600, 1700000000],
                            "IPs": ["192.168.1.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        
        // Act & Assert - throws exception
        new TrafficFilter(TEST_CONFIG_PATH);
    }
    
    @Test(expected = TrafficFilter.MalformedTrafficFilterConfigException.class)
    public void testParseFilterRules_invalidRange_startEqualsEnd() throws Exception {
        // Setup - range where start == end
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700000000],
                            "IPs": ["192.168.1.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        
        // Act & Assert - throws exception
        new TrafficFilter(TEST_CONFIG_PATH);
    }
    
    @Test(expected = TrafficFilter.MalformedTrafficFilterConfigException.class)
    public void testParseFilterRules_rangeExceeds24Hours() throws Exception {
        // Setup - range longer than 24 hours (86400 seconds)
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700086401],
                            "IPs": ["192.168.1.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        
        // Act & Assert - throws exception
        new TrafficFilter(TEST_CONFIG_PATH);
    }
    
    @Test(expected = TrafficFilter.MalformedTrafficFilterConfigException.class)
    public void testParseFilterRules_emptyIPs() throws Exception {
        // Setup - rule with empty IP list
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700003600],
                            "IPs": []
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        
        // Act & Assert - throws exception
        new TrafficFilter(TEST_CONFIG_PATH);
    }
    
    @Test(expected = TrafficFilter.MalformedTrafficFilterConfigException.class)
    public void testParseFilterRules_missingIPs() throws Exception {
        // Setup - rule without IPs field
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700003600]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        
        // Act & Assert - throws exception
        new TrafficFilter(TEST_CONFIG_PATH);
    }
    
    @Test
    public void testIsDenylisted_matchingIPAndTimestamp() throws Exception {
        // Setup - filter with denylist rule
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700003600],
                            "IPs": ["192.168.1.1", "10.0.0.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        SqsParsedMessage message = createTestMessage(1700001800, "192.168.1.1");
        
        // Act & Assert - denylisted
        TrafficFilter filter = new TrafficFilter(TEST_CONFIG_PATH);
        assertTrue(filter.isDenylisted(message));
    }
    
    @Test
    public void testIsDenylisted_matchingIPOutsideTimeRange() throws Exception {
        // Setup - filter with denylist rule
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700003600],
                            "IPs": ["192.168.1.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        TrafficFilter filter = new TrafficFilter(TEST_CONFIG_PATH);
        
        // Act & Assert - message before range not denylisted
        SqsParsedMessage messageBefore = createTestMessage(1699999999, "192.168.1.1");
        assertFalse(filter.isDenylisted(messageBefore));
        // Act & Assert - message after range not denylisted
        SqsParsedMessage messageAfter = createTestMessage(1700003601, "192.168.1.1");
        assertFalse(filter.isDenylisted(messageAfter));
    }
    
    @Test
    public void testIsDenylisted_nonMatchingIP() throws Exception {
        // Setup - filter with denylist rule
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700003600],
                            "IPs": ["192.168.1.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        TrafficFilter filter = new TrafficFilter(TEST_CONFIG_PATH);

        // Act & Assert - non-matching IP not denylisted
        SqsParsedMessage message = createTestMessage(1700001800, "10.0.0.1");
        assertFalse(filter.isDenylisted(message));
    }
    
    @Test
    public void testIsDenylisted_atRangeBoundaries() throws Exception {
        // Setup - filter with denylist rule
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700003600],
                            "IPs": ["192.168.1.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        TrafficFilter filter = new TrafficFilter(TEST_CONFIG_PATH);
        
        // Act & Assert - message at start boundary (inclusive) denylisted
        SqsParsedMessage messageAtStart = createTestMessage(1700000000, "192.168.1.1");
        assertTrue(filter.isDenylisted(messageAtStart));
        
        // Act & Assert - message at end boundary (inclusive) denylisted
        SqsParsedMessage messageAtEnd = createTestMessage(1700003600, "192.168.1.1");
        assertTrue(filter.isDenylisted(messageAtEnd));
    }
    
    @Test
    public void testIsDenylisted_multipleRules() throws Exception {
        // Setup - multiple denylist rules
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700003600],
                            "IPs": ["192.168.1.1"]
                        },
                        {
                            "range": [1700010000, 1700013600],
                            "IPs": ["10.0.0.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        
        TrafficFilter filter = new TrafficFilter(TEST_CONFIG_PATH);
        
        // Act & Assert - message matches first rule
        SqsParsedMessage msg1 = createTestMessage(1700001800, "192.168.1.1");
        assertTrue(filter.isDenylisted(msg1));
        
        // Act & Assert - message matches second rule
        SqsParsedMessage msg2 = createTestMessage(1700011800, "10.0.0.1");
        assertTrue(filter.isDenylisted(msg2));
        
        // Act & Assert - message matches neither rule
        SqsParsedMessage msg3 = createTestMessage(1700005000, "172.16.0.1");
        assertFalse(filter.isDenylisted(msg3));
    }
    
    @Test
    public void testIsDenylisted_nullClientIp() throws Exception {
        // Setup - filter with denylist rule
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700003600],
                            "IPs": ["192.168.1.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        TrafficFilter filter = new TrafficFilter(TEST_CONFIG_PATH);
        
        // Act & Assert - message with null IP not denylisted
        SqsParsedMessage message = createTestMessage(1700001800, null);
        assertFalse(filter.isDenylisted(message));
    }
    
    @Test
    public void testReloadTrafficFilterConfig_success() throws Exception {
        // Setup - config with one rule
        String initialConfig = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700003600],
                            "IPs": ["192.168.1.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), initialConfig);
        
        // Act & Assert - one rule
        TrafficFilter filter = new TrafficFilter(TEST_CONFIG_PATH);
        assertEquals(1, filter.filterRules.size());
        
        // Setup - update config
        String updatedConfig = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700003600],
                            "IPs": ["192.168.1.1"]
                        },
                        {
                            "range": [1700010000, 1700013600],
                            "IPs": ["10.0.0.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), updatedConfig);
        
        // Act & Assert - two rules
        filter.reloadTrafficFilterConfig();
        assertEquals(2, filter.filterRules.size());
    }
    
    @Test(expected = TrafficFilter.MalformedTrafficFilterConfigException.class)
    public void testReloadTrafficFilterConfig_fileNotFound() throws Exception {
        // Setup, Act & Assert - try to create filter with non-existent config
        new TrafficFilter("./non-existent-file.json");
    }
    
    @Test
    public void testParseFilterRules_maxValidRange() throws Exception {
        // Setup - range exactly 24 hours (86400 seconds) - should be valid
        String config = """
                {
                    "denylist_requests": [
                        {
                            "range": [1700000000, 1700086400],
                            "IPs": ["192.168.1.1"]
                        }
                    ]
                }
                """;
        Files.writeString(Path.of(TEST_CONFIG_PATH), config);
        
        // Act & Assert - one rule
        TrafficFilter filter = new TrafficFilter(TEST_CONFIG_PATH);
        assertEquals(1, filter.filterRules.size());
    }
    
    /**
     * Helper method to create test SqsParsedMessage
     */
    private SqsParsedMessage createTestMessage(long timestamp, String clientIp) {
        Message mockMessage = Message.builder().build();
        byte[] hash = new byte[32];
        byte[] id = new byte[32];
        return new SqsParsedMessage(mockMessage, hash, id, timestamp, null, null, clientIp, null);
    }
}
