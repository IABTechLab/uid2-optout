package com.uid2.optout.vertx;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class SqsBatchProcessorTest {

    private static final String VALID_HASH_BASE64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="; // 32 bytes
    private static final String VALID_ID_BASE64 = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="; // 32 bytes
    private static final long TEST_TIMESTAMP_MS = 1699308900000L; // Nov 7, 2023 in ms

    private static final int DEFAULT_WINDOW_SECONDS = 300; // 5 minutes
    
    private SqsBatchProcessor batchProcessor;

    @BeforeEach
    public void setUp() {
        // Pass null for SqsClient - filterEligibleMessages doesn't use it
        batchProcessor = new SqsBatchProcessor(null, "test-queue-url", DEFAULT_WINDOW_SECONDS);
    }

    private Message createValidMessage(String identityHash, String advertisingId, long timestampMs) {
        JsonObject body = new JsonObject()
                .put("identity_hash", identityHash)
                .put("advertising_id", advertisingId);

        Map<MessageSystemAttributeName, String> attributes = new HashMap<>();
        attributes.put(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(timestampMs));

        return Message.builder()
                .body(body.encode())
                .attributes(attributes)
                .messageId("test-message-id-" + timestampMs)
                .receiptHandle("test-receipt-handle")
                .build();
    }

    @Test
    public void testFilterEligibleMessages_allEligible() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        // Create messages from 10 minutes ago (600 seconds)
        long currentTime = 1000L;
        long oldTimestamp = currentTime - 600;
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], oldTimestamp, null, null, null, null));
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], oldTimestamp - 100, null, null, null, null));

        List<SqsParsedMessage> result = batchProcessor.filterEligibleMessages(messages, currentTime);

        assertEquals(2, result.size()); // All should be eligible (> 5 minutes old)
    }

    @Test
    public void testFilterEligibleMessages_noneEligible() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        // Create messages from 1 minute ago (too recent)
        long currentTime = 1000L;
        long recentTimestamp = currentTime - 60;
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], recentTimestamp, null, null, null, null));
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], recentTimestamp + 10, null, null, null, null));

        List<SqsParsedMessage> result = batchProcessor.filterEligibleMessages(messages, currentTime);

        assertEquals(0, result.size()); // None should be eligible (< 5 minutes old)
    }

    @Test
    public void testFilterEligibleMessages_mixedEligibility() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        long currentTime = 1000L;
        
        // Old enough (600 seconds ago)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime - 600, null, null, null, null));
        
        // Exactly at threshold (300 seconds ago)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime - 300, null, null, null, null));
        
        // Too recent (100 seconds ago)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime - 100, null, null, null, null));

        List<SqsParsedMessage> result = batchProcessor.filterEligibleMessages(messages, currentTime);

        assertEquals(2, result.size()); // First two are eligible (>= 300 seconds old)
    }

    @Test
    public void testFilterEligibleMessages_emptyList() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        long currentTime = 1000L;

        List<SqsParsedMessage> result = batchProcessor.filterEligibleMessages(messages, currentTime);

        assertEquals(0, result.size());
    }

    @Test
    public void testFilterEligibleMessages_boundaryCases() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        long currentTime = 1000L;
        int windowSeconds = DEFAULT_WINDOW_SECONDS; // 300
        
        // One second too new (299 seconds ago)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime - windowSeconds + 1, null, null, null, null));
        
        // Exactly at threshold (300 seconds ago)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime - windowSeconds, null, null, null, null));
        
        // One second past threshold (301 seconds ago)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime - windowSeconds - 1, null, null, null, null));

        List<SqsParsedMessage> result = batchProcessor.filterEligibleMessages(messages, currentTime);

        // Should only include the last two (>= threshold)
        assertEquals(2, result.size());
        assertEquals(currentTime - windowSeconds, result.get(0).timestamp());
        assertEquals(currentTime - windowSeconds - 1, result.get(1).timestamp());
    }

    @Test
    public void testFilterEligibleMessages_preservesOrder() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        long currentTime = 1000L;
        
        // Add eligible messages in specific order (all older than 300 seconds)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], 100, null, null, null, null));
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], 200, null, null, null, null));
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], 300, null, null, null, null));
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], 900, null, null, null, null)); // Too recent (100 seconds ago)

        List<SqsParsedMessage> result = batchProcessor.filterEligibleMessages(messages, currentTime);

        assertEquals(3, result.size());
        // Verify order is preserved
        assertEquals(100, result.get(0).timestamp());
        assertEquals(200, result.get(1).timestamp());
        assertEquals(300, result.get(2).timestamp());
    }

    @Test
    public void testFilterEligibleMessages_zeroWindowSeconds() {
        // Create processor with 0 window seconds
        SqsBatchProcessor zeroWindowProcessor = new SqsBatchProcessor(null, "test-queue-url", 0);
        
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        long currentTime = 1000L;
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime, null, null, null, null));   

        List<SqsParsedMessage> result = zeroWindowProcessor.filterEligibleMessages(messages, currentTime);

        assertEquals(1, result.size()); // With 0 window, current time messages should be eligible
    }
}

