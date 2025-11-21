package com.uid2.optout.vertx;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class SqsMessageParserTest {

    private static final String VALID_HASH_BASE64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="; // 32 bytes
    private static final String VALID_ID_BASE64 = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="; // 32 bytes
    private static final long TEST_TIMESTAMP_MS = 1699308900000L; // Nov 7, 2023 in ms
    private static final long TEST_TIMESTAMP_SEC = 1699308900L; // Nov 7, 2023 in seconds

    private Message createValidMessage(String identityHash, String advertisingId, long timestampMs) {
        JsonObject body = new JsonObject()
                .put("identity_hash", identityHash)
                .put("advertising_id", advertisingId);

        Map<MessageSystemAttributeName, String> attributes = new HashMap<>();
        attributes.put(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(timestampMs));

        return Message.builder()
                .body(body.encode())
                .attributes(attributes)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .build();
    }

    private Message createMessageWithoutTimestamp(String identityHash, String advertisingId) {
        JsonObject body = new JsonObject()
                .put("identity_hash", identityHash)
                .put("advertising_id", advertisingId);

        return Message.builder()
                .body(body.encode())
                .attributes(new HashMap<>())
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .build();
    }

    @Test
    public void testParseAndSortMessages_validMessages() {
        // Create messages with different timestamps
        List<Message> messages = Arrays.asList(
                createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS + 2000),
                createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS),
                createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS + 1000)
        );

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(messages);

        assertEquals(3, result.size());
        // Verify sorting (oldest first)
        assertEquals(TEST_TIMESTAMP_SEC, result.get(0).getTimestamp());
        assertEquals(TEST_TIMESTAMP_SEC + 1, result.get(1).getTimestamp());
        assertEquals(TEST_TIMESTAMP_SEC + 2, result.get(2).getTimestamp());
    }

    @Test
    public void testParseAndSortMessages_emptyList() {
        List<Message> messages = new ArrayList<>();
        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(messages);

        assertEquals(0, result.size());
    }

    @Test
    public void testParseAndSortMessages_missingIdentityHash() {
        JsonObject body = new JsonObject()
                .put("advertising_id", VALID_ID_BASE64);

        Map<MessageSystemAttributeName, String> attributes = new HashMap<>();
        attributes.put(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(TEST_TIMESTAMP_MS));

        Message message = Message.builder()
                .body(body.encode())
                .attributes(attributes)
                .messageId("test")
                .receiptHandle("test")
                .build();

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(Arrays.asList(message));

        assertEquals(0, result.size()); // Should skip invalid message
    }

    @Test
    public void testParseAndSortMessages_missingAdvertisingId() {
        JsonObject body = new JsonObject()
                .put("identity_hash", VALID_HASH_BASE64);

        Map<MessageSystemAttributeName, String> attributes = new HashMap<>();
        attributes.put(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(TEST_TIMESTAMP_MS));

        Message message = Message.builder()
                .body(body.encode())
                .attributes(attributes)
                .messageId("test")
                .receiptHandle("test")
                .build();

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(Arrays.asList(message));

        assertEquals(0, result.size()); // Should skip invalid message
    }

    @Test
    public void testParseAndSortMessages_invalidBase64() {
        JsonObject body = new JsonObject()
                .put("identity_hash", "not-valid-base64!!!")
                .put("advertising_id", VALID_ID_BASE64);

        Map<MessageSystemAttributeName, String> attributes = new HashMap<>();
        attributes.put(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(TEST_TIMESTAMP_MS));

        Message message = Message.builder()
                .body(body.encode())
                .attributes(attributes)
                .messageId("test")
                .receiptHandle("test")
                .build();

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(Arrays.asList(message));

        assertEquals(0, result.size()); // Should skip message with invalid base64
    }

    @Test
    public void testParseAndSortMessages_invalidJson() {
        Map<MessageSystemAttributeName, String> attributes = new HashMap<>();
        attributes.put(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(TEST_TIMESTAMP_MS));

        Message message = Message.builder()
                .body("not valid json")
                .attributes(attributes)
                .messageId("test")
                .receiptHandle("test")
                .build();

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(Arrays.asList(message));

        assertEquals(0, result.size()); // Should skip message with invalid JSON
    }

    @Test
    public void testParseAndSortMessages_missingTimestamp() {
        // Message without SentTimestamp should use current time as fallback
        Message message = createMessageWithoutTimestamp(VALID_HASH_BASE64, VALID_ID_BASE64);

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(Arrays.asList(message));

        assertEquals(1, result.size());
        // Timestamp should be close to current time (within 10 seconds)
        long currentTime = System.currentTimeMillis() / 1000;
        assertTrue(Math.abs(result.get(0).getTimestamp() - currentTime) < 10);
    }

    @Test
    public void testParseAndSortMessages_mixValidAndInvalid() {
        List<Message> messages = Arrays.asList(
                createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS),
                createMessageWithoutTimestamp("invalid-base64", VALID_ID_BASE64), // Invalid
                createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS + 1000)
        );

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(messages);

        assertEquals(2, result.size()); // Only valid messages
        assertEquals(TEST_TIMESTAMP_SEC, result.get(0).getTimestamp());
        assertEquals(TEST_TIMESTAMP_SEC + 1, result.get(1).getTimestamp());
    }

    @Test
    public void testFilterEligibleMessages_allEligible() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        // Create messages from 10 minutes ago
        long oldTimestamp = System.currentTimeMillis() / 1000 - 600;
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], oldTimestamp));
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], oldTimestamp - 100));

        long currentTime = System.currentTimeMillis() / 1000;
        List<SqsParsedMessage> result = SqsMessageParser.filterEligibleMessages(messages, 300, currentTime);

        assertEquals(2, result.size()); // All should be eligible (> 5 minutes old)
    }

    @Test
    public void testFilterEligibleMessages_noneEligible() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        // Create messages from 1 minute ago (too recent)
        long recentTimestamp = System.currentTimeMillis() / 1000 - 60;
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], recentTimestamp));
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], recentTimestamp + 10));

        long currentTime = System.currentTimeMillis() / 1000;
        List<SqsParsedMessage> result = SqsMessageParser.filterEligibleMessages(messages, 300, currentTime);

        assertEquals(0, result.size()); // None should be eligible (< 5 minutes old)
    }

    @Test
    public void testFilterEligibleMessages_mixedEligibility() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        long currentTime = 1000L;
        
        // Old enough (600 seconds ago)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime - 600));
        
        // Exactly at threshold (300 seconds ago)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime - 300));
        
        // Too recent (100 seconds ago)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime - 100));

        List<SqsParsedMessage> result = SqsMessageParser.filterEligibleMessages(messages, 300, currentTime);

        assertEquals(2, result.size()); // First two are eligible (>= 300 seconds old)
    }

    @Test
    public void testFilterEligibleMessages_emptyList() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        long currentTime = System.currentTimeMillis() / 1000;

        List<SqsParsedMessage> result = SqsMessageParser.filterEligibleMessages(messages, 300, currentTime);

        assertEquals(0, result.size());
    }

    @Test
    public void testParseAndSortMessages_timestampConversion() {
        // Verify milliseconds to seconds conversion
        Message message = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, 1699308912345L); // ms

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(Arrays.asList(message));

        assertEquals(1, result.size());
        assertEquals(1699308912L, result.get(0).getTimestamp()); // Should be in seconds
    }

    @Test
    public void testParseAndSortMessages_preservesOriginalMessage() {
        Message originalMessage = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(Arrays.asList(originalMessage));

        assertEquals(1, result.size());
        assertSame(originalMessage, result.get(0).getOriginalMessage());
    }

    @Test
    public void testParseAndSortMessages_sortingOrder() {
        // Create messages in random order
        List<Message> messages = Arrays.asList(
                createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, 1699308900000L), // timestamp 0
                createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, 1699308950000L), // timestamp 50
                createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, 1699308920000L), // timestamp 20
                createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, 1699308910000L), // timestamp 10
                createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, 1699308940000L)  // timestamp 40
        );

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(messages);

        assertEquals(5, result.size());
        // Verify ascending order
        for (int i = 1; i < result.size(); i++) {
            assertTrue(result.get(i - 1).getTimestamp() <= result.get(i).getTimestamp(),
                    "Messages should be sorted in ascending order by timestamp");
        }
    }

    @Test
    public void testFilterEligibleMessages_boundaryCases() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        long currentTime = 1000L;
        int windowSeconds = 300;
        
        // One second too new (299 seconds ago)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime - windowSeconds + 1));
        
        // Exactly at threshold (300 seconds ago)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime - windowSeconds));
        
        // One second past threshold (301 seconds ago)
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime - windowSeconds - 1));

        List<SqsParsedMessage> result = SqsMessageParser.filterEligibleMessages(messages, windowSeconds, currentTime);

        // Should only include the last two (>= threshold)
        assertEquals(2, result.size());
        assertEquals(currentTime - windowSeconds, result.get(0).getTimestamp());
        assertEquals(currentTime - windowSeconds - 1, result.get(1).getTimestamp());
    }

    @Test
    public void testParseAndSortMessages_parsesHashAndIdBytes() {
        Message message = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(Arrays.asList(message));

        assertEquals(1, result.size());
        assertNotNull(result.get(0).getHashBytes());
        assertNotNull(result.get(0).getIdBytes());
        assertEquals(32, result.get(0).getHashBytes().length);
        assertEquals(32, result.get(0).getIdBytes().length);
    }

    @Test
    public void testFilterEligibleMessages_preservesOrder() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        long currentTime = 1000L;
        
        // Add eligible messages in specific order
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], 100));
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], 200));
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], 300));
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], 900)); // Too recent

        List<SqsParsedMessage> result = SqsMessageParser.filterEligibleMessages(messages, 300, currentTime);

        assertEquals(3, result.size());
        // Verify order is preserved
        assertEquals(100, result.get(0).getTimestamp());
        assertEquals(200, result.get(1).getTimestamp());
        assertEquals(300, result.get(2).getTimestamp());
    }

    @Test
    public void testParseAndSortMessages_nullBodyFields() {
        JsonObject body = new JsonObject()
                .put("identity_hash", (String) null)
                .put("advertising_id", VALID_ID_BASE64);

        Map<MessageSystemAttributeName, String> attributes = new HashMap<>();
        attributes.put(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(TEST_TIMESTAMP_MS));

        Message message = Message.builder()
                .body(body.encode())
                .attributes(attributes)
                .messageId("test")
                .receiptHandle("test")
                .build();

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(Arrays.asList(message));

        assertEquals(0, result.size()); // Should skip message with null fields
    }

    @Test
    public void testParseAndSortMessages_multipleValidMessages() {
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            messages.add(createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS + i * 1000));
        }

        List<SqsParsedMessage> result = SqsMessageParser.parseAndSortMessages(messages);

        assertEquals(100, result.size());
        // Verify all are sorted
        for (int i = 1; i < result.size(); i++) {
            assertTrue(result.get(i - 1).getTimestamp() <= result.get(i).getTimestamp());
        }
    }

    @Test
    public void testFilterEligibleMessages_zeroWindowSeconds() {
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        long currentTime = 1000L;
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime));

        List<SqsParsedMessage> result = SqsMessageParser.filterEligibleMessages(messages, 0, currentTime);

        assertEquals(1, result.size()); // With 0 window, current time messages should be eligible
    }
}

