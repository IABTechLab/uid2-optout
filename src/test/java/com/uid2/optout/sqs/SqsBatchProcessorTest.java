package com.uid2.optout.sqs;

import com.uid2.optout.delta.S3UploadService;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class SqsBatchProcessorTest {

    private static final String VALID_HASH_BASE64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="; // 32 bytes
    private static final String VALID_ID_BASE64 = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="; // 32 bytes
    private static final long TEST_TIMESTAMP_MS = 1699308900000L; // Nov 7, 2023 in ms
    private static final String TEST_MALFORMED_S3_PATH = "malformed/";

    private static final int DEFAULT_WINDOW_SECONDS = 300; // 5 minutes
    
    private SqsBatchProcessor batchProcessor;

    @BeforeEach
    public void setUp() {
        batchProcessor = new SqsBatchProcessor(null, "test-queue-url", DEFAULT_WINDOW_SECONDS, null, "", 0);
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
        SqsBatchProcessor zeroWindowProcessor = new SqsBatchProcessor(null, "test-queue-url", 0, null, "", 0);
        
        List<SqsParsedMessage> messages = new ArrayList<>();
        Message mockMsg = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, TEST_TIMESTAMP_MS);
        
        long currentTime = 1000L;
        messages.add(new SqsParsedMessage(mockMsg, new byte[32], new byte[32], currentTime, null, null, null, null));   

        List<SqsParsedMessage> result = zeroWindowProcessor.filterEligibleMessages(messages, currentTime);

        assertEquals(1, result.size()); // With 0 window, current time messages should be eligible
    }

    // ==================== Malformed Message Upload Tests ====================

    private Message createMalformedMessage(String messageId, String body) {
        // use old timestamp so message is eligible for processing
        long oldTimestampMs = System.currentTimeMillis() - 600_000; // 10 minutes ago
        Map<MessageSystemAttributeName, String> attributes = new HashMap<>();
        attributes.put(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(oldTimestampMs));

        return Message.builder()
                .body(body)
                .attributes(attributes)
                .messageId(messageId)
                .receiptHandle("receipt-" + messageId)
                .build();
    }

    @Test
    public void testProcessBatch_malformedMessagesUploadedToS3() throws IOException {
        // Setup - mock S3UploadService
        S3UploadService mockS3UploadService = mock(S3UploadService.class);
        SqsBatchProcessor processor = new SqsBatchProcessor(
            null, "test-queue-url", DEFAULT_WINDOW_SECONDS, 
            mockS3UploadService, TEST_MALFORMED_S3_PATH, 0
        );

        // Setup - create batch with one valid and one malformed message
        Message validMessage = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, 
            System.currentTimeMillis() - 600_000); // 10 min ago
        Message malformedMessage = createMalformedMessage("malformed-1", "{ invalid json body }");

        List<Message> batch = Arrays.asList(validMessage, malformedMessage);

        // Act - process batch
        SqsBatchProcessor.BatchProcessingResult result = processor.processBatch(batch, 1);

        // Assert - S3 upload was called
        ArgumentCaptor<byte[]> dataCaptor = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Message>> messagesCaptor = ArgumentCaptor.forClass(List.class);

        verify(mockS3UploadService).uploadAndDeleteMessages(
            dataCaptor.capture(), 
            pathCaptor.capture(), 
            messagesCaptor.capture(),
            isNull()
        );

        // Assert - the S3 path format: {prefix}optout-malformed-{replicaId:03d}_{timestamp}_{randomHex:08x}.json
        String s3Path = pathCaptor.getValue();
        assertTrue(s3Path.startsWith(TEST_MALFORMED_S3_PATH + "optout-malformed-"), 
            "S3 path should start with prefix + optout-malformed-, got: " + s3Path);
        assertTrue(s3Path.matches(".*optout-malformed-\\d{3}_\\d{4}-\\d{2}-\\d{2}T\\d{2}\\.\\d{2}\\.\\d{2}Z_[a-f0-9]{8}\\.json"),
            "S3 path should match format with replica ID, ISO timestamp and 8-char hex, got: " + s3Path);

        // Assert - the uploaded data contains the malformed message
        String uploadedJson = new String(dataCaptor.getValue(), StandardCharsets.UTF_8);
        JsonArray uploadedArray = new JsonArray(uploadedJson);
        assertEquals(1, uploadedArray.size());
        
        JsonObject uploadedMessage = uploadedArray.getJsonObject(0);
        assertEquals("malformed-1", uploadedMessage.getString("messageId"));
        assertEquals("{ invalid json body }", uploadedMessage.getString("body"));

        // Assert - only the malformed message was passed for deletion
        List<Message> deletedMessages = messagesCaptor.getValue();
        assertEquals(1, deletedMessages.size());
        assertEquals("malformed-1", deletedMessages.get(0).messageId());

        // Assert - the valid message was still processed
        assertTrue(result.hasMessages());
    }

    @Test
    public void testProcessBatch_allMalformedMessages() throws IOException {
        // Setup - mock S3UploadService
        S3UploadService mockS3UploadService = mock(S3UploadService.class);
        SqsBatchProcessor processor = new SqsBatchProcessor(
            null, "test-queue-url", DEFAULT_WINDOW_SECONDS,
            mockS3UploadService, TEST_MALFORMED_S3_PATH, 0
        );

        // Setup - create batch with only malformed messages
        Message malformed1 = createMalformedMessage("malformed-1", "{}"); // Missing required fields
        Message malformed2 = createMalformedMessage("malformed-2", "not json at all");

        List<Message> batch = Arrays.asList(malformed1, malformed2);

        // Act - process batch
        SqsBatchProcessor.BatchProcessingResult result = processor.processBatch(batch, 5);

        // Assert - S3 upload was called with both malformed messages
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Message>> messagesCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockS3UploadService).uploadAndDeleteMessages(
            any(byte[].class),
            contains("optout-malformed-"),
            messagesCaptor.capture(),
            isNull()
        );

        List<Message> deletedMessages = messagesCaptor.getValue();
        assertEquals(2, deletedMessages.size());

        // Verify result indicates corrupt messages were handled
        assertFalse(result.hasMessages());
    }

    @Test
    public void testProcessBatch_s3UploadFailure_throwsException() throws IOException {
        // Setup - mock S3UploadService and SqsClient
        S3UploadService mockS3UploadService = mock(S3UploadService.class);
        SqsClient mockSqsClient = mock(SqsClient.class);
        
        // Setup - make S3 upload fail
        doThrow(new IOException("S3 connection failed"))
            .when(mockS3UploadService).uploadAndDeleteMessages(any(), any(), any(), any());

        SqsBatchProcessor processor = new SqsBatchProcessor(
            mockSqsClient, "test-queue-url", DEFAULT_WINDOW_SECONDS,
            mockS3UploadService, TEST_MALFORMED_S3_PATH, 0
        );

        // Setup - create batch with malformed message
        Message malformedMessage = createMalformedMessage("malformed-1", "bad data");
        List<Message> batch = Arrays.asList(malformedMessage);

        // Act - process batch - should throw IOException when S3 upload fails
        IOException thrown = assertThrows(IOException.class, () -> processor.processBatch(batch, 1));
        assertEquals("S3 connection failed", thrown.getMessage());

        // Assert - S3 upload was attempted
        verify(mockS3UploadService).uploadAndDeleteMessages(any(), any(), any(), any());
    }

    @Test
    public void testProcessBatch_noMalformedMessages_noS3Upload() throws IOException {
        // Setup - mock S3UploadService
        S3UploadService mockS3UploadService = mock(S3UploadService.class);
        SqsBatchProcessor processor = new SqsBatchProcessor(
            null, "test-queue-url", DEFAULT_WINDOW_SECONDS,
            mockS3UploadService, TEST_MALFORMED_S3_PATH, 0
        );

        // Setup - create batch with only valid messages (old enough to be eligible)
        Message valid1 = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, 
            System.currentTimeMillis() - 600_000);
        Message valid2 = createValidMessage(VALID_HASH_BASE64, VALID_ID_BASE64, 
            System.currentTimeMillis() - 700_000);

        List<Message> batch = Arrays.asList(valid1, valid2);

        // Act - process batch
        SqsBatchProcessor.BatchProcessingResult result = processor.processBatch(batch, 1);

        // Assert - S3 upload was NOT called (no malformed messages)
        verify(mockS3UploadService, never()).uploadAndDeleteMessages(any(), any(), any(), any());

        // Assert - valid messages were processed
        assertTrue(result.hasMessages());
        assertEquals(2, result.getMessages().size());
    }

    @Test
    public void testProcessBatch_malformedMessageJsonStructure() throws IOException {
        // Setup - mock S3UploadService
        S3UploadService mockS3UploadService = mock(S3UploadService.class);
        int testReplicaId = 42;
        SqsBatchProcessor processor = new SqsBatchProcessor(
            null, "test-queue-url", DEFAULT_WINDOW_SECONDS,
            mockS3UploadService, TEST_MALFORMED_S3_PATH, testReplicaId
        );

        // Setup - create a malformed message with specific content
        String malformedBody = "{\"some_field\": \"some_value\"}"; // Missing identity_hash and advertising_id
        Message malformedMessage = createMalformedMessage("test-msg-123", malformedBody);

        List<Message> batch = Arrays.asList(malformedMessage);
        processor.processBatch(batch, 1);

        // Assert - capture and verify the uploaded JSON structure
        ArgumentCaptor<byte[]> dataCaptor = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
        
        verify(mockS3UploadService).uploadAndDeleteMessages(
            dataCaptor.capture(),
            pathCaptor.capture(),
            any(),
            isNull()
        );

        // Assert - JSON structure
        String uploadedJson = new String(dataCaptor.getValue(), StandardCharsets.UTF_8);
        JsonArray array = new JsonArray(uploadedJson);
        assertEquals(1, array.size());
        
        JsonObject msgJson = array.getJsonObject(0);
        assertTrue(msgJson.containsKey("messageId"), "Should contain messageId");
        assertTrue(msgJson.containsKey("body"), "Should contain body");
        assertTrue(msgJson.containsKey("attributes"), "Should contain attributes");
        
        assertEquals("test-msg-123", msgJson.getString("messageId"));
        assertEquals(malformedBody, msgJson.getString("body"));

        // Assert - path format: {prefix}optout-malformed-{replicaId:03d}_{timestamp}_{randomHex:08x}.json
        String path = pathCaptor.getValue();
        assertTrue(path.matches(".*optout-malformed-042_.*"), "Path should contain replica ID 042, got: " + path);
        assertTrue(path.matches(".*optout-malformed-042_\\d{4}-\\d{2}-\\d{2}T\\d{2}\\.\\d{2}\\.\\d{2}Z_[a-f0-9]{8}\\.json"),
            "Path should match format with replica ID, ISO timestamp and 8-char hex, got: " + path);
    }
}

