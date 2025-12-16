package com.uid2.optout.sqs;

import com.uid2.optout.delta.StopReason;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class SqsWindowReaderTest {

    private SqsClient mockSqsClient;
    private SqsWindowReader windowReader;

    private static final String TEST_QUEUE_URL = "https://sqs.test.amazonaws.com/123456789/test";
    private static final String VALID_HASH_BASE64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="; // 32 bytes
    private static final String VALID_ID_BASE64 = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="; // 32 bytes
    private static final int MAX_MESSAGES_PER_POLL = 10;
    private static final int VISIBILITY_TIMEOUT = 240;
    private static final int DELTA_WINDOW_SECONDS = 300; // 5 minutes
    private static final int MAX_MESSAGES_PER_WINDOW = 100;

    @BeforeEach
    void setUp() {
        mockSqsClient = mock(SqsClient.class);
        windowReader = new SqsWindowReader(
            mockSqsClient, TEST_QUEUE_URL, MAX_MESSAGES_PER_POLL,
            VISIBILITY_TIMEOUT, DELTA_WINDOW_SECONDS, MAX_MESSAGES_PER_WINDOW,
            null, "", 0
        );
    }

    @Test
    void testReadWindow_emptyQueue() throws IOException {
        when(mockSqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());

        SqsWindowReader.WindowReadResult result = windowReader.readWindow();

        assertTrue(result.isEmpty());
        assertEquals(StopReason.QUEUE_EMPTY, result.getStopReason());
        assertEquals(0, result.getRawMessagesRead());
    }

    @Test
    void testReadWindow_singleBatchSingleWindow() throws IOException {
        long windowStartSeconds = System.currentTimeMillis() / 1000 - 600; // 10 minutes ago
        List<Message> messages = Arrays.asList(
            createMessage(windowStartSeconds + 10),
            createMessage(windowStartSeconds + 50),
            createMessage(windowStartSeconds + 100)
        );

        when(mockSqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());

        SqsWindowReader.WindowReadResult result = windowReader.readWindow();

        assertEquals(3, result.getMessages().size());
        assertEquals(StopReason.QUEUE_EMPTY, result.getStopReason());
        assertEquals(3, result.getRawMessagesRead());
    }

    @Test
    void testReadWindow_multipleBatchesSameWindow() throws IOException {
        long windowStartSeconds = System.currentTimeMillis() / 1000 - 600; // 10 minutes ago
        
        List<Message> batch1 = Arrays.asList(
            createMessage(windowStartSeconds + 10),
            createMessage(windowStartSeconds + 20)
        );
        List<Message> batch2 = Arrays.asList(
            createMessage(windowStartSeconds + 100),
            createMessage(windowStartSeconds + 150)
        );

        when(mockSqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(batch1).build())
            .thenReturn(ReceiveMessageResponse.builder().messages(batch2).build())
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());

        SqsWindowReader.WindowReadResult result = windowReader.readWindow();

        assertEquals(4, result.getMessages().size());
        assertEquals(StopReason.QUEUE_EMPTY, result.getStopReason());
        assertEquals(4, result.getRawMessagesRead());
    }

    @Test
    void testReadWindow_messagesTooRecent() throws IOException {
        long currentTimeMs = System.currentTimeMillis();
        List<Message> messages = Arrays.asList(
            createMessageWithTimestampMs(currentTimeMs - 1000), // 1 second ago
            createMessageWithTimestampMs(currentTimeMs - 2000)  // 2 seconds ago
        );

        when(mockSqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(messages).build());

        SqsWindowReader.WindowReadResult result = windowReader.readWindow();

        assertTrue(result.isEmpty());
        assertEquals(StopReason.MESSAGES_TOO_RECENT, result.getStopReason());
        assertEquals(2, result.getRawMessagesRead());
    }

    @Test
    void testReadWindow_messageLimitExceeded() throws IOException {
        SqsWindowReader smallLimitReader = new SqsWindowReader(
            mockSqsClient, TEST_QUEUE_URL, MAX_MESSAGES_PER_POLL,
            VISIBILITY_TIMEOUT, DELTA_WINDOW_SECONDS, 5, // Only 5 messages max
            null, "", 0
        );

        long windowStartSeconds = System.currentTimeMillis() / 1000 - 600;
        List<Message> batch = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            batch.add(createMessage(windowStartSeconds + i * 10));
        }
        List<Message> batch2 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch2.add(createMessage(windowStartSeconds + i * 10));
        }

        when(mockSqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(batch).build())
            .thenReturn(ReceiveMessageResponse.builder().messages(batch2).build());

        SqsWindowReader.WindowReadResult result = smallLimitReader.readWindow();

        assertEquals(StopReason.MESSAGE_LIMIT_EXCEEDED, result.getStopReason());
        assertEquals(10, result.getMessages().size());
    }

    @Test
    void testReadWindow_discoversNewWindow() throws IOException {
        long window1StartSeconds = System.currentTimeMillis() / 1000 - 900; // 15 minutes ago
        long window2StartSeconds = window1StartSeconds + DELTA_WINDOW_SECONDS + 100; // Next window
        
        List<Message> messages = Arrays.asList(
            createMessage(window1StartSeconds + 10),
            createMessage(window1StartSeconds + 50),
            createMessage(window2StartSeconds + 10) // Next window
        );

        when(mockSqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(messages).build());

        SqsWindowReader.WindowReadResult result = windowReader.readWindow();

        assertEquals(3, result.getMessages().size());
        assertEquals(StopReason.NONE, result.getStopReason());
        assertEquals(window1StartSeconds + 10, result.getWindowStart());
    }

    @Test
    void testReadWindow_multipleWindowsMultipleBatchesPerWindow() throws IOException {
        // Window 1: 2 batches, then discovers window 2
        // Window 2: 2 batches (must be > 5 min old for eligibility)
        long window1StartSeconds = System.currentTimeMillis() / 1000 - 1200; // 20 minutes ago
        long window2StartSeconds = window1StartSeconds + DELTA_WINDOW_SECONDS + 100; // ~12 minutes ago
        
        List<Message> window1Batch1 = Arrays.asList(
            createMessage(window1StartSeconds + 10),
            createMessage(window1StartSeconds + 20),
            createMessage(window1StartSeconds + 30)
        );
        
        List<Message> window1Batch2 = Arrays.asList(
            createMessage(window1StartSeconds + 100),
            createMessage(window1StartSeconds + 150),
            createMessage(window1StartSeconds + 200)
        );
        
        // Mixed batch triggers new window detection
        List<Message> mixedBatch = Arrays.asList(
            createMessage(window1StartSeconds + 250),
            createMessage(window2StartSeconds + 10),
            createMessage(window2StartSeconds + 20)
        );
        
        List<Message> window2Batch1 = Arrays.asList(
            createMessage(window2StartSeconds + 50),
            createMessage(window2StartSeconds + 80)
        );
        
        List<Message> window2Batch2 = Arrays.asList(
            createMessage(window2StartSeconds + 120),
            createMessage(window2StartSeconds + 150)
        );
        
        when(mockSqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(window1Batch1).build())
            .thenReturn(ReceiveMessageResponse.builder().messages(window1Batch2).build())
            .thenReturn(ReceiveMessageResponse.builder().messages(mixedBatch).build())
            .thenReturn(ReceiveMessageResponse.builder().messages(window2Batch1).build())
            .thenReturn(ReceiveMessageResponse.builder().messages(window2Batch2).build())
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());

        // First readWindow() returns window 1 + mixed batch (new window detected)
        SqsWindowReader.WindowReadResult result1 = windowReader.readWindow();

        assertEquals(9, result1.getMessages().size());
        assertEquals(StopReason.NONE, result1.getStopReason());
        assertEquals(window1StartSeconds + 10, result1.getWindowStart());
        assertEquals(9, result1.getRawMessagesRead());
        
        // Second readWindow() processes window 2
        SqsWindowReader.WindowReadResult result2 = windowReader.readWindow();
        
        assertEquals(4, result2.getMessages().size());
        assertEquals(StopReason.QUEUE_EMPTY, result2.getStopReason());
        assertEquals(window2StartSeconds + 50, result2.getWindowStart());
        assertEquals(4, result2.getRawMessagesRead());
    }

    @Test
    void testReadWindow_corruptMessagesSkipped() throws IOException {
        long windowStartSeconds = System.currentTimeMillis() / 1000 - 600;
        
        // Corrupt message (missing required fields)
        Message corruptMessage = Message.builder()
            .body("{}")
            .attributes(Map.of(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf((windowStartSeconds + 10) * 1000)))
            .messageId("corrupt-1")
            .receiptHandle("receipt-1")
            .build();
        
        List<Message> validBatch = Arrays.asList(createMessage(windowStartSeconds + 100));

        when(mockSqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(corruptMessage)).build())
            .thenReturn(ReceiveMessageResponse.builder().messages(validBatch).build())
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());

        SqsWindowReader.WindowReadResult result = windowReader.readWindow();

        assertEquals(1, result.getMessages().size());
        assertEquals(StopReason.QUEUE_EMPTY, result.getStopReason());
    }

    @Test
    void testWindowReadResult_factoryMethods() {
        List<SqsParsedMessage> messages = List.of();
        long windowStart = 1700000000L;

        SqsWindowReader.WindowReadResult empty = SqsWindowReader.WindowReadResult.queueEmpty(messages, windowStart, 5);
        assertEquals(StopReason.QUEUE_EMPTY, empty.getStopReason());
        assertEquals(5, empty.getRawMessagesRead());

        SqsWindowReader.WindowReadResult tooRecent = SqsWindowReader.WindowReadResult.messagesTooRecent(messages, windowStart, 5);
        assertEquals(StopReason.MESSAGES_TOO_RECENT, tooRecent.getStopReason());
        assertEquals(5, tooRecent.getRawMessagesRead());

        SqsWindowReader.WindowReadResult limitExceeded = SqsWindowReader.WindowReadResult.messageLimitExceeded(messages, windowStart, 100);
        assertEquals(StopReason.MESSAGE_LIMIT_EXCEEDED, limitExceeded.getStopReason());
        assertEquals(100, limitExceeded.getRawMessagesRead());

        SqsWindowReader.WindowReadResult withMessages = SqsWindowReader.WindowReadResult.withMessages(messages, windowStart, 10);
        assertEquals(StopReason.NONE, withMessages.getStopReason());
        assertEquals(10, withMessages.getRawMessagesRead());
    }

    // ==================== Helper methods ====================

    private Message createMessage(long timestampSeconds) {
        return createMessageWithTimestampMs(timestampSeconds * 1000);
    }

    private Message createMessageWithTimestampMs(long timestampMs) {
        JsonObject body = new JsonObject()
            .put("identity_hash", VALID_HASH_BASE64)
            .put("advertising_id", VALID_ID_BASE64);

        Map<MessageSystemAttributeName, String> attributes = new HashMap<>();
        attributes.put(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(timestampMs));

        return Message.builder()
            .body(body.encode())
            .attributes(attributes)
            .messageId("msg-" + UUID.randomUUID())
            .receiptHandle("receipt-" + UUID.randomUUID())
            .build();
    }
}
