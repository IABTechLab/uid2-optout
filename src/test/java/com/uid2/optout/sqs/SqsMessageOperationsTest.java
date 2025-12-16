package com.uid2.optout.sqs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SqsMessageOperationsTest {

    private SqsClient mockSqsClient;
    private static final String TEST_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789/test-queue";

    @BeforeEach
    void setUp() {
        mockSqsClient = mock(SqsClient.class);
    }

    // ==================== getQueueAttributes tests ====================

    @Test
    void testGetQueueAttributes_success() {
        GetQueueAttributesResponse response = GetQueueAttributesResponse.builder()
            .attributes(Map.of(
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "100",
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "50",
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED, "25"
            ))
            .build();
        when(mockSqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(response);

        SqsMessageOperations.QueueAttributes attrs = SqsMessageOperations.getQueueAttributes(mockSqsClient, TEST_QUEUE_URL);

        assertNotNull(attrs);
        assertEquals(100, attrs.getApproximateNumberOfMessages());
        assertEquals(50, attrs.getApproximateNumberOfMessagesNotVisible());
        assertEquals(25, attrs.getApproximateNumberOfMessagesDelayed());
        assertEquals(175, attrs.getTotalMessages());
    }

    @Test
    void testGetQueueAttributes_exception() {
        when(mockSqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
            .thenThrow(new RuntimeException("SQS error"));

        SqsMessageOperations.QueueAttributes attrs = SqsMessageOperations.getQueueAttributes(mockSqsClient, TEST_QUEUE_URL);

        assertNull(attrs);
    }

    @Test
    void testGetQueueAttributes_missingAttributes() {
        GetQueueAttributesResponse response = GetQueueAttributesResponse.builder()
            .attributes(Map.of()) // empty
            .build();
        when(mockSqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(response);

        SqsMessageOperations.QueueAttributes attrs = SqsMessageOperations.getQueueAttributes(mockSqsClient, TEST_QUEUE_URL);

        assertNotNull(attrs);
        assertEquals(0, attrs.getApproximateNumberOfMessages());
        assertEquals(0, attrs.getTotalMessages());
    }

    // ==================== receiveMessagesFromSqs tests ====================

    @Test
    void testReceiveMessages_success() {
        List<Message> messages = List.of(
            Message.builder().messageId("1").receiptHandle("r1").build(),
            Message.builder().messageId("2").receiptHandle("r2").build()
        );
        ReceiveMessageResponse response = ReceiveMessageResponse.builder().messages(messages).build();
        when(mockSqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);

        List<Message> result = SqsMessageOperations.receiveMessagesFromSqs(mockSqsClient, TEST_QUEUE_URL, 10, 30);

        assertEquals(2, result.size());
    }

    @Test
    void testReceiveMessages_exception() {
        when(mockSqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenThrow(new RuntimeException("SQS error"));

        List<Message> result = SqsMessageOperations.receiveMessagesFromSqs(mockSqsClient, TEST_QUEUE_URL, 10, 30);

        assertTrue(result.isEmpty());
    }

    // ==================== deleteMessagesFromSqs tests ====================

    @Test
    void testDeleteMessages_emptyList() {
        SqsMessageOperations.deleteMessagesFromSqs(mockSqsClient, TEST_QUEUE_URL, List.of());

        verify(mockSqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void testDeleteMessages_allSucceed() {
        List<Message> messages = createMessages(3);
        DeleteMessageBatchResponse response = DeleteMessageBatchResponse.builder()
            .successful(
                DeleteMessageBatchResultEntry.builder().id("0").build(),
                DeleteMessageBatchResultEntry.builder().id("1").build(),
                DeleteMessageBatchResultEntry.builder().id("2").build()
            )
            .failed(List.of())
            .build();
        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenReturn(response);

        SqsMessageOperations.deleteMessagesFromSqs(mockSqsClient, TEST_QUEUE_URL, messages);

        ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(mockSqsClient, times(1)).deleteMessageBatch(captor.capture());
        assertEquals(3, captor.getValue().entries().size());
    }

    @Test
    void testDeleteMessages_someFailThenSucceedOnRetry() {
        List<Message> messages = createMessages(3);

        // First call: 2 succeed, 1 fails
        DeleteMessageBatchResponse firstResponse = DeleteMessageBatchResponse.builder()
            .successful(
                DeleteMessageBatchResultEntry.builder().id("0").build(),
                DeleteMessageBatchResultEntry.builder().id("1").build()
            )
            .failed(BatchResultErrorEntry.builder().id("2").code("Error").build())
            .build();

        // Second call (retry): the failed one succeeds
        DeleteMessageBatchResponse secondResponse = DeleteMessageBatchResponse.builder()
            .successful(DeleteMessageBatchResultEntry.builder().id("2").build())
            .failed(List.of())
            .build();

        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(firstResponse)
            .thenReturn(secondResponse);

        SqsMessageOperations.deleteMessagesFromSqs(mockSqsClient, TEST_QUEUE_URL, messages);

        ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(mockSqsClient, times(2)).deleteMessageBatch(captor.capture());
        List<DeleteMessageBatchRequest> requests = captor.getAllValues();
        assertEquals(3, requests.get(0).entries().size()); // first attempt: all 3
        assertEquals(1, requests.get(1).entries().size()); // retry: only the failed one
    }

    @Test
    void testDeleteMessages_allFailRetryOnceUnconditionally() {
        List<Message> messages = createMessages(2);

        // All fail on first and second attempt
        DeleteMessageBatchResponse failResponse = DeleteMessageBatchResponse.builder()
            .successful(List.of())
            .failed(
                BatchResultErrorEntry.builder().id("0").code("Error").build(),
                BatchResultErrorEntry.builder().id("1").code("Error").build()
            )
            .build();

        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenReturn(failResponse);

        SqsMessageOperations.deleteMessagesFromSqs(mockSqsClient, TEST_QUEUE_URL, messages);

        // Should retry once even with no progress, then stop
        ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(mockSqsClient, times(2)).deleteMessageBatch(captor.capture());
        List<DeleteMessageBatchRequest> requests = captor.getAllValues();
        assertEquals(2, requests.get(0).entries().size()); // first attempt: all 2
        assertEquals(2, requests.get(1).entries().size()); // retry: still all 2 (none succeeded)
    }

    @Test
    void testDeleteMessages_retryWhileMakingProgress() {
        List<Message> messages = createMessages(3);

        // First call: 1 succeeds, 2 fail
        DeleteMessageBatchResponse first = DeleteMessageBatchResponse.builder()
            .successful(DeleteMessageBatchResultEntry.builder().id("0").build())
            .failed(
                BatchResultErrorEntry.builder().id("1").code("Error").build(),
                BatchResultErrorEntry.builder().id("2").code("Error").build()
            )
            .build();

        // Second call: 1 succeeds, 1 fails (still making progress)
        DeleteMessageBatchResponse second = DeleteMessageBatchResponse.builder()
            .successful(DeleteMessageBatchResultEntry.builder().id("1").build())
            .failed(BatchResultErrorEntry.builder().id("2").code("Error").build())
            .build();

        // Third call: last one succeeds
        DeleteMessageBatchResponse third = DeleteMessageBatchResponse.builder()
            .successful(DeleteMessageBatchResultEntry.builder().id("2").build())
            .failed(List.of())
            .build();

        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(first)
            .thenReturn(second)
            .thenReturn(third);

        SqsMessageOperations.deleteMessagesFromSqs(mockSqsClient, TEST_QUEUE_URL, messages);

        ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(mockSqsClient, times(3)).deleteMessageBatch(captor.capture());
        List<DeleteMessageBatchRequest> requests = captor.getAllValues();
        assertEquals(3, requests.get(0).entries().size()); // first: all 3
        assertEquals(2, requests.get(1).entries().size()); // second: 2 failed
        assertEquals(1, requests.get(2).entries().size()); // third: 1 failed
    }

    @Test
    void testDeleteMessages_stopWhenNoProgressAfterRetry() {
        List<Message> messages = createMessages(2);

        // First call: 1 succeeds, 1 fails
        DeleteMessageBatchResponse first = DeleteMessageBatchResponse.builder()
            .successful(DeleteMessageBatchResultEntry.builder().id("0").build())
            .failed(BatchResultErrorEntry.builder().id("1").code("Error").build())
            .build();

        // Second call (retry): still fails, no progress
        DeleteMessageBatchResponse second = DeleteMessageBatchResponse.builder()
            .successful(List.of())
            .failed(BatchResultErrorEntry.builder().id("1").code("Error").build())
            .build();

        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(first)
            .thenReturn(second);

        SqsMessageOperations.deleteMessagesFromSqs(mockSqsClient, TEST_QUEUE_URL, messages);

        // Should stop after second call since no progress was made
        ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(mockSqsClient, times(2)).deleteMessageBatch(captor.capture());
        List<DeleteMessageBatchRequest> requests = captor.getAllValues();
        assertEquals(2, requests.get(0).entries().size()); // first: all 2
        assertEquals(1, requests.get(1).entries().size()); // retry: only the failed one
    }

    @Test
    void testDeleteMessages_batchesMoreThan10Messages() {
        List<Message> messages = createMessages(15);

        DeleteMessageBatchResponse successResponse = DeleteMessageBatchResponse.builder()
            .successful(List.of(
                DeleteMessageBatchResultEntry.builder().id("0").build()
            ))
            .failed(List.of())
            .build();

        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenReturn(successResponse);

        SqsMessageOperations.deleteMessagesFromSqs(mockSqsClient, TEST_QUEUE_URL, messages);

        // Should be called twice: once for 10 messages, once for 5
        ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(mockSqsClient, times(2)).deleteMessageBatch(captor.capture());

        List<DeleteMessageBatchRequest> requests = captor.getAllValues();
        assertEquals(10, requests.get(0).entries().size());
        assertEquals(5, requests.get(1).entries().size());
    }

    @Test
    void testDeleteMessages_exception() {
        List<Message> messages = createMessages(3);
        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenThrow(new RuntimeException("SQS error"));

        // Should not throw, just log error
        assertDoesNotThrow(() -> 
            SqsMessageOperations.deleteMessagesFromSqs(mockSqsClient, TEST_QUEUE_URL, messages));
    }

    // ==================== QueueAttributes tests ====================

    @Test
    void testQueueAttributes_toString() {
        SqsMessageOperations.QueueAttributes attrs = new SqsMessageOperations.QueueAttributes(100, 50, 25);

        String str = attrs.toString();

        assertTrue(str.contains("visible=100"));
        assertTrue(str.contains("invisible=50"));
        assertTrue(str.contains("delayed=25"));
        assertTrue(str.contains("total=175"));
    }

    // ==================== Helper methods ====================

    private List<Message> createMessages(int count) {
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            messages.add(Message.builder()
                .messageId("msg-" + i)
                .receiptHandle("receipt-" + i)
                .build());
        }
        return messages;
    }
}
