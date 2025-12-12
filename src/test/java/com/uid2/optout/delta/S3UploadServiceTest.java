package com.uid2.optout.delta;

import com.uid2.shared.cloud.CloudStorageException;
import com.uid2.shared.cloud.ICloudStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class S3UploadServiceTest {

    private ICloudStorage mockCloudStorage;
    private SqsClient mockSqsClient;
    private S3UploadService s3UploadService;
    
    private static final String TEST_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789/test-queue";
    private static final String TEST_S3_PATH = "test-bucket/test-file.dat";

    @BeforeEach
    void setUp() {
        mockCloudStorage = mock(ICloudStorage.class);
        mockSqsClient = mock(SqsClient.class);
        s3UploadService = new S3UploadService(mockCloudStorage, mockSqsClient, TEST_QUEUE_URL);
    }

    // ==================== uploadAndDeleteMessages tests ====================

    @Test
    void testUploadAndDeleteMessages_success() throws Exception {
        // Setup - create test data and messages
        byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
        List<Message> messages = createMessages(3);
        
        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        // Act - upload and delete messages
        s3UploadService.uploadAndDeleteMessages(data, TEST_S3_PATH, messages, null);

        // Assert - S3 upload was called
        ArgumentCaptor<InputStream> inputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
        verify(mockCloudStorage).upload(inputStreamCaptor.capture(), eq(TEST_S3_PATH));
        
        // Assert - content was uploaded
        byte[] uploadedData = inputStreamCaptor.getValue().readAllBytes();
        assertArrayEquals(data, uploadedData);
        
        // Assert - SQS delete was called
        verify(mockSqsClient).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void testUploadAndDeleteMessages_callsSuccessCallback() throws Exception {
        // Setup - create test data and messages
        byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
        List<Message> messages = createMessages(5);
        AtomicInteger callbackCount = new AtomicInteger(0);
        
        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        // Act - upload and delete messages
        s3UploadService.uploadAndDeleteMessages(data, TEST_S3_PATH, messages, (count) -> {
            callbackCount.set(count);
        });

        // Assert - callback was called
        assertEquals(5, callbackCount.get());
    }

    @Test
    void testUploadAndDeleteMessages_s3UploadFails_throwsIOException() throws Exception {
        // Setup - create test data and messages
        byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
        List<Message> messages = createMessages(2);
        
        doThrow(new CloudStorageException("S3 error"))
            .when(mockCloudStorage).upload(any(InputStream.class), eq(TEST_S3_PATH));

        // Act & Assert - upload and delete messages throws IOException
        IOException exception = assertThrows(IOException.class, () -> {
            s3UploadService.uploadAndDeleteMessages(data, TEST_S3_PATH, messages, null);
        });

        // Assert - exception was thrown
        assertTrue(exception.getMessage().contains("s3 upload failed"));
        
        // Assert - SQS delete was NOT called (messages should remain in queue)
        verify(mockSqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void testUploadAndDeleteMessages_emptyMessageList_skipsSqsDelete() throws Exception {
        // Setup - create test data and messages
        byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
        List<Message> messages = new ArrayList<>();

        // Act - upload and delete messages
        s3UploadService.uploadAndDeleteMessages(data, TEST_S3_PATH, messages, null);

        // Assert - S3 upload was called
        verify(mockCloudStorage).upload(any(InputStream.class), eq(TEST_S3_PATH));
        
        // Assert - SQS delete was NOT called (no messages to delete)
        verify(mockSqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void testUploadAndDeleteMessages_callbackIsOptional() throws Exception {
        // Setup - create test data and messages
        byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
        List<Message> messages = createMessages(1);
        
        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        // Act & Assert - should not throw when callback is null
        assertDoesNotThrow(() -> {
            s3UploadService.uploadAndDeleteMessages(data, TEST_S3_PATH, messages, null);
        });
    }

    @Test
    void testUploadAndDeleteMessages_deletesCorrectMessages() throws Exception {
        // Setup - create test data and messages
        byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
        List<Message> messages = List.of(
            Message.builder().messageId("msg-1").receiptHandle("receipt-1").build(),
            Message.builder().messageId("msg-2").receiptHandle("receipt-2").build()
        );
        
        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        // Act - upload and delete messages
        s3UploadService.uploadAndDeleteMessages(data, TEST_S3_PATH, messages, null);

        // Assert - SQS delete was called
        ArgumentCaptor<DeleteMessageBatchRequest> deleteCaptor = 
            ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(mockSqsClient).deleteMessageBatch(deleteCaptor.capture());
        
        // Assert - correct messages were deleted
        DeleteMessageBatchRequest deleteRequest = deleteCaptor.getValue();
        assertEquals(TEST_QUEUE_URL, deleteRequest.queueUrl());
        assertEquals(2, deleteRequest.entries().size());
    }

    @Test
    void testUploadAndDeleteMessages_largeData() throws Exception {
        // Setup - create 1MB of data and messages
        byte[] data = new byte[1024 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        List<Message> messages = createMessages(10);
        
        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        // Act - upload and delete messages
        s3UploadService.uploadAndDeleteMessages(data, TEST_S3_PATH, messages, null);

        // Assert - S3 upload was called
        ArgumentCaptor<InputStream> inputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
        verify(mockCloudStorage).upload(inputStreamCaptor.capture(), eq(TEST_S3_PATH));
        
        // Assert - file content is correct
        byte[] uploadedData = inputStreamCaptor.getValue().readAllBytes();
        assertArrayEquals(data, uploadedData);
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

