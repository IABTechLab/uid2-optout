package com.uid2.optout.delta;

import com.uid2.optout.sqs.SqsParsedMessage;
import com.uid2.optout.sqs.SqsWindowReader;
import com.uid2.optout.traffic.TrafficCalculator;
import com.uid2.optout.traffic.TrafficFilter;
import com.uid2.shared.optout.OptOutCloudSync;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DeltaProductionOrchestratorTest {

    // test constants
    private static final String TEST_QUEUE_URL = "https://sqs.test.amazonaws.com/123456789/test-queue";
    private static final int REPLICA_ID = 1;
    private static final int DELTA_WINDOW_SECONDS = 300; // 5 minutes
    private static final int JOB_TIMEOUT_SECONDS = 3600; // 1 hour

    // mocks
    private SqsClient mockSqsClient;
    private SqsWindowReader mockWindowReader;
    private DeltaFileWriter mockDeltaFileWriter;
    private S3UploadService mockDeltaUploadService;
    private S3UploadService mockDroppedRequestUploadService;
    private ManualOverrideService mockManualOverrideService;
    private TrafficFilter mockTrafficFilter;
    private TrafficCalculator mockTrafficCalculator;
    private OptOutCloudSync mockCloudSync;
    private DeltaProductionMetrics mockMetrics;

    private DeltaProductionOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        mockSqsClient = mock(SqsClient.class);
        mockWindowReader = mock(SqsWindowReader.class);
        mockDeltaFileWriter = mock(DeltaFileWriter.class);
        mockDeltaUploadService = mock(S3UploadService.class);
        mockDroppedRequestUploadService = mock(S3UploadService.class);
        mockManualOverrideService = mock(ManualOverrideService.class);
        mockTrafficFilter = mock(TrafficFilter.class);
        mockTrafficCalculator = mock(TrafficCalculator.class);
        mockCloudSync = mock(OptOutCloudSync.class);
        mockMetrics = mock(DeltaProductionMetrics.class);

        // default behavior
        when(mockManualOverrideService.isDelayedProcessing()).thenReturn(false);
        when(mockTrafficCalculator.calculateStatus(anyList(), any(), anyInt(), anyInt()))
                .thenReturn(TrafficCalculator.TrafficStatus.DEFAULT);
        when(mockCloudSync.toCloudPath(anyString())).thenAnswer(inv -> "delta/" + inv.getArgument(0));
        
        // mock sqs queue attributes
        GetQueueAttributesResponse queueAttributesResponse = GetQueueAttributesResponse.builder()
                .attributes(Map.of(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0",
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0",
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED, "0"
                ))
                .build();
        when(mockSqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
                .thenReturn(queueAttributesResponse);
        
        // mock delta upload service to invoke callback
        try {
            doAnswer(invocation -> {
                S3UploadService.UploadSuccessCallback callback = invocation.getArgument(3);
                if (callback != null) {
                    List<Message> messages = invocation.getArgument(2);
                    callback.onSuccess(messages.size());
                }
                return null;
            }).when(mockDeltaUploadService).uploadAndDeleteMessages(any(), anyString(), anyList(), any());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        orchestrator = new DeltaProductionOrchestrator(
                mockSqsClient,
                TEST_QUEUE_URL,
                REPLICA_ID,
                DELTA_WINDOW_SECONDS,
                JOB_TIMEOUT_SECONDS,
                mockWindowReader,
                mockDeltaFileWriter,
                mockDeltaUploadService,
                mockDroppedRequestUploadService,
                mockManualOverrideService,
                mockTrafficFilter,
                mockTrafficCalculator,
                mockCloudSync,
                mockMetrics
        );
    }

    // ==================== Manual Override Tests ====================

    @Test
    void testProduceBatchedDeltas_manualOverrideActive_returnsEarly() throws IOException {
        // Setup - manual override is active
        when(mockManualOverrideService.isDelayedProcessing()).thenReturn(true);

        // Act
        DeltaProductionResult result = orchestrator.produceBatchedDeltas(name -> {});

        // Assert - stop reason, totals
        assertEquals(StopReason.MANUAL_OVERRIDE_ACTIVE, result.getStopReason());
        assertEquals(0, result.getDeltasProduced());
        assertEquals(0, result.getEntriesProcessed());
        
        // Assert - nothing read
        verify(mockWindowReader, never()).readWindow();
        // Assert - nothing uploaded
        verify(mockDeltaUploadService, never()).uploadAndDeleteMessages(any(), any(), any(), any());
        verify(mockDroppedRequestUploadService, never()).uploadAndDeleteMessages(any(), any(), any(), any());
    }

    @Test
    void testProduceBatchedDeltas_manualOverrideActiveDuringProcessing_stops() throws IOException {
        // Setup - first call returns false, second call returns true
        when(mockManualOverrideService.isDelayedProcessing())
                .thenReturn(false)
                .thenReturn(true);

        // Setup - window with 5 messages
        List<SqsParsedMessage> messages = createParsedMessages(5);
        SqsWindowReader.WindowReadResult windowResult = SqsWindowReader.WindowReadResult.withMessages(
                messages, System.currentTimeMillis() / 1000 - 600, 5);
        when(mockWindowReader.readWindow()).thenReturn(windowResult);

        // Act
        DeltaProductionResult result = orchestrator.produceBatchedDeltas(name -> {});

        // Assert - stop reason
        assertEquals(StopReason.CIRCUIT_BREAKER_TRIGGERED, result.getStopReason());
        
        // Assert - nothing uploaded
        verify(mockDeltaUploadService, never()).uploadAndDeleteMessages(any(), any(), any(), any());
    }

    // ==================== Empty Queue Tests ====================

    @Test
    void testProduceBatchedDeltas_emptyQueue_returnsQueueEmpty() throws IOException {
        // Setup - empty window
        SqsWindowReader.WindowReadResult emptyResult = SqsWindowReader.WindowReadResult.queueEmpty(
                Collections.emptyList(), 0, 0);
        when(mockWindowReader.readWindow()).thenReturn(emptyResult);

        // Act
        DeltaProductionResult result = orchestrator.produceBatchedDeltas(name -> {});

        // Assert - stop reason, totals
        assertEquals(StopReason.QUEUE_EMPTY, result.getStopReason());
        assertEquals(0, result.getDeltasProduced());
        assertEquals(0, result.getEntriesProcessed());
    }

    @Test
    void testProduceBatchedDeltas_messagesTooRecent_returnsMessagesTooRecent() throws IOException {
        // Setup - window with messages too recent
        SqsWindowReader.WindowReadResult tooRecentResult = SqsWindowReader.WindowReadResult.messagesTooRecent(
                Collections.emptyList(), 0, 5);
        when(mockWindowReader.readWindow()).thenReturn(tooRecentResult);

        // Act
        DeltaProductionResult result = orchestrator.produceBatchedDeltas(name -> {});

        // Assert - stop reason, totals
        assertEquals(StopReason.MESSAGES_TOO_RECENT, result.getStopReason());
        assertEquals(0, result.getDeltasProduced());
    }

    // ==================== Normal Processing Tests ====================

    @Test
    void testProduceBatchedDeltas_singleWindow_producesOneDelta() throws IOException {
        // Setup - window with 10 messages, then empty
        List<SqsParsedMessage> messages = createParsedMessages(10);
        long windowStart = System.currentTimeMillis() / 1000 - 600;
        
        SqsWindowReader.WindowReadResult windowResult = SqsWindowReader.WindowReadResult.withMessages(
                messages, windowStart, 10);
        SqsWindowReader.WindowReadResult emptyResult = SqsWindowReader.WindowReadResult.queueEmpty(
                Collections.emptyList(), 0, 0);
        
        when(mockWindowReader.readWindow())
                .thenReturn(windowResult)
                .thenReturn(emptyResult);

        // Setup - no denylisted messages
        when(mockTrafficFilter.isDenylisted(any())).thenReturn(false);

        // Act
        AtomicReference<String> producedDelta = new AtomicReference<>();
        DeltaProductionResult result = orchestrator.produceBatchedDeltas(producedDelta::set);

        // Assert - stop reason, totals
        assertEquals(StopReason.QUEUE_EMPTY, result.getStopReason());
        assertEquals(1, result.getDeltasProduced());
        assertEquals(10, result.getEntriesProcessed());
        assertEquals(0, result.getDroppedRequestFilesProduced());
        assertNotNull(producedDelta.get());
        
        // Assert - delta file constructed
        verify(mockDeltaFileWriter).writeStartOfDelta(any(), eq(windowStart));
        verify(mockDeltaFileWriter, times(10)).writeOptOutEntry(any(), any(), any(), anyLong());
        verify(mockDeltaFileWriter).writeEndOfDelta(any(), eq(windowStart + DELTA_WINDOW_SECONDS));
        
        // Assert - delta uploaded
        verify(mockDeltaUploadService).uploadAndDeleteMessages(any(), anyString(), anyList(), any());
    }

    @Test
    void testProduceBatchedDeltas_multipleWindows_producesMultipleDeltas() throws IOException {
        // Setup - window with 5 messages, then window with 3 messages, then empty
        List<SqsParsedMessage> messages1 = createParsedMessages(5);
        List<SqsParsedMessage> messages2 = createParsedMessages(3);
        long windowStart1 = System.currentTimeMillis() / 1000 - 900;
        long windowStart2 = System.currentTimeMillis() / 1000 - 600;
        
        SqsWindowReader.WindowReadResult windowResult1 = SqsWindowReader.WindowReadResult.withMessages(
                messages1, windowStart1, 5);
        SqsWindowReader.WindowReadResult windowResult2 = SqsWindowReader.WindowReadResult.withMessages(
                messages2, windowStart2, 3);
        SqsWindowReader.WindowReadResult emptyResult = SqsWindowReader.WindowReadResult.queueEmpty(
                Collections.emptyList(), 0, 0);
        
        when(mockWindowReader.readWindow())
                .thenReturn(windowResult1)
                .thenReturn(windowResult2)
                .thenReturn(emptyResult);
        
        // Setup - no denylisted messages
        when(mockTrafficFilter.isDenylisted(any())).thenReturn(false);

        // Act
        AtomicInteger deltaCount = new AtomicInteger(0);
        DeltaProductionResult result = orchestrator.produceBatchedDeltas(name -> deltaCount.incrementAndGet());

        // Assert - stop reason, totals
        assertEquals(StopReason.QUEUE_EMPTY, result.getStopReason());
        assertEquals(2, result.getDeltasProduced());
        assertEquals(8, result.getEntriesProcessed());
        assertEquals(2, deltaCount.get());

        // Assert - delta file constructed
        verify(mockDeltaFileWriter, times(2)).writeStartOfDelta(any(), anyLong());
        verify(mockDeltaFileWriter, times(2)).writeEndOfDelta(any(), anyLong());
        verify(mockDeltaFileWriter, times(8)).writeOptOutEntry(any(), any(), any(), anyLong());

        // Assert - delta uploaded
        verify(mockDeltaUploadService, times(2)).uploadAndDeleteMessages(any(), anyString(), anyList(), any());
    }

    // ==================== Traffic Filter (Denylist) Tests ====================

    @Test
    void testProduceBatchedDeltas_withDenylistedMessages_uploadsDroppedRequests() throws IOException {
        // Setup - window with 10 messages, then empty
        List<SqsParsedMessage> messages = createParsedMessages(10);
        long windowStart = System.currentTimeMillis() / 1000 - 600;
        
        SqsWindowReader.WindowReadResult windowResult = SqsWindowReader.WindowReadResult.withMessages(
                messages, windowStart, 10);
        SqsWindowReader.WindowReadResult emptyResult = SqsWindowReader.WindowReadResult.queueEmpty(
                Collections.emptyList(), 0, 0);

        when(mockWindowReader.readWindow())
                .thenReturn(windowResult)
                .thenReturn(emptyResult);
        
        // Setup - first 3 messages are denylisted
        when(mockTrafficFilter.isDenylisted(any()))
                .thenReturn(true, true, true, false, false, false, false, false, false, false);

        // Act
        DeltaProductionResult result = orchestrator.produceBatchedDeltas(name -> {});

        // Assert - stop reason, totals
        assertEquals(StopReason.QUEUE_EMPTY, result.getStopReason());
        assertEquals(1, result.getDeltasProduced());
        assertEquals(7, result.getEntriesProcessed());
        assertEquals(1, result.getDroppedRequestFilesProduced());
        assertEquals(3, result.getDroppedRequestsProcessed());
        
        // Assert - delta file constructed
        verify(mockDeltaFileWriter, times(1)).writeStartOfDelta(any(), anyLong());
        verify(mockDeltaFileWriter, times(1)).writeEndOfDelta(any(), anyLong());
        verify(mockDeltaFileWriter, times(7)).writeOptOutEntry(any(), any(), any(), anyLong());

        // Assert - delta & dropped requests uploaded
        verify(mockDeltaUploadService).uploadAndDeleteMessages(any(), anyString(), anyList(), any());
        verify(mockDroppedRequestUploadService).uploadAndDeleteMessages(any(), anyString(), anyList(), any());
    }

    @Test
    void testProduceBatchedDeltas_allMessagesDenylisted_noDeltaUpload() throws IOException {
        // Setup - window with 5 messages
        List<SqsParsedMessage> messages = createParsedMessages(5);
        long windowStart = System.currentTimeMillis() / 1000 - 600;
        
        SqsWindowReader.WindowReadResult windowResult = SqsWindowReader.WindowReadResult.withMessages(
                messages, windowStart, 5);
        SqsWindowReader.WindowReadResult emptyResult = SqsWindowReader.WindowReadResult.queueEmpty(
                Collections.emptyList(), 0, 0);
        when(mockWindowReader.readWindow())
                .thenReturn(windowResult)
                .thenReturn(emptyResult);

         // Setup - all messages are denylisted
        when(mockTrafficFilter.isDenylisted(any())).thenReturn(true);

        // Act
        DeltaProductionResult result = orchestrator.produceBatchedDeltas(name -> {});

        // Assert - stop reason, totals
        assertEquals(StopReason.QUEUE_EMPTY, result.getStopReason());
        assertEquals(0, result.getDeltasProduced());
        assertEquals(0, result.getEntriesProcessed());
        assertEquals(1, result.getDroppedRequestFilesProduced());
        assertEquals(5, result.getDroppedRequestsProcessed());
        
        // Assert - delta not uploaded, dropped requests uploaded
        verify(mockDeltaUploadService, never()).uploadAndDeleteMessages(any(), anyString(), anyList(), any());
        verify(mockDroppedRequestUploadService).uploadAndDeleteMessages(any(), anyString(), anyList(), any());
    }

    // ==================== Circuit Breaker Tests ====================

    @Test
    void testProduceBatchedDeltas_circuitBreakerTriggered_stopsAndSetsOverride() throws IOException {
        // Setup - window with 10 messages
        List<SqsParsedMessage> messages = createParsedMessages(10);
        long windowStart = System.currentTimeMillis() / 1000 - 600;
        
        SqsWindowReader.WindowReadResult windowResult = SqsWindowReader.WindowReadResult.withMessages(
                messages, windowStart, 10);
        
        when(mockWindowReader.readWindow()).thenReturn(windowResult);

        // Setup - no denylisted messages
        when(mockTrafficFilter.isDenylisted(any())).thenReturn(false);

        // Setup - circuit breaker triggered
        when(mockTrafficCalculator.calculateStatus(anyList(), any(), anyInt(), anyInt()))
                .thenReturn(TrafficCalculator.TrafficStatus.DELAYED_PROCESSING);

        DeltaProductionResult result = orchestrator.produceBatchedDeltas(name -> {});

        // Assert - stop reason, totals
        assertEquals(StopReason.CIRCUIT_BREAKER_TRIGGERED, result.getStopReason());
        assertEquals(0, result.getDeltasProduced());
        assertEquals(0, result.getEntriesProcessed());
        assertEquals(0, result.getDroppedRequestFilesProduced());
        assertEquals(0, result.getDroppedRequestsProcessed());
        
        // Assert - manual override set
        verify(mockManualOverrideService).setDelayedProcessing();
        
        // Assert - delta not uploaded, dropped requests not uploaded
        verify(mockDeltaUploadService, never()).uploadAndDeleteMessages(any(), anyString(), anyList(), any());
        verify(mockDroppedRequestUploadService, never()).uploadAndDeleteMessages(any(), anyString(), anyList(), any());
    }

    // ==================== Callback Tests ====================

    @Test
    void testProduceBatchedDeltas_callbackInvokedWithDeltaName() throws IOException {
        // Setup - window with 5 messages, then empty
        List<SqsParsedMessage> messages = createParsedMessages(5);
        long windowStart = System.currentTimeMillis() / 1000 - 600;
        
        SqsWindowReader.WindowReadResult windowResult = SqsWindowReader.WindowReadResult.withMessages(
                messages, windowStart, 5);
        SqsWindowReader.WindowReadResult emptyResult = SqsWindowReader.WindowReadResult.queueEmpty(
                Collections.emptyList(), 0, 0);
        
        when(mockWindowReader.readWindow())
                .thenReturn(windowResult)
                .thenReturn(emptyResult);

        // Setup - no denylisted messages
        when(mockTrafficFilter.isDenylisted(any())).thenReturn(false);

        // Act
        List<String> producedDeltas = new ArrayList<>();
        DeltaProductionResult result = orchestrator.produceBatchedDeltas(producedDeltas::add);

        // Assert - stop reason, totals
        assertEquals(StopReason.QUEUE_EMPTY, result.getStopReason());
        assertEquals(1, result.getDeltasProduced());
        assertEquals(5, result.getEntriesProcessed());
        assertEquals(0, result.getDroppedRequestFilesProduced());
        assertEquals(0, result.getDroppedRequestsProcessed());

        // Assert - delta uploaded
        verify(mockDeltaUploadService).uploadAndDeleteMessages(any(), anyString(), anyList(), any());
        
        // Assert - callback invoked with delta name
        assertEquals(1, producedDeltas.size());
        assertTrue(producedDeltas.get(0).contains("optout-delta"));

        // Assert - metrics recorded
        verify(mockMetrics).recordDeltaProduced(5);
    }

    // ==================== Helper Methods ====================

    private List<SqsParsedMessage> createParsedMessages(int count) {
        List<SqsParsedMessage> messages = new ArrayList<>();
        long baseTimestamp = System.currentTimeMillis() / 1000 - 600;
        
        for (int i = 0; i < count; i++) {
            Message rawMessage = createRawMessage("msg-" + i, baseTimestamp + i);
            byte[] hashBytes = new byte[32];
            byte[] idBytes = new byte[32];
            Arrays.fill(hashBytes, (byte) i);
            Arrays.fill(idBytes, (byte) (i + 1));
            
            SqsParsedMessage parsed = new SqsParsedMessage(
                    rawMessage,
                    hashBytes,
                    idBytes,
                    baseTimestamp + i,
                    "test" + i + "@email.com",  // email
                    null,  // phone
                    "192.168.1." + i,  // clientIp
                    "trace-" + i  // traceId
            );
            messages.add(parsed);
        }
        return messages;
    }

    private Message createRawMessage(String messageId, long timestampSeconds) {
        Map<MessageSystemAttributeName, String> attrs = new HashMap<>();
        attrs.put(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(timestampSeconds * 1000));
        
        return Message.builder()
                .messageId(messageId)
                .receiptHandle("receipt-" + messageId)
                .body("{\"identity_hash\":\"test\",\"advertising_id\":\"test\"}")
                .attributes(attrs)
                .build();
    }
}
