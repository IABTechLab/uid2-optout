package com.uid2.optout.sqs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for SQS message operations.
 */
public class SqsMessageOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsMessageOperations.class);
    private static final int SQS_MAX_DELETE_BATCH_SIZE = 10;

    /**
     * Result of getting queue attributes from SQS.
     */
    public static class QueueAttributes {
        private final int approximateNumberOfMessages;
        private final int approximateNumberOfMessagesNotVisible;
        private final int approximateNumberOfMessagesDelayed;

        public QueueAttributes(int approximateNumberOfMessages, 
                               int approximateNumberOfMessagesNotVisible,
                               int approximateNumberOfMessagesDelayed) {
            this.approximateNumberOfMessages = approximateNumberOfMessages;
            this.approximateNumberOfMessagesNotVisible = approximateNumberOfMessagesNotVisible;
            this.approximateNumberOfMessagesDelayed = approximateNumberOfMessagesDelayed;
        }

        /** Number of messages available for retrieval from the queue (visible messages) */
        public int getApproximateNumberOfMessages() {
            return approximateNumberOfMessages;
        }

        /** Number of messages that are in flight (being processed by consumers, invisible) */
        public int getApproximateNumberOfMessagesNotVisible() {
            return approximateNumberOfMessagesNotVisible;
        }

        /** Number of messages in the queue that are delayed and not available yet */
        public int getApproximateNumberOfMessagesDelayed() {
            return approximateNumberOfMessagesDelayed;
        }

        /** Total messages in queue = visible + invisible + delayed */
        public int getTotalMessages() {
            return approximateNumberOfMessages + approximateNumberOfMessagesNotVisible + approximateNumberOfMessagesDelayed;
        }

        @Override
        public String toString() {
            return String.format("QueueAttributes{visible=%d, invisible=%d, delayed=%d, total=%d}",
                approximateNumberOfMessages, approximateNumberOfMessagesNotVisible, 
                approximateNumberOfMessagesDelayed, getTotalMessages());
        }
    }

    /**
     * Gets queue attributes from SQS including message counts.
     * 
     * @param sqsClient The SQS client
     * @param queueUrl The queue URL
     * @return QueueAttributes with message counts, or null if failed
     */
    public static QueueAttributes getQueueAttributes(SqsClient sqsClient, String queueUrl) {
        try {
            GetQueueAttributesRequest request = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(
                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED
                )
                .build();

            GetQueueAttributesResponse response = sqsClient.getQueueAttributes(request);
            Map<QueueAttributeName, String> attrs = response.attributes();

            int visible = parseIntOrDefault(attrs.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES), 0);
            int invisible = parseIntOrDefault(attrs.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE), 0);
            int delayed = parseIntOrDefault(attrs.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED), 0);

            QueueAttributes queueAttributes = new QueueAttributes(visible, invisible, delayed);
            LOGGER.info("queue attributes: {}", queueAttributes);
            return queueAttributes;

        } catch (Exception e) {
            LOGGER.info("sqs_error: error getting queue attributes", e);
            return null;
        }
    }

    private static int parseIntOrDefault(String value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Receives a batch of messages from SQS.
     * 
     * @param sqsClient The SQS client
     * @param queueUrl The queue URL
     * @param maxMessages Maximum number of messages to receive (max 10)
     * @param visibilityTimeout Visibility timeout in seconds
     * @return List of received messages
     */
    public static List<Message> receiveMessagesFromSqs(
            SqsClient sqsClient,
            String queueUrl,
            int maxMessages,
            int visibilityTimeout) {
        
        try {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxMessages)
                .visibilityTimeout(visibilityTimeout)
                .waitTimeSeconds(0) // non-blocking poll
                .messageSystemAttributeNames(MessageSystemAttributeName.SENT_TIMESTAMP) // request sqs system timestamp
                .build();

            ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);

            LOGGER.info("received {} messages", response.messages().size());
            return response.messages();

        } catch (Exception e) {
            LOGGER.error("sqs_error: failed to receive messages", e);
            return new ArrayList<>();
        }
    }

    /**
     * Deletes messages from SQS in batches (max 10 per batch).
     * Retries failed deletes as long as progress is being made.
     * 
     * @param sqsClient The SQS client
     * @param queueUrl The queue URL
     * @param messages Messages to delete
     */
    public static void deleteMessagesFromSqs(SqsClient sqsClient, String queueUrl, List<Message> messages) {
        if (messages.isEmpty()) {
            return;
        }

        try {
            int totalDeleted = 0;
            List<DeleteMessageBatchRequestEntry> batch = new ArrayList<>();

            for (int i = 0; i < messages.size(); i++) {
                batch.add(DeleteMessageBatchRequestEntry.builder()
                    .id(String.valueOf(i))
                    .receiptHandle(messages.get(i).receiptHandle())
                    .build());

                // send batch when we reach 10 messages or end of list
                if (batch.size() == SQS_MAX_DELETE_BATCH_SIZE || i == messages.size() - 1) {
                    totalDeleted += deleteBatchWithRetry(sqsClient, queueUrl, batch);
                    batch.clear();
                }
            }

            LOGGER.info("deleted {} messages", totalDeleted);
        } catch (Exception e) {
            LOGGER.error("sqs_error: error deleting messages", e);
        }
    }

    /** Deletes batch, retrying failed entries. Retries once unconditionally, then only while making progress. */
    private static int deleteBatchWithRetry(SqsClient sqsClient, String queueUrl, List<DeleteMessageBatchRequestEntry> entries) {
        int deleted = 0;
        List<DeleteMessageBatchRequestEntry> toDelete = entries;
        boolean retriedOnce = false;

        while (!toDelete.isEmpty()) {
            DeleteMessageBatchResponse response = sqsClient.deleteMessageBatch(
                DeleteMessageBatchRequest.builder().queueUrl(queueUrl).entries(toDelete).build());

            int succeeded = response.successful().size();
            deleted += succeeded;

            if (response.failed().isEmpty()) {
                break; // all done
            }

            // retry once unconditionally, then only if making progress
            if (retriedOnce && succeeded == 0) {
                LOGGER.error("sqs_error: {} deletes failed with no progress", response.failed().size());
                break;
            }
            retriedOnce = true;

            // retry deletion on the failed messages only
            var failedIds = response.failed().stream().map(BatchResultErrorEntry::id).collect(Collectors.toSet());
            toDelete = toDelete.stream().filter(e -> failedIds.contains(e.id())).toList();
        }

        return deleted;
    }
}

