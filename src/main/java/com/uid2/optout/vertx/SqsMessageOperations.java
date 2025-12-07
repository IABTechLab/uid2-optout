package com.uid2.optout.vertx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
            LOGGER.info("error getting queue attributes", e);
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
                .waitTimeSeconds(0) // Non-blocking poll
                .messageSystemAttributeNames(MessageSystemAttributeName.SENT_TIMESTAMP) // Request SQS system timestamp
                .build();

            ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);

            LOGGER.info("received {} messages", response.messages().size());
            return response.messages();

        } catch (Exception e) {
            LOGGER.error("error receiving messages", e);
            return new ArrayList<>();
        }
    }

    /**
     * Deletes messages from SQS in batches (max 10 per batch).
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
            List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>();
            int batchId = 0;
            int totalDeleted = 0;

            for (Message msg : messages) {
                entries.add(DeleteMessageBatchRequestEntry.builder()
                    .id(String.valueOf(batchId++))
                    .receiptHandle(msg.receiptHandle())
                    .build());

                // Send batch when we reach 10 messages or at the end
                if (entries.size() == SQS_MAX_DELETE_BATCH_SIZE || batchId == messages.size()) {
                    DeleteMessageBatchRequest deleteRequest = DeleteMessageBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(entries)
                        .build();

                    DeleteMessageBatchResponse deleteResponse = sqsClient.deleteMessageBatch(deleteRequest);

                    if (!deleteResponse.failed().isEmpty()) {
                        LOGGER.error("failed to delete {} messages", deleteResponse.failed().size());
                    } else {
                        totalDeleted += entries.size();
                    }

                    entries.clear();
                }
            }

            LOGGER.info("deleted {} messages", totalDeleted);

        } catch (Exception e) {
            LOGGER.error("error deleting messages", e);
        }
    }
}

