package com.uid2.optout.vertx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for SQS message operations.
 */
public class SqsMessageOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsMessageOperations.class);
    private static final int SQS_MAX_DELETE_BATCH_SIZE = 10;

    /**
     * Receives all available messages from an SQS queue up to a maximum number of batches.
     * 
     * @param sqsClient The SQS client
     * @param queueUrl The queue URL
     * @param maxMessagesPerPoll Maximum messages to receive per poll (max 10)
     * @param visibilityTimeout Visibility timeout in seconds
     * @param maxBatches Maximum number of receive batches
     * @return List of all received messages
     */
    public static List<Message> receiveAllAvailableMessages(
            SqsClient sqsClient,
            String queueUrl,
            int maxMessagesPerPoll,
            int visibilityTimeout,
            int maxBatches) {
        
        List<Message> allMessages = new ArrayList<>();
        int batchCount = 0;

        // Keep receiving messages until we get an empty batch or hit the limit
        while (batchCount < maxBatches) {
            List<Message> batch = receiveMessagesFromSqs(sqsClient, queueUrl, maxMessagesPerPoll, visibilityTimeout);
            if (batch.isEmpty()) {
                break;
            }
            allMessages.addAll(batch);
            batchCount++;

            // If we got fewer messages than the max (of 10), the queue is likely empty
            if (batch.size() < maxMessagesPerPoll) {
                break;
            }
        }

        return allMessages;
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

            LOGGER.debug("Received {} messages from SQS", response.messages().size());
            return response.messages();

        } catch (Exception e) {
            LOGGER.error("Error receiving messages from SQS", e);
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
                        LOGGER.error("Failed to delete {} messages from SQS", deleteResponse.failed().size());
                    } else {
                        totalDeleted += entries.size();
                    }

                    entries.clear();
                }
            }

            LOGGER.info("Deleted {} messages from SQS", totalDeleted);

        } catch (Exception e) {
            LOGGER.error("Error deleting messages from SQS", e);
        }
    }
}

