package com.uid2.optout.delta;

import com.uid2.optout.sqs.SqsMessageOperations;
import com.uid2.shared.cloud.ICloudStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

/**
 * Service for uploading data to S3 and deleting messages from SQS after successful upload.
 * 
 * This class encapsulates the critical "upload then delete" pattern that ensures
 * data is persisted to S3 before messages are removed from the queue.
 */
public class S3UploadService {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3UploadService.class);
    
    private final ICloudStorage cloudStorage;
    private final SqsClient sqsClient;
    private final String queueUrl;
    
    /**
     * Callback interface for after successful upload.
     */
    @FunctionalInterface
    public interface UploadSuccessCallback {
        /**
         * Called after successful S3 upload, before SQS message deletion.
         * 
         * @param messageCount Number of messages in the uploaded batch
         */
        void onSuccess(int messageCount);
    }
    
    /**
     * Create an S3UploadService.
     * 
     * @param cloudStorage Cloud storage client for S3 operations
     * @param sqsClient SQS client for message deletion
     * @param queueUrl SQS queue URL
     */
    public S3UploadService(ICloudStorage cloudStorage, SqsClient sqsClient, String queueUrl) {
        this.cloudStorage = cloudStorage;
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
    }
    
    /**
     * Upload data to S3 and delete associated messages from SQS after successful upload.
     * 
     * <p><strong>Critical behavior:</strong> Messages are ONLY deleted from SQS after
     * the S3 upload succeeds. This ensures no data loss if upload fails.</p>
     * 
     * @param data Data to upload
     * @param s3Path S3 path (key) for the upload
     * @param messages SQS messages to delete after successful upload
     * @param onSuccess Callback invoked after successful upload (before message deletion)
     * @throws IOException if upload fails
     */
    public void uploadAndDeleteMessages(byte[] data, String s3Path, List<Message> messages, UploadSuccessCallback onSuccess) throws IOException {
        LOGGER.info("uploading to s3: path={}, size={} bytes, messages={}", s3Path, data.length, messages.size());
        
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {
            cloudStorage.upload(inputStream, s3Path);
            
            if (onSuccess != null) {
                onSuccess.onSuccess(messages.size());
            }
        } catch (Exception e) {
            LOGGER.error("failed to upload to s3: path={}", s3Path, e);
            throw new IOException("s3 upload failed: " + s3Path, e);
        }
        
        if (!messages.isEmpty()) {
            SqsMessageOperations.deleteMessagesFromSqs(sqsClient, queueUrl, messages);
        }
    }
}
