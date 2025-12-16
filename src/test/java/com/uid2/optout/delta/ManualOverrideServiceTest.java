package com.uid2.optout.delta;

import com.uid2.shared.cloud.CloudStorageException;
import com.uid2.shared.cloud.ICloudStorage;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ManualOverrideServiceTest {

    private ICloudStorage mockCloudStorage;
    private ManualOverrideService manualOverrideService;
    
    private static final String TEST_OVERRIDE_PATH = "config/manual-override.json";

    @BeforeEach
    void setUp() {
        mockCloudStorage = mock(ICloudStorage.class);
        manualOverrideService = new ManualOverrideService(mockCloudStorage, TEST_OVERRIDE_PATH);
    }

    // ==================== isDelayedProcessing tests ====================

    @Test
    void testIsDelayedProcessing_returnsTrue_whenOverrideSet() throws Exception {
        JsonObject config = new JsonObject().put("manual_override", "DELAYED_PROCESSING");
        InputStream inputStream = new ByteArrayInputStream(config.encode().getBytes(StandardCharsets.UTF_8));
        when(mockCloudStorage.download(TEST_OVERRIDE_PATH)).thenReturn(inputStream);

        assertTrue(manualOverrideService.isDelayedProcessing());
    }

    @Test
    void testIsDelayedProcessing_returnsFalse_whenOverrideNotSet() throws Exception {
        JsonObject config = new JsonObject().put("manual_override", "");
        InputStream inputStream = new ByteArrayInputStream(config.encode().getBytes(StandardCharsets.UTF_8));
        when(mockCloudStorage.download(TEST_OVERRIDE_PATH)).thenReturn(inputStream);

        assertFalse(manualOverrideService.isDelayedProcessing());
    }

    @Test
    void testIsDelayedProcessing_returnsFalse_whenFileNotFound() throws Exception {
        when(mockCloudStorage.download(TEST_OVERRIDE_PATH))
            .thenThrow(new CloudStorageException("File not found"));

        assertFalse(manualOverrideService.isDelayedProcessing());
    }

    @Test
    void testIsDelayedProcessing_returnsFalse_whenDifferentValue() throws Exception {
        JsonObject config = new JsonObject().put("manual_override", "SOME_OTHER_VALUE");
        InputStream inputStream = new ByteArrayInputStream(config.encode().getBytes(StandardCharsets.UTF_8));
        when(mockCloudStorage.download(TEST_OVERRIDE_PATH)).thenReturn(inputStream);

        assertFalse(manualOverrideService.isDelayedProcessing());
    }

    @Test
    void testIsDelayedProcessing_returnsFalse_whenKeyMissing() throws Exception {
        JsonObject config = new JsonObject().put("other_key", "value");
        InputStream inputStream = new ByteArrayInputStream(config.encode().getBytes(StandardCharsets.UTF_8));
        when(mockCloudStorage.download(TEST_OVERRIDE_PATH)).thenReturn(inputStream);

        assertFalse(manualOverrideService.isDelayedProcessing());
    }

    @Test
    void testIsDelayedProcessing_returnsFalse_whenInvalidJson() throws Exception {
        InputStream inputStream = new ByteArrayInputStream("not valid json".getBytes(StandardCharsets.UTF_8));
        when(mockCloudStorage.download(TEST_OVERRIDE_PATH)).thenReturn(inputStream);

        assertFalse(manualOverrideService.isDelayedProcessing());
    }

    @Test
    void testIsDelayedProcessing_caseInsensitive() throws Exception {
        JsonObject config = new JsonObject().put("manual_override", "delayed_processing");
        InputStream inputStream = new ByteArrayInputStream(config.encode().getBytes(StandardCharsets.UTF_8));
        when(mockCloudStorage.download(TEST_OVERRIDE_PATH)).thenReturn(inputStream);

        assertTrue(manualOverrideService.isDelayedProcessing());
    }

    @Test
    void testIsDelayedProcessing_emptyFile() throws Exception {
        InputStream inputStream = new ByteArrayInputStream("{}".getBytes(StandardCharsets.UTF_8));
        when(mockCloudStorage.download(TEST_OVERRIDE_PATH)).thenReturn(inputStream);

        assertFalse(manualOverrideService.isDelayedProcessing());
    }

    @Test
    void testIsDelayedProcessing_nullValue() throws Exception {
        InputStream inputStream = new ByteArrayInputStream("{\"manual_override\":null}".getBytes(StandardCharsets.UTF_8));
        when(mockCloudStorage.download(TEST_OVERRIDE_PATH)).thenReturn(inputStream);

        assertFalse(manualOverrideService.isDelayedProcessing());
    }

    // ==================== setDelayedProcessing tests ====================

    @Test
    void testSetDelayedProcessing_success() throws Exception {
        boolean result = manualOverrideService.setDelayedProcessing();

        assertTrue(result);
        
        ArgumentCaptor<InputStream> inputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
        verify(mockCloudStorage).upload(inputStreamCaptor.capture(), eq(TEST_OVERRIDE_PATH));
        
        // Verify the uploaded content
        String uploadedContent = new String(inputStreamCaptor.getValue().readAllBytes(), StandardCharsets.UTF_8);
        JsonObject uploadedJson = new JsonObject(uploadedContent);
        assertEquals("DELAYED_PROCESSING", uploadedJson.getString("manual_override"));
    }

    @Test
    void testSetDelayedProcessing_failsOnCloudStorageException() throws Exception {
        doThrow(new CloudStorageException("Upload failed"))
            .when(mockCloudStorage).upload(any(InputStream.class), eq(TEST_OVERRIDE_PATH));

        boolean result = manualOverrideService.setDelayedProcessing();

        assertFalse(result);
    }

    @Test
    void testSetDelayedProcessing_uploadsToCorrectPath() throws Exception {
        manualOverrideService.setDelayedProcessing();

        verify(mockCloudStorage).upload(any(InputStream.class), eq(TEST_OVERRIDE_PATH));
    }
}

