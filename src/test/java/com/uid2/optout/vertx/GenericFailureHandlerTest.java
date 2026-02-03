package com.uid2.optout.vertx;

import io.vertx.core.http.HttpClosedException;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class GenericFailureHandlerTest {
    @Mock
    private RoutingContext routingContext;
    @Mock
    private HttpServerResponse response;
    
    private GenericFailureHandler handler;

    @BeforeEach
    public void setup() {
        handler = new GenericFailureHandler();
        when(routingContext.response()).thenReturn(response);
        when(routingContext.normalizedPath()).thenReturn("/test/path");
        when(response.ended()).thenReturn(false);
        when(response.closed()).thenReturn(false);
        // Mock setStatusCode to return response for method chaining
        when(response.setStatusCode(anyInt())).thenReturn(response);
    }

    @Test
    public void testMultipartMethodMismatch_returns400() {
        String errorMsg = "Request method must be one of POST, PUT, PATCH or DELETE to decode a multipart request";
        IllegalStateException exception = new IllegalStateException(errorMsg);
        
        when(routingContext.statusCode()).thenReturn(-1);
        when(routingContext.failure()).thenReturn(exception);
        
        handler.handle(routingContext);
        
        ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<String> bodyCaptor = ArgumentCaptor.forClass(String.class);
        
        verify(response).setStatusCode(statusCaptor.capture());
        verify(response).end(bodyCaptor.capture());
        
        assertEquals(400, statusCaptor.getValue());
        assertEquals(errorMsg, bodyCaptor.getValue());
    }

    @Test
    public void testMultipartMethodMismatch_caseInsensitive() {
        IllegalStateException exception = new IllegalStateException(
            "REQUEST METHOD MUST BE ONE OF POST, PUT, PATCH OR DELETE TO DECODE A MULTIPART REQUEST");
        
        when(routingContext.statusCode()).thenReturn(-1);
        when(routingContext.failure()).thenReturn(exception);
        
        handler.handle(routingContext);
        
        ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(response).setStatusCode(statusCaptor.capture());
        assertEquals(400, statusCaptor.getValue());
    }

    @Test
    public void testIllegalStateException_withDifferentMessage_handledNormally() {
        IllegalStateException exception = new IllegalStateException("Different error message");
        
        when(routingContext.statusCode()).thenReturn(500);
        when(routingContext.failure()).thenReturn(exception);
        
        handler.handle(routingContext);
        
        // Should not return 400, but use the status code from context
        ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(response).setStatusCode(statusCaptor.capture());
        assertEquals(500, statusCaptor.getValue());
    }

    @Test
    public void testMultipartMethodMismatch_withNullMessage_handledNormally() {
        IllegalStateException exception = new IllegalStateException((String) null);
        
        when(routingContext.statusCode()).thenReturn(500);
        when(routingContext.failure()).thenReturn(exception);
        
        handler.handle(routingContext);
        
        // Should not return 400, but use the status code from context
        ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(response).setStatusCode(statusCaptor.capture());
        assertEquals(500, statusCaptor.getValue());
    }

    @Test
    public void testTooManyFormFieldsException_withNettyClassNamePattern_returns400() {
        // Test with an exception that has a class name matching the Netty pattern
        // Actual Netty exception: io.netty.handler.codec.http.multipart.HttpPostRequestDecoder$TooManyFormFieldsException
        // The handler checks if className.contains("TooManyFormFieldsException"), so this should work
        NettyTooManyFormFieldsException exception = new NettyTooManyFormFieldsException();
        
        when(routingContext.statusCode()).thenReturn(-1);
        when(routingContext.failure()).thenReturn(exception);
        
        handler.handle(routingContext);
        
        ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<String> bodyCaptor = ArgumentCaptor.forClass(String.class);
        
        verify(response).setStatusCode(statusCaptor.capture());
        verify(response).end(bodyCaptor.capture());
        
        assertEquals(400, statusCaptor.getValue());
        assertEquals("Bad Request: Too many form fields", bodyCaptor.getValue());
        
        // Verify the class name contains "TooManyFormFieldsException" (matching handler logic)
        assertTrue(exception.getClass().getName().contains("TooManyFormFieldsException"));
    }

    @Test
    public void testHttpClosedException_ignored() {
        HttpClosedException exception = new HttpClosedException("Connection closed");
        
        when(routingContext.statusCode()).thenReturn(-1);
        when(routingContext.failure()).thenReturn(exception);
        
        handler.handle(routingContext);
        
        // Should only call end() without setting status code
        verify(response, never()).setStatusCode(anyInt());
        verify(response).end();
    }

    @Test
    public void testHttpClosedException_responseAlreadyEnded_doesNotCallEnd() {
        HttpClosedException exception = new HttpClosedException("Connection closed");
        
        when(routingContext.statusCode()).thenReturn(-1);
        when(routingContext.failure()).thenReturn(exception);
        when(response.ended()).thenReturn(true);
        
        handler.handle(routingContext);
        
        verify(response, never()).setStatusCode(anyInt());
        verify(response, never()).end();
    }

    @Test
    public void testHttpClosedException_responseClosed_doesNotCallEnd() {
        HttpClosedException exception = new HttpClosedException("Connection closed");
        
        when(routingContext.statusCode()).thenReturn(-1);
        when(routingContext.failure()).thenReturn(exception);
        when(response.closed()).thenReturn(true);
        
        handler.handle(routingContext);
        
        verify(response, never()).setStatusCode(anyInt());
        verify(response, never()).end();
    }

    @Test
    public void test500StatusCode_logsError() {
        RuntimeException exception = new RuntimeException("Internal server error");
        
        when(routingContext.statusCode()).thenReturn(500);
        when(routingContext.failure()).thenReturn(exception);
        
        handler.handle(routingContext);
        
        ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(response).setStatusCode(statusCaptor.capture());
        assertEquals(500, statusCaptor.getValue());
        verify(response).end(anyString());
    }

    @Test
    public void test503StatusCode_logsError() {
        RuntimeException exception = new RuntimeException("Service unavailable");
        
        when(routingContext.statusCode()).thenReturn(503);
        when(routingContext.failure()).thenReturn(exception);
        
        handler.handle(routingContext);
        
        ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(response).setStatusCode(statusCaptor.capture());
        assertEquals(503, statusCaptor.getValue());
        verify(response).end(anyString());
    }

    @Test
    public void test400StatusCode_logsWarning() {
        IllegalArgumentException exception = new IllegalArgumentException("Bad request");
        
        when(routingContext.statusCode()).thenReturn(400);
        when(routingContext.failure()).thenReturn(exception);
        
        handler.handle(routingContext);
        
        ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(response).setStatusCode(statusCaptor.capture());
        assertEquals(400, statusCaptor.getValue());
        verify(response).end(anyString());
    }

    @Test
    public void test404StatusCode_logsWarning() {
        RuntimeException exception = new RuntimeException("Not found");
        
        when(routingContext.statusCode()).thenReturn(404);
        when(routingContext.failure()).thenReturn(exception);
        
        handler.handle(routingContext);
        
        ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(response).setStatusCode(statusCaptor.capture());
        assertEquals(404, statusCaptor.getValue());
        verify(response).end(anyString());
    }

    @Test
    public void testNoStatusCode_defaultsTo500() {
        RuntimeException exception = new RuntimeException("Unknown error");
        
        when(routingContext.statusCode()).thenReturn(-1);
        when(routingContext.failure()).thenReturn(exception);
        
        handler.handle(routingContext);
        
        ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(response).setStatusCode(statusCaptor.capture());
        assertEquals(500, statusCaptor.getValue());
        verify(response).end(anyString());
    }

    @Test
    public void testSuccessStatus_status200_responseAlreadyEnded() {
        // Sucess status case: Status code is 200 (success) and response is already ended
        // The handler should not modify anything since the response is already complete
        RuntimeException exception = new RuntimeException("Exception but response already sent");
        
        when(routingContext.statusCode()).thenReturn(200);
        when(routingContext.failure()).thenReturn(exception);
        when(response.ended()).thenReturn(true); // Response already ended - happy case
        
        handler.handle(routingContext);
        
        // Handler should not modify the response since it's already ended
        verify(response, never()).setStatusCode(anyInt());
        verify(response, never()).end();
        verify(response, never()).end(anyString());
    }

    @Test
    public void testResponseAlreadyEnded_doesNotSetStatusCode() {
        RuntimeException exception = new RuntimeException("Error");
        
        when(routingContext.statusCode()).thenReturn(500);
        when(routingContext.failure()).thenReturn(exception);
        when(response.ended()).thenReturn(true);
        
        handler.handle(routingContext);
        
        verify(response, never()).setStatusCode(anyInt());
        verify(response, never()).end();
    }

    @Test
    public void testResponseClosed_doesNotSetStatusCode() {
        RuntimeException exception = new RuntimeException("Error");
        
        when(routingContext.statusCode()).thenReturn(500);
        when(routingContext.failure()).thenReturn(exception);
        when(response.closed()).thenReturn(true);
        
        handler.handle(routingContext);
        
        verify(response, never()).setStatusCode(anyInt());
        verify(response, never()).end();
    }

    @Test
    public void testExceptionWithNullMessage() {
        // RuntimeException with no message will have getMessage() return null
        RuntimeException exception = new RuntimeException((String) null);
        
        when(routingContext.statusCode()).thenReturn(500);
        when(routingContext.failure()).thenReturn(exception);
        
        handler.handle(routingContext);
        
        ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(response).setStatusCode(statusCaptor.capture());
        assertEquals(500, statusCaptor.getValue());
        verify(response).end(anyString());
    }

    @Test
    public void testMultipartMethodMismatch_responseAlreadyEnded_doesNotCallEnd() {
        IllegalStateException exception = new IllegalStateException(
            "Request method must be one of POST, PUT, PATCH or DELETE to decode a multipart request");
        
        when(routingContext.statusCode()).thenReturn(-1);
        when(routingContext.failure()).thenReturn(exception);
        when(response.ended()).thenReturn(true);
        
        handler.handle(routingContext);
        
        verify(response, never()).setStatusCode(anyInt());
        verify(response, never()).end();
    }

    @Test
    public void testTooManyFormFieldsException_responseAlreadyEnded_doesNotCallEnd() {
        TestTooManyFormFieldsException exception = new TestTooManyFormFieldsException("Too many form fields");
        
        when(routingContext.statusCode()).thenReturn(-1);
        when(routingContext.failure()).thenReturn(exception);
        when(response.ended()).thenReturn(true);
        
        handler.handle(routingContext);
        
        verify(response, never()).setStatusCode(anyInt());
        verify(response, never()).end();
    }

    // Helper class to simulate TooManyFormFieldsException
    // The class name must contain "TooManyFormFieldsException" for the handler to recognize it
    private static class TestTooManyFormFieldsException extends RuntimeException {
        public TestTooManyFormFieldsException(String message) {
            super(message);
        }
    }

    // Helper class to simulate the actual Netty exception class name pattern
    // This matches: io.netty.handler.codec.http.multipart.HttpPostRequestDecoder$TooManyFormFieldsException
    // The $ indicates it's an inner class in Netty
    private static class NettyTooManyFormFieldsException extends RuntimeException {
        public NettyTooManyFormFieldsException() {
            super();
        }
    }
}
