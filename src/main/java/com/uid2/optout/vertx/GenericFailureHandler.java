package com.uid2.optout.vertx;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClosedException;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.http.impl.EnglishReasonPhraseCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericFailureHandler implements Handler<RoutingContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GenericFailureHandler.class);

    @Override
    public void handle(RoutingContext ctx) {
        int statusCode = ctx.statusCode();
        HttpServerResponse response = ctx.response();
        String url = ctx.normalizedPath();
        Throwable t = ctx.failure();

        String errorMsg = t.getMessage();
        String className = t.getClass().getName();

        // Handle multipart method mismatch (IllegalStateException)
        if (t instanceof IllegalStateException &&
            errorMsg != null &&
            errorMsg.equalsIgnoreCase(
                "Request method must be one of POST, PUT, PATCH or DELETE to decode a multipart request")) {
            if (!response.ended() && !response.closed()) {
                response.setStatusCode(400)
                        .end("Bad Request: multipart content not allowed with this HTTP method");
            }
            LOGGER.warn("URL: [{}] - Multipart method mismatch - Error:", url, t);
            return;
        }

        // Handle TooManyFormFieldsException
        if (className.contains("TooManyFormFieldsException")) {
            if (!response.ended() && !response.closed()) {
                response.setStatusCode(400)
                        .end("Bad Request: Too many form fields");
            }
            LOGGER.warn("URL: [{}] - Too many form fields - Error:", url, t);
            return;
        }

        // Handle HttpClosedException - ignore as it's usually caused by users and has no impact
        if (t instanceof HttpClosedException) {
            LOGGER.warn("Ignoring exception - URL: [{}] - Error:", url, t);
            if (!response.ended() && !response.closed()) {
                response.end();
            }
            return;
        }

        // Handle other exceptions based on status code
        // If no status code was set, default to 500
        int finalStatusCode = statusCode == -1 ? 500 : statusCode;

        if (finalStatusCode >= 500 && finalStatusCode < 600) { // 5xx is server error, so error
            LOGGER.error("URL: [{}] - Error response code: [{}] - Error:", url, finalStatusCode, t);
        } else if (finalStatusCode >= 400 && finalStatusCode < 500) { // 4xx is user error, so just warn
            LOGGER.warn("URL: [{}] - Error response code: [{}] - Error:", url, finalStatusCode, t);
        } else {
            // Status code not in 4xx or 5xx range, log as error
            LOGGER.error("URL: [{}] - Unexpected status code: [{}] - Error:", url, finalStatusCode, t);
        }

        if (!response.ended() && !response.closed()) {
            response.setStatusCode(finalStatusCode)
                    .end(EnglishReasonPhraseCatalog.INSTANCE.getReason(finalStatusCode, null));
        }
    }
}
