package com.uid2.optout.util;

import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;

/**
 * Utility class for HTTP JSON response handling.
 * Ensures consistent response format across handlers.
 */
public class HttpResponseHelper {
    
    /**
     * Send a JSON response with the specified status code.
     */
    public static void sendJson(HttpServerResponse resp, int statusCode, JsonObject body) {
        resp.setStatusCode(statusCode)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            .end(body.encode());
    }
    
    /**
     * Send a 200 OK response with JSON body.
     */
    public static void sendSuccess(HttpServerResponse resp, JsonObject body) {
        sendJson(resp, 200, body);
    }
    
    /**
     * Send a 200 OK response with status and message.
     */
    public static void sendSuccess(HttpServerResponse resp, String status, String message) {
        sendJson(resp, 200, new JsonObject().put("status", status).put("message", message));
    }

    /**
     * Send a 200 OK response with idle status and message.
     */
    public static void sendIdle(HttpServerResponse resp, String message) {
        sendJson(resp, 200, new JsonObject().put("status", "idle").put("message", message));
    }
    /**
     * Send a 202 Accepted response indicating async job started.
     */
    public static void sendAccepted(HttpServerResponse resp, String message) {
        sendJson(resp, 202, new JsonObject().put("status", "accepted").put("message", message));
    }
    
    /**
     * Send a 409 Conflict response.
     */
    public static void sendConflict(HttpServerResponse resp, String reason) {
        sendJson(resp, 409, new JsonObject().put("status", "conflict").put("reason", reason));
    }
    
    /**
     * Send a 500 Internal Server Error response.
     */
    public static void sendError(HttpServerResponse resp, String error) {
        sendJson(resp, 500, new JsonObject().put("status", "failed").put("error", error));
    }
    
    /**
     * Send a 500 Internal Server Error response from an exception.
     */
    public static void sendError(HttpServerResponse resp, Exception e) {
        sendError(resp, e.getMessage());
    }
}

