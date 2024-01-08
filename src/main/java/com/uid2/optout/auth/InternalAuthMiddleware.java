package com.uid2.optout.auth;

import com.uid2.shared.auth.OperatorKey;
import com.uid2.shared.middleware.AuthMiddleware;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

public class InternalAuthMiddleware {
    private static class InternalAuthHandler {
        private static final String AUTHORIZATION_HEADER = "Authorization";
        private static final String BEARER_TOKEN_PREFIX = "bearer ";

        private final Handler<RoutingContext> innerHandler;
        private final String internalApiToken;

        private InternalAuthHandler(Handler<RoutingContext> handler, String internalApiToken) {
            this.innerHandler = handler;
            this.internalApiToken = internalApiToken;
        }

        private static String extractBearerToken(final String headerValue) {
            if (headerValue == null) {
                return null;
            }

            final String v = headerValue.trim();
            if (v.length() < BEARER_TOKEN_PREFIX.length()) {
                return null;
            }

            final String givenPrefix = v.substring(0, BEARER_TOKEN_PREFIX.length());

            if (!BEARER_TOKEN_PREFIX.equals(givenPrefix.toLowerCase())) {
                return null;
            }
            return v.substring(BEARER_TOKEN_PREFIX.length());
        }

        public void handle(RoutingContext rc) {
            final String authHeaderValue = rc.request().getHeader(AUTHORIZATION_HEADER);
            final String authKey = extractBearerToken(authHeaderValue);
            if (authKey == null || !authKey.equals(internalApiToken)) {
                // auth key doesn't match internal key
                rc.fail(401);
            } else {
                AuthMiddleware.setAuthClient(rc, new OperatorKey("", "", "internal", "internal", "internal", 0, false, ""));
                this.innerHandler.handle(rc);
            }
        }
    }

    private final String internalApiToken;

    public InternalAuthMiddleware(String internalApiToken) {
        this.internalApiToken = internalApiToken;
    }

    public Handler<RoutingContext> handle(Handler<RoutingContext> handler) {
        final InternalAuthHandler h = new InternalAuthHandler(handler, this.internalApiToken);
        return h::handle;
    }
}
