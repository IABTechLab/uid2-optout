package com.uid2.optout.auth;

import com.uid2.shared.audit.Audit;
import com.uid2.shared.audit.AuditParams;
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
    private final Audit audit;
    private final String internalApiToken;

    private Handler<RoutingContext> logAndHandle(Handler<RoutingContext> handler, AuditParams auditParams) {
        return ctx -> {
            ctx.addBodyEndHandler(v -> this.audit.log(ctx, auditParams));
            handler.handle(ctx);
        };
    }

    public InternalAuthMiddleware(String internalApiToken, String auditSource) {
        this.internalApiToken = internalApiToken;
        this.audit = new Audit(auditSource);
    }

    public Handler<RoutingContext> handleWithAudit(Handler<RoutingContext> handler) {
        InternalAuthHandler h;
        final Handler<RoutingContext> loggedHandler = logAndHandle(handler, new AuditParams());
        h = new InternalAuthHandler(loggedHandler, this.internalApiToken);
        return h::handle;
    }
}
