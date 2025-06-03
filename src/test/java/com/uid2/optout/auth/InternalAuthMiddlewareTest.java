package com.uid2.optout.auth;

import org.junit.jupiter.api.BeforeEach;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class InternalAuthMiddlewareTest {
    @Mock
    private RoutingContext routingContext;
    @Mock
    private HttpServerRequest request;
    @Mock
    private Handler<RoutingContext> nextHandler;
    private InternalAuthMiddleware internalAuth;

    @BeforeEach
    public void setup(){
        internalAuth = new InternalAuthMiddleware("apiToken", "test");
        when(routingContext.request()).thenReturn(request);
    }

    @Test
    public void internalAuthHandlerSucceed() {
        when(request.getHeader("Authorization")).thenReturn("Bearer apiToken");
        Handler<RoutingContext> handler = internalAuth.handleWithAudit(nextHandler);
        handler.handle(routingContext);
        verify(nextHandler).handle(routingContext);
        verify(routingContext, times(0)).fail(any());
        verify(routingContext, times(1)).addBodyEndHandler(ArgumentMatchers.<Handler<Void>>any());
    }

    @Test
    public void internalAuthHandlerNoAuthorizationHeader() {
        Handler<RoutingContext> handler = internalAuth.handleWithAudit(nextHandler);
        handler.handle(routingContext);
        verifyNoInteractions(nextHandler);
        verify(routingContext).fail(401);
        verify(routingContext, times(0)).addBodyEndHandler(ArgumentMatchers.<Handler<Void>>any());
    }

    @Test public void authHandlerInvalidAuthorizationHeader() {
        when(request.getHeader("Authorization")).thenReturn("Bogus Header Value");
        Handler<RoutingContext> handler = internalAuth.handleWithAudit(nextHandler);
        handler.handle(routingContext);
        verifyNoInteractions(nextHandler);
        verify(routingContext).fail(401);
        verify(routingContext, times(0)).addBodyEndHandler(ArgumentMatchers.<Handler<Void>>any());
    }

    @Test public void authHandlerUnknownKey() {
        when(request.getHeader("Authorization")).thenReturn("Bearer unknown-key");
        Handler<RoutingContext> handler = internalAuth.handleWithAudit(nextHandler);
        handler.handle(routingContext);
        verifyNoInteractions(nextHandler);
        verify(routingContext).fail(401);
        verify(routingContext, times(0)).addBodyEndHandler(ArgumentMatchers.<Handler<Void>>any());
    }
}
