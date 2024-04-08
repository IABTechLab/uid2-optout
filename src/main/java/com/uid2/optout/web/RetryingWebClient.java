package com.uid2.optout.web;

import com.google.common.base.Stopwatch;
import io.netty.handler.codec.http.HttpMethod;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.BiFunction;

public class RetryingWebClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryingWebClient.class);
    private final URI uri;
    private final HttpMethod method;
    private final int retryCount;
    private final int retryBackoffMs;
    private final HttpClient httpClient;
    private Vertx vertx;

    public RetryingWebClient(Vertx vertx, String uri, HttpMethod method, int retryCount, int retryBackoffMs) {
        this.vertx = vertx;
        this.uri = URI.create(uri);
        this.method = method;
        this.httpClient = HttpClient.newHttpClient();

        this.retryCount = retryCount;
        this.retryBackoffMs = retryBackoffMs;
    }

    public Future<Void> send(BiFunction<URI, HttpMethod, HttpRequest> requestCreator, Function<HttpResponse, Boolean> responseValidator) {
        final UUID requestId = UUID.randomUUID();
        return this.send(requestCreator, responseValidator, 0, requestId)
                .onFailure(ex -> LOGGER.error("requestId={} Request to {} failed", requestId, uri, ex));
    }

    public Future<Void> send(BiFunction<URI, HttpMethod, HttpRequest> requestCreator, Function<HttpResponse, Boolean> responseValidator, int currentRetries, UUID requestId) {
        HttpRequest request = requestCreator.apply(this.uri, this.method);

        LOGGER.info("requestId={} Sending request to {}, currentRetries={}", requestId, uri, currentRetries);

        final Stopwatch sw = Stopwatch.createStarted();

        final CompletableFuture<HttpResponse<Void>> asyncResponse = this.httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding());

        return Future.fromCompletionStage(asyncResponse, vertx.getOrCreateContext()).compose(response -> {
            sw.stop();

            LOGGER.info("requestId={} Request to {} completed in {}ms, currentRetries={}, status={}, version={}", requestId, uri, sw.elapsed(TimeUnit.MILLISECONDS), currentRetries, response.statusCode(), response.version());

            Boolean responseOK = responseValidator.apply(response);
            if (responseOK == null) {
                return Future.failedFuture(new RuntimeException("Response validator returned null"));
            }

            if (responseOK) {
                return Future.succeededFuture();
            }

            if (currentRetries < this.retryCount) {
                LOGGER.error("requestId={} failed sending to {}, currentRetries={}, backing off for {}ms before retrying", requestId, uri, currentRetries, this.retryBackoffMs);
                if (this.retryBackoffMs > 0) {
                    return vertx.timer(this.retryBackoffMs)
                            .compose(v -> send(requestCreator, responseValidator, currentRetries + 1, requestId));
                } else {
                    return send(requestCreator, responseValidator, currentRetries + 1, requestId);
                }
            }

            LOGGER.error("requestId={} retry count exceeded for sending to {}", requestId, this.uri);
            return Future.failedFuture(new TooManyRetriesException(currentRetries));
        });
    }
}
