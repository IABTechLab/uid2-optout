package com.uid2.optout.web;

import io.netty.handler.codec.http.HttpMethod;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
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
        return this.send(requestCreator, responseValidator, 0);
    }

    public Future<Void> send(BiFunction<URI, HttpMethod, HttpRequest> requestCreator, Function<HttpResponse, Boolean> responseValidator, int currentRetries) {
        HttpRequest request = requestCreator.apply(this.uri, this.method);

        final CompletableFuture<HttpResponse<Void>> asyncResponse = this.httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding());

        return Future.fromCompletionStage(asyncResponse, vertx.getOrCreateContext()).compose(response -> {
            Boolean responseOK = responseValidator.apply(response);
            if (responseOK == null) {
                return Future.failedFuture(new RuntimeException("Response validator returned null"));
            }

            if (responseOK) {
                return Future.succeededFuture();
            }

            if (currentRetries < this.retryCount) {
                LOGGER.error("failed sending to " + uri + ", currentRetries: " + currentRetries + ", backing off before retrying");
                if (this.retryBackoffMs > 0) {
                    return vertx.timer(this.retryBackoffMs)
                            .compose(v -> send(requestCreator, responseValidator, currentRetries + 1));
                } else {
                    return send(requestCreator, responseValidator, currentRetries + 1);
                }
            }

            LOGGER.error("retry count exceeded for sending to " + this.uri);
            return Future.failedFuture(new TooManyRetriesException(currentRetries));
        });
    }
}
