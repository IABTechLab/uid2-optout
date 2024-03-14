package com.uid2.optout.web;

import io.netty.handler.codec.http.HttpMethod;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
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
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

        this.retryCount = retryCount;
        this.retryBackoffMs = retryBackoffMs;
    }

    public Future<Void> send(BiFunction<URI, HttpMethod, HttpRequest> requestCreator, Function<HttpResponse, Boolean> responseValidator) {
        return this.send(requestCreator, responseValidator, 0);
    }

    public Future<Void> send(BiFunction<URI, HttpMethod, HttpRequest> requestCreator, Function<HttpResponse, Boolean> responseValidator, int currentRetries) {
        Promise<Void> promise = Promise.promise();

        HttpRequest request = requestCreator.apply(this.uri, this.method);
        CompletableFuture<HttpResponse<String>> asyncResponse = this.httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());

        asyncResponse.thenAccept(response -> {
            try {
                Boolean responseOK = responseValidator.apply(response);
                if (responseOK == null) {
                    throw new RuntimeException("Response validator returned null");
                }

                if (responseOK) {
                    promise.complete();
                } else if (currentRetries < this.retryCount) {
                    LOGGER.error("failed sending to " + uri + ", currentRetries: " + currentRetries + ", backing off before retrying");
                    if (this.retryBackoffMs > 0) {
                        vertx.setTimer(this.retryBackoffMs, i -> {
                            send(requestCreator, responseValidator, currentRetries + 1)
                                    .onComplete(ar2 -> promise.handle(ar2));
                        });
                    } else {
                        send(requestCreator, responseValidator, currentRetries + 1)
                                .onComplete(ar2 -> promise.handle(ar2));
                    }
                } else {
                    LOGGER.error("retry count exceeded for sending to " + this.uri);
                    throw new TooManyRetriesException(currentRetries);
                }
            }
            catch (Throwable ex) {
                promise.fail(ex);
            }
        });

        asyncResponse.exceptionally(ex -> {
            promise.fail(ex);
            return null;
        });


        return promise.future();
    }
}
