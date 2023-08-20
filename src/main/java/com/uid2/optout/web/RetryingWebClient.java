package com.uid2.optout.web;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

import java.net.URI;
import java.util.function.Function;

public class RetryingWebClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryingWebClient.class);
    private final URI uri;
    private final HttpMethod method;
    private final WebClient client;
    private final int retryCount;
    private final int retryBackoffMs;
    private Vertx vertx;

    public RetryingWebClient(Vertx vertx, String uri, HttpMethod method, int retryCount, int retryBackoffMs) {
        this.vertx = vertx;
        this.uri = URI.create(uri);
        this.method = method;

        WebClientOptions options = new WebClientOptions();

        // Disabling Temporary Measure to skip https validation
        // options.setVerifyHost(false);

        this.client = WebClient.create(vertx, options);

        this.retryCount = retryCount;
        this.retryBackoffMs = retryBackoffMs;
    }

    public Future<Void> send(Function<HttpRequest<Buffer>, HttpRequest<Buffer>> requestCreator, Function<HttpResponse<Buffer>, Boolean> responseValidator) {
        return this.send(requestCreator, responseValidator, 0);
    }

    public Future<Void> send(Function<HttpRequest<Buffer>, HttpRequest<Buffer>> requestCreator, Function<HttpResponse<Buffer>, Boolean> responseValidator, int currentRetries) {
        Promise<Void> promise = Promise.promise();
        HttpRequest<Buffer> req = this.client.requestAbs(method, this.uri.toString());
        requestCreator.apply(req).send(ar -> {
            try {
                if (ar.failed()) {
                    throw ar.cause();
                }

                Boolean responseOK = responseValidator.apply(ar.result());
                if (responseOK == null) {
                    throw new RuntimeException("Response validator returned null");
                }

                if (ar.succeeded() && responseOK) {
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
        return promise.future();
    }
}
