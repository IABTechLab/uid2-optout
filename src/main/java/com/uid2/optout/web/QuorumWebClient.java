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

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class QuorumWebClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumWebClient.class);

    final URI[] uris;
    final WebClient[] clients;

    public QuorumWebClient(Vertx vertx, String[] uris) {
        this.uris = new URI[uris.length];
        this.clients = new WebClient[uris.length];
        for (int i = 0; i < uris.length; ++i) {
            LOGGER.info("creating QuorumWebClient " + i + " " + uris[i].toString());
            this.uris[i] = URI.create(uris[i]);
            this.clients[i] = WebClient.create(vertx);
        }
    }

    public Future<HttpResponse<Buffer>[]> get(Function<HttpRequest<Buffer>, HttpRequest<Buffer>> requestCreator) {
        Promise<HttpResponse<Buffer>[]> promise = Promise.promise();
        HttpResponse<Buffer>[] resps = new HttpResponse[this.uris.length];
        AtomicInteger succeeded = new AtomicInteger(0);
        AtomicInteger completed = new AtomicInteger(0);
        for (int i = 0; i < this.uris.length; ++i) {
            final int iterations = i;
            final URI uri = this.uris[i];
            HttpRequest<Buffer> req = this.clients[i].requestAbs(HttpMethod.GET, uri.toString());
            requestCreator.apply(req).send(ar -> {
                final int quorum = this.quorumThreshold();
                if (ar.succeeded()) {
                    HttpResponse<Buffer> resp = ar.result();
                    resps[iterations] = resp;

                    if (resp.statusCode() != 200) {
                        LOGGER.error("remote optout/write request " + uri + " returned " + resp.statusCode());
                    } else if (succeeded.incrementAndGet() == quorum) {
                        promise.complete(resps);
                        return;
                    }
                } else {
                    LOGGER.error("Failed sending request to " + uri, ar.cause());
                }

                if (completed.incrementAndGet() == this.uris.length) {
                    // TODO construct aggregate errors
                    promise.fail(new Throwable("Failed on quorum"));
                }
            });
        }
        return promise.future();
    }

    private int quorumThreshold() {
        //// Uncomment this to use simple majority quorum
        // return this.uris.length / 2 + 1;

        // As of now, quorum is simply 1 replica returns 200
        return 1;
    }
}
