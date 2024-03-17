package com.uid2.optout.web;

import io.netty.handler.codec.http.HttpMethod;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@RunWith(VertxUnitRunner.class)
public class RetryingWebClientTest {

    private Vertx vertx;

    @Before
    public void setup(TestContext ctx) {
        vertx = Vertx.vertx();
        Random rand = new Random();

        vertx.createHttpServer()
            .requestHandler(req -> {
                try {
                    String subPath = req.path().substring(1);
                    if (subPath.startsWith("random")) {
                        // random/500_404_200
                        String[] statusCodes = subPath.split("/")[1].split("_");
                        // pick a random code and respond with it
                        int statusCode = Integer.valueOf(statusCodes[rand.nextInt(statusCodes.length)]);
                        req.response().setStatusCode(statusCode).end();
                    } else if (subPath.startsWith("delayed")) {
                        vertx.setTimer(1000, id -> {
                            try {
                                req.response().setStatusCode(200).end();
                            }
                            catch (Exception ex) {}
                        });
                    } else {
                        int statusCode = Integer.valueOf(subPath);
                        req.response().setStatusCode(statusCode).end();
                    }

                } catch (Exception ex) {
                    req.response().setStatusCode(500).end();
                }
            })
            .listen(18082, ctx.asyncAssertSuccess());
    }

    @After
    public void tearDown(TestContext ctx) {
        vertx.close(ctx.asyncAssertSuccess());
    }

    @Test
    public void get_expectSuccess(TestContext ctx) {
        expectSuccess(ctx, HttpMethod.POST);
    }

    @Test
    public void post_expectSuccess(TestContext ctx) {
        expectSuccess(ctx, HttpMethod.POST);
    }

    private void expectSuccess(TestContext ctx, HttpMethod testMethod) {
        RetryingWebClient c = new RetryingWebClient(vertx, "http://localhost:18082/200", testMethod, 0, 0);
        c.send((URI uri, HttpMethod method) -> {
            return HttpRequest.newBuilder().uri(uri).method(method.toString(), HttpRequest.BodyPublishers.noBody()).build();
        }, resp -> {
            ctx.assertEquals(200, resp.statusCode());
            return 200 == resp.statusCode();
        }).onComplete(ctx.asyncAssertSuccess());
    }

    @Test
    public void get_expectRetryFailure_zeroBackoff(TestContext ctx) {
        expectRetryFailure_zeroBackoff(ctx, HttpMethod.GET);
    }

    @Test
    public void post_expectRetryFailure_zeroBackoff(TestContext ctx) {
        expectRetryFailure_zeroBackoff(ctx, HttpMethod.POST);
    }

    private void expectRetryFailure_zeroBackoff(TestContext ctx, HttpMethod testMethod) {
        AtomicInteger totalAttempts = new AtomicInteger(0);
        RetryingWebClient c = new RetryingWebClient(vertx, "http://localhost:18082/404", testMethod, 3, 0);
        c.send((URI uri, HttpMethod method) -> {
            return HttpRequest.newBuilder().uri(uri).method(method.toString(), HttpRequest.BodyPublishers.noBody()).build();
        }, resp -> {
            totalAttempts.incrementAndGet();
            ctx.assertEquals(404, resp.statusCode());
            // returning false for retry
            return false;
        }).onComplete(ctx.asyncAssertFailure(v -> ctx.assertTrue(v instanceof TooManyRetriesException)));
    }

    @Test
    public void get_expectRetryFailure_withBackoff(TestContext ctx) {
        expectRetryFailure_withBackoff(ctx, HttpMethod.GET);
    }

    @Test
    public void post_expectRetryFailure_withBackoff(TestContext ctx) {
        expectRetryFailure_withBackoff(ctx, HttpMethod.POST);
    }

    private void expectRetryFailure_withBackoff(TestContext ctx, HttpMethod testMethod) {
        AtomicInteger totalAttempts = new AtomicInteger(0);
        RetryingWebClient c = new RetryingWebClient(vertx, "http://localhost:18082/404", testMethod, 3, 1);
        c.send((URI uri, HttpMethod method) -> {
            return HttpRequest.newBuilder().uri(uri).method(method.toString(), HttpRequest.BodyPublishers.noBody()).build();
        }, resp -> {
            totalAttempts.incrementAndGet();
            ctx.assertEquals(404, resp.statusCode());
            // returning false for retry
            return false;
        }).onComplete(ctx.asyncAssertFailure(v -> {
            ctx.assertEquals(4, (int) totalAttempts.get());
            ctx.assertTrue(v instanceof TooManyRetriesException);
        }));
    }

    @Test
    public void get_expectSuccess_withRandomFailures(TestContext ctx) {
        expectSuccess_withRandomFailures(ctx, HttpMethod.GET);
    }

    @Test
    public void post_expectSuccess_withRandomFailures(TestContext ctx) {
        expectSuccess_withRandomFailures(ctx, HttpMethod.POST);
    }

    private void expectSuccess_withRandomFailures(TestContext ctx, HttpMethod testMethod) {
        for (int i = 0; i < 10; ++i) {
            AtomicInteger totalAttempts = new AtomicInteger(0);
            RetryingWebClient c = new RetryingWebClient(vertx, "http://localhost:18082/random/500_500_500_200",
                testMethod, 100, 1);
            c.send((URI uri, HttpMethod method) -> {
                return HttpRequest.newBuilder().uri(uri).method(method.toString(), HttpRequest.BodyPublishers.noBody()).build();
            }, resp -> {
                totalAttempts.incrementAndGet();
                return resp.statusCode() == 200;
            }).onComplete(ctx.asyncAssertSuccess(v -> {
                ctx.assertTrue(totalAttempts.get() >= 1);
                ctx.assertTrue(totalAttempts.get() <= 101);
            }));
        }
    }

    @Test
    public void get_expectImmediateFailure_withNonRetryErrors(TestContext ctx) {
        expectImmediateFailure_withNonRetryErrors(ctx, HttpMethod.GET);
    }

    @Test
    public void post_expectImmediateFailure_withNonRetryErrors(TestContext ctx) {
        expectImmediateFailure_withNonRetryErrors(ctx, HttpMethod.POST);
    }

    private void expectImmediateFailure_withNonRetryErrors(TestContext ctx, HttpMethod testMethod) {
        for (int i = 0; i < 10; ++i) {
            AtomicInteger totalAttempts = new AtomicInteger(0);
            RetryingWebClient c = new RetryingWebClient(vertx, "http://localhost:18082/404", testMethod, 100, 1);
            c.send((URI uri, HttpMethod method) -> {
                return HttpRequest.newBuilder().uri(uri).method(method.toString(), HttpRequest.BodyPublishers.noBody()).build();
            }, resp -> {
                totalAttempts.incrementAndGet();
                if (resp.statusCode() == 200) return true;
                else if (resp.statusCode() == 500) return false;
                else throw new UnexpectedStatusCodeException(resp.statusCode());
            }).onComplete(ctx.asyncAssertFailure(v -> {
                // check that it only attempted once and failed
                ctx.assertEquals(1, totalAttempts.get());
                ctx.assertTrue(v instanceof UnexpectedStatusCodeException);
            }));
        }
    }

    public Function<HttpResponse, Boolean> assertStatusCodeFactory(TestContext ctx, int code) {
        return result -> {
            ctx.assertEquals(code, result.statusCode());
            return code == result.statusCode();
        };
    }
    public Handler<AsyncResult<Void>> ensureAsyncExceptionFactory(TestContext ctx, Class<? extends Exception> exceptionClass) {
         return ctx.asyncAssertFailure(cause -> {
            ctx.assertTrue(cause.getClass() == exceptionClass, "Expected a " + exceptionClass.toString() + " but got a " + cause);
        });
    }

    @Test
    public void longRequest_longerTimeout_expectSuccess(TestContext ctx) {
        testDelayedResponse(ctx, assertStatusCodeFactory(ctx, 200), 1500)
                .onComplete(ctx.asyncAssertSuccess());
    }

    @Test
    public void longRequest_shorterTimeout_expectFailure(TestContext ctx) {
        testDelayedResponse(ctx, req -> true, 500)
                .onComplete(ensureAsyncExceptionFactory(ctx, TimeoutException.class));
    }

    private Future<Void> testDelayedResponse(TestContext ctx, Function<HttpResponse, Boolean> assertion, int resultTimeoutMs) {
        Async async = ctx.async();

        RetryingWebClient c = new RetryingWebClient(vertx, "http://localhost:18082/delayed", HttpMethod.GET, 0, 0, resultTimeoutMs);
        return c.send((URI uri, HttpMethod method) -> HttpRequest.newBuilder().uri(uri).method(method.toString(), HttpRequest.BodyPublishers.noBody()).build(), assertion)
                .andThen(r -> async.complete());
    }
}
