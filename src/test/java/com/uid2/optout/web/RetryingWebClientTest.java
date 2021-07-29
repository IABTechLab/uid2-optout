// Copyright (c) 2021 The Trade Desk, Inc
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice,
//    this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

package com.uid2.optout.web;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

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

    private void expectSuccess(TestContext ctx, HttpMethod method) {
        RetryingWebClient c = new RetryingWebClient(vertx, "http://localhost:18082/200", method, 0, 0);
        c.send(req -> {
            return req;
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

    private void expectRetryFailure_zeroBackoff(TestContext ctx, HttpMethod method) {
        AtomicInteger totalAttempts = new AtomicInteger(0);
        RetryingWebClient c = new RetryingWebClient(vertx, "http://localhost:18082/404", method, 3, 0);
        c.send(req -> {
            return req;
        }, resp -> {
            totalAttempts.incrementAndGet();
            ctx.assertEquals(404, resp.statusCode());
            // returning false for retry
            return false;
        }).onComplete(ctx.asyncAssertFailure());
    }

    @Test
    public void get_expectRetryFailure_withBackoff(TestContext ctx) {
        expectRetryFailure_withBackoff(ctx, HttpMethod.GET);
    }

    @Test
    public void post_expectRetryFailure_withBackoff(TestContext ctx) {
        expectRetryFailure_withBackoff(ctx, HttpMethod.POST);
    }

    private void expectRetryFailure_withBackoff(TestContext ctx, HttpMethod method) {
        AtomicInteger totalAttempts = new AtomicInteger(0);
        RetryingWebClient c = new RetryingWebClient(vertx, "http://localhost:18082/404", method, 3, 1);
        c.send(req -> {
            return req;
        }, resp -> {
            totalAttempts.incrementAndGet();
            ctx.assertEquals(404, resp.statusCode());
            // returning false for retry
            return false;
        }).onComplete(ctx.asyncAssertFailure(v -> ctx.assertEquals(4, (int) totalAttempts.get())));
    }

    @Test
    public void get_expectSuccess_withRandomFailures(TestContext ctx) {
        expectSuccess_withRandomFailures(ctx, HttpMethod.GET);
    }

    @Test
    public void post_expectSuccess_withRandomFailures(TestContext ctx) {
        expectSuccess_withRandomFailures(ctx, HttpMethod.POST);
    }

    private void expectSuccess_withRandomFailures(TestContext ctx, HttpMethod method) {
        for (int i = 0; i < 10; ++i) {
            AtomicInteger totalAttempts = new AtomicInteger(0);
            RetryingWebClient c = new RetryingWebClient(vertx, "http://localhost:18082/random/500_500_500_200",
                method, 100, 1);
            c.send(req -> {
                return req;
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

    private void expectImmediateFailure_withNonRetryErrors(TestContext ctx, HttpMethod method) {
        for (int i = 0; i < 10; ++i) {
            AtomicInteger totalAttempts = new AtomicInteger(0);
            RetryingWebClient c = new RetryingWebClient(vertx, "http://localhost:18082/404", method, 100, 1);
            c.send(req -> {
                return req;
            }, resp -> {
                totalAttempts.incrementAndGet();
                if (resp.statusCode() == 200) return true;
                else if (resp.statusCode() == 500) return false;
                else return null;
            }).onComplete(ctx.asyncAssertFailure(v -> {
                // check that it only attempted once and failed
                ctx.assertEquals(1, totalAttempts.get());
            }));
        }
    }
}
