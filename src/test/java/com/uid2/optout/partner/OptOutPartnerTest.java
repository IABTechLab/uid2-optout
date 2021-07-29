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

package com.uid2.optout.partner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.uid2.shared.optout.OptOutEntry;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Instant;
import java.util.InvalidPropertiesFormatException;

@RunWith(VertxUnitRunner.class)
public class OptOutPartnerTest {

    private static final String PARAM_UID2 = "uid2";
    private static final String PARAM_TIMESTAMP = "timestamp";

    private Vertx vertx;

    @Before
    public void setup(TestContext ctx) {
        vertx = Vertx.vertx();
    }

    @After
    public void tearDown(TestContext ctx) {
        vertx.close(ctx.asyncAssertSuccess());
    }

    @Test
    public void internalSite_expectSuccess(TestContext ctx) throws JsonProcessingException, InvalidPropertiesFormatException {
        String partnerConfigStr = "{" +
            "      \"name\": \"ttd\",\n" +
            "      \"url\": \"http://localhost:18083/uid2optout\",\n" +
            "      \"method\": \"GET\",\n" +
            "      \"query_params\": [\n" +
            "        \"uid2=${ADVERTISING_ID}\",\n" +
            "        \"timestamp=${OPTOUT_EPOCH}\"\n" +
            "      ],\n" +
            "      \"additional_headers\": [\n" +
            "        \"Authorization: Bearer 111-1111111\"\n" +
            "      ],\n" +
            "      \"retry_count\": 600,\n" +
            "      \"retry_backoff_ms\": 6000" +
            "}";
        testConfig_expectSuccess(ctx, partnerConfigStr, true);
    }

    @Test
    public void externalHttpSite_expectSuccess(TestContext ctx) throws JsonProcessingException, InvalidPropertiesFormatException {
        testSite_expectSuccess(ctx, "http://httpstat.us/200");
    }

    @Test
    public void externalHttpsSite_expectSuccess(TestContext ctx) throws JsonProcessingException, InvalidPropertiesFormatException {
        testSite_expectSuccess(ctx, "https://httpstat.us/200");
    }

    private void testSite_expectSuccess(TestContext ctx, String site) throws JsonProcessingException, InvalidPropertiesFormatException {
        String partnerConfigStr = "{" +
            "      \"name\": \"ttd\",\n" +
            "      \"url\": \"" + site + "\",\n" +
            "      \"method\": \"GET\",\n" +
            "      \"query_params\": [\n" +
            "        \"uid2=${ADVERTISING_ID}\",\n" +
            "        \"timestamp=${OPTOUT_EPOCH}\"\n" +
            "      ],\n" +
            "      \"additional_headers\": [\n" +
            "        \"Authorization: Bearer 111-1111111\"\n" +
            "      ],\n" +
            "      \"retry_count\": 600,\n" +
            "      \"retry_backoff_ms\": 6000" +
            "}";
        testConfig_expectSuccess(ctx, partnerConfigStr, false);
    }

    private void testConfig_expectSuccess(TestContext ctx, String partnerConfigStr, boolean createInternalTestServer) throws JsonProcessingException, InvalidPropertiesFormatException {

        EndpointConfig partnerConfig = EndpointConfig.fromJsonString(partnerConfigStr);

        byte[] idHash = OptOutEntry.idHashFromLong(4567);
        byte[] advertisingId = OptOutEntry.idHashFromLong(1234);
        long timestamp = Instant.now().getEpochSecond();
        OptOutEntry entry = new OptOutEntry(idHash, advertisingId, timestamp);

        if (createInternalTestServer) {
            Async async = ctx.async();
            HttpServer server = this.createTestServer(ctx, req -> {
                ctx.assertEquals("/uid2optout", req.path());

                String uid2Expected = OptOutEntry.idHashB64FromLong(1234);
                String uid2 = req.getParam(OptOutPartnerTest.PARAM_UID2);
                ctx.assertEquals(uid2Expected, uid2);

                String tsExpected = String.valueOf(timestamp);
                String ts = req.getParam(OptOutPartnerTest.PARAM_TIMESTAMP);
                ctx.assertEquals(tsExpected, ts);

                String hExpected = "Bearer 111-1111111";
                String h = req.getHeader("Authorization");
                ctx.assertEquals(hExpected, h);
                async.complete();
            });
        }

        OptOutPartnerEndpoint remote = new OptOutPartnerEndpoint(vertx, partnerConfig);
        remote.send(entry).onComplete(ctx.asyncAssertSuccess());
    }

    private HttpServer createTestServer(TestContext ctx, Handler<HttpServerRequest> requestValidator) {
        return vertx.createHttpServer()
            .requestHandler(req -> {
                requestValidator.handle(req);
                req.response().setStatusCode(200).end();
            })
            .listen(18083, ctx.asyncAssertSuccess());
    }
}
