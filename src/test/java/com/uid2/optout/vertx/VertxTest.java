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

package com.uid2.optout.vertx;

import com.uid2.shared.optout.OptOutUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@RunWith(VertxUnitRunner.class)
public class VertxTest {
    Vertx vertx;

    @Before
    public void setUp(TestContext ctx) {
        this.vertx = Vertx.vertx();
    }

    @After
    public void tearDown(TestContext ctx) {
        this.vertx.close();
    }

    @Test
    public void compositeFuture_tests(TestContext ctx) {
        Async async = ctx.async();

        Promise<Integer> p1 = Promise.promise();
        Future<Integer> f1 = p1.future();

        Promise<Integer> p2 = Promise.promise();
        Future<Integer> f2 = p2.future();

        CompositeFuture.all(f1, f2).onSuccess(cf -> {
            System.out.println("f1: " + cf.resultAt(0));
            System.out.println("f2: " + cf.resultAt(1));
            async.complete();
        });

        p1.complete(1);
        p2.complete(2);
    }

    @Test
    public void futureComposition_tests(TestContext ctx) {
        Future failedFuture = Future.failedFuture(new Exception("failed"));
        Future nextFuture = failedFuture.onSuccess(v -> ctx.fail("failed future should not call onSuccess"));
        // we should be able to fail the nextFuture as well
        nextFuture.onComplete(ctx.asyncAssertFailure());
    }

    @Test
    public void readTimestampFromFile_tests(TestContext ctx) throws IOException {
        Path file1 = Files.createTempFile("getTimestampFromFile_tests", ".txt");
        OptOutUtils.readTimestampFromFile(vertx, file1, 100).onComplete(ctx.asyncAssertSuccess(val -> {
            ctx.assertEquals(100L, val);

            // read the same file again, leads to 100, not 200 (new default)
            OptOutUtils.readTimestampFromFile(vertx, file1, 200).onComplete(ctx.asyncAssertSuccess(val2 -> {
                ctx.assertEquals(100L, val2);
            }));
        }));
    }

    @Test
    public void writeTimestampToFile_tests(TestContext ctx) throws IOException {
        Path file1 = Files.createTempFile("writeTimestampToFile_tests", ".txt");
        OptOutUtils.readTimestampFromFile(vertx, file1, 100).onComplete(ctx.asyncAssertSuccess(val -> {
            ctx.assertEquals(100L, val);

            // write timestamp, verify success
            OptOutUtils.writeTimestampToFile(vertx, file1, 1000).onComplete(ctx.asyncAssertSuccess(v -> {
                // read the same file again, result should be 1000, not 200 (new default), not 100 (old default)
                OptOutUtils.readTimestampFromFile(vertx, file1, 200).onComplete(ctx.asyncAssertSuccess(val2 -> {
                    ctx.assertEquals(1000L, val2);
                }));
            }));
        }));
    }

    @Test
    public void appendAndReadFileAsLines_tests(TestContext ctx) throws IOException {
        Path file1 = Files.createTempFile("appendAndReadFileAsLines", ".txt");
        OptOutUtils.appendLinesToFile(vertx, file1, OptOutUtils.toList("aaa", "bbb", "ccc")).onComplete(ctx.asyncAssertSuccess(v -> {
            OptOutUtils.readLinesFromFile(vertx, file1).onComplete(ctx.asyncAssertSuccess(lines -> {
                ctx.assertEquals(3, lines.length);
                ctx.assertEquals("aaa", lines[0]);
                ctx.assertEquals("bbb", lines[1]);
                ctx.assertEquals("ccc", lines[2]);
            }));
        }));

        Path file2 = Files.createTempFile("appendAndReadFileAsLines", ".txt");
        OptOutUtils.appendLinesToFile(vertx, file2, OptOutUtils.toList("aaa", "bbb", "ccc")).onComplete(ctx.asyncAssertSuccess(v -> {
            OptOutUtils.appendLinesToFile(vertx, file2, OptOutUtils.toList("ddd", "eee", "fff")).onComplete(ctx.asyncAssertSuccess(v2 -> {
                OptOutUtils.readLinesFromFile(vertx, file2).onComplete(ctx.asyncAssertSuccess(lines -> {
                    ctx.assertEquals(6, lines.length);
                    ctx.assertEquals("aaa", lines[0]);
                    ctx.assertEquals("bbb", lines[1]);
                    ctx.assertEquals("ccc", lines[2]);
                    ctx.assertEquals("ddd", lines[3]);
                    ctx.assertEquals("eee", lines[4]);
                    ctx.assertEquals("fff", lines[5]);
                }));
            }));
        }));

        // use a definitely non-existing file
        Path file3 = Paths.get(file1.toString() + "_nonexisting.txt");
        OptOutUtils.appendLinesToFile(vertx, file3, OptOutUtils.toList("aaa", "bbb", "ccc")).onComplete(ctx.asyncAssertSuccess(v -> {
            OptOutUtils.readLinesFromFile(vertx, file3).onComplete(ctx.asyncAssertSuccess(lines -> {
                ctx.assertEquals(3, lines.length);
                ctx.assertEquals("aaa", lines[0]);
                ctx.assertEquals("bbb", lines[1]);
                ctx.assertEquals("ccc", lines[2]);
            }));
        }));
    }
}
