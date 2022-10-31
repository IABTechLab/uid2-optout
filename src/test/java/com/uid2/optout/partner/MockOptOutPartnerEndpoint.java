package com.uid2.optout.partner;

import com.uid2.shared.optout.OptOutEntry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.function.Function;

public class MockOptOutPartnerEndpoint implements IOptOutPartnerEndpoint {
    public static final String NAME = "mock";
    private Function<OptOutEntry, Boolean> entryCallback = null;

    public MockOptOutPartnerEndpoint(Vertx vertx, Function<OptOutEntry, Boolean> entryCallback) {
        this.entryCallback = entryCallback;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Future<Void> send(OptOutEntry entry) {
        if (entryCallback.apply(entry))
            return Future.succeededFuture();
        else
            return Future.failedFuture("entryCallback returned false");
    }
}
