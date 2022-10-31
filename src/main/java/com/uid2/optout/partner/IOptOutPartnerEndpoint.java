package com.uid2.optout.partner;

import com.uid2.shared.optout.OptOutEntry;
import io.vertx.core.Future;

public interface IOptOutPartnerEndpoint {
    String name();

    Future<Void> send(OptOutEntry entry);
}
