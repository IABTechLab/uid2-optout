package com.uid2.optout.vertx;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum Endpoints {
    OPS_HEALTHCHECK("/ops/healthcheck"),
    OPTOUT_REFRESH("/optout/refresh"),
    OPTOUT_WRITE("/optout/write"),
    OPTOUT_REPLICATE("/optout/replicate"),
    OPTOUT_QUEUE("/optout/queue"),
    OPTOUT_PARTNER_MOCK("/optout/partner_mock");
    private final String path;

    Endpoints(final String path) {
        this.path = path;
    }

    public static Set<String> pathSet() {
        return Stream.of(Endpoints.values()).map(Endpoints::toString).collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return path;
    }
}
