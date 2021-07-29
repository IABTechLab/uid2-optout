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

import com.uid2.optout.web.RetryingWebClient;
import com.uid2.shared.Utils;
import com.uid2.shared.optout.OptOutEntry;
import com.uid2.shared.optout.OptOutUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.regex.Pattern;

public class OptOutPartnerEndpoint implements IOptOutPartnerEndpoint {
    public static final String VALUEREF_ADVERTISING_ID = "${ADVERTISING_ID}";
    public static final String VALUEREF_OPTOUT_EPOCH = "${OPTOUT_EPOCH}";
    public static final String QUOTEDVREF_ADVERTISING_ID = Pattern.quote(OptOutPartnerEndpoint.VALUEREF_ADVERTISING_ID);
    public static final String QUOTEDVEF_OPTOUT_EPOCH = Pattern.quote(OptOutPartnerEndpoint.VALUEREF_OPTOUT_EPOCH);
    private static final Logger LOGGER = LoggerFactory.getLogger(OptOutPartnerEndpoint.class);
    private final EndpointConfig config;
    private final RetryingWebClient retryingClient;

    public OptOutPartnerEndpoint(Vertx vertx, EndpointConfig config) {
        this.config = config;
        this.retryingClient = new RetryingWebClient(vertx, config.url(), config.method(), config.retryCount(), config.retryBackoffMs());
    }

    @Override
    public String name() {
        return this.config.name();
    }

    @Override
    public Future<Void> send(OptOutEntry entry) {
        return this.retryingClient.send(
            req -> {
                for (String queryParam : this.config.queryParams()) {
                    int indexOfEqualSign = queryParam.indexOf('=');
                    String paramName = queryParam.substring(0, indexOfEqualSign);
                    String paramValue = queryParam.substring(indexOfEqualSign + 1);
                    String replacedValue = replaceValueReferences(entry, paramValue);
                    req.setQueryParam(paramName, replacedValue);
                }

                for (String additionalHeader : this.config.additionalHeaders()) {
                    int indexOfColonSign = additionalHeader.indexOf(':');
                    String headerName = additionalHeader.substring(0, indexOfColonSign);
                    String headerValue = additionalHeader.substring(indexOfColonSign + 1);
                    String replacedValue = replaceValueReferences(entry, headerValue);
                    req.headers().add(headerName, replacedValue);
                }

                LOGGER.info("replaying optout " + config.url() + " - advertising_id: " + Utils.maskPii(entry.advertisingId) + ", epoch: " + String.valueOf(entry.timestamp));

                return req;
            },
            resp -> {
                // returning tri-state boolean
                // - TRUE: result looks good
                // - FALSE: retry-able error code returned
                // - NULL: failed and should not retry
                if (resp == null) return false;
                else if (resp.statusCode() == 200) return true;
                else if (resp.statusCode() == 500) return false;
                else return null;
            }
        );
    }

    private String replaceValueReferences(OptOutEntry entry, String val) {
        if (val.contains(OptOutPartnerEndpoint.VALUEREF_ADVERTISING_ID)) {
            val = val.replaceAll(OptOutPartnerEndpoint.QUOTEDVREF_ADVERTISING_ID, OptOutUtils.byteArrayToBase64String(entry.advertisingId));
        }
        if (val.contains(OptOutPartnerEndpoint.VALUEREF_OPTOUT_EPOCH)) {
            val = val.replaceAll(OptOutPartnerEndpoint.QUOTEDVEF_OPTOUT_EPOCH, String.valueOf(entry.timestamp));
        }
        return val;
    }
}
