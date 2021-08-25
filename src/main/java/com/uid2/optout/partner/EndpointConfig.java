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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uid2.shared.Const;
import com.uid2.shared.auth.ClientKey;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Objects;

public class EndpointConfig {
    private static ObjectMapper mapper = new ObjectMapper();

    @JsonProperty("name")
    private String name;

    @JsonProperty("url")
    private String url;

    @JsonProperty("method")
    private HttpMethod method;

    @JsonProperty("query_params")
    private List<String> queryParams = new ArrayList<>();

    @JsonProperty("additional_headers")
    private List<String> additionalHeaders = new ArrayList<>();

    @JsonProperty("retry_count")
    private int retryCount;

    @JsonProperty("retry_backoff_ms")
    private int retryBackoffMs;

    public static EndpointConfig fromJsonObject(JsonObject obj) throws JsonProcessingException, InvalidPropertiesFormatException {
        return EndpointConfig.fromJsonString(obj.toString());
    }

    public static EndpointConfig fromJsonString(String str) throws JsonProcessingException, InvalidPropertiesFormatException {
        EndpointConfig ret = EndpointConfig.mapper.readValue(str, EndpointConfig.class);

        // normalize name and URL, query_params and additional_headers are spared)
        ret.url = ret.url.toLowerCase();
        ret.name = ret.name.toLowerCase();

        // validate query_params, param should be in the format of key=val
        for (String queryParam : ret.queryParams) {
            if (queryParam.split("=").length != 2) {
                throw new InvalidPropertiesFormatException("bad query param format: " + queryParam);
            }
        }

        // validate additional_headers: header has to have ':' character in it
        for (String additionalHeader : ret.additionalHeaders) {
            if (additionalHeader.indexOf(':') == -1) {
                throw new InvalidPropertiesFormatException("bad http header format: " + additionalHeader);
            }
        }

        return ret;
    }

    public String name() {
        return this.name;
    }

    public String url() {
        return this.url;
    }

    public HttpMethod method() {
        return this.method;
    }

    public List<String> queryParams() {
        return this.queryParams;
    }

    public List<String> additionalHeaders() {
        return this.additionalHeaders;
    }

    public int retryCount() {
        return this.retryCount;
    }

    public int retryBackoffMs() {
        return this.retryBackoffMs;
    }

    public String toJsonString() throws JsonProcessingException {
        return EndpointConfig.mapper.writeValueAsString(this);
    }

    public static EndpointConfig valueOf(JsonObject json) {
        try {
            return EndpointConfig.fromJsonString(json.encode());
        } catch (Exception ex) {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof EndpointConfig)) {
            return false;
        }
        EndpointConfig m = (EndpointConfig) o;
        return Objects.equals(name, m.name)
            && Objects.equals(url, m.url)
            && Objects.equals(queryParams, m.queryParams)
            && Objects.equals(additionalHeaders, m.additionalHeaders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.url, this.queryParams, this.additionalHeaders);
    }
}
