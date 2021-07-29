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
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Objects;

public class PartnersConfig {
    private static ObjectMapper mapper = new ObjectMapper();

    @JsonProperty("version")
    private long version;

    @JsonProperty("generated")
    private long generated;

    @JsonProperty("partner_endpoints")
    private List<EndpointConfig> remoteEndpoints = new ArrayList<>();

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof PartnersConfig)) {
            return false;
        }
        PartnersConfig m = (PartnersConfig) o;
        return Objects.equals(version, m.version)
            && Objects.equals(generated, m.generated)
            && remoteEndpoints.equals(m.remoteEndpoints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.version, this.generated, this.remoteEndpoints);
    }

    public static PartnersConfig fromJsonObject(JsonObject obj) throws JsonProcessingException, InvalidPropertiesFormatException {
        return PartnersConfig.fromJsonString(obj.toString());
    }

    public static PartnersConfig fromJsonString(String str) throws JsonProcessingException, InvalidPropertiesFormatException {
        return PartnersConfig.mapper.readValue(str, PartnersConfig.class);
    }

    public long version() {
        return this.version;
    }

    public long generated() {
        return this.generated;
    }

    public List<EndpointConfig> remoteEndpoints() {
        return this.remoteEndpoints;
    }
}
