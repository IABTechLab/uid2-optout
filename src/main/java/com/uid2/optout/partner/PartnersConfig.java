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
