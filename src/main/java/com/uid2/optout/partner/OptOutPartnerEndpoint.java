package com.uid2.optout.partner;

import com.uid2.optout.web.RetryingWebClient;
import com.uid2.optout.web.UnexpectedStatusCodeException;
import com.uid2.shared.Utils;
import com.uid2.shared.optout.OptOutEntry;
import com.uid2.shared.optout.OptOutUtils;
import io.netty.handler.codec.http.HttpMethod;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Set;
import java.util.regex.Pattern;

public class OptOutPartnerEndpoint implements IOptOutPartnerEndpoint {
    public static final String VALUEREF_ADVERTISING_ID = "${ADVERTISING_ID}";
    public static final String VALUEREF_OPTOUT_EPOCH = "${OPTOUT_EPOCH}";
    public static final String QUOTEDVREF_ADVERTISING_ID = Pattern.quote(OptOutPartnerEndpoint.VALUEREF_ADVERTISING_ID);
    public static final String QUOTEDVEF_OPTOUT_EPOCH = Pattern.quote(OptOutPartnerEndpoint.VALUEREF_OPTOUT_EPOCH);

    private static final Set<Integer> SUCCESS_STATUS_CODES = Set.of(200, 204);
    private static final Set<Integer> RETRYABLE_STATUS_CODES = Set.of(429, 500, 502, 503, 504);
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
            (URI uri, HttpMethod method) -> {
                URIBuilder uriBuilder = new URIBuilder(uri);

                for (String queryParam : config.queryParams()) {
                    int indexOfEqualSign = queryParam.indexOf('=');
                    String paramName = queryParam.substring(0, indexOfEqualSign);
                    String paramValue = queryParam.substring(indexOfEqualSign + 1);
                    String replacedValue = replaceValueReferences(entry, paramValue);

                    uriBuilder.addParameter(paramName, replacedValue);
                }

                URI uriWithParams;
                try {
                    uriWithParams = uriBuilder.build();
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }

                HttpRequest.Builder builder = HttpRequest.newBuilder()
                        .uri(uriWithParams)
                        .method(method.toString(), HttpRequest.BodyPublishers.noBody());

                for (String additionalHeader : this.config.additionalHeaders()) {
                    int indexOfColonSign = additionalHeader.indexOf(':');
                    String headerName = additionalHeader.substring(0, indexOfColonSign);
                    String headerValue = additionalHeader.substring(indexOfColonSign + 1);
                    String replacedValue = replaceValueReferences(entry, headerValue);
                    builder.header(headerName, replacedValue);
                }

                LOGGER.info("replaying optout " + config.url() + " - advertising_id: " + Utils.maskPii(entry.advertisingId) + ", epoch: " + entry.timestamp);

                return builder.timeout(Duration.ofSeconds(30)).build();
            },
            resp -> {
                if (resp == null) {
                    throw new RuntimeException("response is null");
                }

                if (SUCCESS_STATUS_CODES.contains(resp.statusCode())) {
                    return true;
                }

                LOGGER.info("received non-200 response: " + resp.statusCode() + "-" + resp.body() + " for optout " + config.url() + " - advertising_id: " + Utils.maskPii(entry.advertisingId) + ", epoch: " + entry.timestamp);
                if (RETRYABLE_STATUS_CODES.contains(resp.statusCode())) {
                    return false;
                } else {
                    throw new UnexpectedStatusCodeException(resp.statusCode());
                }
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
