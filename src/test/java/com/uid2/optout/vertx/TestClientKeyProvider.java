package com.uid2.optout.vertx;

import com.uid2.shared.Utils;
import com.uid2.shared.auth.ClientKey;
import com.uid2.shared.auth.IAuthorizable;
import com.uid2.shared.auth.Role;
import com.uid2.shared.store.IClientKeyProvider;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

public class TestClientKeyProvider implements IClientKeyProvider {
    public static final TestClientKeyProvider INSTANCE = new TestClientKeyProvider();
    private static final ClientKey TEST_OPERATOR_KEY = new ClientKey("test-operator-key", Utils.toBase64String("test-operator-secret".getBytes(StandardCharsets.UTF_8)))
        .withNameAndContact("test-operator")
        .withRoles(Role.OPERATOR);
    private static final Collection<ClientKey> ALL_KEYS = new ArrayList<>();

    static {
        ALL_KEYS.add(TEST_OPERATOR_KEY);
    }

    @Override
    public ClientKey getClientKey(String token) {
        return TEST_OPERATOR_KEY;
    }

    @Override
    public Collection<ClientKey> getAll() {
        return ALL_KEYS;
    }

    @Override
    public IAuthorizable get(String key) {
        return getClientKey(key);
    }
}
