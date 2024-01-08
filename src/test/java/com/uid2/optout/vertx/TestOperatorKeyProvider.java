package com.uid2.optout.vertx;

import com.uid2.shared.auth.IAuthorizable;
import com.uid2.shared.auth.OperatorKey;
import com.uid2.shared.auth.Role;
import com.uid2.shared.store.IOperatorKeyProvider;

import java.util.*;

public class TestOperatorKeyProvider implements IOperatorKeyProvider {
    public static final OperatorKey TEST_OPERATOR_KEY;
    public static final OperatorKey TEST_OPTOUT_KEY;
    private static final Map<String, OperatorKey> ALL_KEYS;

    static {
        TEST_OPERATOR_KEY = new OperatorKey("test-operator-keyhash", "test-operator-keysalt", "test_operator", "test_operator", "trusted", 0,
                false, 5, Set.of(Role.OPERATOR), "test-operator-key-id");
        TEST_OPTOUT_KEY = new OperatorKey("test-optout-operator-keyhash", "test-optout-operator-keysalt", "test_optout_operator", "test_optout_operator", "trusted", 0,
                false, 5, Set.of(Role.OPERATOR, Role.OPTOUT), "test-operator-key-id");

        ALL_KEYS = Map.of(
                "test-operator-key", TEST_OPERATOR_KEY,
                "test-optout-operator-key", TEST_OPTOUT_KEY
        );
    }

    @Override
    public OperatorKey getOperatorKey(String token) {
        return ALL_KEYS.entrySet().stream()
                .filter(entry -> entry.getKey().equals(token))
                .map(Map.Entry::getValue)
                .findAny()
                .orElse(TEST_OPERATOR_KEY);
    }

    @Override
    public OperatorKey getOperatorKeyFromHash(String hash) {
        return ALL_KEYS.entrySet().stream()
                .filter(entry -> entry.getValue().getKeyHash().equals(hash))
                .map(Map.Entry::getValue)
                .findAny()
                .orElse(TEST_OPERATOR_KEY);
    }

    @Override
    public Collection<OperatorKey> getAll() {
        return ALL_KEYS.values();
    }

    @Override
    public IAuthorizable get(String key) {
        return getOperatorKey(key);
    }
}
