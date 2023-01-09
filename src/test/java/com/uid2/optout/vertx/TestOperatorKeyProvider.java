package com.uid2.optout.vertx;

import com.uid2.shared.auth.IAuthorizable;
import com.uid2.shared.auth.OperatorKey;
import com.uid2.shared.auth.Role;
import com.uid2.shared.store.IOperatorKeyProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

public class TestOperatorKeyProvider implements IOperatorKeyProvider {
    public static final TestOperatorKeyProvider INSTANCE = new TestOperatorKeyProvider();
    private static final OperatorKey TEST_OPERATOR_KEY = new OperatorKey("test-operator-key", "test_operator", "test_operator", "trusted", 0,
        false, 5, new HashSet<>(Arrays.asList(Role.OPERATOR)));

    private static final OperatorKey TEST_OPTOUT_KEY = new OperatorKey("test-optout-operator-key", "test_optout_operator", "test_optout_operator", "trusted", 0,
            false, 5, new HashSet<>(Arrays.asList(Role.OPERATOR, Role.OPTOUT)));

    private static final Collection<OperatorKey> ALL_KEYS = new ArrayList<>();

    static {
        ALL_KEYS.add(TEST_OPERATOR_KEY);
        ALL_KEYS.add(TEST_OPTOUT_KEY);
    }

    @Override
    public OperatorKey getOperatorKey(String token) {
        return ALL_KEYS.stream().filter(t -> t.getKey().equals(token)).
                findAny().orElse(TEST_OPERATOR_KEY);
    }

    @Override
    public Collection<OperatorKey> getAll() {
        return ALL_KEYS;
    }

    @Override
    public IAuthorizable get(String key) {
        return getOperatorKey(key);
    }
}
