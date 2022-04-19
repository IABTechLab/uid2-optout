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
