/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.auth.authorization.strategy;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.authorization.provider.DefaultAuthorizationProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.action.Action;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression test for apache/rocketmq#11182: a whitelisted request must not leak its ALLOW
 * into the {@link StatefulAuthorizationStrategy} cache for a non-whitelisted request that
 * shares the same channel/subject/resource/actions/sourceIp.
 *
 * <p>The strategy now (1) short-circuits whitelisted rpcCodes before consulting the cache and
 * (2) includes {@code rpcCode} in the cache key, so a non-whitelisted request always runs its
 * ACL check.
 */
public class StatefulAuthorizationWhitelistCacheTest {

    private static final String WHITELISTED_RPC = "whitelisted-rpc";
    private static final String NON_WHITELISTED_RPC = "non-whitelisted-rpc";

    private final AuthorizationEvaluator evaluator = buildEvaluator();

    @Test
    public void nonWhitelistedRpcWithSameCacheKeyIsDenied() {
        // 1. A whitelisted request is allowed and must NOT populate the cache.
        evaluator.evaluate(Collections.singletonList(context(WHITELISTED_RPC, "t1")));

        // 2. Control: a non-whitelisted request with a DIFFERENT resource is denied.
        Assert.assertThrows(AuthorizationException.class,
            () -> evaluator.evaluate(Collections.singletonList(context(NON_WHITELISTED_RPC, "t2"))));

        // 3. A non-whitelisted request with the SAME resource as step 1 is also denied: it must
        //    not reuse a cached ALLOW from the whitelisted request (regression for #11182).
        Assert.assertThrows(AuthorizationException.class,
            () -> evaluator.evaluate(Collections.singletonList(context(NON_WHITELISTED_RPC, "t1"))));
    }

    private static DefaultAuthorizationContext context(String rpcCode, String topic) {
        DefaultAuthorizationContext context = DefaultAuthorizationContext.of(
            User.of("alice"), Resource.ofTopic(topic), Action.SUB, "127.0.0.1");
        context.setChannelId("channel-1");
        context.setRpcCode(rpcCode);
        return context;
    }

    private static AuthorizationEvaluator buildEvaluator() {
        AuthConfig config = new AuthConfig();
        config.setConfigName("whitelist-cache-test-" + System.nanoTime());
        config.setAuthorizationEnabled(true);
        config.setAuthorizationProvider(AlwaysDenyAuthorizationProvider.class.getName());
        config.setAuthorizationStrategy(StatefulAuthorizationStrategy.class.getName());
        config.setAuthorizationWhitelist(WHITELISTED_RPC);
        config.setStatefulAuthorizationCacheExpiredSecond(60);
        config.setStatefulAuthorizationCacheMaxNum(100);
        return AuthorizationFactory.getEvaluator(config);
    }

    /**
     * A provider whose ACL evaluation always denies. It is never consulted for whitelisted
     * requests (the whitelist short-circuits in {@code doEvaluate}), so any request that reaches
     * this provider represents a genuine ACL check.
     */
    public static class AlwaysDenyAuthorizationProvider extends DefaultAuthorizationProvider {
        @Override
        public CompletableFuture<Void> authorize(DefaultAuthorizationContext context) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new AuthorizationException("denied by test provider"));
            return future;
        }
    }
}
