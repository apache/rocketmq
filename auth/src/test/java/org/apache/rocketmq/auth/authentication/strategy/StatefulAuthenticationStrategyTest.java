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
package org.apache.rocketmq.auth.authentication.strategy;

import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class StatefulAuthenticationStrategyTest {

    @Test
    public void authenticationWhitelistBypassIsNotCachedAsSuccess() {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setConfigName("stateful-whitelist-" + System.nanoTime());
        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthenticationWhitelist("PUBLIC_RPC");

        StatefulAuthenticationStrategy strategy = spy(
            new StatefulAuthenticationStrategy(authConfig, null));
        strategy.evaluate(context("PUBLIC_RPC"));

        doThrow(new AuthenticationException("signature is invalid"))
            .when(strategy).doEvaluate(any(AuthenticationContext.class));

        assertThatThrownBy(() -> strategy.evaluate(context("PROTECTED_RPC")))
            .isInstanceOf(AuthenticationException.class)
            .hasMessageContaining("signature is invalid");
    }

    private DefaultAuthenticationContext context(String rpcCode) {
        DefaultAuthenticationContext context = new DefaultAuthenticationContext();
        context.setChannelId("channel-id");
        context.setRpcCode(rpcCode);
        context.setUsername("claimed-super-user");
        return context;
    }
}
