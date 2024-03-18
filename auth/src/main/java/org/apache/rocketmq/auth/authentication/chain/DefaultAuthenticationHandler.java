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
package org.apache.rocketmq.auth.authentication.chain;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclSigner;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.enums.UserStatus;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.chain.Handler;
import org.apache.rocketmq.common.chain.HandlerChain;

public class DefaultAuthenticationHandler implements Handler<DefaultAuthenticationContext, CompletableFuture<Void>> {

    private final AuthenticationMetadataProvider authenticationMetadataProvider;

    public DefaultAuthenticationHandler(AuthConfig config, Supplier<?> metadataService) {
        this.authenticationMetadataProvider = AuthenticationFactory.getMetadataProvider(config, metadataService);
    }

    @Override
    public CompletableFuture<Void> handle(DefaultAuthenticationContext context,
        HandlerChain<DefaultAuthenticationContext, CompletableFuture<Void>> chain) {
        return getUser(context).thenAccept(user -> doAuthenticate(context, user));
    }

    protected CompletableFuture<User> getUser(DefaultAuthenticationContext context) {
        if (StringUtils.isEmpty(context.getUsername())) {
            throw new AuthenticationException("username cannot be null.");
        }
        return this.authenticationMetadataProvider.getUser(context.getUsername());
    }

    protected void doAuthenticate(DefaultAuthenticationContext context, User user) {
        if (user == null) {
            throw new AuthenticationException("User:{} is not found.", context.getUsername());
        }
        if (user.getUserStatus() == UserStatus.DISABLE) {
            throw new AuthenticationException("User:{} is disabled.", context.getUsername());
        }
        String signature = AclSigner.calSignature(context.getContent(), user.getPassword());
        if (!StringUtils.equals(signature, context.getSignature())) {
            throw new AuthenticationException("check signature failed.");
        }
    }
}
