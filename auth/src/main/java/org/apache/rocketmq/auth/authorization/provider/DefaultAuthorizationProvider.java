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
package org.apache.rocketmq.auth.authorization.provider;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authorization.builder.AuthorizationContextBuilder;
import org.apache.rocketmq.auth.authorization.builder.DefaultAuthorizationContextBuilder;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.handler.AclAuthorizationHandler;
import org.apache.rocketmq.auth.authorization.handler.UserAuthorizationHandler;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.chain.HandlerChain;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DefaultAuthorizationProvider implements AuthorizationProvider<DefaultAuthorizationContext> {

    protected AuthConfig authConfig;
    protected Supplier<?> metadataService;
    protected AuthorizationContextBuilder authorizationContextBuilder;

    @Override
    public void initialize(AuthConfig config) {
        this.initialize(config, null);
    }

    @Override
    public void initialize(AuthConfig config, Supplier<?> metadataService) {
        this.authConfig = config;
        this.metadataService = metadataService;
        this.authorizationContextBuilder = new DefaultAuthorizationContextBuilder(config);
    }

    @Override
    public CompletableFuture<Void> authorize(DefaultAuthorizationContext context) {
        return this.newHandlerChain().handle(context);
    }

    @Override
    public List<DefaultAuthorizationContext> newContexts(Metadata metadata, GeneratedMessageV3 message) {
        return this.authorizationContextBuilder.build(metadata, message);
    }

    @Override
    public List<DefaultAuthorizationContext> newContexts(RemotingCommand command, String remoteAddr) {
        return this.authorizationContextBuilder.build(command, remoteAddr);
    }

    protected HandlerChain<DefaultAuthorizationContext, CompletableFuture<Void>> newHandlerChain() {
        return HandlerChain.<DefaultAuthorizationContext, CompletableFuture<Void>>create()
            .addNext(new UserAuthorizationHandler(authConfig, metadataService))
            .addNext(new AclAuthorizationHandler(authConfig, metadataService));
    }
}
