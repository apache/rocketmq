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
package org.apache.rocketmq.auth.authentication.provider;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.builder.AuthenticationContextBuilder;
import org.apache.rocketmq.auth.authentication.builder.DefaultAuthenticationContextBuilder;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.handler.DefaultAuthenticationHandler;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.chain.HandlerChain;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DefaultAuthenticationProvider implements AuthenticationProvider<DefaultAuthenticationContext> {

    protected AuthConfig authConfig;
    protected Supplier<?> metadataService;
    protected AuthenticationContextBuilder<DefaultAuthenticationContext> authenticationContextBuilder;

    @Override
    public void initialize(AuthConfig config, Supplier<?> metadataService) {
        this.authConfig = config;
        this.metadataService = metadataService;
        this.authenticationContextBuilder = new DefaultAuthenticationContextBuilder();
    }

    @Override
    public CompletableFuture<Void> authenticate(DefaultAuthenticationContext context) {
        return this.newHandlerChain().handle(context);
    }

    @Override
    public DefaultAuthenticationContext newContext(Metadata metadata, GeneratedMessageV3 request) {
        return this.authenticationContextBuilder.build(metadata, request);
    }

    @Override
    public DefaultAuthenticationContext newContext(RemotingCommand command) {
        return this.authenticationContextBuilder.build(command);
    }

    protected HandlerChain<DefaultAuthenticationContext, CompletableFuture<Void>> newHandlerChain() {
        return HandlerChain.<DefaultAuthenticationContext, CompletableFuture<Void>>create()
            .addNext(new DefaultAuthenticationHandler(this.authConfig, metadataService));
    }
}
