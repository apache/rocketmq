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
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.rocketmq.auth.authorization.builder.AuthorizationContextBuilder;
import org.apache.rocketmq.auth.authorization.builder.DefaultAuthorizationContextBuilder;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.chain.AclAuthorizationHandler;
import org.apache.rocketmq.auth.authorization.chain.UserAuthorizationHandler;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.chain.HandlerChain;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultAuthorizationProvider implements AuthorizationProvider<DefaultAuthorizationContext> {

    protected final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_AUTH_AUDIT_LOGGER_NAME);
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
        return this.newHandlerChain().handle(context)
            .whenComplete((nil, ex) -> doAuditLog(context, ex));
    }

    @Override
    public List<DefaultAuthorizationContext> newContexts(Metadata metadata, GeneratedMessageV3 message) {
        return this.authorizationContextBuilder.build(metadata, message);
    }

    @Override
    public List<DefaultAuthorizationContext> newContexts(ChannelHandlerContext context, RemotingCommand command) {
        return this.authorizationContextBuilder.build(context, command);
    }

    protected HandlerChain<DefaultAuthorizationContext, CompletableFuture<Void>> newHandlerChain() {
        return HandlerChain.<DefaultAuthorizationContext, CompletableFuture<Void>>create()
            .addNext(new UserAuthorizationHandler(authConfig, metadataService))
            .addNext(new AclAuthorizationHandler(authConfig, metadataService));
    }

    private void doAuditLog(DefaultAuthorizationContext context, Throwable ex) {
        Decision decision = Decision.ALLOW;
        if (ex != null) {
            decision = Decision.DENY;
        }
        String subject = context.getSubject().getSubjectKey();
        String actions = context.getActions().stream().map(Action::getName)
            .collect(Collectors.joining(","));
        String sourceIp = context.getSourceIp();
        String resource = context.getResource().getResourceKey();
        String request = context.getRpcCode();
        String format = "[AUTHORIZATION] Subject = {} is {} Action = {} from sourceIp = {} on resource = {} for request = {}.";
        if (decision == Decision.ALLOW) {
            log.debug(format, subject, decision.getName(), actions, sourceIp, resource, request);
        } else {
            log.info(format, subject, decision.getName(), actions, sourceIp, resource, request);
        }
    }
}
