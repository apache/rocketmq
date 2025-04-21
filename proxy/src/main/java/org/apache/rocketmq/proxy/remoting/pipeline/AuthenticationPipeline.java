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

package org.apache.rocketmq.proxy.remoting.pipeline;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AuthenticationPipeline implements RequestPipeline {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final AuthConfig authConfig;
    private final AuthenticationEvaluator authenticationEvaluator;

    public AuthenticationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.authConfig = authConfig;
        this.authenticationEvaluator = AuthenticationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) throws Exception {
        if (!authConfig.isAuthenticationEnabled()) {
            return;
        }
        try {
            AuthenticationContext authenticationContext = newContext(ctx, request, context);
            authenticationEvaluator.evaluate(authenticationContext);
        } catch (AuthenticationException ex) {
            throw ex;
        } catch (Throwable ex) {
            LOGGER.error("authenticate failed, request:{}", request, ex);
            throw ex;
        }
    }

    protected AuthenticationContext newContext(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) {
        return AuthenticationFactory.newContext(authConfig, ctx, request);
    }
}
