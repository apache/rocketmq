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

package org.apache.rocketmq.broker.auth.pipeline;

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

public class AuthorizationPipeline implements RequestPipeline {
    protected static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final AuthConfig authConfig;
    private final AuthorizationEvaluator evaluator;

    public AuthorizationPipeline(AuthConfig authConfig) {
        this.authConfig = authConfig;
        this.evaluator = AuthorizationFactory.getEvaluator(authConfig);
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        if (!authConfig.isAuthorizationEnabled()) {
            return;
        }
        try {
            List<AuthorizationContext> contexts = newContexts(ctx, request);
            evaluator.evaluate(contexts);
        } catch (AuthorizationException | AuthenticationException ex) {
            throw new AbortProcessException(ResponseCode.NO_PERMISSION, ex.getMessage());
        }  catch (Throwable ex) {
            LOGGER.error("authorization failed, request:{}", request, ex);
            throw ex;
        }
    }

    protected List<AuthorizationContext> newContexts(ChannelHandlerContext ctx, RemotingCommand request) {
        return AuthorizationFactory.newContexts(authConfig, ctx, request);
    }
}
