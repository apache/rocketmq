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
import java.util.List;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AuthenticationPipeline implements RequestPipeline {

    private final List<AccessValidator> accessValidatorList;

    private final AuthConfig authConfig;

    private final AuthenticationEvaluator authenticationEvaluator;

    public AuthenticationPipeline(List<AccessValidator> accessValidatorList, AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.accessValidatorList = accessValidatorList;
        this.authConfig = authConfig;
        this.authenticationEvaluator = AuthenticationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) throws Exception {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        if (config.isEnableACL()) {
            for (AccessValidator accessValidator : accessValidatorList) {
                AccessResource accessResource = accessValidator.parse(request, context.getRemoteAddress());
                accessValidator.validate(accessResource);
            }
        }

        if (!authConfig.isAuthenticationEnabled()) {
            return;
        }

        AuthenticationContext authenticationContext = newContext(context, request);
        if (authenticationContext == null) {
            return;
        }
        
        authenticationEvaluator.evaluate(authenticationContext);
    }

    protected AuthenticationContext newContext(ProxyContext context, RemotingCommand request) {
        return AuthenticationFactory.newContext(authConfig, request);
    }
}
