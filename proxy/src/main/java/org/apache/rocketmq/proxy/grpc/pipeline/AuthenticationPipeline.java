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
package org.apache.rocketmq.proxy.grpc.pipeline;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Context;
import io.grpc.Metadata;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class AuthenticationPipeline implements RequestPipeline {

    private final AuthConfig authConfig;

    private final AuthenticationEvaluator authenticationEvaluator;

    public AuthenticationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.authConfig = authConfig;
        this.authenticationEvaluator = AuthenticationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
    }

    @Override
    public void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        if (!authConfig.isAuthenticationEnabled()) {
            return;
        }
        Metadata metadata = GrpcConstants.METADATA.get(Context.current());
        AuthenticationContext authenticationContext = newContext(context, metadata, request);
        authenticationEvaluator.evaluate(authenticationContext);
    }

    /**
     * Create Context, for extension
     *
     * @param context for extension
     * @param headers gRPC headers
     * @param request
     * @return
     */
    protected AuthenticationContext newContext(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        AuthenticationContext result = AuthenticationFactory.newContext(authConfig, headers, request);
        if (result instanceof DefaultAuthenticationContext) {
            DefaultAuthenticationContext defaultAuthenticationContext = (DefaultAuthenticationContext) result;
            if (StringUtils.isNotBlank(defaultAuthenticationContext.getUsername())) {
                headers.put(GrpcConstants.AUTHORIZATION_AK, defaultAuthenticationContext.getUsername());
            }
        }
        return result;
    }
}
