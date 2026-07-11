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
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.utils.GrpcUtils;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class AuthenticationPipeline implements RequestPipeline {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final AuthConfig authConfig;
    private final AuthenticationEvaluator authenticationEvaluator;
    private final boolean subjectRequiresAuthentication;

    public AuthenticationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this(authConfig, messagingProcessor, false);
    }

    protected AuthenticationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor,
        boolean subjectRequiresAuthentication) {
        this.authConfig = authConfig;
        this.authenticationEvaluator = AuthenticationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
        this.subjectRequiresAuthentication = subjectRequiresAuthentication;
    }

    public static AuthenticationPipeline forProxyAdmin(AuthConfig authConfig,
        MessagingProcessor messagingProcessor) {
        return new AuthenticationPipeline(authConfig, messagingProcessor, true);
    }

    @Override
    public void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        if (!authConfig.isAuthenticationEnabled()) {
            return;
        }
        try {
            Metadata metadata = headers == null ? GrpcConstants.METADATA.get(Context.current()) : headers;
            if (metadata == null) {
                metadata = new Metadata();
            }
            if (this.subjectRequiresAuthentication) {
                metadata.removeAll(GrpcConstants.AUTHORIZATION_AK);
            }
            AuthenticationContext authenticationContext = newContext(context, metadata, request);
            if (!this.subjectRequiresAuthentication) {
                publishParsedSubjectIfAbsent(metadata, authenticationContext);
            }
            authenticationEvaluator.evaluate(authenticationContext);
            if (this.subjectRequiresAuthentication) {
                publishAuthenticatedSubject(metadata, authenticationContext);
            }
        } catch (AuthenticationException ex) {
            throw ex;
        } catch (Throwable ex) {
            LOGGER.error("authenticate failed, request:{}", request, ex);
            throw ex;
        }
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
        return AuthenticationFactory.newContext(authConfig, headers, request);
    }

    private void publishParsedSubjectIfAbsent(Metadata headers, AuthenticationContext authenticationContext) {
        if (!(authenticationContext instanceof DefaultAuthenticationContext)) {
            return;
        }
        String username = StringUtils.trimToNull(
            ((DefaultAuthenticationContext) authenticationContext).getUsername());
        if (username != null) {
            GrpcUtils.putHeaderIfNotExist(headers, GrpcConstants.AUTHORIZATION_AK, username);
        }
    }

    private void publishAuthenticatedSubject(Metadata headers, AuthenticationContext authenticationContext) {
        if (!(authenticationContext instanceof DefaultAuthenticationContext)
            || isAuthenticationWhitelisted(authenticationContext)) {
            return;
        }
        String username = StringUtils.trimToNull(
            ((DefaultAuthenticationContext) authenticationContext).getUsername());
        if (username != null) {
            headers.put(GrpcConstants.AUTHORIZATION_AK, username);
        }
    }

    private boolean isAuthenticationWhitelisted(AuthenticationContext authenticationContext) {
        String[] whitelist = StringUtils.split(this.authConfig.getAuthenticationWhitelist(), ',');
        if (whitelist == null) {
            return false;
        }
        for (String rpcCode : whitelist) {
            if (StringUtils.trimToEmpty(rpcCode).equals(authenticationContext.getRpcCode())) {
                return true;
            }
        }
        return false;
    }
}
