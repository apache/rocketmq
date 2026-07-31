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

package org.apache.rocketmq.proxy.grpc.admin;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.constant.CommonConstants;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.resource.ResourcePattern;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

/**
 * gRPC server interceptor for Proxy Admin ACL 2.0 authentication and authorization.
 * <p>
 * RIP-2 defines the ACL resource pattern as: proxy.admin.client
 * - ListClients: proxy.admin.client read
 * - DescribeClient: proxy.admin.client read
 * - ListClientsByGroup: proxy.admin.client read
 * - ListClientsByTopic: proxy.admin.client read
 * - GetConfig: proxy.admin.client read
 * - UpdateConfig: proxy.admin.client write
 * <p>
 * All admin RPCs require read permission on the proxy.admin.client resource.
 * UpdateConfig requires write permission.
 */
public class ProxyAdminAuthInterceptor implements ServerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /** ACL 2.0 resource prefix for proxy admin operations */
    public static final String ADMIN_RESOURCE_PREFIX = "proxy.admin";

    /** Resource for client admin operations */
    public static final String CLIENT_ADMIN_RESOURCE = "proxy.admin.client";

    private final AuthConfig authConfig;
    private final AuthenticationEvaluator authenticationEvaluator;
    private final AuthorizationEvaluator authorizationEvaluator;

    public ProxyAdminAuthInterceptor(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.authConfig = authConfig;
        if (authConfig.isAuthenticationEnabled() || authConfig.isAuthorizationEnabled()) {
            this.authenticationEvaluator = AuthenticationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
            this.authorizationEvaluator = AuthorizationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
        } else {
            this.authenticationEvaluator = null;
            this.authorizationEvaluator = null;
        }
    }

    @Override
    public <REQT, RESPT> ServerCall.Listener<REQT> interceptCall(
        ServerCall<REQT, RESPT> call, Metadata headers, ServerCallHandler<REQT, RESPT> next) {

        if (!authConfig.isAuthenticationEnabled() && !authConfig.isAuthorizationEnabled()) {
            return next.startCall(call, headers);
        }

        String methodName = call.getMethodDescriptor().getFullMethodName();

        try {
            // Step 1: Authentication
            if (authConfig.isAuthenticationEnabled()) {
                authenticate(headers, methodName);
            }

            // Step 2: Authorization
            if (authConfig.isAuthorizationEnabled()) {
                authorize(headers, methodName);
            }

        } catch (AuthenticationException e) {
            log.warn("Admin authentication failed for method:{}, user:{}, error:{}",
                methodName, extractUsername(headers), e.getMessage());
            call.close(Status.UNAUTHENTICATED
                .withDescription("Authentication failed: " + e.getMessage()), headers);
            return new ServerCall.Listener<REQT>() { };
        } catch (AuthorizationException e) {
            log.warn("Admin authorization failed for method:{}, user:{}, error:{}",
                methodName, extractUsername(headers), e.getMessage());
            call.close(Status.PERMISSION_DENIED
                .withDescription("Authorization failed: " + e.getMessage()), headers);
            return new ServerCall.Listener<REQT>() { };
        } catch (Exception e) {
            log.error("Admin auth error for method:{}", methodName, e);
            call.close(Status.INTERNAL
                .withDescription("Internal auth error: " + e.getMessage()), headers);
            return new ServerCall.Listener<REQT>() { };
        }

        return next.startCall(call, headers);
    }

    /**
     * Perform authentication using ACL 2.0 evaluator.
     * Manually constructs the authentication context from gRPC headers
     * since admin requests are not protobuf GeneratedMessageV3 types.
     */
    private void authenticate(Metadata headers, String methodName) {
        DefaultAuthenticationContext authContext = new DefaultAuthenticationContext();
        authContext.setChannelId(headers.get(GrpcConstants.CHANNEL_ID));
        authContext.setRpcCode(methodName);

        // Parse Authorization header for credentials
        String authorization = headers.get(GrpcConstants.AUTHORIZATION);
        if (StringUtils.isNotBlank(authorization)) {
            String datetime = headers.get(GrpcConstants.DATE_TIME);
            if (StringUtils.isNotBlank(datetime)) {
                String[] result = authorization.split(CommonConstants.SPACE, 2);
                if (result.length == 2) {
                    String[] keyValues = result[1].split(CommonConstants.COMMA);
                    for (String keyValue : keyValues) {
                        String[] kv = keyValue.trim().split(CommonConstants.EQUAL, 2);
                        if (kv.length == 2) {
                            if ("Credential".equals(kv[0])) {
                                String[] credential = kv[1].split(CommonConstants.SLASH);
                                if (credential.length > 0) {
                                    authContext.setUsername(credential[0]);
                                }
                            } else if ("Signature".equals(kv[0])) {
                                authContext.setSignature(kv[1]);
                            }
                        }
                    }
                    authContext.setContent(datetime.getBytes(StandardCharsets.UTF_8));
                }
            }
        }

        authenticationEvaluator.evaluate(authContext);
    }

    /**
     * Perform authorization using ACL 2.0 evaluator.
     * Admin operations require read permission on proxy.admin.client resource.
     * <p>
     * RIP-2 §7.2: read covers list/describe/get operations.
     * - DescribeClient uses Action.GET (single resource read)
     * - List* operations use Action.LIST (collection read)
     */
    private void authorize(Metadata headers, String methodName) {
        // Extract subject (username) from headers
        String username = extractUsername(headers);
        User subject = User.of(username);

        // Build resource: use ANY type with the admin resource name
        Resource resource = Resource.of(ResourceType.ANY, CLIENT_ADMIN_RESOURCE, ResourcePattern.LITERAL);

        // Determine action based on method type (RIP-2 §7.2)
        Action action = resolveAction(methodName);

        // Build authorization context
        DefaultAuthorizationContext context = DefaultAuthorizationContext.of(
            subject, resource, action, extractSourceIp(headers));

        List<AuthorizationContext> contexts = Collections.singletonList(context);
        authorizationEvaluator.evaluate(contexts);
    }

    /**
     * Resolve the authorization action for the given admin RPC method.
     * RIP-2 §7.2: read = list/describe/get, write = update
     * - DescribeClient → GET (single resource read)
     * - GetConfig → GET (single config read)
     * - UpdateConfig → UPDATE (config write)
     * - ListClients / ListClientsByGroup / ListClientsByTopic → LIST (collection read)
     */
    private Action resolveAction(String methodName) {
        if (methodName != null && methodName.contains("UpdateConfig")) {
            return Action.UPDATE;
        }
        if (methodName != null && (methodName.contains("DescribeClient")
            || methodName.contains("GetConfig"))) {
            return Action.GET;
        }
        return Action.LIST;
    }

    /**
     * Extract username from gRPC headers for logging and authorization.
     */
    private String extractUsername(Metadata headers) {
        if (headers == null) {
            return "unknown";
        }
        String ak = headers.get(GrpcConstants.AUTHORIZATION_AK);
        return StringUtils.isNotBlank(ak) ? ak : "unknown";
    }

    /**
     * Extract source IP from gRPC headers.
     */
    private String extractSourceIp(Metadata headers) {
        if (headers == null) {
            return null;
        }
        // Try to get source IP from the gRPC metadata
        String sourceIp = headers.get(Metadata.Key.of("x-forwarded-for", Metadata.ASCII_STRING_MARSHALLER));
        return StringUtils.isNotBlank(sourceIp) ? sourceIp : null;
    }
}