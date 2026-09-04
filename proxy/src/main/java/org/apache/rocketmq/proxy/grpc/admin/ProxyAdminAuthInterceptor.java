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

import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.resource.ResourcePattern;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

/**
 * RIP-2 D2 authorization interceptor for the dedicated Proxy Admin gRPC server.
 *
 * <p>Every admin RPC is bound to a dedicated ACL 2.0 resource under the
 * {@code proxy.admin.*} namespace with a distinct action, giving true
 * read-only / high-privilege isolation on top of the standard ACL 2.0 policy
 * engine:
 *
 * <pre>
 *   proxy.admin.client      ListConsumerConnection / ListSubscription / DescribeSubscription /
 *                           DescribeGroupAccumulation / GetConsumerRunningInfo / QueryTimeSpan (GET/LIST)
 *   proxy.admin.config      ChangeLogLevel (UPDATE)
 *   proxy.admin.connection  PrintThreadStackTrace / VerifyMessage (UPDATE, high privilege)
 *   proxy.admin.route       GetTopicRoute (GET)
 *   proxy.admin.ops         broker-facing operations served by the Admin service
 *                           (GET/LIST for queries; UPDATE/DELETE/PUB for mutations)
 * </pre>
 *
 * <p>Resources are modeled as {@code CLUSTER}-typed literal resources
 * (resource key {@code cluster:proxy.admin.<module>}) because the ACL 2.0
 * resource model only defines cluster/namespace/topic/group types; a
 * cluster-typed literal gives exact least-privilege matching without
 * colliding with real cluster names.
 *
 * <p>Behavior modes:
 * <ul>
 *   <li>Cluster auth disabled and {@code proxyAdminRequireAuth=false}: the
 *       admin surface is open (same semantics as the data plane).</li>
 *   <li>Cluster auth enabled: requests are authenticated from the standard
 *       {@code Authorization} gRPC metadata and authorized against the
 *       per-method {@code proxy.admin.*} resource, exactly like the data
 *       plane does for topic/group resources.</li>
 *   <li>{@code proxyAdminRequireAuth=true}: fail-closed mode. Requests
 *       without verifiable credentials are rejected even if the cluster-wide
 *       authentication switch is off.</li>
 * </ul>
 */
public class ProxyAdminAuthInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final Logger logAudit = LoggerFactory.getLogger(LoggerName.ROCKETMQ_AUTH_AUDIT_LOGGER_NAME);

    public static final String RESOURCE_CLIENT = "proxy.admin.client";
    public static final String RESOURCE_CONFIG = "proxy.admin.config";
    public static final String RESOURCE_CONNECTION = "proxy.admin.connection";
    public static final String RESOURCE_ROUTE = "proxy.admin.route";
    public static final String RESOURCE_OPS = "proxy.admin.ops";

    private static final Map<String, ResourceAction> METHOD_PERMISSIONS = new HashMap<>();

    static {
        // Admin service (rocketmq-apis v2 admin.proto) served on the dedicated admin server.
        METHOD_PERMISSIONS.put("GetProxyRuntimeStats", new ResourceAction(RESOURCE_OPS, Action.GET));
        METHOD_PERMISSIONS.put("GetTopicRoute", new ResourceAction(RESOURCE_ROUTE, Action.GET));
        METHOD_PERMISSIONS.put("DescribeTopicStatus", new ResourceAction(RESOURCE_OPS, Action.GET));
        METHOD_PERMISSIONS.put("ListSubscription", new ResourceAction(RESOURCE_CLIENT, Action.LIST));
        METHOD_PERMISSIONS.put("DescribeSubscription", new ResourceAction(RESOURCE_CLIENT, Action.GET));
        METHOD_PERMISSIONS.put("ListConsumerConnection", new ResourceAction(RESOURCE_CLIENT, Action.LIST));
        METHOD_PERMISSIONS.put("DescribeGroupAccumulation", new ResourceAction(RESOURCE_CLIENT, Action.GET));
        METHOD_PERMISSIONS.put("GetConsumerRunningInfo", new ResourceAction(RESOURCE_CLIENT, Action.GET));
        METHOD_PERMISSIONS.put("QueryTimeSpan", new ResourceAction(RESOURCE_CLIENT, Action.GET));
        METHOD_PERMISSIONS.put("QueryMessage", new ResourceAction(RESOURCE_OPS, Action.GET));
        METHOD_PERMISSIONS.put("ChangeLogLevel", new ResourceAction(RESOURCE_CONFIG, Action.UPDATE));
        // High-privilege mutations: strictly separated from the read-only actions above.
        METHOD_PERMISSIONS.put("DeleteSubscription", new ResourceAction(RESOURCE_OPS, Action.DELETE));
        METHOD_PERMISSIONS.put("ResetGroupOffset", new ResourceAction(RESOURCE_OPS, Action.UPDATE));
        METHOD_PERMISSIONS.put("AdminSendMessage", new ResourceAction(RESOURCE_OPS, Action.PUB));
        METHOD_PERMISSIONS.put("PrintThreadStackTrace", new ResourceAction(RESOURCE_CONNECTION, Action.UPDATE));
        METHOD_PERMISSIONS.put("VerifyMessage", new ResourceAction(RESOURCE_CONNECTION, Action.UPDATE));
    }

    private final AuthConfig authConfig;
    private final AuthenticationEvaluator authenticationEvaluator;
    private final AuthorizationEvaluator authorizationEvaluator;

    public ProxyAdminAuthInterceptor(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.authConfig = authConfig;
        this.authenticationEvaluator = AuthenticationFactory.getEvaluator(authConfig,
            messagingProcessor::getMetadataService);
        this.authorizationEvaluator = AuthorizationFactory.getEvaluator(authConfig,
            messagingProcessor::getMetadataService);
    }

    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(ServerCall<R, W> call, Metadata headers,
        ServerCallHandler<R, W> next) {
        String method = call.getMethodDescriptor().getBareMethodName();
        long startNanos = System.nanoTime();
        try {
            ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
            boolean requireAuth = proxyConfig != null && proxyConfig.isProxyAdminRequireAuth();
            boolean authnEnabled = authConfig != null && authConfig.isAuthenticationEnabled();
            boolean authzEnabled = authConfig != null && authConfig.isAuthorizationEnabled();

            // Open mode: identical semantics to the data plane when cluster auth is off.
            if (!requireAuth && !authnEnabled && !authzEnabled) {
                return next.startCall(call, headers);
            }

            // Fail-closed: credentials are demanded but cannot be verified at all.
            if (requireAuth && !authnEnabled) {
                call.close(Status.UNAUTHENTICATED.withDescription(
                    "proxyAdminRequireAuth is on but cluster authenticationEnabled is off; "
                        + "enable authentication before using the admin surface in fail-closed mode"), new Metadata());
                return noopListener();
            }

            String username = null;
            if (authnEnabled || requireAuth) {
                DefaultAuthenticationContext authenticationContext = buildAuthenticationContext(call, headers);
                username = authenticationContext.getUsername();
                if (StringUtils.isBlank(username)) {
                    if (requireAuth) {
                        call.close(Status.UNAUTHENTICATED.withDescription("missing credentials for proxy admin"),
                            new Metadata());
                        return noopListener();
                    }
                    authenticationEvaluator.evaluate(authenticationContext);
                } else {
                    authenticationEvaluator.evaluate(authenticationContext);
                }
            }

            ResourceAction resourceAction = METHOD_PERMISSIONS.get(method);
            if (resourceAction != null && (authzEnabled || requireAuth)) {
                if (StringUtils.isBlank(username)) {
                    call.close(Status.UNAUTHENTICATED.withDescription("missing credentials for proxy admin"),
                        new Metadata());
                    return noopListener();
                }
                DefaultAuthorizationContext authorizationContext = DefaultAuthorizationContext.of(
                    User.of(username),
                    Resource.of(ResourceType.CLUSTER, resourceAction.resource, ResourcePattern.LITERAL),
                    resourceAction.action,
                    resolveSourceIp(call));
                authorizationContext.setRpcCode(call.getMethodDescriptor().getFullMethodName());
                authorizationEvaluator.evaluate(Collections.singletonList(authorizationContext));
            }

            logAudit.info("[PROXY-ADMIN-AUDIT] subject = {} method = {} resource = {} action = {} sourceIp = {}",
                StringUtils.isBlank(username) ? "anonymous" : username, method,
                resourceAction == null ? "unmapped" : resourceAction.resource,
                resourceAction == null ? "unknown" : resourceAction.action.getName(),
                resolveSourceIp(call));
            return next.startCall(call, headers);
        } catch (AuthenticationException e) {
            ProxyAdminMetricsManager.recordError(method, (System.nanoTime() - startNanos) / 1_000_000L, e);
            log.warn("RIP-2 admin authentication failed. method:{}, cause:{}", method, e.getMessage());
            call.close(Status.UNAUTHENTICATED.withDescription(e.getMessage()), new Metadata());
            return noopListener();
        } catch (AuthorizationException e) {
            ProxyAdminMetricsManager.recordError(method, (System.nanoTime() - startNanos) / 1_000_000L, e);
            log.warn("RIP-2 admin authorization denied. method:{}, cause:{}", method, e.getMessage());
            call.close(Status.PERMISSION_DENIED.withDescription(e.getMessage()), new Metadata());
            return noopListener();
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError(method, (System.nanoTime() - startNanos) / 1_000_000L, t);
            log.error("RIP-2 admin auth interceptor error. method:{}", method, t);
            call.close(Status.INTERNAL.withDescription(t.getMessage()), new Metadata());
            return noopListener();
        }
    }

    private <R, W> DefaultAuthenticationContext buildAuthenticationContext(ServerCall<R, W> call,
        Metadata headers) {
        // The builder only uses the message for its descriptor name; pass the shared
        // Status default instance and overwrite rpcCode with the real admin method.
        Object context = AuthenticationFactory.newContext(authConfig, headers,
            apache.rocketmq.v2.Status.getDefaultInstance());
        if (!(context instanceof DefaultAuthenticationContext)) {
            throw new AuthenticationException("unsupported authentication context type for proxy admin");
        }
        DefaultAuthenticationContext authenticationContext = (DefaultAuthenticationContext) context;
        authenticationContext.setRpcCode(call.getMethodDescriptor().getFullMethodName());
        return authenticationContext;
    }

    private static <R, W> String resolveSourceIp(ServerCall<R, W> call) {
        try {
            InetSocketAddress remoteAddress = (InetSocketAddress) call.getAttributes()
                .get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
            if (remoteAddress != null && remoteAddress.getAddress() != null) {
                return remoteAddress.getAddress().getHostAddress();
            }
        } catch (Throwable ignore) {
            // best-effort only
        }
        return "";
    }

    private static <R> ServerCall.Listener<R> noopListener() {
        return new ServerCall.Listener<R>() {
        };
    }

    static ResourceAction resolveResourceAction(String method) {
        return METHOD_PERMISSIONS.get(method);
    }

    static final class ResourceAction {
        final String resource;
        final Action action;

        ResourceAction(String resource, Action action) {
            this.resource = resource;
            this.action = action;
        }
    }
}
