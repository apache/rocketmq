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
package org.apache.rocketmq.auth.authorization.factory;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManager;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManagerImpl;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationMetadataProvider;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AuthorizationFactory {

    private static final ConcurrentMap<String, Object> INSTANCE_MAP = new ConcurrentHashMap<>();
    private static final String PROVIDER_PREFIX = "PROVIDER_";
    private static final String METADATA_PROVIDER_PREFIX = "METADATA_PROVIDER_";
    private static final String EVALUATOR_PREFIX = "EVALUATOR_";

    @SuppressWarnings("unchecked")
    public static AuthorizationProvider<AuthorizationContext> getProvider(AuthConfig config) {
        if (config == null) {
            return null;
        }
        return computeIfAbsent(PROVIDER_PREFIX + config.getConfigName(), key -> {
            String clazzName = config.getAuthorizationProvider();
            if (config.isAuthorizationEnabled() && StringUtils.isEmpty(clazzName)) {
                throw new RuntimeException("The authorization provider can not be null");
            }
            if (StringUtils.isEmpty(clazzName)) {
                return null;
            }
            AuthorizationProvider<AuthorizationContext> result;
            try {
                result = (AuthorizationProvider<AuthorizationContext>) Class.forName(clazzName)
                    .getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to load the authorization provider", e);
            }
            return result;
        });
    }

    public static AuthorizationMetadataProvider getMetadataProvider(AuthConfig config) {
        return getMetadataProvider(config, null);
    }

    public static AuthorizationMetadataManager getMetadataManager(AuthConfig config) {
        return new AuthorizationMetadataManagerImpl(config);
    }

    public static AuthorizationMetadataProvider getMetadataProvider(AuthConfig config, Supplier<?> metadataService) {
        if (config == null) {
            return null;
        }
        return computeIfAbsent(METADATA_PROVIDER_PREFIX + config.getConfigName(), key -> {
            String clazzName = config.getAuthorizationMetadataProvider();
            if (config.isAuthorizationEnabled() && StringUtils.isEmpty(clazzName)) {
                throw new RuntimeException("The authorization metadata provider can not be null");
            }
            if (StringUtils.isEmpty(clazzName)) {
                return null;
            }

            AuthorizationMetadataProvider result;
            try {
                result = (AuthorizationMetadataProvider) Class.forName(clazzName)
                    .getDeclaredConstructor().newInstance();
                result.initialize(config, metadataService);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load the authorization metadata provider", e);
            }
            return result;
        });
    }

    public static AuthorizationEvaluator getEvaluator(AuthConfig config) {
        return computeIfAbsent(EVALUATOR_PREFIX + config.getConfigName(), key -> new AuthorizationEvaluator(config));
    }

    public static AuthorizationEvaluator getEvaluator(AuthConfig config, Supplier<?> metadataService) {
        return computeIfAbsent(EVALUATOR_PREFIX + config.getConfigName(), key -> new AuthorizationEvaluator(config, metadataService));
    }

    public static List<AuthorizationContext> newContexts(AuthConfig config, Metadata metadata,
        GeneratedMessageV3 message) {
        AuthorizationProvider<AuthorizationContext> authorizationProvider = getProvider(config);
        if (authorizationProvider == null) {
            return null;
        }
        return authorizationProvider.newContexts(metadata, message);
    }

    public static List<AuthorizationContext> newContexts(AuthConfig config, ChannelHandlerContext context,
        RemotingCommand command) {
        AuthorizationProvider<AuthorizationContext> authorizationProvider = getProvider(config);
        if (authorizationProvider == null) {
            return null;
        }
        return authorizationProvider.newContexts(context, command);
    }

    @SuppressWarnings("unchecked")
    private static <V> V computeIfAbsent(String key, Function<String, ? extends V> function) {
        Object value = INSTANCE_MAP.computeIfAbsent(key, function);
        return value != null ? (V) value : null;
    }
}
