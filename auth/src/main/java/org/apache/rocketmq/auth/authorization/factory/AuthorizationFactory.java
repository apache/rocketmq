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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManager;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManagerImpl;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationDisabledMetadataProvider;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationMetadataProvider;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationProvider;
import org.apache.rocketmq.auth.authorization.provider.DefaultAuthorizationProvider;
import org.apache.rocketmq.auth.authorization.strategy.AuthorizationStrategy;
import org.apache.rocketmq.auth.authorization.strategy.StatelessAuthorizationStrategy;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AuthorizationFactory {

    private static final Map<String, Object> INSTANCE_MAP = new HashMap<>();
    private static final String PROVIDER_PREFIX = "PROVIDER_";
    private static final String METADATA_PROVIDER_PREFIX = "METADATA_PROVIDER_";
    private static final String EVALUATOR_PREFIX = "EVALUATOR_";

    @SuppressWarnings("unchecked")
    public static AuthorizationProvider<AuthorizationContext> getProvider(AuthConfig config) {
        if (config == null) {
            return null;
        }
        return computeIfAbsent(PROVIDER_PREFIX + config.getConfigName(), key -> {
            try {
                Class<? extends AuthorizationProvider<? extends AuthorizationContext>> clazz =
                    DefaultAuthorizationProvider.class;
                if (StringUtils.isNotBlank(config.getAuthorizationProvider())) {
                    clazz = (Class<? extends AuthorizationProvider<? extends AuthorizationContext>>) Class.forName(config.getAuthorizationProvider());
                }
                return (AuthorizationProvider<AuthorizationContext>) clazz
                    .getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to load the authorization provider.", e);
            }
        });
    }

    public static AuthorizationMetadataProvider getMetadataProvider(AuthConfig config) {
        return getMetadataProvider(config, null);
    }

    public static AuthorizationMetadataManager getMetadataManager(AuthConfig config) {
        return new AuthorizationMetadataManagerImpl(config);
    }

    @SuppressWarnings("unchecked")
    public static AuthorizationMetadataProvider getMetadataProvider(AuthConfig config, Supplier<?> metadataService) {
        if (config == null) {
            return null;
        }
        return computeIfAbsent(METADATA_PROVIDER_PREFIX + config.getConfigName(), key -> {
            try {
                if (!config.isAuthorizationEnabled()) {
                    return AuthorizationDisabledMetadataProvider.INSTANCE;
                }
                if (StringUtils.isBlank(config.getAuthorizationMetadataProvider())) {
                    return null;
                }
                Class<? extends AuthorizationMetadataProvider> clazz = (Class<? extends AuthorizationMetadataProvider>)
                    Class.forName(config.getAuthorizationMetadataProvider());
                AuthorizationMetadataProvider result = clazz.getDeclaredConstructor().newInstance();
                result.initialize(config, metadataService);
                return result;
            } catch (Exception e) {
                throw new RuntimeException("Failed to load the authorization metadata provider.", e);
            }
        });
    }

    public static AuthorizationEvaluator getEvaluator(AuthConfig config) {
        return computeIfAbsent(EVALUATOR_PREFIX + config.getConfigName(), key -> new AuthorizationEvaluator(config));
    }

    public static AuthorizationEvaluator getEvaluator(AuthConfig config, Supplier<?> metadataService) {
        return computeIfAbsent(EVALUATOR_PREFIX + config.getConfigName(), key -> new AuthorizationEvaluator(config, metadataService));
    }

    @SuppressWarnings("unchecked")
    public static AuthorizationStrategy getStrategy(AuthConfig config, Supplier<?> metadataService) {
        try {
            Class<? extends AuthorizationStrategy> clazz = StatelessAuthorizationStrategy.class;
            if (StringUtils.isNotBlank(config.getAuthorizationStrategy())) {
                clazz = (Class<? extends AuthorizationStrategy>) Class.forName(config.getAuthorizationStrategy());
            }
            return clazz.getDeclaredConstructor(AuthConfig.class, Supplier.class).newInstance(config, metadataService);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        Object result = null;
        if (INSTANCE_MAP.containsKey(key)) {
            result = INSTANCE_MAP.get(key);
        }
        if (result == null) {
            synchronized (INSTANCE_MAP) {
                if (INSTANCE_MAP.containsKey(key)) {
                    result = INSTANCE_MAP.get(key);
                }
                if (result == null) {
                    result = function.apply(key);
                    if (result != null) {
                        INSTANCE_MAP.put(key, result);
                    }
                }
            }
        }
        return result != null ? (V) result : null;
    }
}
