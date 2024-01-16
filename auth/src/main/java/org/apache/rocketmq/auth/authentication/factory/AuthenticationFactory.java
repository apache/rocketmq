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
package org.apache.rocketmq.auth.authentication.factory;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import io.netty.channel.ChannelHandlerContext;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManagerImpl;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationProvider;
import org.apache.rocketmq.auth.authentication.provider.DefaultAuthenticationProvider;
import org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authentication.strategy.AuthenticationStrategy;
import org.apache.rocketmq.auth.authentication.strategy.StatefulAuthenticationStrategy;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AuthenticationFactory {

    private static final Map<String, Object> INSTANCE_MAP = new HashMap<>();
    private static final String PROVIDER_PREFIX = "PROVIDER_";
    private static final String METADATA_PROVIDER_PREFIX = "METADATA_PROVIDER_";
    private static final String EVALUATOR_PREFIX = "EVALUATOR_";

    @SuppressWarnings("unchecked")
    public static AuthenticationProvider<AuthenticationContext> getProvider(AuthConfig config) {
        if (config == null) {
            return null;
        }
        return computeIfAbsent(PROVIDER_PREFIX + config.getConfigName(), key -> {
            try {
                Class<? extends AuthenticationProvider<? extends AuthenticationContext>> clazz =
                    DefaultAuthenticationProvider.class;
                if (StringUtils.isNotBlank(config.getAuthenticationProvider())) {
                    clazz = (Class<? extends AuthenticationProvider<? extends AuthenticationContext>>) Class.forName(config.getAuthenticationProvider());
                }
                return (AuthenticationProvider<AuthenticationContext>) clazz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to load the authentication provider", e);
            }
        });
    }

    public static AuthenticationMetadataProvider getMetadataProvider(AuthConfig config) {
        return getMetadataProvider(config, null);
    }

    public static AuthenticationMetadataManager getMetadataManager(AuthConfig config) {
        return new AuthenticationMetadataManagerImpl(config);
    }

    @SuppressWarnings("unchecked")
    public static AuthenticationMetadataProvider getMetadataProvider(AuthConfig config, Supplier<?> metadataService) {
        if (config == null) {
            return null;
        }
        return computeIfAbsent(METADATA_PROVIDER_PREFIX + config.getConfigName(), key -> {
            try {
                Class<? extends AuthenticationMetadataProvider> clazz = LocalAuthenticationMetadataProvider.class;
                if (StringUtils.isNotBlank(config.getAuthenticationMetadataProvider())) {
                    clazz = (Class<? extends AuthenticationMetadataProvider>) Class.forName(config.getAuthenticationMetadataProvider());
                }
                AuthenticationMetadataProvider result = clazz.getDeclaredConstructor().newInstance();
                result.initialize(config, metadataService);
                return result;
            } catch (Exception e) {
                throw new RuntimeException("Failed to load the authentication metadata provider", e);
            }
        });
    }

    public static AuthenticationEvaluator getEvaluator(AuthConfig config) {
        return computeIfAbsent(EVALUATOR_PREFIX + config.getConfigName(), key -> new AuthenticationEvaluator(config));
    }

    public static AuthenticationEvaluator getEvaluator(AuthConfig config, Supplier<?> metadataService) {
        return computeIfAbsent(EVALUATOR_PREFIX + config.getConfigName(), key -> new AuthenticationEvaluator(config, metadataService));
    }

    @SuppressWarnings("unchecked")
    public static AuthenticationStrategy getStrategy(AuthConfig config, Supplier<?> metadataService) {
        try {
            Class<? extends AuthenticationStrategy> clazz = StatefulAuthenticationStrategy.class;
            if (StringUtils.isNotBlank(config.getAuthenticationStrategy())) {
                clazz = (Class<? extends AuthenticationStrategy>) Class.forName(config.getAuthenticationStrategy());
            }
            return clazz.getDeclaredConstructor(AuthConfig.class, Supplier.class).newInstance(config, metadataService);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static AuthenticationContext newContext(AuthConfig config, Metadata metadata, GeneratedMessageV3 request) {
        AuthenticationProvider<AuthenticationContext> authenticationProvider = getProvider(config);
        if (authenticationProvider == null) {
            return null;
        }
        return authenticationProvider.newContext(metadata, request);
    }

    public static AuthenticationContext newContext(AuthConfig config, ChannelHandlerContext context,
        RemotingCommand command) {
        AuthenticationProvider<AuthenticationContext> authenticationProvider = getProvider(config);
        if (authenticationProvider == null) {
            return null;
        }
        return authenticationProvider.newContext(context, command);
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
                    INSTANCE_MAP.put(key, result);
                }
            }
        }
        return result != null ? (V) result : null;
    }
}