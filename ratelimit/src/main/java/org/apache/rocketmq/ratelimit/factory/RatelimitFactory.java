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
package org.apache.rocketmq.ratelimit.factory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.ratelimit.manager.RatelimitMetadataManager;
import org.apache.rocketmq.ratelimit.manager.RatelimitMetadataManagerImpl;
import org.apache.rocketmq.ratelimit.provider.RatelimitMetadataProvider;
import org.apache.rocketmq.ratelimit.provider.RatelimitProvider;
import org.apache.rocketmq.ratelimit.provider.DefaultRatelimitProvider;
import org.apache.rocketmq.ratelimit.config.RatelimitConfig;

public class RatelimitFactory {

    private static final Map<String, Object> INSTANCE_MAP = new HashMap<>();
    private static final String PROVIDER_KEY = "PROVIDER";
    private static final String METADATA_PROVIDER_KEY = "METADATA_PROVIDER";
    private static final String METADATA_MANAGER_KEY = "METADATA_MANAGER";

    @SuppressWarnings("unchecked")
    public static RatelimitProvider getProvider(RatelimitConfig config) {
        if (config == null) {
            return null;
        }
        return computeIfAbsent(PROVIDER_KEY, key -> {
            try {
                Class<? extends RatelimitProvider> clazz = DefaultRatelimitProvider.class;
                if (StringUtils.isNotBlank(config.getRatelimitProvider())) {
                    clazz = (Class<? extends RatelimitProvider>) Class.forName(config.getRatelimitProvider());
                }
                return (RatelimitProvider) clazz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to load the authentication provider.", e);
            }
        });
    }

    public static RatelimitMetadataProvider getMetadataProvider(RatelimitConfig config) {
        return getMetadataProvider(config, null);
    }

    public static RatelimitMetadataManager getMetadataManager(RatelimitConfig config) {
        return computeIfAbsent(METADATA_MANAGER_KEY, key -> new RatelimitMetadataManagerImpl(config));
    }

    @SuppressWarnings("unchecked")
    public static RatelimitMetadataProvider getMetadataProvider(RatelimitConfig config, Supplier<?> metadataService) {
        if (config == null) {
            return null;
        }
        return computeIfAbsent(METADATA_PROVIDER_KEY, key -> {
            try {
                if (StringUtils.isBlank(config.getRatelimitMetadataProvider())) {
                    return null;
                }
                Class<? extends RatelimitMetadataProvider> clazz = (Class<? extends RatelimitMetadataProvider>)
                    Class.forName(config.getRatelimitMetadataProvider());
                RatelimitMetadataProvider result = clazz.getDeclaredConstructor().newInstance();
                result.initialize(config, metadataService);
                return result;
            } catch (Exception e) {
                throw new RuntimeException("Failed to load the authentication metadata provider", e);
            }
        });
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