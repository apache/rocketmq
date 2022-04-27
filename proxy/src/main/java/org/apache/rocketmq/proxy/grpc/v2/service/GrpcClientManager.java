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

package org.apache.rocketmq.proxy.grpc.v2.service;

import apache.rocketmq.v2.ExponentialBackoff;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.RetryPolicy;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import com.google.protobuf.util.Durations;
import io.grpc.Context;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;

public class GrpcClientManager {

    // TODO: read config from topic or subscription configManager
    protected static final Settings DEFAULT_PRODUCER_SETTINGS = Settings.newBuilder()
        .setBackoffPolicy(RetryPolicy.newBuilder()
            .setMaxAttempts(3)
            .setExponentialBackoff(ExponentialBackoff.newBuilder()
                .setInitial(Durations.fromSeconds(1))
                .setMax(Durations.fromSeconds(3))
                .setMultiplier(2)
                .build())
            .build())
        .setPublishing(Publishing.newBuilder()
            .setCompressBodyThreshold(4 * 1024)
            .setMaxBodySize(4 * 1024 * 1024)
            .build())
        .build();
    protected static final Settings DEFAULT_CONSUMER_SETTINGS = Settings.newBuilder()
        .setBackoffPolicy(RetryPolicy.newBuilder()
            .setMaxAttempts(3)
            .setExponentialBackoff(ExponentialBackoff.newBuilder()
                .setInitial(Durations.fromSeconds(1))
                .setMax(Durations.fromSeconds(3))
                .setMultiplier(2)
                .build())
            .build())
        .setSubscription(Subscription.newBuilder()
            .setFifo(false)
            .setReceiveBatchSize(ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST)
            .setLongPollingTimeout(Durations.fromSeconds(30))
            .build())
        .build();
    protected static final Map<String, Settings> CLIENT_SETTINGS_MAP = new ConcurrentHashMap<>();

    public Settings getClientSettings(Context ctx) {
        String clientId = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
        return CLIENT_SETTINGS_MAP.get(clientId);
    }

    public Settings getClientSettings(String clientId) {
        return CLIENT_SETTINGS_MAP.get(clientId);
    }

    public void updateClientSettings(String clientId, Settings settings) {
        if (settings.hasPublishing()) {
            settings = DEFAULT_PRODUCER_SETTINGS.toBuilder().mergeFrom(settings).build();
        } else if (settings.hasSubscription()) {
            settings = DEFAULT_CONSUMER_SETTINGS.toBuilder().mergeFrom(settings).build();
        }
        CLIENT_SETTINGS_MAP.put(clientId, settings);
    }

    public Settings removeClientSettings(String clientId) {
        return CLIENT_SETTINGS_MAP.remove(clientId);
    }
}
