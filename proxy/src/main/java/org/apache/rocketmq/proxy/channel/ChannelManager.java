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

package org.apache.rocketmq.proxy.channel;

import com.google.common.base.Strings;
import io.grpc.Context;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.proxy.configuration.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.adapter.channel.SendMessageChannel;
import org.apache.rocketmq.proxy.grpc.common.InterceptorConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);
    private final ConcurrentMap<String, SimpleChannel> clientIdChannelMap = new ConcurrentHashMap<>();

    public SimpleChannel createChannel() {
        final String clientId = anonymousChannelId();
        if (Strings.isNullOrEmpty(clientId)) {
            LOGGER.warn("ClientId is unexpected null or empty");
            return createChannelInner();
        }

        if (!clientIdChannelMap.containsKey(clientId)) {
            clientIdChannelMap.putIfAbsent(clientId, createChannelInner());
        }

        SimpleChannel channel = clientIdChannelMap.get(clientId);
        channel.updateLastAccessTime();
        return channel;
    }

    private String anonymousChannelId() {
        final String clientHost = InterceptorConstants.METADATA.get(Context.current())
            .get(InterceptorConstants.REMOTE_ADDRESS);
        final String localAddress = InterceptorConstants.METADATA.get(Context.current())
            .get(InterceptorConstants.LOCAL_ADDRESS);
        return clientHost + "@" + localAddress;
    }

    private SimpleChannel createChannelInner() {
        final String clientHost = InterceptorConstants.METADATA.get(Context.current())
            .get(InterceptorConstants.REMOTE_ADDRESS);
        final String localAddress = InterceptorConstants.METADATA.get(Context.current())
            .get(InterceptorConstants.LOCAL_ADDRESS);
        return new SimpleChannel(null, clientHost, localAddress, ConfigurationManager.getProxyConfig().getExpiredChannelTimeSec());
    }

    /**
     * Scan and remove inactive mocking channels; Scan and clean expired requests;
     */
    public void scanAndCleanChannels() {
        try {
            Iterator<Map.Entry<String, SimpleChannel>> iterator = clientIdChannelMap.entrySet()
                .iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, SimpleChannel> entry = iterator.next();
                if (!entry.getValue()
                    .isActive()) {
                    iterator.remove();
                } else {
                    if (entry.getValue() instanceof SendMessageChannel) {
                        SendMessageChannel channel = (SendMessageChannel) entry.getValue();
                        channel.cleanExpiredRequests();
                    }
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Unexpected exception", e);
        }
    }
}
