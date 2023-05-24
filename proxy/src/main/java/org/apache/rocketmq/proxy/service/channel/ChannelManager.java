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

package org.apache.rocketmq.proxy.service.channel;

import com.google.common.base.Strings;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class ChannelManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final ConcurrentMap<String, SimpleChannel> clientIdChannelMap = new ConcurrentHashMap<>();

    public SimpleChannel createChannel(ProxyContext context) {
        final String clientId = anonymousChannelId(context);
        if (Strings.isNullOrEmpty(clientId)) {
            log.warn("ClientId is unexpected null or empty");
            return createChannelInner(context);
        }
        SimpleChannel channel = ConcurrentHashMapUtils.computeIfAbsent(this.clientIdChannelMap,clientId, k -> createChannelInner(context));
        channel.updateLastAccessTime();
        return channel;
    }

    public SimpleChannel createInvocationChannel(ProxyContext context) {
        final String clientId = anonymousChannelId(InvocationChannel.class.getName(), context);
        final String clientHost = context.getRemoteAddress();
        final String localAddress = context.getLocalAddress();
        if (Strings.isNullOrEmpty(clientId)) {
            log.warn("ClientId is unexpected null or empty");
            return new InvocationChannel(clientHost, localAddress);
        }

        SimpleChannel channel = clientIdChannelMap.computeIfAbsent(clientId, k -> new InvocationChannel(clientHost, localAddress));
        channel.updateLastAccessTime();
        return channel;
    }

    private String anonymousChannelId(ProxyContext context) {
        final String clientHost = context.getRemoteAddress();
        final String localAddress = context.getLocalAddress();
        return clientHost + "@" + localAddress;
    }

    private String anonymousChannelId(String key, ProxyContext context) {
        final String clientHost = context.getRemoteAddress();
        final String localAddress = context.getLocalAddress();
        return key + "@" + clientHost + "@" + localAddress;
    }

    private SimpleChannel createChannelInner(ProxyContext context) {
        return new SimpleChannel(context.getRemoteAddress(), context.getLocalAddress());
    }

    public void scanAndCleanChannels() {
        try {
            Iterator<Map.Entry<String, SimpleChannel>> iterator = clientIdChannelMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, SimpleChannel> entry = iterator.next();
                if (!entry.getValue().isActive()) {
                    iterator.remove();
                } else {
                    entry.getValue().clearExpireContext();
                }
            }
        } catch (Throwable e) {
            log.error("Unexpected exception", e);
        }
    }
}
