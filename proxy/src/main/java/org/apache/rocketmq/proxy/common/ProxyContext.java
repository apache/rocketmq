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

package org.apache.rocketmq.proxy.common;

import io.netty.channel.Channel;
import org.apache.rocketmq.proxy.common.context.ContextNode;
import org.apache.rocketmq.proxy.common.context.ContextVariable;

public class ProxyContext {
    public static final String INNER_ACTION_PREFIX = "Inner";
    private final ContextNode contextNode;

    ProxyContext() {
        this.contextNode = new ContextNode();
    }

    ProxyContext(ContextNode parent) {
        this.contextNode = parent;
    }

    ProxyContext(ProxyContext that) {
        this.contextNode = that.contextNode;
    }

    public static ProxyContext create() {
        return new ProxyContext();
    }

    public static ProxyContext createForInner(String actionName) {
        return create().withAction(INNER_ACTION_PREFIX + actionName);
    }

    public static ProxyContext createForInner(Class<?> clazz) {
        return createForInner(clazz.getSimpleName());
    }

    public ProxyContext withValue(String key, Object val) {
        return new ProxyContext(contextNode.withValue(key, val));
    }

    public <T> T getValue(String key) {
        return (T) contextNode.getValue(key);
    }

    public <T> T getValue(String key, Class<T> classType) {
        return (T) contextNode.getValue(key, classType);
    }

    public ProxyContext withLocalAddress(String localAddress) {
        return this.withValue(ContextVariable.LOCAL_ADDRESS, localAddress);
    }

    public String getLocalAddress() {
        return contextNode.getValue(ContextVariable.LOCAL_ADDRESS, String.class);
    }

    public ProxyContext withRemoteAddress(String remoteAddress) {
        return this.withValue(ContextVariable.REMOTE_ADDRESS, remoteAddress);
    }

    public String getRemoteAddress() {
        return contextNode.getValue(ContextVariable.REMOTE_ADDRESS, String.class);
    }

    public ProxyContext withClientID(String clientID) {
        return this.withValue(ContextVariable.CLIENT_ID, clientID);
    }

    public String getClientID() {
        return contextNode.getValue(ContextVariable.CLIENT_ID, String.class);
    }

    public ProxyContext withChannel(Channel channel) {
        return this.withValue(ContextVariable.CHANNEL, channel);
    }

    public Channel getChannel() {
        return contextNode.getValue(ContextVariable.CHANNEL, Channel.class);
    }

    public ProxyContext withLanguage(String language) {
        return this.withValue(ContextVariable.LANGUAGE, language);
    }

    public String getLanguage() {
        return contextNode.getValue(ContextVariable.LANGUAGE, String.class);
    }

    public ProxyContext withClientVersion(String clientVersion) {
        return this.withValue(ContextVariable.CLIENT_VERSION, clientVersion);
    }

    public String getClientVersion() {
        return contextNode.getValue(ContextVariable.CLIENT_VERSION, String.class);
    }

    public ProxyContext withRemainingMs(Long remainingMs) {
        return this.withValue(ContextVariable.REMAINING_MS, remainingMs);
    }

    public Long getRemainingMs() {
        return contextNode.getValue(ContextVariable.REMAINING_MS, Long.class);
    }

    public ProxyContext withAction(String action) {
        return this.withValue(ContextVariable.ACTION, action);
    }

    public String getAction() {
        return contextNode.getValue(ContextVariable.ACTION, String.class);
    }

    public ProxyContext withProtocolType(String protocol) {
        return this.withValue(ContextVariable.PROTOCOL_TYPE, protocol);
    }

    public String getProtocolType() {
        return contextNode.getValue(ContextVariable.PROTOCOL_TYPE, String.class);
    }
}
