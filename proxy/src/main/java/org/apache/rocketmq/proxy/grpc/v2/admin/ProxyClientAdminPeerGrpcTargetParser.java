/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.admin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;

public final class ProxyClientAdminPeerGrpcTargetParser {
    private static final ProxyClientAdminPeerGrpcTargetParser INSTANCE =
        new ProxyClientAdminPeerGrpcTargetParser();

    private ProxyClientAdminPeerGrpcTargetParser() {
    }

    public static ProxyClientAdminPeerGrpcTargetParser getInstance() {
        return INSTANCE;
    }

    public List<ProxyClientAdminPeerGrpcTarget> parse(String targetConfig) {
        if (StringUtils.trimToNull(targetConfig) == null) {
            return Collections.emptyList();
        }
        Map<String, ProxyClientAdminPeerGrpcTarget> targets = new TreeMap<>();
        String[] entries = StringUtils.splitPreserveAllTokens(targetConfig, ',');
        for (String entry : entries) {
            ProxyClientAdminPeerGrpcTarget target = this.parseEntry(entry);
            if (targets.containsKey(target.getProxyId())) {
                throw new IllegalArgumentException("Duplicate proxyId: " + target.getProxyId());
            }
            targets.put(target.getProxyId(), target);
        }
        return Collections.unmodifiableList(new ArrayList<>(targets.values()));
    }

    private ProxyClientAdminPeerGrpcTarget parseEntry(String entry) {
        String normalizedEntry = StringUtils.trimToNull(entry);
        if (normalizedEntry == null) {
            throw new IllegalArgumentException("Invalid proxy client admin peer target: " + entry);
        }
        int delimiterIndex = normalizedEntry.indexOf('=');
        if (delimiterIndex < 0) {
            throw new IllegalArgumentException("Invalid proxy client admin peer target: " + entry);
        }
        String proxyId = normalizedEntry.substring(0, delimiterIndex);
        Address address = this.parseAddress(normalizedEntry.substring(delimiterIndex + 1));
        return new ProxyClientAdminPeerGrpcTarget(proxyId, address.host, address.port);
    }

    private Address parseAddress(String address) {
        String normalizedAddress = StringUtils.trimToNull(address);
        if (normalizedAddress == null) {
            throw new IllegalArgumentException("target address must be host:port");
        }
        if (normalizedAddress.startsWith("[")) {
            return this.parseBracketedAddress(normalizedAddress);
        }
        int portDelimiterIndex = normalizedAddress.lastIndexOf(':');
        if (portDelimiterIndex < 0 || portDelimiterIndex == normalizedAddress.length() - 1) {
            throw new IllegalArgumentException("target address must be host:port");
        }
        String host = normalizedAddress.substring(0, portDelimiterIndex);
        String portText = normalizedAddress.substring(portDelimiterIndex + 1);
        return new Address(host, this.parsePort(portText));
    }

    private Address parseBracketedAddress(String address) {
        int hostEndIndex = address.indexOf(']');
        if (hostEndIndex < 0 || hostEndIndex == address.length() - 1
            || address.charAt(hostEndIndex + 1) != ':') {
            throw new IllegalArgumentException("target address must be host:port");
        }
        String host = address.substring(1, hostEndIndex);
        String portText = address.substring(hostEndIndex + 2);
        return new Address(host, this.parsePort(portText));
    }

    private int parsePort(String portText) {
        String normalizedPortText = StringUtils.trimToNull(portText);
        if (normalizedPortText == null) {
            throw new IllegalArgumentException("port must be a number");
        }
        int port;
        try {
            port = Integer.parseInt(normalizedPortText);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("port must be a number", e);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        return port;
    }

    private static class Address {
        private final String host;
        private final int port;

        private Address(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }
}
