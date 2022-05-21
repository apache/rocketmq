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

package org.apache.rocketmq.thinclient.route;

public enum AddressScheme {
    /**
     * Scheme for domain name.
     */
    DOMAIN_NAME("dns:"),
    /**
     * Scheme for ipv4 address.
     */
    IPv4("ipv4:"),
    /**
     * Scheme for ipv6 address.
     */
    IPv6("ipv6:");

    private final String prefix;

    AddressScheme(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return this.prefix;
    }

    public apache.rocketmq.v2.AddressScheme toProtobuf() {
        switch (this) {
            case IPv4:
                return apache.rocketmq.v2.AddressScheme.IPv4;
            case IPv6:
                return apache.rocketmq.v2.AddressScheme.IPv6;
            case DOMAIN_NAME:
            default:
                return apache.rocketmq.v2.AddressScheme.DOMAIN_NAME;
        }
    }

    public static AddressScheme fromPrefix(String prefix) {
        if (AddressScheme.DOMAIN_NAME.getPrefix().equals(prefix)) {
            return AddressScheme.DOMAIN_NAME;
        }
        if (AddressScheme.IPv4.getPrefix().equals(prefix)) {
            return AddressScheme.IPv4;
        }
        if (AddressScheme.IPv6.getPrefix().equals(prefix)) {
            return AddressScheme.IPv6;
        }
        throw new IllegalArgumentException("Unrecognized address scheme prefix: " + prefix);
    }
}
