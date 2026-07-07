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

import java.util.List;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientAdminPeerGrpcTargetParserTest {

    @Test
    public void parseReturnsStableTargetsSortedByProxyId() {
        List<ProxyClientAdminPeerGrpcTarget> targets = ProxyClientAdminPeerGrpcTargetParser.getInstance().parse(
            " proxy-b = 192.168.0.2:8081, proxy-a=127.0.0.1:8080 "
        );

        assertThat(targets)
            .extracting(ProxyClientAdminPeerGrpcTarget::getProxyId)
            .containsExactly("proxy-a", "proxy-b");
        assertThat(targets)
            .extracting(ProxyClientAdminPeerGrpcTarget::getHost)
            .containsExactly("127.0.0.1", "192.168.0.2");
        assertThat(targets)
            .extracting(ProxyClientAdminPeerGrpcTarget::getPort)
            .containsExactly(8080, 8081);
    }

    @Test
    public void parseSupportsBracketedIpv6Hosts() {
        List<ProxyClientAdminPeerGrpcTarget> targets = ProxyClientAdminPeerGrpcTargetParser.getInstance().parse(
            "proxy-v6=[2001:db8::1]:18080"
        );

        assertThat(targets).hasSize(1);
        assertThat(targets.get(0).getProxyId()).isEqualTo("proxy-v6");
        assertThat(targets.get(0).getHost()).isEqualTo("2001:db8::1");
        assertThat(targets.get(0).getPort()).isEqualTo(18080);
    }

    @Test
    public void parseRejectsUnbracketedIpv6Hosts() {
        assertThatThrownBy(() -> ProxyClientAdminPeerGrpcTargetParser.getInstance().parse(
            "proxy-v6=2001:db8::1:18080"
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("IPv6 target address must be bracketed");
    }

    @Test
    public void parseReturnsEmptyListForBlankConfig() {
        assertThat(ProxyClientAdminPeerGrpcTargetParser.getInstance().parse(null)).isEmpty();
        assertThat(ProxyClientAdminPeerGrpcTargetParser.getInstance().parse(" ")).isEmpty();
    }

    @Test
    public void parseRejectsDuplicateProxyIds() {
        assertThatThrownBy(() -> ProxyClientAdminPeerGrpcTargetParser.getInstance().parse(
            "proxy-a=127.0.0.1:8080, proxy-a = 127.0.0.2:8081"
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Duplicate proxyId")
            .hasMessageContaining("proxy-a");
    }

    @Test
    public void parseRejectsMalformedEntries() {
        assertThatThrownBy(() -> ProxyClientAdminPeerGrpcTargetParser.getInstance().parse("proxy-a"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid proxy client admin peer target");
        assertThatThrownBy(() -> ProxyClientAdminPeerGrpcTargetParser.getInstance().parse(
            "proxy-a=127.0.0.1:8080,,proxy-b=127.0.0.2:8081"
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid proxy client admin peer target");
        assertThatThrownBy(() -> ProxyClientAdminPeerGrpcTargetParser.getInstance().parse(
            "proxy-a=127.0.0.1:8080,"
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid proxy client admin peer target");
        assertThatThrownBy(() -> ProxyClientAdminPeerGrpcTargetParser.getInstance().parse("=127.0.0.1:8080"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId is required");
        assertThatThrownBy(() -> ProxyClientAdminPeerGrpcTargetParser.getInstance().parse("proxy-a=:8080"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("host is required");
        assertThatThrownBy(() -> ProxyClientAdminPeerGrpcTargetParser.getInstance().parse("proxy-a=127.0.0.1"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("host:port");
    }

    @Test
    public void parseRejectsInvalidPorts() {
        assertThatThrownBy(() -> ProxyClientAdminPeerGrpcTargetParser.getInstance().parse("proxy-a=127.0.0.1:abc"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("port must be a number");
        assertThatThrownBy(() -> ProxyClientAdminPeerGrpcTargetParser.getInstance().parse("proxy-a=127.0.0.1:0"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("port must be between 1 and 65535");
        assertThatThrownBy(() -> ProxyClientAdminPeerGrpcTargetParser.getInstance().parse("proxy-a=127.0.0.1:65536"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("port must be between 1 and 65535");
    }
}
