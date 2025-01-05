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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.proxy.grpc;

import io.grpc.Attributes;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import io.grpc.netty.shaded.io.netty.handler.codec.haproxy.HAProxyTLV;
import java.nio.charset.StandardCharsets;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProxyAndTlsProtocolNegotiatorTest {

    private ProxyAndTlsProtocolNegotiator negotiator;

    @Before
    public void setUp() throws Exception {
        if (!MixAll.isJdk8()) {
            return;
        }
        ConfigurationManager.intConfig();
        ConfigurationManager.getProxyConfig().setTlsTestModeEnable(true);
        negotiator = new ProxyAndTlsProtocolNegotiator();
    }

    @Test
    public void handleHAProxyTLV() {
        if (!MixAll.isJdk8()) {
            return;
        }
        ByteBuf content = Unpooled.buffer();
        content.writeBytes("xxxx".getBytes(StandardCharsets.UTF_8));
        HAProxyTLV haProxyTLV = new HAProxyTLV((byte) 0xE1, content);
        negotiator.handleHAProxyTLV(haProxyTLV, Attributes.newBuilder());
    }
}
