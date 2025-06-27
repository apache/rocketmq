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
package org.apache.rocketmq.client;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class ClientConfigTest {

    private ClientConfig clientConfig;

    private final String resource = "resource";

    @Before
    public void init() {
        clientConfig = createClientConfig();
    }

    @Test
    public void testWithNamespace() {
        Set<String> resources = clientConfig.withNamespace(Collections.singleton(resource));
        assertTrue(resources.contains("lmq%resource"));
    }

    @Test
    public void testWithoutNamespace() {
        String actual = clientConfig.withoutNamespace(resource);
        assertEquals(resource, actual);
        Set<String> resources = clientConfig.withoutNamespace(Collections.singleton(resource));
        assertTrue(resources.contains(resource));
    }

    @Test
    public void testQueuesWithNamespace() {
        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setTopic("defaultTopic");
        Collection<MessageQueue> messageQueues = clientConfig.queuesWithNamespace(Collections.singleton(messageQueue));
        assertTrue(messageQueues.contains(messageQueue));
        assertEquals("lmq%defaultTopic", messageQueues.iterator().next().getTopic());
    }

    @Test
    public void testClientIpOverrideWithSystemProperty() {
        String ip = "192.168.8.8";
        System.setProperty("rocketmq.client.ip", ip);
        ClientConfig clientConfig = new ClientConfig();
        assertEquals(ip, clientConfig.getClientIP());
        System.clearProperty("rocketmq.client.ip");
        clientConfig = new ClientConfig();
        assertEquals(NetworkUtil.getLocalAddress(), clientConfig.getClientIP());
    }

    private ClientConfig createClientConfig() {
        ClientConfig result = new ClientConfig();
        result.setUnitName("unitName");
        result.setClientIP("127.0.0.1");
        result.setClientCallbackExecutorThreads(1);
        result.setPollNameServerInterval(1000 * 30);
        result.setHeartbeatBrokerInterval(1000 * 30);
        result.setPersistConsumerOffsetInterval(1000 * 5);
        result.setPullTimeDelayMillsWhenException(1000);
        result.setUnitMode(true);
        result.setSocksProxyConfig("{}");
        result.setLanguage(LanguageCode.JAVA);
        result.setDecodeReadBody(true);
        result.setDecodeDecompressBody(true);
        result.setAccessChannel(AccessChannel.LOCAL);
        result.setMqClientApiTimeout(1000 * 3);
        result.setEnableStreamRequestType(true);
        result.setSendLatencyEnable(true);
        result.setEnableHeartbeatChannelEventListener(true);
        result.setDetectTimeout(200);
        result.setDetectInterval(1000 * 2);
        result.setUseHeartbeatV2(false);
        result.buildMQClientId();
        result.setNamespace("lmq");
        return result;
    }
}
