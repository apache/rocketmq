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

package org.apache.rocketmq.proxy.remoting;

import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RemotingProtocolServerTest extends InitConfigTest {
    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private TlsCertificateManager tlsCertificateManager;
    @Mock
    private ProxyRelayService proxyRelayService;
    private RemotingProtocolServer remotingProtocolServer;

    @Before
    public void setUp() throws Exception {
        when(messagingProcessor.getProxyRelayService()).thenReturn(proxyRelayService);
        remotingProtocolServer = new RemotingProtocolServer(messagingProcessor, tlsCertificateManager);
    }

    @After
    public void tearDown() throws Exception {
        if (remotingProtocolServer != null) {
            remotingProtocolServer.shutdown();
        }
    }

    @Test
    public void testRegisterGetConsumerRunningInfoProcessor() {
        RemotingServer remotingServer = mock(RemotingServer.class);

        remotingProtocolServer.registerRemotingServer(remotingServer);

        verify(remotingServer).registerProcessor(
            eq(RequestCode.GET_CONSUMER_RUNNING_INFO),
            same(remotingProtocolServer.consumerManagerActivity),
            same(remotingProtocolServer.defaultExecutor));
    }
}
