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
import org.apache.rocketmq.proxy.remoting.activity.AckMessageActivity;
import org.apache.rocketmq.proxy.remoting.activity.ChangeInvisibleTimeActivity;
import org.apache.rocketmq.proxy.remoting.activity.PopMessageActivity;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RequestHeaderRegistry;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

public class RemotingProtocolServerTest extends InitConfigTest {

    @Test
    public void shouldInitializeRequestHeaderRegistryWhenRegisteringProcessors() {
        RemotingProtocolServer protocolServer =
            mock(RemotingProtocolServer.class, Mockito.CALLS_REAL_METHODS);
        RemotingServer remotingServer = mock(RemotingServer.class);
        RequestHeaderRegistry requestHeaderRegistry = mock(RequestHeaderRegistry.class);

        try (MockedStatic<RequestHeaderRegistry> registry = mockStatic(RequestHeaderRegistry.class)) {
            registry.when(RequestHeaderRegistry::getInstance).thenReturn(requestHeaderRegistry);

            protocolServer.registerRemotingServer(remotingServer);

            verify(requestHeaderRegistry).initialize();
        }
    }

    @Test
    public void shouldRegisterThePopAckAndChangeInvisibleActivities() throws Exception {
        // POP_MESSAGE / ACK_MESSAGE / CHANGE_MESSAGE_INVISIBLETIME have dedicated activities; routing
        // them elsewhere cannot work: the pull activity decodes a pull request, and the consumer
        // manager activity has no case for them, so it returns no response at all (the requester
        // only fails by its own timeout, and the ack is never applied).
        RemotingProtocolServer protocolServer =
            new RemotingProtocolServer(mock(MessagingProcessor.class), mock(TlsCertificateManager.class));
        RemotingServer remotingServer = mock(RemotingServer.class);
        RequestHeaderRegistry requestHeaderRegistry = mock(RequestHeaderRegistry.class);
        try (MockedStatic<RequestHeaderRegistry> registry = mockStatic(RequestHeaderRegistry.class)) {
            registry.when(RequestHeaderRegistry::getInstance).thenReturn(requestHeaderRegistry);
            ArgumentCaptor<NettyRequestProcessor> processorCaptor =
                ArgumentCaptor.forClass(NettyRequestProcessor.class);

            protocolServer.registerRemotingServer(remotingServer);

            verify(remotingServer).registerProcessor(eq(RequestCode.POP_MESSAGE), processorCaptor.capture(), any());
            assertTrue("POP_MESSAGE has to be served by the pop activity",
                processorCaptor.getValue() instanceof PopMessageActivity);
            verify(remotingServer).registerProcessor(eq(RequestCode.ACK_MESSAGE), processorCaptor.capture(), any());
            assertTrue("ACK_MESSAGE has to be served by the ack activity",
                processorCaptor.getValue() instanceof AckMessageActivity);
            verify(remotingServer).registerProcessor(eq(RequestCode.CHANGE_MESSAGE_INVISIBLETIME),
                processorCaptor.capture(), any());
            assertTrue("CHANGE_MESSAGE_INVISIBLETIME has to be served by the change-invisible-time activity",
                processorCaptor.getValue() instanceof ChangeInvisibleTimeActivity);
        } finally {
            protocolServer.shutdown();
        }
    }
}
