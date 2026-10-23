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

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannelManager;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.protocol.RequestHeaderRegistry;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class RemotingProtocolServerTest {

    @Test
    public void testShutdownStopsTheExpiredRequestTimer() throws Exception {
        RemotingProtocolServer protocolServer = mock(RemotingProtocolServer.class, Mockito.CALLS_REAL_METHODS);
        FieldUtils.writeDeclaredField(protocolServer, "tlsCertificateManager", mock(TlsCertificateManager.class), true);
        FieldUtils.writeDeclaredField(protocolServer, "remotingChannelManager", mock(RemotingChannelManager.class), true);
        FieldUtils.writeDeclaredField(protocolServer, "defaultRemotingServer", mock(RemotingServer.class), true);
        for (String field : new String[] {"sendMessageExecutor", "pullMessageExecutor", "heartbeatExecutor",
            "updateOffsetExecutor", "topicRouteExecutor", "defaultExecutor"}) {
            FieldUtils.writeDeclaredField(protocolServer, field, mock(ThreadPoolExecutor.class), true);
        }
        ScheduledExecutorService timerExecutor = mock(ScheduledExecutorService.class);
        FieldUtils.writeDeclaredField(protocolServer, "timerExecutor", timerExecutor, true);

        protocolServer.shutdown();

        // The constructor schedules the expired-request cleanup on this executor; a shutdown that
        // leaves it running keeps a non-daemon thread (and its 100ms task) alive after the server.
        verify(timerExecutor, times(1)).shutdown();
    }

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
}
