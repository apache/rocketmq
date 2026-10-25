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
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.protocol.RequestHeaderRegistry;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

public class RemotingProtocolServerTest {

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

    @Test(timeout = 1000)
    public void testCleanExpiredRequestInQueueBreaksWhenQueueAccessFails() {
        RemotingProtocolServer server = Mockito.mock(RemotingProtocolServer.class, Mockito.CALLS_REAL_METHODS);
        ThreadPoolExecutor executor = Mockito.mock(ThreadPoolExecutor.class);
        Mockito.when(executor.getQueue()).thenThrow(new RuntimeException("queue unavailable"));

        server.cleanExpiredRequestInQueue(executor, 1);

        Mockito.verify(executor, Mockito.atMost(2)).getQueue();
    }
}
