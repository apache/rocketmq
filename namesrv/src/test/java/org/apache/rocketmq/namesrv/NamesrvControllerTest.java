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

package org.apache.rocketmq.namesrv;

import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class NamesrvControllerTest {

    @Mock
    private NettyServerConfig nettyServerConfig;
    @Mock
    private RemotingServer remotingServer;

    private NamesrvController namesrvController;

    @Before
    public void setUp() throws Exception {
        NamesrvConfig namesrvConfig = new NamesrvConfig();
        namesrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
    }

    @Test
    public void testShutdownMarksStateBeforeClosingResources() throws Exception {
        RemotingClient remotingClient = mock(RemotingClient.class);
        FieldUtils.writeField(namesrvController, "remotingClient", remotingClient, true);
        FieldUtils.writeField(namesrvController, "defaultExecutor", mock(ExecutorService.class), true);
        FieldUtils.writeField(namesrvController, "clientRequestExecutor", mock(ExecutorService.class), true);
        namesrvController.setRemotingServer(remotingServer);
        doAnswer(invocation -> {
            Assert.assertTrue(namesrvController.isShutdown());
            return null;
        }).when(remotingClient).shutdown();
        doAnswer(invocation -> {
            Assert.assertTrue(namesrvController.isShutdown());
            return null;
        }).when(remotingServer).shutdown();

        Assert.assertFalse(namesrvController.isShutdown());
        namesrvController.shutdown();
        Assert.assertTrue(namesrvController.isShutdown());
        verify(remotingClient).shutdown();
        verify(remotingServer).shutdown();
    }

    @Test
    public void getNamesrvConfig() {
        NamesrvConfig namesrvConfig = namesrvController.getNamesrvConfig();
        Assert.assertNotNull(namesrvConfig);
    }

    @Test
    public void getNettyServerConfig() {
        NettyServerConfig nettyServerConfig = namesrvController.getNettyServerConfig();
        Assert.assertNotNull(nettyServerConfig);
    }

    @Test
    public void getKvConfigManager() {
        KVConfigManager manager = namesrvController.getKvConfigManager();
        Assert.assertNotNull(manager);
    }

    @Test
    public void getRouteInfoManager() {
        RouteInfoManager manager = namesrvController.getRouteInfoManager();
        Assert.assertNotNull(manager);
    }

    @Test
    public void getRemotingServer() {
        RemotingServer server = namesrvController.getRemotingServer();
        Assert.assertNull(server);
    }

    @Test
    public void setRemotingServer() {
        namesrvController.setRemotingServer(remotingServer);
        RemotingServer server = namesrvController.getRemotingServer();
        Assert.assertEquals(remotingServer, server);
    }

    @Test
    public void getConfiguration() {
        Configuration configuration = namesrvController.getConfiguration();
        Assert.assertNotNull(configuration);
    }
}
