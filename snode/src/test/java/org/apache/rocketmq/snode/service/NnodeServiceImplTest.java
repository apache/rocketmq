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
package org.apache.rocketmq.snode.service;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.ServerConfig;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.rocketmq.NettyRemotingClient;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.SnodeTestBase;
import org.apache.rocketmq.snode.config.SnodeConfig;
import org.apache.rocketmq.snode.service.impl.NnodeServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NnodeServiceImplTest extends SnodeTestBase {

    @Spy
    private SnodeController snodeController = new SnodeController(new ServerConfig(), new ClientConfig(), new SnodeConfig());

    @Mock
    private NettyRemotingClient remotingClient;

    private NnodeService nnodeService;

    @Before
    public void init() {
        snodeController.setRemotingClient(remotingClient);
        nnodeService = new NnodeServiceImpl(snodeController);
    }

    @Test
    public void registerSnodeTest() throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException {
        when(snodeController.getRemotingClient().getNameServerAddressList()).thenReturn(createNnodeList());
        when(snodeController.getRemotingClient().invokeSync(anyString(), any(RemotingCommand.class), anyLong())).thenReturn(createSuccessResponse());
        try {
            nnodeService.registerSnode(createSnodeConfig());
        } catch (Exception ex) {
            assertThat(ex).isNull();
        }
    }

    private List createNnodeList() {
        List<String> addresses = new ArrayList<>();
        addresses.add("127.0.0.1:9876");
        return addresses;
    }

    private SnodeConfig createSnodeConfig() {
        SnodeConfig snodeConfig = new SnodeConfig();
        snodeConfig.setClusterName("defaultCluster");
        snodeConfig.setSnodeIP1("127.0.0.1:10911");
        snodeConfig.setSnodeName("snode-a");
        return snodeConfig;
    }
}
