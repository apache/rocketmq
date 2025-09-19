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
package org.apache.rocketmq.controller.impl;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.controller.impl.heartbeat.BrokerIdentityInfo;
import org.apache.rocketmq.controller.impl.heartbeat.RaftBrokerHeartBeatManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RaftBrokerHeartBeatManagerTest {
    @Mock
    private JRaftController jRaftController;
    @Mock
    private Channel brokerChannel;
    private RaftBrokerHeartBeatManager heartbeatManager;
    private final ControllerConfig config = new ControllerConfig();

    @Before
    public void init() {
        when(jRaftController.isLeaderState()).thenReturn(true);
        config.setScanNotActiveBrokerInterval(1000);
        this.heartbeatManager = new RaftBrokerHeartBeatManager(config);
        this.heartbeatManager.setController(jRaftController);
        this.heartbeatManager.initialize();
        this.heartbeatManager.start();
    }

    @Test
    public void testDetectBrokerAlive() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        this.heartbeatManager.registerBrokerLifecycleListener((clusterName, brokerName, brokerId) -> {
            latch.countDown(); // onBrokerInactive
        });
        String clusterName = "cluster-1";
        String brokerName = "broker-1";
        String brokerAddr = "127.0.0.1:10911";
        long brokerId = 1L;
        RemotingCommand onBrokerHeartbeat = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        RemotingCommand checkNotActiveResp1 = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        checkNotActiveResp1.setBody(JSON.toJSONBytes(Collections.emptyList()));
        RemotingCommand checkNotActiveResp2 = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        checkNotActiveResp2.setBody(JSON.toJSONBytes(Collections.singletonList(new BrokerIdentityInfo(clusterName, brokerName, brokerId))));
        when(jRaftController.onBrokerHeartBeat(any()))
            .thenReturn(CompletableFuture.completedFuture(onBrokerHeartbeat));
        when(jRaftController.checkNotActiveBroker(any()))
            .thenReturn(CompletableFuture.completedFuture(checkNotActiveResp1))
            .thenReturn(CompletableFuture.completedFuture(checkNotActiveResp2));
        DefaultChannelPromise channelPromise = new DefaultChannelPromise(brokerChannel);
        channelPromise.setSuccess();
        when(brokerChannel.close()).thenReturn(channelPromise);
        this.heartbeatManager.onBrokerHeartbeat(clusterName, brokerName, brokerAddr, brokerId, 3000L, brokerChannel,
            1, 1L, -1L, 0);
        assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
        this.heartbeatManager.shutdown();
    }
}
