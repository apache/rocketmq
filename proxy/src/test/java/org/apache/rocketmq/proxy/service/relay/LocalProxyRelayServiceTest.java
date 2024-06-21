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

package org.apache.rocketmq.proxy.service.relay;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.proxy.service.transaction.TransactionService;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class LocalProxyRelayServiceTest {
    private LocalProxyRelayService localProxyRelayService;
    @Mock
    private BrokerController brokerControllerMock;
    @Mock
    private TransactionService transactionService;
    @Mock
    private NettyRemotingServer nettyRemotingServerMock;

    @Before
    public void setUp() {
        localProxyRelayService = new LocalProxyRelayService(brokerControllerMock, transactionService);
        Mockito.when(brokerControllerMock.getRemotingServer()).thenReturn(nettyRemotingServerMock);
    }

    @Test
    public void testProcessGetConsumerRunningInfo() {
        ConsumerRunningInfo runningInfo = new ConsumerRunningInfo();
        runningInfo.setJstack("jstack");
        String remark = "ok";
        int opaque = 123;
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, null);
        remotingCommand.setOpaque(opaque);
        GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
        requestHeader.setJstackEnable(true);
        ArgumentCaptor<RemotingCommand> argumentCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> future =
            localProxyRelayService.processGetConsumerRunningInfo(ProxyContext.create(), remotingCommand, requestHeader);
        future.complete(new ProxyRelayResult<>(ResponseCode.SUCCESS, remark, runningInfo));
        Mockito.verify(nettyRemotingServerMock, Mockito.times(1))
            .processResponseCommand(Mockito.any(SimpleChannelHandlerContext.class), argumentCaptor.capture());
        RemotingCommand remotingCommand1 = argumentCaptor.getValue();
        assertThat(remotingCommand1.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(remotingCommand1.getRemark()).isEqualTo(remark);
        assertThat(remotingCommand1.getBody()).isEqualTo(runningInfo.encode());
    }

    @Test
    public void testProcessConsumeMessageDirectly() {
        ConsumeMessageDirectlyResultRequestHeader requestHeader = new ConsumeMessageDirectlyResultRequestHeader();
        String remark = "ok";
        int opaque = 123;
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, null);
        remotingCommand.setOpaque(opaque);
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setConsumeResult(CMResult.CR_SUCCESS);
        ArgumentCaptor<RemotingCommand> argumentCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> future =
            localProxyRelayService.processConsumeMessageDirectly(ProxyContext.create(), remotingCommand, requestHeader);
        future.complete(new ProxyRelayResult<>(ResponseCode.SUCCESS, remark, result));
        Mockito.verify(nettyRemotingServerMock, Mockito.times(1))
            .processResponseCommand(Mockito.any(SimpleChannelHandlerContext.class), argumentCaptor.capture());
        RemotingCommand remotingCommand1 = argumentCaptor.getValue();
        assertThat(remotingCommand1.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(remotingCommand1.getRemark()).isEqualTo(remark);
        assertThat(remotingCommand1.getBody()).isEqualTo(result.encode());
    }
}
