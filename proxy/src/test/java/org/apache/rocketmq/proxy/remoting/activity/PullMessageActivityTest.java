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

package org.apache.rocketmq.proxy.remoting.activity;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PullMessageActivityTest extends InitConfigTest {
    PullMessageActivity pullMessageActivity;

    @Mock
    MessagingProcessor messagingProcessorMock;
    @Mock
    ConsumerGroupInfo consumerGroupInfoMock;

    String topic = "topic";
    String group = "group";
    String brokerName = "brokerName";
    String subString = "sub";
    String type = "type";
    @Spy
    ChannelHandlerContext ctx = new SimpleChannelHandlerContext(new SimpleChannel(null, "1", "2")) {
        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            return null;
        }
    };

    @Before
    public void setup() throws Exception {
        pullMessageActivity = new PullMessageActivity(null, messagingProcessorMock);
    }

    @Test
    public void testPullMessageWithoutSub() throws Exception {
        when(messagingProcessorMock.getConsumerGroupInfo(eq(group)))
            .thenReturn(consumerGroupInfoMock);
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setSubString(subString);
        subscriptionData.setExpressionType(type);
        when(consumerGroupInfoMock.findSubscriptionData(eq(topic)))
            .thenReturn(subscriptionData);

        PullMessageRequestHeader header = new PullMessageRequestHeader();
        header.setTopic(topic);
        header.setConsumerGroup(group);
        header.setQueueId(0);
        header.setQueueOffset(0L);
        header.setMaxMsgNums(16);
        header.setSysFlag(PullSysFlag.buildSysFlag(true, false, false, false));
        header.setCommitOffset(0L);
        header.setSuspendTimeoutMillis(1000L);
        header.setSubVersion(0L);
        header.setBname(brokerName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, header);
        request.makeCustomHeaderToNet();
        RemotingCommand expectResponse = RemotingCommand.createResponseCommand(ResponseCode.NO_MESSAGE, "success");
        PullMessageRequestHeader newHeader = new PullMessageRequestHeader();
        newHeader.setTopic(topic);
        newHeader.setConsumerGroup(group);
        newHeader.setQueueId(0);
        newHeader.setQueueOffset(0L);
        newHeader.setMaxMsgNums(16);
        newHeader.setSysFlag(PullSysFlag.buildSysFlag(true, false, true, false));
        newHeader.setCommitOffset(0L);
        newHeader.setSuspendTimeoutMillis(1000L);
        newHeader.setSubVersion(0L);
        newHeader.setBname(brokerName);
        newHeader.setSubscription(subString);
        newHeader.setExpressionType(type);
        RemotingCommand matchRequest = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, newHeader);
        matchRequest.setOpaque(request.getOpaque());
        matchRequest.makeCustomHeaderToNet();

        ArgumentCaptor<RemotingCommand> captor = ArgumentCaptor.forClass(RemotingCommand.class);
        when(messagingProcessorMock.request(any(), eq(brokerName), captor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(expectResponse));
        RemotingCommand response = pullMessageActivity.processRequest0(ctx, request, null);
        assertThat(captor.getValue().getExtFields()).isEqualTo(matchRequest.getExtFields());
        assertThat(response).isNull();
        verify(ctx, times(1)).writeAndFlush(eq(expectResponse));
    }

    @Test
    public void testPullMessageWithSub() throws Exception {
        when(messagingProcessorMock.getConsumerGroupInfo(eq(group)))
            .thenReturn(consumerGroupInfoMock);
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setSubString(subString);
        subscriptionData.setExpressionType(type);
        when(consumerGroupInfoMock.findSubscriptionData(eq(topic)))
            .thenReturn(subscriptionData);

        PullMessageRequestHeader header = new PullMessageRequestHeader();
        header.setTopic(topic);
        header.setConsumerGroup(group);
        header.setQueueId(0);
        header.setQueueOffset(0L);
        header.setMaxMsgNums(16);
        header.setSysFlag(PullSysFlag.buildSysFlag(true, true, false, false));
        header.setCommitOffset(0L);
        header.setSuspendTimeoutMillis(1000L);
        header.setSubVersion(0L);
        header.setBname(brokerName);
        header.setSubscription(subString);
        header.setExpressionType(type);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, header);
        request.makeCustomHeaderToNet();
        RemotingCommand expectResponse = RemotingCommand.createResponseCommand(ResponseCode.NO_MESSAGE, "success");

        ArgumentCaptor<RemotingCommand> captor = ArgumentCaptor.forClass(RemotingCommand.class);
        when(messagingProcessorMock.request(any(), eq(brokerName), captor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(expectResponse));
        RemotingCommand response = pullMessageActivity.processRequest0(ctx, request, null);
        assertThat(captor.getValue().getExtFields()).isEqualTo(request.getExtFields());
        assertThat(response).isNull();
        verify(ctx, times(1)).writeAndFlush(eq(expectResponse));
    }
}