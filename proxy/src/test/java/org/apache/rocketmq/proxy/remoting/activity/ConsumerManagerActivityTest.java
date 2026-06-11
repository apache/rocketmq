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
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.proxy.service.relay.ProxyChannel;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.NotifyUnsubscribeLiteRequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerManagerActivityTest extends InitConfigTest {
    private static final String GROUP = "group";
    private static final String CLIENT_ID = "clientId";
    
    private ConsumerManagerActivity consumerManagerActivity;
    
    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private ConsumerGroupInfo consumerGroupInfo;
    @Mock
    private ProxyRelayService consumerProxyRelayService;
    private CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> consumerRunningInfoFuture;
    private ProxyChannel consumerChannel;
    @Spy
    private ChannelHandlerContext ctx = new SimpleChannelHandlerContext(new SimpleChannel(null, "1", "2")) {
        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            return null;
        }
    };
    
    @Before
    public void setUp() {
        this.consumerManagerActivity = new ConsumerManagerActivity(null, messagingProcessor);
        this.consumerRunningInfoFuture = new CompletableFuture<>();
        this.consumerChannel = new ProxyChannel(consumerProxyRelayService, null, "127.0.0.1:1", "127.0.0.1:2") {
            @Override
            public boolean isOpen() {
                return true;
            }
            
            @Override
            public boolean isActive() {
                return true;
            }
            
            @Override
            protected CompletableFuture<Void> processOtherMessage(Object msg) {
                return CompletableFuture.completedFuture(null);
            }
            
            @Override
            protected CompletableFuture<Void> processCheckTransaction(CheckTransactionStateRequestHeader header,
                MessageExt messageExt, TransactionData transactionData,
                CompletableFuture<ProxyRelayResult<Void>> responseFuture) {
                return CompletableFuture.completedFuture(null);
            }
            
            @Override
            protected CompletableFuture<Void> processNotifyUnsubscribeLite(NotifyUnsubscribeLiteRequestHeader header) {
                return CompletableFuture.completedFuture(null);
            }
            
            @Override
            protected CompletableFuture<Void> processGetConsumerRunningInfo(RemotingCommand command,
                GetConsumerRunningInfoRequestHeader header,
                CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> responseFuture) {
                consumerRunningInfoFuture.thenAccept(responseFuture::complete);
                return CompletableFuture.completedFuture(null);
            }
            
            @Override
            protected CompletableFuture<Void> processConsumeMessageDirectly(RemotingCommand command,
                ConsumeMessageDirectlyResultRequestHeader header, MessageExt messageExt,
                CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> responseFuture) {
                return CompletableFuture.completedFuture(null);
            }
        };
    }
    
    @Test
    public void testGetConsumerRunningInfo() throws Exception {
        GetConsumerRunningInfoRequestHeader header = new GetConsumerRunningInfoRequestHeader();
        header.setConsumerGroup(GROUP);
        header.setClientId(CLIENT_ID);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, header);
        request.makeCustomHeaderToNet();
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(consumerChannel, CLIENT_ID, LanguageCode.JAVA, 0);

        when(messagingProcessor.getConsumerGroupInfo(any(), eq(GROUP))).thenReturn(consumerGroupInfo);
        when(consumerGroupInfo.findChannel(eq(CLIENT_ID))).thenReturn(clientChannelInfo);

        RemotingCommand response = consumerManagerActivity.processRequest0(ctx, request, null);
        assertThat(response).isNull();

        ConsumerRunningInfo runningInfo = new ConsumerRunningInfo();
        runningInfo.setJstack("jstack");
        consumerRunningInfoFuture.complete(new ProxyRelayResult<>(ResponseCode.SUCCESS, "ok", runningInfo));

        ArgumentCaptor<RemotingCommand> captor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(ctx, times(1)).writeAndFlush(captor.capture());
        assertThat(captor.getValue().getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(captor.getValue().getRemark()).isEqualTo("ok");
        assertThat(captor.getValue().getBody()).isEqualTo(runningInfo.encode());
    }
    
    @Test
    public void testGetConsumerRunningInfoWhenConsumerNotOnline() throws Exception {
        GetConsumerRunningInfoRequestHeader header = new GetConsumerRunningInfoRequestHeader();
        header.setConsumerGroup(GROUP);
        header.setClientId(CLIENT_ID);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, header);
        request.makeCustomHeaderToNet();

        when(messagingProcessor.getConsumerGroupInfo(any(), eq(GROUP))).thenReturn(null);

        RemotingCommand response = consumerManagerActivity.processRequest0(ctx, request, null);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains(GROUP, CLIENT_ID, "not online");
    }
}
