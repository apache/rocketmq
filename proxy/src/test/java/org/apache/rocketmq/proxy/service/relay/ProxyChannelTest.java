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

package org.apache.rocketmq.proxy.service.relay;

import io.netty.channel.Channel;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyChannelTest {

    @Mock
    private ProxyRelayService proxyRelayService;

    protected abstract static class MockProxyChannel extends ProxyChannel {

        protected MockProxyChannel(ProxyRelayService proxyRelayService, Channel parent,
            String remoteAddress, String localAddress) {
            super(proxyRelayService, parent, remoteAddress, localAddress);
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public boolean isActive() {
            return false;
        }
    }

    @Test
    public void testWriteAndFlush() throws Exception {
        when(this.proxyRelayService.processCheckTransactionState(any(), any(), any(), any()))
            .thenReturn(new RelayData<>(mock(TransactionData.class), new CompletableFuture<>()));

        ArgumentCaptor<ConsumeMessageDirectlyResultRequestHeader> consumeMessageDirectlyArgumentCaptor =
            ArgumentCaptor.forClass(ConsumeMessageDirectlyResultRequestHeader.class);
        when(this.proxyRelayService.processConsumeMessageDirectly(any(), any(), consumeMessageDirectlyArgumentCaptor.capture()))
            .thenReturn(new CompletableFuture<>());

        ArgumentCaptor<GetConsumerRunningInfoRequestHeader> getConsumerRunningInfoArgumentCaptor =
            ArgumentCaptor.forClass(GetConsumerRunningInfoRequestHeader.class);
        when(this.proxyRelayService.processGetConsumerRunningInfo(any(), any(), getConsumerRunningInfoArgumentCaptor.capture()))
            .thenReturn(new CompletableFuture<>());

        CheckTransactionStateRequestHeader checkTransactionStateRequestHeader = new CheckTransactionStateRequestHeader();
        checkTransactionStateRequestHeader.setTransactionId(MessageClientIDSetter.createUniqID());
        RemotingCommand checkTransactionRequest = RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, checkTransactionStateRequestHeader);
        MessageExt transactionMessageExt = new MessageExt();
        transactionMessageExt.setTopic("topic");
        transactionMessageExt.setTags("tags");
        transactionMessageExt.setBornHost(RemotingUtil.string2SocketAddress("127.0.0.2:8888"));
        transactionMessageExt.setStoreHost(RemotingUtil.string2SocketAddress("127.0.0.1:10911"));
        transactionMessageExt.setBody(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        transactionMessageExt.setMsgId(MessageClientIDSetter.createUniqID());
        checkTransactionRequest.setBody(MessageDecoder.encode(transactionMessageExt, false));

        GetConsumerRunningInfoRequestHeader consumerRunningInfoRequestHeader = new GetConsumerRunningInfoRequestHeader();
        consumerRunningInfoRequestHeader.setConsumerGroup("group");
        consumerRunningInfoRequestHeader.setClientId("clientId");
        RemotingCommand consumerRunningInfoRequest = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, consumerRunningInfoRequestHeader);

        ConsumeMessageDirectlyResultRequestHeader consumeMessageDirectlyResultRequestHeader = new ConsumeMessageDirectlyResultRequestHeader();
        consumeMessageDirectlyResultRequestHeader.setConsumerGroup("group");
        consumeMessageDirectlyResultRequestHeader.setClientId("clientId");
        MessageExt consumeMessageDirectlyMessageExt = new MessageExt();
        consumeMessageDirectlyMessageExt.setTopic("topic");
        consumeMessageDirectlyMessageExt.setTags("tags");
        consumeMessageDirectlyMessageExt.setBornHost(RemotingUtil.string2SocketAddress("127.0.0.2:8888"));
        consumeMessageDirectlyMessageExt.setStoreHost(RemotingUtil.string2SocketAddress("127.0.0.1:10911"));
        consumeMessageDirectlyMessageExt.setBody(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        consumeMessageDirectlyMessageExt.setMsgId(MessageClientIDSetter.createUniqID());
        RemotingCommand consumeMessageDirectlyResult = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, consumeMessageDirectlyResultRequestHeader);
        consumeMessageDirectlyResult.setBody(MessageDecoder.encode(consumeMessageDirectlyMessageExt, false));

        MockProxyChannel channel = new MockProxyChannel(this.proxyRelayService, null, "127.0.0.2:8888", "127.0.0.1:10911") {
            @Override
            protected CompletableFuture<Void> processOtherMessage(Object msg) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            protected CompletableFuture<Void> processCheckTransaction(CheckTransactionStateRequestHeader header,
                MessageExt messageExt, TransactionData transactionData, CompletableFuture<ProxyRelayResult<Void>> responseFuture) {
                assertEquals(checkTransactionStateRequestHeader, header);
                assertArrayEquals(transactionMessageExt.getBody(), messageExt.getBody());
                return CompletableFuture.completedFuture(null);
            }

            @Override
            protected CompletableFuture<Void> processGetConsumerRunningInfo(RemotingCommand command,
                GetConsumerRunningInfoRequestHeader header,
                CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> responseFuture) {
                assertEquals(consumerRunningInfoRequestHeader, getConsumerRunningInfoArgumentCaptor.getValue());
                assertEquals(consumerRunningInfoRequestHeader, header);
                return CompletableFuture.completedFuture(null);
            }

            @Override
            protected CompletableFuture<Void> processConsumeMessageDirectly(RemotingCommand command,
                ConsumeMessageDirectlyResultRequestHeader header, MessageExt messageExt,
                CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> responseFuture) {
                assertEquals(consumeMessageDirectlyResultRequestHeader, consumeMessageDirectlyArgumentCaptor.getValue());
                assertEquals(consumeMessageDirectlyResultRequestHeader, header);
                assertArrayEquals(consumeMessageDirectlyMessageExt.getBody(), messageExt.getBody());
                return CompletableFuture.completedFuture(null);
            }
        };

        assertTrue(channel.writeAndFlush(checkTransactionRequest).isSuccess());
        assertTrue(channel.writeAndFlush(consumerRunningInfoRequest).isSuccess());
        assertTrue(channel.writeAndFlush(consumeMessageDirectlyResult).isSuccess());
    }
}