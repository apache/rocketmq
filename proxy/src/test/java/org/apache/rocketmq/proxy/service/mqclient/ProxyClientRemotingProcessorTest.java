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

package org.apache.rocketmq.proxy.service.mqclient;

import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.proxy.service.client.ProxyClientRemotingProcessor;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.relay.RelayData;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyClientRemotingProcessorTest {
    @Mock
    private ProducerManager producerManager;
    @Mock
    private GrpcClientSettingsManager grpcClientSettingsManager;
    @Mock
    private ProxyRelayService proxyRelayService;

    @Test
    public void testTransactionCheck() throws Exception {
        CompletableFuture<ProxyRelayResult<Void>> proxyRelayResultFuture = new CompletableFuture<>();
        when(proxyRelayService.processCheckTransactionState(any(), any(), any(), any()))
            .thenReturn(new RelayData<>(
                new TransactionData("brokerName", 0, 0, "id", System.currentTimeMillis(), 3000),
                proxyRelayResultFuture));

        GrpcClientChannel grpcClientChannel = new GrpcClientChannel(proxyRelayService, grpcClientSettingsManager, null,
            ProxyContext.create().setRemoteAddress("127.0.0.1:8888").setLocalAddress("127.0.0.1:10911"), "clientId");
        when(producerManager.getAvailableChannel(anyString()))
            .thenReturn(grpcClientChannel);

        ProxyClientRemotingProcessor processor = new ProxyClientRemotingProcessor(producerManager);
        CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, requestHeader);
        MessageExt message = new MessageExt();
        message.setQueueId(0);
        message.setFlag(12);
        message.setQueueOffset(0L);
        message.setCommitLogOffset(100L);
        message.setSysFlag(0);
        message.setBornTimestamp(System.currentTimeMillis());
        message.setBornHost(new InetSocketAddress("127.0.0.1", 10));
        message.setStoreTimestamp(System.currentTimeMillis());
        message.setStoreHost(new InetSocketAddress("127.0.0.1", 11));
        message.setBody("body".getBytes());
        message.setTopic("topic");
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_PRODUCER_GROUP, "group");
        command.setBody(MessageDecoder.encode(message, false));

        processor.processRequest(new MockChannelHandlerContext(null), command);

        ServerCallStreamObserver<TelemetryCommand> observer = mock(ServerCallStreamObserver.class);
        grpcClientChannel.setClientObserver(observer);

        processor.processRequest(new MockChannelHandlerContext(null), command);
        verify(observer, times(1)).onNext(any());

        // throw exception to test clear observer
        doThrow(new StatusRuntimeException(Status.CANCELLED)).when(observer).onNext(any());

        ExecutorService executorService = Executors.newCachedThreadPool();
        AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < 100; i++) {
            executorService.submit(() -> {
                try {
                    processor.processRequest(new MockChannelHandlerContext(null), command);
                    count.incrementAndGet();
                } catch (RemotingCommandException ignored) {
                }
            });
        }
        await().atMost(Duration.ofSeconds(1)).until(() -> count.get() == 100);
        verify(observer, times(2)).onNext(any());
    }

    protected static class MockChannelHandlerContext extends SimpleChannelHandlerContext {

        public MockChannelHandlerContext(Channel channel) {
            super(channel);
        }

        @Override
        public Channel channel() {
            Channel channel = mock(Channel.class);
            when(channel.remoteAddress()).thenReturn(NetworkUtil.string2SocketAddress("127.0.0.1:10911"));
            return channel;
        }
    }
}
