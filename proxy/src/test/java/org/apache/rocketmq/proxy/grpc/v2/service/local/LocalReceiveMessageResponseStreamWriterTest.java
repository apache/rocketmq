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

package org.apache.rocketmq.proxy.grpc.v2.service.local;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.ChangeInvisibleTimeProcessor;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyException;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.service.ReceiveMessageResultFilter;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class LocalReceiveMessageResponseStreamWriterTest {
    @Mock
    private ServerCallStreamObserver<ReceiveMessageResponse> streamObserverMock;
    @Mock
    private ChannelManager channelManagerMock;
    @Mock
    private BrokerController brokerControllerMock;
    @Mock
    private ReceiveMessageResultFilter receiveMessageResultFilterMock;
    @Mock
    private ChangeInvisibleTimeProcessor changeInvisibleTimeProcessorMock;

    private LocalReceiveMessageResponseStreamWriter localReceiveMessageResponseStreamWriter;

    @Before
    public void setup() {
        Mockito.when(receiveMessageResultFilterMock.filterMessage(Mockito.any(), Mockito.any(), Mockito.anyList())).thenAnswer((Answer<List<Message>>) invocation -> {
            List<Message> messageList = new ArrayList<>();
            List<MessageExt> messageExtList = invocation.getArgument(2);
            for (MessageExt messageExt : messageExtList) {
                messageList.add(GrpcConverter.buildMessage(messageExt));
            }
            return messageList;
        });
        localReceiveMessageResponseStreamWriter
            = new LocalReceiveMessageResponseStreamWriter(streamObserverMock, null, channelManagerMock, brokerControllerMock, receiveMessageResultFilterMock);
    }

    @Test
    public void testWrite() {
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic("topic");
        messageExt.setQueueOffset(0L);
        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 10911));
        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        messageExt.setBody("body".getBytes(StandardCharsets.UTF_8));
        messageExt.putUserProperty("key", "value");
        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(0L)
            .invisibleTime(1000L)
            .nextVisibleTime(1000L)
            .reviveQueueId(0)
            .topicType("0")
            .brokerName("brokerName")
            .queueId(0)
            .offset(0L)
            .build().encode());
        messageExt.putUserProperty("key", "value");
        List<MessageExt> messageExtList = new ArrayList<>();
        messageExtList.add(messageExt);
        localReceiveMessageResponseStreamWriter.write(Context.current(), ReceiveMessageRequest.newBuilder().build(), PopStatus.FOUND, messageExtList);
        ArgumentCaptor<ReceiveMessageResponse> argument = ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        Mockito.verify(streamObserverMock, Mockito.times(2)).onNext(argument.capture());
        assertThat(argument.getAllValues().get(0)).isEqualTo(ReceiveMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name())).build());
        assertThat(argument.getAllValues().get(1)).isEqualTo(ReceiveMessageResponse.newBuilder()
            .setMessage(GrpcConverter.buildMessage(messageExt)).build());
    }

    @Test
    public void testWriteWhenNoMessage() {
        localReceiveMessageResponseStreamWriter.write(Context.current(), ReceiveMessageRequest.newBuilder().build(), PopStatus.FOUND, new ArrayList<>());
        Mockito.verify(streamObserverMock, Mockito.times(1)).onNext(Mockito.eq(ReceiveMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.OK, "no new message")).build()));
    }

    @Test
    public void testWriteWhenCancel() throws RemotingCommandException {
        AtomicInteger onNextCallTimes = new AtomicInteger(0);
        Mockito.doAnswer(mock -> {
            if (onNextCallTimes.get() <=0) {
                onNextCallTimes.incrementAndGet();
                return null;
            }
            throw new StatusRuntimeException(Status.CANCELLED);
        }).when(streamObserverMock).onNext(Mockito.any());
        Mockito.when(brokerControllerMock.getChangeInvisibleTimeProcessor()).thenReturn(changeInvisibleTimeProcessorMock);
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic("topic");
        messageExt.setQueueOffset(0L);
        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 10911));
        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        messageExt.setBody("body".getBytes(StandardCharsets.UTF_8));
        messageExt.putUserProperty("key", "value");
        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(0L)
            .invisibleTime(1000L)
            .nextVisibleTime(1000L)
            .reviveQueueId(0)
            .topicType("0")
            .brokerName("brokerName")
            .queueId(0)
            .offset(0L)
            .build().encode());
        messageExt.putUserProperty("key", "value");
        List<MessageExt> messageExtList = new ArrayList<>();
        messageExtList.add(messageExt);
        localReceiveMessageResponseStreamWriter.write(Context.current(), ReceiveMessageRequest.newBuilder().build(), PopStatus.FOUND, messageExtList);
        Mockito.verify(streamObserverMock, Mockito.times(1)).onNext(Mockito.eq(ReceiveMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name())).build()));
        Mockito.verify(changeInvisibleTimeProcessorMock, Mockito.times(1)).processRequest(Mockito.any(), Mockito.any());
    }

    @Test
    public void testWriteError() {
        String info = "error";
        localReceiveMessageResponseStreamWriter.write(Context.current(), ReceiveMessageRequest.newBuilder().build(), new ProxyException(Code.ILLEGAL_MESSAGE, info));
        Mockito.verify(streamObserverMock, Mockito.times(1)).onNext(Mockito.eq(ReceiveMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.ILLEGAL_MESSAGE, info)).build()));
    }
}