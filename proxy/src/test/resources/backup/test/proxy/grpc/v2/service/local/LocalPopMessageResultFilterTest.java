///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.rocketmq.proxy.grpc.v2.service.local;
//
//import apache.rocketmq.v2.FilterExpression;
//import apache.rocketmq.v2.ReceiveMessageRequest;
//import apache.rocketmq.v2.Settings;
//import io.grpc.Context;
//import java.net.InetSocketAddress;
//import java.nio.charset.StandardCharsets;
//import java.util.ArrayList;
//import java.util.List;
//import org.apache.rocketmq.broker.BrokerController;
//import org.apache.rocketmq.broker.processor.AckMessageProcessor;
//import org.apache.rocketmq.broker.processor.SendMessageProcessor;
//import org.apache.rocketmq.common.consumer.ReceiptHandle;
//import org.apache.rocketmq.common.message.MessageConst;
//import org.apache.rocketmq.common.message.MessageExt;
//import org.apache.rocketmq.common.protocol.RequestCode;
//import org.apache.rocketmq.common.protocol.ResponseCode;
//import org.apache.rocketmq.proxy.grpc.v2.common.ChannelManager;
//import org.apache.rocketmq.proxy.channel.SimpleChannel;
//import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
//import org.apache.rocketmq.proxy.grpc.v2.service.GrpcClientManager;
//import org.apache.rocketmq.remoting.exception.RemotingCommandException;
//import org.apache.rocketmq.remoting.protocol.RemotingCommand;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.Mockito;
//import org.mockito.junit.MockitoJUnitRunner;
//
//@RunWith(MockitoJUnitRunner.class)
//public class LocalPopMessageResultFilterTest extends InitConfigAndLoggerTest {
//    @Mock
//    private ChannelManager channelManagerMock;
//    @Mock
//    private BrokerController brokerControllerMock;
//    @Mock
//    private GrpcClientManager grpcClientManagerMock;
//    @Mock
//    private AckMessageProcessor ackMessageProcessorMock;
//    @Mock
//    private SendMessageProcessor sendMessageProcessorMock;
//
//    private String topic = "topic";
//
//    @Test
//    public void testFilterMessageWhenNotMatch() throws RemotingCommandException {
//        RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
//        Mockito.when(channelManagerMock.createChannel(Mockito.any(Context.class))).thenReturn(new SimpleChannel(null, null, null, 0));
//        Mockito.when(brokerControllerMock.getAckMessageProcessor()).thenReturn(ackMessageProcessorMock);
//        Mockito.when(ackMessageProcessorMock.processRequest(Mockito.any(), Mockito.any())).thenReturn(remotingCommand);
//        Mockito.when(grpcClientManagerMock.getClientSettings(Mockito.any(Context.class))).thenReturn(Settings.newBuilder().build());
//        List<MessageExt> messageExtList = new ArrayList<>();
//        MessageExt messageExt = new MessageExt();
//        messageExt.setTopic(topic);
//        messageExt.setQueueOffset(0L);
//        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 10911));
//        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
//        messageExt.setBody("body".getBytes(StandardCharsets.UTF_8));
//        messageExt.putUserProperty("key", "value");
//        messageExt.setTags("b");
//        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, ReceiptHandle.builder()
//            .startOffset(0L)
//            .retrieveTime(0L)
//            .invisibleTime(1000L)
//            .nextVisibleTime(1000L)
//            .reviveQueueId(0)
//            .topicType("0")
//            .brokerName("brokerName")
//            .queueId(0)
//            .offset(0L)
//            .build().encode());
//        messageExtList.add(messageExt);
//        LocalReceiveMessageResultFilter localReceiveMessageResultFilter = new LocalReceiveMessageResultFilter(channelManagerMock, brokerControllerMock, grpcClientManagerMock);
//        localReceiveMessageResultFilter.filterMessage(Context.current(), ReceiveMessageRequest.newBuilder()
//            .setFilterExpression(FilterExpression.newBuilder()
//                .setExpression("a").build()).build(), messageExtList);
//        Mockito.verify(ackMessageProcessorMock, Mockito.times(1)).processRequest(Mockito.any(), Mockito.argThat(r -> r.getCode() == RequestCode.ACK_MESSAGE));
//    }
//
//    @Test
//    public void testFilterMessageWhenDLQ() throws RemotingCommandException {
//        RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
//        Mockito.when(channelManagerMock.createChannel(Mockito.any(Context.class))).thenReturn(new SimpleChannel(null, null, null, 0));
//        Mockito.when(brokerControllerMock.getSendMessageProcessor()).thenReturn(sendMessageProcessorMock);
//        Mockito.when(sendMessageProcessorMock.processRequest(Mockito.any(), Mockito.any())).thenReturn(remotingCommand);
//        Mockito.when(brokerControllerMock.getAckMessageProcessor()).thenReturn(ackMessageProcessorMock);
//        Mockito.when(ackMessageProcessorMock.processRequest(Mockito.any(), Mockito.any())).thenReturn(remotingCommand);
//        Mockito.when(grpcClientManagerMock.getClientSettings(Mockito.any(Context.class))).thenReturn(Settings.newBuilder().build());
//        List<MessageExt> messageExtList = new ArrayList<>();
//        MessageExt messageExt = new MessageExt();
//        messageExt.setTopic(topic);
//        messageExt.setQueueOffset(0L);
//        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 10911));
//        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
//        messageExt.setBody("body".getBytes(StandardCharsets.UTF_8));
//        messageExt.putUserProperty("key", "value");
//        messageExt.setTags("a");
//        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, ReceiptHandle.builder()
//            .startOffset(0L)
//            .retrieveTime(0L)
//            .invisibleTime(1000L)
//            .nextVisibleTime(1000L)
//            .reviveQueueId(0)
//            .topicType("0")
//            .brokerName("brokerName")
//            .queueId(0)
//            .offset(0L)
//            .build().encode());
//        messageExtList.add(messageExt);
//        LocalReceiveMessageResultFilter localReceiveMessageResultFilter = new LocalReceiveMessageResultFilter(channelManagerMock, brokerControllerMock, grpcClientManagerMock);
//        localReceiveMessageResultFilter.filterMessage(Context.current(), ReceiveMessageRequest.newBuilder()
//            .setFilterExpression(FilterExpression.newBuilder()
//                .setExpression("a").build()).build(), messageExtList);
//        Mockito.verify(sendMessageProcessorMock, Mockito.times(1)).processRequest(Mockito.any(), Mockito.argThat(r -> r.getCode() == RequestCode.CONSUMER_SEND_MSG_BACK));
//        Mockito.verify(ackMessageProcessorMock, Mockito.times(1)).processRequest(Mockito.any(), Mockito.argThat(r -> r.getCode() == RequestCode.ACK_MESSAGE));
//    }
//}