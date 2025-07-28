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
package org.apache.rocketmq.broker.processor;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.topic.TopicRouteInfoManager;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragelyByCircle;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueConsistentHash;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.broker.processor.PullMessageProcessorTest.createConsumerData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryAssignmentProcessorTest {
    private QueryAssignmentProcessor queryAssignmentProcessor;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());

    @Mock
    private TopicRouteInfoManager topicRouteInfoManager;
    @Mock
    private ChannelHandlerContext handlerContext;
    @Mock
    private MessageStore messageStore;
    @Mock
    private Channel channel;

    private String broker = "defaultBroker";
    private String topic = "FooBar";
    private String group = "FooBarGroup";
    private String clientId = "127.0.0.1";
    private ClientChannelInfo clientInfo;

    @Before
    public void init() throws IllegalAccessException, NoSuchFieldException {
        clientInfo = new ClientChannelInfo(channel, "127.0.0.1", LanguageCode.JAVA, 0);
        brokerController.setMessageStore(messageStore);
        doReturn(topicRouteInfoManager).when(brokerController).getTopicRouteInfoManager();
        when(topicRouteInfoManager.getTopicSubscribeInfo(topic)).thenReturn(ImmutableSet.of(new MessageQueue(topic, "broker-1", 0), new MessageQueue(topic, "broker-2", 1)));
        queryAssignmentProcessor = new QueryAssignmentProcessor(brokerController);
        brokerController.getTopicConfigManager().getTopicConfigTable().put(topic, new TopicConfig());
        ConsumerData consumerData = createConsumerData(group, topic);
        brokerController.getConsumerManager().registerConsumer(
            consumerData.getGroupName(),
            clientInfo,
            consumerData.getConsumeType(),
            consumerData.getMessageModel(),
            consumerData.getConsumeFromWhere(),
            consumerData.getSubscriptionDataSet(),
            false);
    }

    @Test
    public void testQueryAssignment() throws Exception {
        brokerController.getProducerManager().registerProducer(group, clientInfo);
        final RemotingCommand request = createQueryAssignmentRequest();
        RemotingCommand responseToReturn = queryAssignmentProcessor.processRequest(handlerContext, request);
        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(responseToReturn.getBody()).isNotNull();
        QueryAssignmentResponseBody responseBody = QueryAssignmentResponseBody.decode(responseToReturn.getBody(), QueryAssignmentResponseBody.class);
        assertThat(responseBody.getMessageQueueAssignments()).size().isEqualTo(2);
    }

    @Test
    public void testSetMessageRequestMode_Success() throws Exception {
        brokerController.getProducerManager().registerProducer(group, clientInfo);
        final RemotingCommand request = createSetMessageRequestModeRequest(topic);
        RemotingCommand responseToReturn = queryAssignmentProcessor.processRequest(handlerContext, request);
        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testSetMessageRequestMode_RetryTopic() throws Exception {
        brokerController.getProducerManager().registerProducer(group, clientInfo);
        final RemotingCommand request = createSetMessageRequestModeRequest(MixAll.RETRY_GROUP_TOPIC_PREFIX + topic);
        RemotingCommand responseToReturn = queryAssignmentProcessor.processRequest(handlerContext, request);
        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
    }

    @Test
    public void testDoLoadBalance() throws Exception {
        Method method = queryAssignmentProcessor.getClass()
            .getDeclaredMethod("doLoadBalance", String.class, String.class, String.class, MessageModel.class,
                String.class, SetMessageRequestModeRequestBody.class, ChannelHandlerContext.class);
        method.setAccessible(true);

        Set<MessageQueue> mqs1 = (Set<MessageQueue>) method.invoke(
            queryAssignmentProcessor, MixAll.LMQ_PREFIX + topic, group, "127.0.0.1", MessageModel.CLUSTERING,
            new AllocateMessageQueueAveragely().getName(), new SetMessageRequestModeRequestBody(), handlerContext);
        Set<MessageQueue> mqs2 = (Set<MessageQueue>) method.invoke(
            queryAssignmentProcessor, MixAll.LMQ_PREFIX + topic, group, "127.0.0.2", MessageModel.CLUSTERING,
            new AllocateMessageQueueAveragely().getName(), new SetMessageRequestModeRequestBody(), handlerContext);

        assertThat(mqs1).hasSize(1);
        assertThat(mqs2).isEmpty();
    }

    @Test
    public void testDoLoadBalanceWithCache() throws Exception {
        for (int i = 2; i < 6; i++) {
            Channel channel = Mockito.mock(Channel.class);
            String clientId = "127.0.0." + i;
            ClientChannelInfo clientInfo = new ClientChannelInfo(channel, clientId, LanguageCode.JAVA, 0);
            ConsumerData consumerData = createConsumerData(group, topic);
            brokerController.getConsumerManager().registerConsumer(
                    consumerData.getGroupName(),
                    clientInfo,
                    consumerData.getConsumeType(),
                    consumerData.getMessageModel(),
                    consumerData.getConsumeFromWhere(),
                    consumerData.getSubscriptionDataSet(),
                    false
            );
        }

        Method method = queryAssignmentProcessor.getClass()
                .getDeclaredMethod("doLoadBalance", String.class, String.class, String.class, MessageModel.class,
                        String.class, SetMessageRequestModeRequestBody.class, ChannelHandlerContext.class);
        method.setAccessible(true);

        SetMessageRequestModeRequestBody setMessageRequestModeRequestBody = new SetMessageRequestModeRequestBody();
        setMessageRequestModeRequestBody.setPopShareQueueNum(1);
        setMessageRequestModeRequestBody.setMode(MessageRequestMode.POP);

        Set<MessageQueue> initialQueues = new HashSet<>();
        for (int i = 0; i < 4; i++) {
            initialQueues.add(new MessageQueue(topic, "broker-1", i));
        }
        when(topicRouteInfoManager.getTopicSubscribeInfo(topic)).thenReturn(initialQueues);

        // First call: Perform load balancing calculations and cache the results
        Set<MessageQueue> firstResult = (Set<MessageQueue>) method.invoke(
                queryAssignmentProcessor, topic, group, "127.0.0.1", MessageModel.CLUSTERING,
                new AllocateMessageQueueAveragely().getName(), setMessageRequestModeRequestBody, handlerContext);

        assertThat(firstResult).isNotNull();
        assertThat(firstResult).isNotEmpty();

        // Second call with the same parameters: the same result should be obtained from the cache
        Set<MessageQueue> secondResult = (Set<MessageQueue>) method.invoke(
                queryAssignmentProcessor, topic, group, "127.0.0.1", MessageModel.CLUSTERING,
                new AllocateMessageQueueAveragely().getName(), setMessageRequestModeRequestBody, handlerContext);

        assertThat(secondResult).isNotNull();
        assertThat(secondResult).isEqualTo(firstResult);

        Set<MessageQueue> changedQueues = new HashSet<>();
        for (int i = 0; i < 6; i++) {
            changedQueues.add(new MessageQueue(topic, "broker-1", i));
        }
        when(topicRouteInfoManager.getTopicSubscribeInfo(topic)).thenReturn(changedQueues);
    }

    @Test
    public void testAllocate4Pop() {
        testAllocate4Pop(new AllocateMessageQueueAveragely());
        testAllocate4Pop(new AllocateMessageQueueAveragelyByCircle());
        testAllocate4Pop(new AllocateMessageQueueConsistentHash());
    }

    private void testAllocate4Pop(AllocateMessageQueueStrategy strategy) {
        int testNum = 16;
        List<MessageQueue> mqAll = new ArrayList<>();
        for (int mqSize = 0; mqSize < testNum; mqSize++) {
            mqAll.add(new MessageQueue(topic, broker, mqSize));

            List<String> cidAll = new ArrayList<>();
            for (int cidSize = 0; cidSize < testNum; cidSize++) {
                String clientId = String.valueOf(cidSize);
                cidAll.add(clientId);

                for (int popShareQueueNum = 0; popShareQueueNum < testNum; popShareQueueNum++) {
                    List<MessageQueue> allocateResult =
                        queryAssignmentProcessor.allocate4Pop(strategy, group, clientId, mqAll, cidAll, popShareQueueNum);
                    Assert.assertTrue(checkAllocateResult(popShareQueueNum, mqAll.size(), cidAll.size(), allocateResult.size(), strategy));
                }
            }
        }
    }

    private boolean checkAllocateResult(int popShareQueueNum, int mqSize, int cidSize, int allocateSize,
        AllocateMessageQueueStrategy strategy) {

        //The maximum size of allocations will not exceed mqSize.
        if (allocateSize > mqSize) {
            return false;
        }

        //It is not allowed that the client is not assigned to the consumeQueue.
        if (allocateSize <= 0) {
            return false;
        }

        if (popShareQueueNum <= 0 || popShareQueueNum >= cidSize - 1) {
            return allocateSize == mqSize;
        } else if (mqSize < cidSize) {
            return allocateSize == 1;
        }

        if (strategy instanceof AllocateMessageQueueAveragely
            || strategy instanceof AllocateMessageQueueAveragelyByCircle) {

            if (mqSize % cidSize == 0) {
                return allocateSize == (mqSize / cidSize) * (popShareQueueNum + 1);
            } else {
                int avgSize = mqSize / cidSize;
                return allocateSize >= avgSize * (popShareQueueNum + 1)
                    && allocateSize <= (avgSize + 1) * (popShareQueueNum + 1);
            }
        }

        if (strategy instanceof AllocateMessageQueueConsistentHash) {
            //Just skip
            return true;
        }

        return false;
    }

    private RemotingCommand createQueryAssignmentRequest() {
        QueryAssignmentRequestBody requestBody = new QueryAssignmentRequestBody();
        requestBody.setTopic(topic);
        requestBody.setConsumerGroup(group);
        requestBody.setClientId(clientId);
        requestBody.setMessageModel(MessageModel.CLUSTERING);
        requestBody.setStrategyName("AVG");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_ASSIGNMENT, null);
        request.setBody(requestBody.encode());
        return request;
    }

    private RemotingCommand createSetMessageRequestModeRequest(String topic) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SET_MESSAGE_REQUEST_MODE, null);

        SetMessageRequestModeRequestBody requestBody = new SetMessageRequestModeRequestBody();
        requestBody.setTopic(topic);
        requestBody.setConsumerGroup(group);
        requestBody.setMode(MessageRequestMode.POP);
        requestBody.setPopShareQueueNum(0);
        request.setBody(requestBody.encode());

        return request;
    }

    private RemotingCommand createResponse(int code, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(code);
        response.setOpaque(request.getOpaque());
        return response;
    }
}
