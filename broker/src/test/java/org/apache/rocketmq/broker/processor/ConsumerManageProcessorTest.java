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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.broker.topic.TopicQueueMappingManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingContext;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.rpc.RpcClient;
import org.apache.rocketmq.remoting.rpc.RpcException;
import org.apache.rocketmq.remoting.rpc.RpcResponse;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerManageProcessorTest {
    private ConsumerManageProcessor consumerManageProcessor;
    @Mock
    private ChannelHandlerContext handlerContext;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private MessageStore messageStore;
    @Mock
    private Channel channel;
    @Mock
    private ConsumerOffsetManager consumerOffsetManager;
    @Mock
    private BrokerOuterAPI brokerOuterAPI;
    @Mock
    private RpcClient rpcClient;
    @Mock
    private Future<RpcResponse> responseFuture;
    @Mock
    private TopicQueueMappingContext mappingContext;

    private String topic = "FooBar";
    private String group = "FooBarGroup";

    @Before
    public void init() throws RpcException {
        brokerController.setMessageStore(messageStore);
        TopicConfigManager topicConfigManager = new TopicConfigManager(brokerController);
        topicConfigManager.getTopicConfigTable().put(topic, new TopicConfig(topic));
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        SubscriptionGroupManager subscriptionGroupManager = new SubscriptionGroupManager(brokerController);
        subscriptionGroupManager.getSubscriptionGroupTable().put(group, new SubscriptionGroupConfig());
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        consumerManageProcessor = new ConsumerManageProcessor(brokerController);
        when(brokerController.getBrokerOuterAPI()).thenReturn(brokerOuterAPI);
        when(brokerOuterAPI.getRpcClient()).thenReturn(rpcClient);
        when(rpcClient.invoke(any(),anyLong())).thenReturn(responseFuture);
        TopicQueueMappingDetail topicQueueMappingDetail = new TopicQueueMappingDetail();
        topicQueueMappingDetail.setBname("BrokerA");
        when(mappingContext.getMappingDetail()).thenReturn(topicQueueMappingDetail);
    }

    @Test
    public void testUpdateConsumerOffset_InvalidTopic() throws Exception {
        RemotingCommand request = buildUpdateConsumerOffsetRequest(group, "InvalidTopic", 0, 0);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
    }

    @Test
    public void testUpdateConsumerOffset_GroupNotExist() throws Exception {
        RemotingCommand request = buildUpdateConsumerOffsetRequest("NotExistGroup", topic, 0, 0);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
    }

    @Test
    public void testUpdateConsumerOffset() throws RemotingCommandException {
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.hasOffsetReset(anyString(),anyString(),anyInt())).thenReturn(true);
        RemotingCommand request = buildUpdateConsumerOffsetRequest(group, topic, 0, 0);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(consumerOffsetManager.hasOffsetReset(anyString(),anyString(),anyInt())).thenReturn(false);
        response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetConsumerListByGroup() throws RemotingCommandException {
        GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
        requestHeader.setConsumerGroup(group);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);
        request.makeCustomHeaderToNet();
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        brokerController.getConsumerManager().getConsumerTable().put(group,new ConsumerGroupInfo(group));
        response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        ConsumerGroupInfo consumerGroupInfo =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(
                        requestHeader.getConsumerGroup());
        consumerGroupInfo.getChannelInfoTable().put(channel,new ClientChannelInfo(channel));
        response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testQueryConsumerOffset() throws RemotingCommandException, ExecutionException, InterruptedException {
        RemotingCommand request = buildQueryConsumerOffsetRequest(group, topic, 0, true);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.QUERY_NOT_FOUND);

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.queryOffset(anyString(),anyString(),anyInt())).thenReturn(0L);
        response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(consumerOffsetManager.queryOffset(anyString(),anyString(),anyInt())).thenReturn(-1L);
        when(messageStore.getMinOffsetInQueue(anyString(),anyInt())).thenReturn(-1L);
        when(messageStore.checkInMemByConsumeOffset(anyString(),anyInt(),anyLong(),anyInt())).thenReturn(true);
        response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        TopicQueueMappingManager topicQueueMappingManager = mock(TopicQueueMappingManager.class);
        when(brokerController.getTopicQueueMappingManager()).thenReturn(topicQueueMappingManager);
        when(topicQueueMappingManager.buildTopicQueueMappingContext(any(QueryConsumerOffsetRequestHeader.class))).thenReturn(mappingContext);
        response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.NOT_LEADER_FOR_QUEUE);

        List<LogicQueueMappingItem> items = new ArrayList<>();
        LogicQueueMappingItem item1 = createLogicQueueMappingItem("BrokerC", 0, 0L, 0L);
        items.add(item1);
        when(mappingContext.getMappingItemList()).thenReturn(items);
        when(mappingContext.getLeaderItem()).thenReturn(item1);
        when(mappingContext.getCurrentItem()).thenReturn(item1);
        when(mappingContext.isLeader()).thenReturn(true);
        response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        LogicQueueMappingItem item2 = createLogicQueueMappingItem("BrokerA", 0, 0L, 0L);
        items.add(item2);
        QueryConsumerOffsetResponseHeader queryConsumerOffsetResponseHeader = new QueryConsumerOffsetResponseHeader();
        queryConsumerOffsetResponseHeader.setOffset(0L);
        RpcResponse rpcResponse = new RpcResponse(ResponseCode.SUCCESS,queryConsumerOffsetResponseHeader,null);
        when(responseFuture.get()).thenReturn(rpcResponse);
        response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        queryConsumerOffsetResponseHeader.setOffset(-1L);
        rpcResponse = new RpcResponse(ResponseCode.SUCCESS,queryConsumerOffsetResponseHeader,null);
        when(responseFuture.get()).thenReturn(rpcResponse);
        response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.QUERY_NOT_FOUND);
    }

    @Test
    public void testRewriteRequestForStaticTopic() throws RpcException, ExecutionException, InterruptedException {
        UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
        requestHeader.setConsumerGroup(group);
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(0);
        requestHeader.setCommitOffset(0L);

        RemotingCommand response = consumerManageProcessor.rewriteRequestForStaticTopic(requestHeader, mappingContext);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.NOT_LEADER_FOR_QUEUE);

        List<LogicQueueMappingItem> items = new ArrayList<>();
        LogicQueueMappingItem item = createLogicQueueMappingItem("BrokerC", 0, 0L, 0L);
        items.add(item);
        when(mappingContext.getMappingItemList()).thenReturn(items);
        when(mappingContext.isLeader()).thenReturn(true);
        RpcResponse rpcResponse = new RpcResponse(ResponseCode.SUCCESS,new UpdateConsumerOffsetResponseHeader(),null);
        when(responseFuture.get()).thenReturn(rpcResponse);
        response = consumerManageProcessor.rewriteRequestForStaticTopic(requestHeader, mappingContext);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    public RemotingCommand buildQueryConsumerOffsetRequest(String group, String topic, int queueId,boolean setZeroIfNotFound) {
        QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
        requestHeader.setConsumerGroup(group);
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setSetZeroIfNotFound(setZeroIfNotFound);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    public LogicQueueMappingItem createLogicQueueMappingItem(String brokerName, int queueId, long startOffset, long logicOffset) {
        LogicQueueMappingItem item = new LogicQueueMappingItem();
        item.setBname(brokerName);
        item.setQueueId(queueId);
        item.setStartOffset(startOffset);
        item.setLogicOffset(logicOffset);
        return item;
    }

    private RemotingCommand buildUpdateConsumerOffsetRequest(String group, String topic, int queueId, long offset) {
        UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
        requestHeader.setConsumerGroup(group);
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setCommitOffset(offset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }
}
