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
package org.apache.rocketmq.broker.rebalance;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.processor.ConsumerManageProcessor;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.AllocateMessageQueueRequestBody;
import org.apache.rocketmq.common.protocol.header.AllocateMessageQueueRequestHeader;
import org.apache.rocketmq.common.protocol.header.AllocateMessageQueueResponseBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.rebalance.AllocateMessageQueueStrategyConstants;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.broker.processor.PullMessageProcessorTest.createConsumerData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class AllocateMessageQueueStickyTest {
    private ConsumerManageProcessor consumerManageProcessor;
    @Spy
    private final BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private ChannelHandlerContext handlerContext;

    private static final String CID_PREFIX = "CID-";

    private final String group = "FooBarGroup";
    private final String topic = "FooBar";
    private List<MessageQueue> messageQueueList;
    private List<String> consumerIdList;

    @Before
    public void init() {
        consumerManageProcessor = new ConsumerManageProcessor(brokerController);
        messageQueueList = new ArrayList<MessageQueue>();
        consumerIdList = new ArrayList<String>();
    }

    @Test
    public void testCurrentCIDNotExists() throws RemotingCommandException {
        String currentCID = String.valueOf(Integer.MAX_VALUE);
        createConsumerIdList(4);
        createMessageQueueList(15);
        RemotingCommand request = buildAllocateMessageQueueRequest(currentCID, messageQueueList);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        Assert.assertEquals(AllocateMessageQueueRequestBody.decode(response.getBody(),
            AllocateMessageQueueResponseBody.class).getAllocateResult().size(), 0);
    }

    @Test
    public void testCurrentCIDIllegalArgument() throws RemotingCommandException {
        String currentCID = "";
        createConsumerIdList(4);
        createMessageQueueList(15);
        RemotingCommand request = buildAllocateMessageQueueRequest(currentCID, messageQueueList);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.ALLOCATE_MESSAGE_QUEUE_FAILED);
        assertThat(response.getRemark()).isEqualTo("currentCID is empty");
    }

    @Test
    public void testMessageQueueIllegalArgument() throws RemotingCommandException {
        String currentCID = CID_PREFIX + 0;
        createConsumerIdList(4);
        createMessageQueueList(0);
        RemotingCommand request = buildAllocateMessageQueueRequest(currentCID, messageQueueList);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.ALLOCATE_MESSAGE_QUEUE_FAILED);
        assertThat(response.getRemark()).isEqualTo("mqAll is null or mqAll empty");
    }

    @Test
    public void testConsumerIdIllegalArgument() throws RemotingCommandException {
        String currentCID = CID_PREFIX + 0;
        createConsumerIdList(0);
        createMessageQueueList(15);
        RemotingCommand request = buildAllocateMessageQueueRequest(currentCID, messageQueueList);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.ALLOCATE_MESSAGE_QUEUE_FAILED);
        assertThat(response.getRemark()).isEqualTo("cidAll is null or cidAll empty");
    }

    @Test
    public void testAllocateMessageQueue1() throws RemotingCommandException {
        createConsumerIdList(4);
        createMessageQueueList(10);
        for (String clientId : consumerIdList) {
            RemotingCommand request = buildAllocateMessageQueueRequest(clientId, messageQueueList);
            RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
            assertThat(response.getBody()).isNotNull();
            System.out.println(AllocateMessageQueueRequestBody.decode(response.getBody(),
                AllocateMessageQueueResponseBody.class).getAllocateResult());
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testAllocateMessageQueue2() throws RemotingCommandException {
        createConsumerIdList(10);
        createMessageQueueList(4);
        for (String clientId : consumerIdList) {
            RemotingCommand request = buildAllocateMessageQueueRequest(clientId, messageQueueList);
            RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testRun10RandomCase() throws RemotingCommandException {
        for (int i = 0; i < 10; i++) {
            int consumerSize = new Random().nextInt(20) + 2;
            int queueSize = new Random().nextInt(20) + 4;
            testAllocateMessageQueue(consumerSize, queueSize);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void testAllocateMessageQueue(int consumerSize, int queueSize) throws RemotingCommandException {
        createConsumerIdList(consumerSize);
        createMessageQueueList(queueSize);

        Map<MessageQueue, String> allocateToAllOrigin = new TreeMap<MessageQueue, String>();
        List<MessageQueue> allocatedResAll = new ArrayList<MessageQueue>();
        // test allocate all
        {
            List<String> cidBegin = new ArrayList<String>(consumerIdList);
            for (String cid : cidBegin) {
                RemotingCommand request = buildAllocateMessageQueueRequest(cid, messageQueueList);
                RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
                assertThat(response.getBody()).isNotNull();
                List<MessageQueue> rs = AllocateMessageQueueRequestBody.decode(response.getBody(),
                    AllocateMessageQueueResponseBody.class).getAllocateResult();
                for (MessageQueue mq : rs) {
                    allocateToAllOrigin.put(mq, cid);
                }
                allocatedResAll.addAll(rs);
                assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            }

            Assert.assertTrue(verifyAllocateAllConsumer(cidBegin, messageQueueList, allocatedResAll));
        }

        Map<MessageQueue, String> allocateToAllAfterRemoveOneConsumer = new TreeMap<MessageQueue, String>();
        List<String> cidAfterRemoveOneConsumer = new ArrayList<String>(consumerIdList);
        // test allocate after removing one cid
        {
            String removeCID = cidAfterRemoveOneConsumer.remove(0);
            unregisterConsumer(removeCID);
            List<MessageQueue> mqShouldBeChanged = new ArrayList<MessageQueue>();
            for (Map.Entry<MessageQueue, String> entry : allocateToAllOrigin.entrySet()) {
                if (entry.getValue().equals(removeCID)) {
                    mqShouldBeChanged.add(entry.getKey());
                }
            }

            List<MessageQueue> allocatedResAllAfterRemoveOneConsumer = new ArrayList<MessageQueue>();
            for (String cid : cidAfterRemoveOneConsumer) {
                RemotingCommand request = buildAllocateMessageQueueRequest(cid, messageQueueList);
                RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
                assertThat(response.getBody()).isNotNull();
                List<MessageQueue> rs = AllocateMessageQueueRequestBody.decode(response.getBody(),
                    AllocateMessageQueueResponseBody.class).getAllocateResult();
                allocatedResAllAfterRemoveOneConsumer.addAll(rs);
                for (MessageQueue mq : rs) {
                    allocateToAllAfterRemoveOneConsumer.put(mq, cid);
                }
                assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            }

            Assert.assertTrue(verifyAllocateAllConsumer(cidAfterRemoveOneConsumer, messageQueueList, allocatedResAllAfterRemoveOneConsumer));
            verifyAfterRemoveConsumer(allocateToAllOrigin, allocateToAllAfterRemoveOneConsumer, removeCID);
        }

        Map<MessageQueue, String> allocateToAllAfterAddOneConsumer = new TreeMap<MessageQueue, String>();
        List<String> cidAfterAddOneConsumer = new ArrayList<String>(cidAfterRemoveOneConsumer);
        // test allocate after adding one more cid
        {
            String newCid = CID_PREFIX + "NEW";
            cidAfterAddOneConsumer.add(newCid);
            registerConsumer(newCid);

            List<MessageQueue> mqShouldOnlyChanged = new ArrayList<MessageQueue>();
            List<MessageQueue> allocatedResAllAfterAddOneConsumer = new ArrayList<MessageQueue>();
            for (String cid : cidAfterAddOneConsumer) {
                RemotingCommand request = buildAllocateMessageQueueRequest(cid, messageQueueList);
                RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
                assertThat(response.getBody()).isNotNull();
                List<MessageQueue> rs = AllocateMessageQueueRequestBody.decode(response.getBody(),
                    AllocateMessageQueueResponseBody.class).getAllocateResult();
                allocatedResAllAfterAddOneConsumer.addAll(rs);
                for (MessageQueue mq : rs) {
                    allocateToAllAfterAddOneConsumer.put(mq, cid);
                    if (cid.equals(newCid)) {
                        mqShouldOnlyChanged.add(mq);
                    }
                }
                assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            }

            Assert.assertTrue(verifyAllocateAllConsumer(cidAfterAddOneConsumer, messageQueueList, allocatedResAllAfterAddOneConsumer));
            verifyAfterAddConsumer(allocateToAllAfterRemoveOneConsumer, allocateToAllAfterAddOneConsumer, newCid);
        }

        Map<MessageQueue, String> allocateToAllAfterRemoveTwoMq = new TreeMap<MessageQueue, String>();
        List<MessageQueue> mqAfterRemoveTwoMq = new ArrayList<>(messageQueueList);
        // test allocate after removing two message queues
        {
            for (int i = 0; i < 2; i++) {
                mqAfterRemoveTwoMq.remove(i);
            }

            List<MessageQueue> allocatedResAfterRemoveTwoMq = new ArrayList<MessageQueue>();
            for (String cid : cidAfterAddOneConsumer) {
                RemotingCommand request = buildAllocateMessageQueueRequest(cid, mqAfterRemoveTwoMq);
                RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
                assertThat(response.getBody()).isNotNull();
                List<MessageQueue> rs = AllocateMessageQueueRequestBody.decode(response.getBody(),
                    AllocateMessageQueueResponseBody.class).getAllocateResult();
                allocatedResAfterRemoveTwoMq.addAll(rs);
                for (MessageQueue mq : rs) {
                    allocateToAllAfterRemoveTwoMq.put(mq, cid);
                }
                assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            }

            Assert.assertTrue(verifyAllocateAllConsumer(cidAfterAddOneConsumer, mqAfterRemoveTwoMq, allocatedResAfterRemoveTwoMq));
        }
    }

    private void verifyAfterAddConsumer(Map<MessageQueue, String> allocateBefore,
        Map<MessageQueue, String> allocateAfter, String newCID) {
        for (MessageQueue mq : allocateAfter.keySet()) {
            String allocateToOrigin = allocateBefore.get(mq);
            String allocateToAfter = allocateAfter.get(mq);
            if (!allocateToAfter.equals(newCID)) { // the rest queues should be the same
                Assert.assertEquals(allocateToAfter, allocateToOrigin);
            }
        }
    }

    private void verifyAfterRemoveConsumer(Map<MessageQueue, String> allocateToBefore,
        Map<MessageQueue, String> allocateAfter, String removeCID) {
        for (MessageQueue mq : allocateToBefore.keySet()) {
            String allocateToOrigin = allocateToBefore.get(mq);
            String allocateToAfter = allocateAfter.get(mq);
            if (!allocateToOrigin.equals(removeCID)) { // the rest queues should be the same
                Assert.assertEquals(allocateToAfter, allocateToOrigin); // should be the same
            }
        }
    }

    private boolean verifyAllocateAllConsumer(List<String> cidAll, List<MessageQueue> mqAll,
        List<MessageQueue> allocatedResAll) {
        if (cidAll.isEmpty()) {
            return allocatedResAll.isEmpty();
        }
        return mqAll.containsAll(allocatedResAll) && allocatedResAll.containsAll(mqAll);
    }

    private void registerConsumer(String clientId) {
        Channel channel = mock(Channel.class);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel, clientId, LanguageCode.JAVA, 100);
        ConsumerData consumerData = createConsumerData(group, topic);
        brokerController.getConsumerManager().registerConsumer(
            consumerData.getGroupName(),
            clientChannelInfo,
            consumerData.getConsumeType(),
            consumerData.getMessageModel(),
            consumerData.getConsumeFromWhere(),
            consumerData.getSubscriptionDataSet(),
            false);
        consumerIdList.add(clientId);
    }

    private void unregisterConsumer(String clientId) {
        Channel channel = brokerController.getConsumerManager().getConsumerGroupInfo(group).findChannel(clientId).getChannel();
        brokerController.getConsumerManager().unregisterConsumer(group,
            new ClientChannelInfo(channel, clientId, LanguageCode.JAVA, 100), true);
        consumerIdList.remove(clientId);
    }

    private RemotingCommand buildAllocateMessageQueueRequest(String clientId, List<MessageQueue> messageQueueList) {
        AllocateMessageQueueRequestHeader requestHeader = new AllocateMessageQueueRequestHeader();
        requestHeader.setConsumerGroup(group);
        requestHeader.setClientID(clientId);
        requestHeader.setStrategyName(AllocateMessageQueueStrategyConstants.ALLOCATE_MESSAGE_QUEUE_STICKY);

        AllocateMessageQueueRequestBody requestBody = new AllocateMessageQueueRequestBody();
        requestBody.setMqAll(messageQueueList);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ALLOCATE_MESSAGE_QUEUE, requestHeader);
        request.setBody(requestBody.encode());
        request.makeCustomHeaderToNet();

        return request;
    }

    private void createConsumerIdList(int size) {
        for (int i = 0; i < size; i++) {
            String clientId = CID_PREFIX + i;
            registerConsumer(clientId);
        }
    }

    private void createMessageQueueList(int size) {
        for (int i = 0; i < size; i++) {
            MessageQueue mq = new MessageQueue(topic, "brokerName", i);
            messageQueueList.add(mq);
        }
    }
}
