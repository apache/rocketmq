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
package org.apache.rocketmq.client.producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OrderMQProducerTest {
    @Spy
    private MQClientInstance mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(new ClientConfig());
    @Mock
    private MQClientAPIImpl mQClientAPIImpl;

    private OrderMQProducer producer;
    private String topic = "FooBar";
    private String producerGroupPrefix = "FooBar_PID";
    private ConcurrentHashMap<String /*batch*/, Set<Integer>> sentMQMap;

    final int orderMsgBatches = 1000;
    final int numEachBatch = 10;
    final CountDownLatch countDownLatch = new CountDownLatch( orderMsgBatches * numEachBatch);

    @Before
    public void init() throws Exception {
        String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
        producer = new OrderMQProducer(producerGroupTemp) {
            @Override
            String shardingKey(Message msg) {
                String batch = msg.getUserProperty("batchId");
                return batch;
            }
        };

        producer.setNamesrvAddr("127.0.0.1:9876");
        sentMQMap = new ConcurrentHashMap<String,Set <Integer>>();

        producer.getDefaultMQProducerImpl().registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "check";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                String batch = context.getMessage().getProperty("batchId");
                int queueId = context.getMq().getQueueId();
                sentMQMap.putIfAbsent(batch, Collections.synchronizedSet(new HashSet<Integer>()));
                sentMQMap.get(batch).add(queueId);
                countDownLatch.countDown();
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {

            }
        });
        producer.start();

        Field field = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(producer.getDefaultMQProducerImpl(), mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        producer.getDefaultMQProducerImpl().getmQClientFactory().registerProducer(producerGroupTemp, producer.getDefaultMQProducerImpl());

        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class))).thenCallRealMethod();
        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendCallback.class), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenReturn(createSendResult(SendStatus.SEND_OK));
    }

    @After
    public void terminate() {
        producer.shutdown();
    }


    @Test
    public void testBasic() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        ExecutorService es = Executors.newFixedThreadPool(5);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute(8));
        mQClientFactory.updateTopicRouteInfoFromNameServer(topic);

        for (int i = 0 ; i <orderMsgBatches ;i ++) {// batches
            for (int j = 0 ; j <numEachBatch; j++) {// several messages for each batch
                final int batchIndex = i;
                es.submit(new Runnable() {
                    @Override public void run() {
                        try {
                            final Message msg = new Message(topic, (batchIndex+"").getBytes());
                            msg.putUserProperty("batchId","batch"+batchIndex);
                            SendResult sendResult = producer.send(msg);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });

            }
        }

        countDownLatch.await(10, TimeUnit.SECONDS);
        es.shutdown();
        assertThat(countDownLatch.getCount()).isEqualTo(0);
        for (String sendBatch : sentMQMap.keySet()) {
            assertThat(sentMQMap.get(sendBatch).size()).isEqualTo(1);// only one mq is sent for each batch, meaning that they are all sent to one batches
        }

    }

    @Test
    public void testQueueClose() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute(8));

        for (int i = 0 ; i <orderMsgBatches ;i ++) {
            final Message msg = new Message(topic, (i+"").getBytes());
            msg.putUserProperty("batchId","influence-batch"+i);
            SendResult sendResult = producer.send(msg);
        }

        //one queue is closed after 100 ms
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute(7));

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.schedule(new Runnable() {
            @Override public void run() {
                mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        },10,TimeUnit.MILLISECONDS);

        //send the rest after q changes
        for (int i = 0 ; i <orderMsgBatches ; i++) {
            for (int j = 1; j < numEachBatch ; j++) {
                final Message msg = new Message(topic, (i + "").getBytes());
                msg.putUserProperty("batchId", "influence-batch" + i);
                SendResult sendResult = producer.send(msg);
            }
        }

        scheduledExecutorService.shutdown();
        for (String sendBatch : sentMQMap.keySet()) {
            if (!sentMQMap.get(sendBatch).contains(7)) {//the messages which are sent to the existing mq will not be influenced
                assertThat(sentMQMap.get(sendBatch).size()).isEqualTo(1);
            } else {
                assertThat(sentMQMap.get(sendBatch).size()).isLessThanOrEqualTo(2);
            }
        }
    }

    @Test
    public void testQueueScale() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute(8));

        final int orderMsgBatches = 1000;
        final int numEachBatch = 10;

        for (int i = 0 ; i <orderMsgBatches ;i ++) {
            final Message msg = new Message(topic, (i+"").getBytes());
            msg.putUserProperty("batchId","influence-batch"+i);
            SendResult sendResult = producer.send(msg);
        }

        //2 queue is scaled
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute(9));
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.schedule(new Runnable() {
            @Override public void run() {
                mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        },100,TimeUnit.MILLISECONDS);

        //send another rest after q changes
        for (int i = 0 ; i <orderMsgBatches ;i ++) {// 1000 batches
            for (int j=1; j <= numEachBatch ;j++) {
                final Message msg = new Message(topic, (i + "").getBytes());
                msg.putUserProperty("batchId", "influence-batch" + i);
                SendResult sendResult = producer.send(msg);
            }
        }

        scheduledExecutorService.shutdown();
        for (String sendBatch : sentMQMap.keySet()) {
            if (!sentMQMap.get(sendBatch).contains(8)) {//not be influenced if they are not sharded to new mq
                assertThat(sentMQMap.get(sendBatch).size()).isEqualTo(1);
            } else {
                assertThat(sentMQMap.get(sendBatch).size()).isLessThanOrEqualTo(2);
            }
        }
    }


    public static TopicRouteData createTopicRoute(int queueNum) {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("BrokerA");
        brokerData.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "127.0.0.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);

        List<QueueData> queueDataList = new ArrayList<QueueData>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("BrokerA");
        queueData.setPerm(6);
        queueData.setReadQueueNums(queueNum);
        queueData.setWriteQueueNums(queueNum);
        queueData.setTopicSynFlag(0);
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);
        return topicRouteData;
    }

    private SendResult createSendResult(SendStatus sendStatus) {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId("123");
        sendResult.setOffsetMsgId("123");
        sendResult.setQueueOffset(456);
        sendResult.setSendStatus(sendStatus);
        sendResult.setRegionId("HZ");
        return sendResult;
    }

}