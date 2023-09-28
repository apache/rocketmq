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
package org.apache.rocketmq.client.impl.factory;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.RebalanceService;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class MQClientInstanceTest {
    private MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    private String topic = "FooBar";
    private String group = "FooBarGroup";
    private ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();


    @Before
    public void init() throws Exception {
        FieldUtils.writeDeclaredField(mqClientInstance, "brokerAddrTable", brokerAddrTable, true);
    }

    @Test
    public void testTopicRouteData2TopicPublishInfo() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<>());
        List<BrokerData> brokerDataList = new ArrayList<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("BrokerA");
        brokerData.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, "127.0.0.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);

        List<QueueData> queueDataList = new ArrayList<>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("BrokerA");
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSysFlag(0);
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);

        TopicPublishInfo topicPublishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);

        assertThat(topicPublishInfo.isHaveTopicRouterInfo()).isFalse();
        assertThat(topicPublishInfo.getMessageQueueList().size()).isEqualTo(4);
    }

    @Test
    public void testFindBrokerAddressInSubscribe() {
        // dledger normal case
        String brokerName = "BrokerA";
        HashMap<Long, String> addrMap = new HashMap<>();
        addrMap.put(0L, "127.0.0.1:10911");
        addrMap.put(1L, "127.0.0.1:10912");
        addrMap.put(2L, "127.0.0.1:10913");
        brokerAddrTable.put(brokerName, addrMap);
        long brokerId = 1;
        FindBrokerResult brokerResult = mqClientInstance.findBrokerAddressInSubscribe(brokerName, brokerId, false);
        assertThat(brokerResult).isNotNull();
        assertThat(brokerResult.getBrokerAddr()).isEqualTo("127.0.0.1:10912");
        assertThat(brokerResult.isSlave()).isTrue();

        // dledger case, when node n0 was voted as the leader
        brokerName = "BrokerB";
        HashMap<Long, String> addrMapNew = new HashMap<>();
        addrMapNew.put(0L, "127.0.0.1:10911");
        addrMapNew.put(2L, "127.0.0.1:10912");
        addrMapNew.put(3L, "127.0.0.1:10913");
        brokerAddrTable.put(brokerName, addrMapNew);
        brokerResult = mqClientInstance.findBrokerAddressInSubscribe(brokerName, brokerId, false);
        assertThat(brokerResult).isNotNull();
        assertThat(brokerResult.getBrokerAddr()).isEqualTo("127.0.0.1:10912");
        assertThat(brokerResult.isSlave()).isTrue();
    }

    @Test
    public void testRegisterProducer() {
        boolean flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        assertThat(flag).isTrue();

        flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        assertThat(flag).isFalse();

        mqClientInstance.unregisterProducer(group);
        flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        assertThat(flag).isTrue();
    }

    @Test
    public void testRegisterConsumer() throws RemotingException, InterruptedException, MQBrokerException {
        boolean flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isTrue();

        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isFalse();

        mqClientInstance.unregisterConsumer(group);
        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isTrue();
    }


    @Test
    public void testConsumerRunningInfoWhenConsumersIsEmptyOrNot() throws RemotingException, InterruptedException, MQBrokerException {
        MQConsumerInner mockConsumerInner = mock(MQConsumerInner.class);
        ConsumerRunningInfo mockConsumerRunningInfo = mock(ConsumerRunningInfo.class);
        when(mockConsumerInner.consumerRunningInfo()).thenReturn(mockConsumerRunningInfo);
        when(mockConsumerInner.consumeType()).thenReturn(ConsumeType.CONSUME_PASSIVELY);
        Properties properties = new Properties();
        when(mockConsumerRunningInfo.getProperties()).thenReturn(properties);
        mqClientInstance.unregisterConsumer(group);

        ConsumerRunningInfo runningInfo = mqClientInstance.consumerRunningInfo(group);
        assertThat(runningInfo).isNull();
        boolean flag = mqClientInstance.registerConsumer(group, mockConsumerInner);
        assertThat(flag).isTrue();

        runningInfo = mqClientInstance.consumerRunningInfo(group);
        assertThat(runningInfo).isNotNull();
        assertThat(mockConsumerInner.consumerRunningInfo().getProperties().get(ConsumerRunningInfo.PROP_CONSUME_TYPE)).isNotNull();

        mqClientInstance.unregisterConsumer(group);
        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isTrue();
    }

    @Test
    public void testRegisterAdminExt() {
        boolean flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        assertThat(flag).isTrue();

        flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        assertThat(flag).isFalse();

        mqClientInstance.unregisterAdminExt(group);
        flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        assertThat(flag).isTrue();
    }


    @Test
    public void testOnlyRegisterProducer() throws MQClientException, NoSuchFieldException, IllegalAccessException, MQBrokerException, RemotingException, InterruptedException {
        MQClientAPIImpl mQClientAPIImpl = mock(MQClientAPIImpl.class);

        DefaultMQProducer producer = new DefaultMQProducer("szz_producer");
        producer.setInstanceName("szz_producer_instanceName");
        producer.setNamesrvAddr("127.0.0.1:9876");

        MQClientInstance mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(producer);
        FieldUtils.writeDeclaredField(mQClientFactory, "mQClientAPIImpl", mQClientAPIImpl, true);

        MQClientInstance mQClientFactorySpy = spy(mQClientFactory);
        lenient().doReturn(false).when(mQClientFactorySpy).updateTopicRouteInfoFromNameServer(anyString());
        lenient().doReturn(null).when(mQClientFactorySpy).queryAssignment(anyString(), anyString(), anyString(), any(MessageModel.class), anyInt());
        lenient().doReturn(new FindBrokerResult("127.0.0.1:10911", false)).when(mQClientFactorySpy).findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean());

        ConcurrentMap<String, MQClientInstance> factoryTable = (ConcurrentMap<String, MQClientInstance>) FieldUtils.readDeclaredField(MQClientManager.getInstance(), "factoryTable", true);
        MQClientInstance instance1 = factoryTable.get(producer.buildMQClientId());
        // already created
        assertThat(instance1).isEqualTo(mQClientFactory);

        producer.start();


        // only producer shouldn't start inner DefaultMQProducer and pullRequest
        ServiceState serviceState = mQClientFactory.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceState).isEqualTo(ServiceState.CREATE_JUST);

        // pullMessageService can't stared
        Field field =  PullMessageService.class.getSuperclass().getDeclaredField("started");
        field.setAccessible(true);

        AtomicBoolean pullStared = (AtomicBoolean) field.get(mQClientFactory.getPullMessageService());
        assertThat(pullStared.get()).isEqualTo(false);


        //rebalance

        // balanceService started
        RebalanceService rebalanceService = (RebalanceService)FieldUtils.readDeclaredField(mQClientFactory,"rebalanceService",true);
        assertThat(((AtomicBoolean)field.get(rebalanceService)).get()).isEqualTo(false);

        producer.shutdown();

        ServiceState serviceStateAfter = mQClientFactory.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceStateAfter).isEqualTo(ServiceState.SHUTDOWN_ALREADY);

        ServiceState producerServiceState = producer.getDefaultMQProducerImpl().getServiceState();
        assertThat(producerServiceState).isEqualTo(ServiceState.SHUTDOWN_ALREADY);

    }

    @Test
    public void testOnlyRegisterConsumer() throws MQClientException, IllegalAccessException, MQBrokerException, RemotingException, InterruptedException, NoSuchFieldException {

        MQClientAPIImpl mQClientAPIImpl = mock(MQClientAPIImpl.class);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("szz_consumer");
        consumer.setInstanceName("szz_consumer_instanceName");
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.setNamesrvAddr("127.0.0.1:9876");

        MQClientInstance mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(consumer);
        FieldUtils.writeDeclaredField(mQClientFactory, "mQClientAPIImpl", mQClientAPIImpl, true);

        MQClientInstance mQClientFactorySpy = spy(mQClientFactory);
        lenient().doReturn(false).when(mQClientFactorySpy).updateTopicRouteInfoFromNameServer(anyString());
        lenient().doReturn(null).when(mQClientFactorySpy).queryAssignment(anyString(), anyString(), anyString(), any(MessageModel.class), anyInt());
        lenient().doReturn(new FindBrokerResult("127.0.0.1:10911", false)).when(mQClientFactorySpy).findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean());

        ConcurrentMap<String, MQClientInstance> factoryTable = (ConcurrentMap<String, MQClientInstance>) FieldUtils.readDeclaredField(MQClientManager.getInstance(), "factoryTable", true);
        MQClientInstance instance1 = factoryTable.get(consumer.buildMQClientId());
        // already created
        assertThat(instance1).isEqualTo(mQClientFactory);



        consumer.start();


        // inner producer running
        ServiceState serviceState = mQClientFactory.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceState).isEqualTo(ServiceState.RUNNING);


        Field field =  PullMessageService.class.getSuperclass().getDeclaredField("started");
        field.setAccessible(true);

        // pullMessageService stared
        AtomicBoolean pullStared = (AtomicBoolean) field.get(mQClientFactory.getPullMessageService());
        assertThat(pullStared.get()).isEqualTo(true);

        // balanceService started
        RebalanceService rebalanceService = (RebalanceService)FieldUtils.readDeclaredField(mQClientFactory,"rebalanceService",true);
        AtomicBoolean rebalancedStared = (AtomicBoolean)field.get(rebalanceService);
        assertThat(rebalancedStared.get()).isEqualTo(true);


        consumer.shutdown();

        // pullMessageService close
        AtomicBoolean pullClose = (AtomicBoolean) field.get(mQClientFactory.getPullMessageService());
        assertThat(pullClose.get()).isEqualTo(false);

        AtomicBoolean rebalancedClose = (AtomicBoolean)field.get(rebalanceService);
        assertThat(rebalancedClose.get()).isEqualTo(false);

        ServiceState serviceStateAfter = mQClientFactory.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceStateAfter).isEqualTo(ServiceState.SHUTDOWN_ALREADY);

    }

    /**
     * test
     * 1. When a single producer client is created, no consumer-related services will be created.
     * 2. When the consumer client is created, consumer-related services will be created.
     * 3. The producer and the consumer client share the same MQClientInstance. Creating the producer first will not start the consumer-related services. When the consumer client is started, the consumer-related services will be started.
     * 4. When new clients are added to the same MQClientInstance instance multiple times, they will try to start consumer-related services, but those that have already been started will be ignored.
     *
     *
     * @throws MQClientException
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     */

    @Test
    public void testBothProducerAndConsumer() throws MQClientException, IllegalAccessException, NoSuchFieldException, MQBrokerException, RemotingException, InterruptedException {
        MQClientAPIImpl mQClientAPIImpl = mock(MQClientAPIImpl.class);

        DefaultMQProducer producer = new DefaultMQProducer("szz_producer");
        producer.setInstanceName("szz_producerAndConsumer_instanceName");
        producer.setNamesrvAddr("127.0.0.1:9876");

        MQClientInstance mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(producer);
        FieldUtils.writeDeclaredField(mQClientFactory, "mQClientAPIImpl", mQClientAPIImpl, true);

        MQClientInstance mQClientFactorySpy = spy(mQClientFactory);
        lenient().doReturn(false).when(mQClientFactorySpy).updateTopicRouteInfoFromNameServer(anyString());
        lenient().doReturn(null).when(mQClientFactorySpy).queryAssignment(anyString(), anyString(), anyString(), any(MessageModel.class), anyInt());
        lenient().doReturn(new FindBrokerResult("127.0.0.1:10911", false)).when(mQClientFactorySpy).findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean());

        ConcurrentMap<String, MQClientInstance> factoryTable = (ConcurrentMap<String, MQClientInstance>) FieldUtils.readDeclaredField(MQClientManager.getInstance(), "factoryTable", true);
        MQClientInstance instance1 = factoryTable.get(producer.buildMQClientId());
        // already created
        assertThat(instance1).isEqualTo(mQClientFactory);

        producer.start();

        // only producer shouldn't start inner DefaultMQProducer and pullRequest
        ServiceState serviceState = mQClientFactory.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceState).isEqualTo(ServiceState.CREATE_JUST);

        // pullMessageService can't start
        Field field =  PullMessageService.class.getSuperclass().getDeclaredField("started");
        field.setAccessible(true);

        AtomicBoolean pullStared = (AtomicBoolean) field.get(mQClientFactory.getPullMessageService());
        assertThat(pullStared.get()).isEqualTo(false);

        // balanceService started
        RebalanceService rebalanceService = (RebalanceService)FieldUtils.readDeclaredField(mQClientFactory,"rebalanceService",true);
        AtomicBoolean rebalancedStared = (AtomicBoolean)field.get(rebalanceService);
        assertThat(rebalancedStared.get()).isEqualTo(false);




        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("szz_consumer");
        consumer.setInstanceName("szz_producerAndConsumer_instanceName");
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.start();

        // already created
        MQClientInstance mQClientFactory2 = MQClientManager.getInstance().getOrCreateMQClientInstance(consumer);
        assertThat(mQClientFactory2).isEqualTo(mQClientFactory);

        // inner producer running
        ServiceState innerProducerServiceState = mQClientFactory2.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(innerProducerServiceState).isEqualTo(ServiceState.RUNNING);


        Field startedField =  PullMessageService.class.getSuperclass().getDeclaredField("started");
        startedField.setAccessible(true);

        // pullMessageService stared
        AtomicBoolean consumerPullStared = (AtomicBoolean) startedField.get(mQClientFactory2.getPullMessageService());
        assertThat(consumerPullStared.get()).isEqualTo(true);

        // balanceService started
        RebalanceService rebalanceService2 = (RebalanceService)FieldUtils.readDeclaredField(mQClientFactory2,"rebalanceService",true);
        AtomicBoolean rebalancedStared2 = (AtomicBoolean)field.get(rebalanceService2);
        assertThat(rebalancedStared2.get()).isEqualTo(true);


        // start second consumer

        DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer("szz_consumer2");
        consumer2.setInstanceName("szz_producerAndConsumer_instanceName");
        consumer2.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer2.setNamesrvAddr("127.0.0.1:9876");
        consumer2.start();



        consumer.shutdown();

        // only all client shutdown then can shutdown all service
        // pullMessageService close
        AtomicBoolean pullClose = (AtomicBoolean) field.get(mQClientFactory2.getPullMessageService());
        assertThat(pullClose.get()).isEqualTo(true);

        AtomicBoolean rebalancedClose = (AtomicBoolean)field.get(rebalanceService);
        assertThat(rebalancedClose.get()).isEqualTo(true);

        ServiceState serviceStateAfter = mQClientFactory2.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceStateAfter).isEqualTo(ServiceState.RUNNING);



        producer.shutdown();
        consumer2.shutdown();

        ServiceState producerServiceState = producer.getDefaultMQProducerImpl().getServiceState();
        assertThat(producerServiceState).isEqualTo(ServiceState.SHUTDOWN_ALREADY);

        AtomicBoolean pullClose2 = (AtomicBoolean) field.get(mQClientFactory2.getPullMessageService());
        assertThat(pullClose2.get()).isEqualTo(false);

        AtomicBoolean rebalancedClose2 = (AtomicBoolean)field.get(rebalanceService);
        assertThat(rebalancedClose2.get()).isEqualTo(false);

        ServiceState serviceStateAfter2 = mQClientFactory2.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceStateAfter2).isEqualTo(ServiceState.SHUTDOWN_ALREADY);

    }




}
