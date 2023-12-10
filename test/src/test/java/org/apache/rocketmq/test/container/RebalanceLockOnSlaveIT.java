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

package org.apache.rocketmq.test.container;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Test lock on slave when acting master enabled
 */
@Ignore
public class RebalanceLockOnSlaveIT extends ContainerIntegrationTestBase {
    private static final String THREE_REPLICA_CONSUMER_GROUP = "SyncConsumerOffsetIT_ConsumerThreeReplica";

    private static DefaultMQProducer mqProducer;
    private static DefaultMQPushConsumer mqConsumerThreeReplica1;
    private static DefaultMQPushConsumer mqConsumerThreeReplica2;
    private static DefaultMQPushConsumer mqConsumerThreeReplica3;

    public RebalanceLockOnSlaveIT() {
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        mqProducer = createProducer("SyncConsumerOffsetIT_Producer");
        mqProducer.start();

        mqConsumerThreeReplica1 = createPushConsumer(THREE_REPLICA_CONSUMER_GROUP);
        mqConsumerThreeReplica1.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        mqConsumerThreeReplica1.subscribe(THREE_REPLICAS_TOPIC, "*");

        mqConsumerThreeReplica2 = createPushConsumer(THREE_REPLICA_CONSUMER_GROUP);
        mqConsumerThreeReplica2.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        mqConsumerThreeReplica2.subscribe(THREE_REPLICAS_TOPIC, "*");

        mqConsumerThreeReplica3 = createPushConsumer(THREE_REPLICA_CONSUMER_GROUP);
        mqConsumerThreeReplica3.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        mqConsumerThreeReplica3.subscribe(THREE_REPLICAS_TOPIC, "*");
    }

    @AfterClass
    public static void afterClass() {
        if (mqProducer != null) {
            mqProducer.shutdown();
        }
    }

    @Test
    public void lockFromSlave() throws Exception {
        awaitUntilSlaveOK();

        mqConsumerThreeReplica3.registerMessageListener((MessageListenerOrderly) (msgs, context) -> ConsumeOrderlyStatus.SUCCESS);
        mqConsumerThreeReplica3.start();

        final Set<MessageQueue> mqSet = mqConsumerThreeReplica3.fetchSubscribeMessageQueues(THREE_REPLICAS_TOPIC);

        assertThat(targetTopicMqCount(mqSet, THREE_REPLICAS_TOPIC)).isEqualTo(24);

        for (MessageQueue mq : mqSet) {
            await().atMost(Duration.ofSeconds(60)).until(() -> mqConsumerThreeReplica3.getDefaultMQPushConsumerImpl().getRebalanceImpl().lock(mq));
        }

        isolateBroker(master3With3Replicas);

        mqConsumerThreeReplica3.getDefaultMQPushConsumerImpl().getMQClientFactory().updateTopicRouteInfoFromNameServer(THREE_REPLICAS_TOPIC);
        FindBrokerResult result = mqConsumerThreeReplica3.getDefaultMQPushConsumerImpl().getMQClientFactory().findBrokerAddressInSubscribe(
            master3With3Replicas.getBrokerConfig().getBrokerName(), MixAll.MASTER_ID, true);
        assertThat(result).isNotNull();

        for (MessageQueue mq : mqSet) {
            if (mq.getBrokerName().equals(master3With3Replicas.getBrokerConfig().getBrokerName())) {
                await().atMost(Duration.ofSeconds(60)).until(() -> mqConsumerThreeReplica3.getDefaultMQPushConsumerImpl().getRebalanceImpl().lock(mq));
            }
        }

        removeSlaveBroker(1, brokerContainer1, master3With3Replicas);
        assertThat(brokerContainer1.getSlaveBrokers().size()).isEqualTo(1);

        mqConsumerThreeReplica3.getDefaultMQPushConsumerImpl().getMQClientFactory().updateTopicRouteInfoFromNameServer(THREE_REPLICAS_TOPIC);

        for (MessageQueue mq : mqSet) {
            if (mq.getBrokerName().equals(master3With3Replicas.getBrokerConfig().getBrokerName())) {
                await().atMost(Duration.ofSeconds(60)).until(() -> !mqConsumerThreeReplica3.getDefaultMQPushConsumerImpl().getRebalanceImpl().lock(mq));
            }
        }

        cancelIsolatedBroker(master3With3Replicas);
        createAndAddSlave(1, brokerContainer1, master3With3Replicas);
        awaitUntilSlaveOK();

        mqConsumerThreeReplica3.shutdown();
        await().atMost(100, TimeUnit.SECONDS).until(() -> mqConsumerThreeReplica3.getDefaultMQPushConsumerImpl().getServiceState() == ServiceState.SHUTDOWN_ALREADY);
    }

    @Ignore
    @Test
    public void multiConsumerLockFromSlave() throws MQClientException, InterruptedException {
        awaitUntilSlaveOK();

        mqConsumerThreeReplica1.registerMessageListener((MessageListenerOrderly) (msgs, context) -> ConsumeOrderlyStatus.SUCCESS);
        mqConsumerThreeReplica1.start();

        mqConsumerThreeReplica1.getDefaultMQPushConsumerImpl().doRebalance();
        Set<MessageQueue> mqSet1 = filterMessageQueue(mqConsumerThreeReplica1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet(), THREE_REPLICAS_TOPIC);

        assertThat(mqSet1.size()).isEqualTo(24);

        isolateBroker(master3With3Replicas);

        System.out.printf("%s isolated%n", master3With3Replicas.getBrokerConfig().getCanonicalName());

        Thread.sleep(5000);

        mqConsumerThreeReplica2.registerMessageListener((MessageListenerOrderly) (msgs, context) -> ConsumeOrderlyStatus.SUCCESS);
        mqConsumerThreeReplica2.start();

        Thread.sleep(5000);

        mqConsumerThreeReplica1.getDefaultMQPushConsumerImpl().getMQClientFactory().updateTopicRouteInfoFromNameServer(THREE_REPLICAS_TOPIC);
        mqConsumerThreeReplica2.getDefaultMQPushConsumerImpl().getMQClientFactory().updateTopicRouteInfoFromNameServer(THREE_REPLICAS_TOPIC);

        assertThat(mqConsumerThreeReplica1.getDefaultMQPushConsumerImpl().getMQClientFactory().findBrokerAddressInSubscribe(
            master3With3Replicas.getBrokerConfig().getBrokerName(), MixAll.MASTER_ID, true)).isNotNull();

        mqConsumerThreeReplica2.getDefaultMQPushConsumerImpl().getMQClientFactory().findBrokerAddressInSubscribe(
            master3With3Replicas.getBrokerConfig().getBrokerName(), MixAll.MASTER_ID, true);
        assertThat(mqConsumerThreeReplica2.getDefaultMQPushConsumerImpl().getMQClientFactory().findBrokerAddressInSubscribe(
            master3With3Replicas.getBrokerConfig().getBrokerName(), MixAll.MASTER_ID, true)).isNotNull();

        mqConsumerThreeReplica1.getDefaultMQPushConsumerImpl().doRebalance();
        mqConsumerThreeReplica2.getDefaultMQPushConsumerImpl().doRebalance();

        Set<MessageQueue> mqSet2 = filterMessageQueue(mqConsumerThreeReplica2.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet(), THREE_REPLICAS_TOPIC);

        mqSet1 = filterMessageQueue(mqConsumerThreeReplica1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet(), THREE_REPLICAS_TOPIC);

        List<MessageQueue> mqList = new ArrayList<>();

        for (MessageQueue mq : mqSet2) {
            if (mq.getTopic().equals(THREE_REPLICAS_TOPIC)) {
                mqList.add(mq);
            }
        }

        for (MessageQueue mq : mqSet1) {
            if (mq.getTopic().equals(THREE_REPLICAS_TOPIC)) {
                mqList.add(mq);
            }
        }

        await().atMost(Duration.ofSeconds(30)).until(() -> mqList.size() == 24);

        cancelIsolatedBroker(master3With3Replicas);
        awaitUntilSlaveOK();

        mqConsumerThreeReplica1.shutdown();
        mqConsumerThreeReplica2.shutdown();

        await().atMost(100, TimeUnit.SECONDS).until(() ->
            mqConsumerThreeReplica1.getDefaultMQPushConsumerImpl().getServiceState() == ServiceState.SHUTDOWN_ALREADY &&
                mqConsumerThreeReplica2.getDefaultMQPushConsumerImpl().getServiceState() == ServiceState.SHUTDOWN_ALREADY
        );
    }

    private static int targetTopicMqCount(Set<MessageQueue> mqSet, String topic) {
        int count = 0;
        for (MessageQueue mq : mqSet) {
            if (mq.getTopic().equals(topic)) {
                count++;
            }
        }
        return count;
    }
}
