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
package org.apache.rocketmq.example.lmq;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

public class LMQPushConsumer {
    public static final String CLUSTER_NAME = "DefaultCluster";

    public static final String BROKER_NAME = "broker-a";

    public static final String TOPIC = "TopicLMQParent";

    public static final String LMQ_TOPIC = MixAll.LMQ_PREFIX + "123";

    public static final String CONSUMER_GROUP = "CID_LMQ_1";

    public static final String NAMESRV_ADDR = "127.0.0.1:9876";

    public static final HashMap<Long, String> BROKER_ADDR_MAP = new HashMap<Long, String>() {
        {
            put(MixAll.MASTER_ID, "127.0.0.1:10911");
        }
    };

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.subscribe(LMQ_TOPIC, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();

        // use parent topic to fill up broker addr table
        consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(TOPIC);

        final TopicRouteData topicRouteData = new TopicRouteData();
        final BrokerData brokerData = new BrokerData();
        brokerData.setCluster(CLUSTER_NAME);
        brokerData.setBrokerName(BROKER_NAME);
        brokerData.setBrokerAddrs(BROKER_ADDR_MAP);
        topicRouteData.setBrokerDatas(Lists.newArrayList(brokerData));
        // compensate LMQ topic route for MQClientInstance#findBrokerAddrByTopic
        consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getTopicRouteTable().put(LMQ_TOPIC, topicRouteData);
        // compensate for RebalanceImpl#topicSubscribeInfoTable
        consumer.getDefaultMQPushConsumerImpl().updateTopicSubscribeInfo(LMQ_TOPIC,
            new HashSet<>(Arrays.asList(new MessageQueue(LMQ_TOPIC, BROKER_NAME, (int) MixAll.LMQ_QUEUE_ID))));
        // re-balance immediately to start pulling messages
        consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().doRebalance();

        System.out.printf("Consumer Started.%n");
    }
}
