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
package org.apache.rocketmq.test.lmq;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.RebalanceImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.test.lmq.benchmark.BenchLmqStore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestBenchLmqStore {
    @Test
    public void test() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        System.setProperty("sendThreadNum", "1");
        System.setProperty("pullConsumerNum", "1");
        System.setProperty("consumerThreadNum", "1");
        BenchLmqStore.defaultMQProducer = mock(DefaultMQProducer.class);
        SendResult sendResult = new SendResult();
        when(BenchLmqStore.defaultMQProducer.send(any(Message.class))).thenReturn(sendResult);
        BenchLmqStore.doSend();
        Thread.sleep(100L);
        //verify(BenchLmqStore.defaultMQProducer, atLeastOnce()).send(any(Message.class));
        BenchLmqStore.defaultMQPullConsumers = new DefaultMQPullConsumer[1];
        BenchLmqStore.defaultMQPullConsumers[0] = mock(DefaultMQPullConsumer.class);
        BenchLmqStore.doPull(new ConcurrentHashMap<>(), new MessageQueue(), 1L);
        verify(BenchLmqStore.defaultMQPullConsumers[0], atLeastOnce()).pullBlockIfNotFound(any(MessageQueue.class), anyString(), anyLong(), anyInt(), any(
            PullCallback.class));
    }

    @Test
    public void testOffset() throws RemotingException, InterruptedException, MQClientException, MQBrokerException, IllegalAccessException {
        System.setProperty("sendThreadNum", "1");
        DefaultMQPullConsumer defaultMQPullConsumer = mock(DefaultMQPullConsumer.class);
        BenchLmqStore.defaultMQPullConsumers = new DefaultMQPullConsumer[1];
        BenchLmqStore.defaultMQPullConsumers[0] = defaultMQPullConsumer;
        DefaultMQPullConsumerImpl defaultMQPullConsumerImpl = mock(DefaultMQPullConsumerImpl.class);
        when(defaultMQPullConsumer.getDefaultMQPullConsumerImpl()).thenReturn(defaultMQPullConsumerImpl);
        RebalanceImpl rebalanceImpl = mock(RebalanceImpl.class);
        when(defaultMQPullConsumerImpl.getRebalanceImpl()).thenReturn(rebalanceImpl);
        MQClientInstance mqClientInstance = mock(MQClientInstance.class);
        when(rebalanceImpl.getmQClientFactory()).thenReturn(mqClientInstance);
        MQClientAPIImpl mqClientAPI = mock(MQClientAPIImpl.class);
        when(mqClientInstance.getMQClientAPIImpl()).thenReturn(mqClientAPI);
        TopicRouteData topicRouteData = new TopicRouteData();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, "test");
        List<BrokerData> brokerData = Collections.singletonList(new BrokerData("test", "test", brokerAddrs));
        topicRouteData.setBrokerDatas(brokerData);
        FieldUtils.writeStaticField(BenchLmqStore.class, "lmqTopic", "test", true);
        when(mqClientAPI.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);
        BenchLmqStore.doBenchOffset();
        Thread.sleep(100L);
        verify(mqClientAPI, atLeastOnce()).queryConsumerOffset(anyString(), any(QueryConsumerOffsetRequestHeader.class), anyLong());
        verify(mqClientAPI, atLeastOnce()).updateConsumerOffset(anyString(), any(UpdateConsumerOffsetRequestHeader.class), anyLong());
    }
}
