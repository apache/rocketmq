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
package org.apache.rocketmq.client.impl;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MQAdminImplTest {

    private MQAdminImpl mqAdminImpl;

    private final String defaultTopic = "defaultTopic";

    private final String defaultBroker = "defaultBroker";

    private final String defaultBrokerAddr = "127.0.0.1:10911";

    private final long defaultTimeout = 3000L;

    @Before
    public void init() throws RemotingException, InterruptedException, MQClientException {
        MQClientInstance mQClientFactory = mock(MQClientInstance.class);
        MQClientAPIImpl mqClientAPI = mock(MQClientAPIImpl.class);
        when(mQClientFactory.getMQClientAPIImpl()).thenReturn(mqClientAPI);
        when(mqClientAPI.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createRouteData());
        ClientConfig clientConfig = mock(ClientConfig.class);
        when(mQClientFactory.getClientConfig()).thenReturn(clientConfig);
        when(mQClientFactory.findBrokerAddressInPublish(any())).thenReturn(defaultBrokerAddr);
        when(mQClientFactory.getAnExistTopicRouteData(any())).thenReturn(createRouteData());
        mqAdminImpl = new MQAdminImpl(mQClientFactory);
    }

    @Test
    public void assertTimeoutMillis() {
        assertEquals(6000L, mqAdminImpl.getTimeoutMillis());
        mqAdminImpl.setTimeoutMillis(defaultTimeout);
        assertEquals(defaultTimeout, mqAdminImpl.getTimeoutMillis());
    }

    @Test
    public void testCreateTopic() throws MQClientException {
        mqAdminImpl.createTopic("", defaultTopic, 6);
    }

    @Test
    public void assertFetchPublishMessageQueues() throws MQClientException {
        List<MessageQueue> queueList = mqAdminImpl.fetchPublishMessageQueues(defaultTopic);
        assertNotNull(queueList);
        assertEquals(6, queueList.size());
        for (MessageQueue each : queueList) {
            assertEquals(defaultTopic, each.getTopic());
            assertEquals(defaultBroker, each.getBrokerName());
        }
    }

    @Test
    public void assertFetchSubscribeMessageQueues() throws MQClientException {
        Set<MessageQueue> queueList = mqAdminImpl.fetchSubscribeMessageQueues(defaultTopic);
        assertNotNull(queueList);
        assertEquals(6, queueList.size());
        for (MessageQueue each : queueList) {
            assertEquals(defaultTopic, each.getTopic());
            assertEquals(defaultBroker, each.getBrokerName());
        }
    }

    @Test
    public void assertSearchOffset() throws MQClientException {
        assertEquals(0, mqAdminImpl.searchOffset(new MessageQueue(), defaultTimeout));
    }

    @Test
    public void assertMaxOffset() throws MQClientException {
        assertEquals(0, mqAdminImpl.maxOffset(new MessageQueue()));
    }

    @Test
    public void assertMinOffset() throws MQClientException {
        assertEquals(0, mqAdminImpl.minOffset(new MessageQueue()));
    }

    @Test
    public void assertEarliestMsgStoreTime() throws MQClientException {
        assertEquals(0, mqAdminImpl.earliestMsgStoreTime(new MessageQueue()));
    }

    @Test(expected = MQClientException.class)
    public void assertViewMessage() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        MessageExt actual = mqAdminImpl.viewMessage(defaultTopic, "1");
        assertNotNull(actual);
    }

    @Test(expected = MQClientException.class)
    public void assertQueryMessage() throws InterruptedException, MQClientException {
        QueryResult actual = mqAdminImpl.queryMessage(defaultTopic, "", 100, 1L, 50L);
        assertNotNull(actual);
    }

    @Test(expected = MQClientException.class)
    public void assertQueryMessageByUniqKey() throws InterruptedException, MQClientException {
        MessageExt actual = mqAdminImpl.queryMessageByUniqKey(defaultTopic, "");
        assertNotNull(actual);
    }

    private TopicRouteData createRouteData() {
        TopicRouteData result = new TopicRouteData();
        result.setBrokerDatas(createBrokerData());
        result.setQueueDatas(createQueueData());
        return result;
    }

    private List<BrokerData> createBrokerData() {
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, defaultBrokerAddr);
        return Collections.singletonList(new BrokerData("defaultCluster", defaultBroker, brokerAddrs));
    }

    private List<QueueData> createQueueData() {
        QueueData queueData = new QueueData();
        queueData.setPerm(6);
        queueData.setBrokerName(defaultBroker);
        queueData.setReadQueueNums(6);
        queueData.setWriteQueueNums(6);
        return Collections.singletonList(queueData);
    }
}
