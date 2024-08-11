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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PullAPIWrapperTest {

    @Mock
    private MQClientInstance mQClientFactory;

    @Mock
    private MQClientAPIImpl mqClientAPIImpl;

    private PullAPIWrapper pullAPIWrapper;

    private final String defaultGroup = "defaultGroup";

    private final String defaultBroker = "defaultBroker";

    private final String defaultTopic = "defaultTopic";

    private final String defaultBrokerAddr = "127.0.0.1:10911";

    private final long defaultTimeout = 3000L;

    @Before
    public void init() throws Exception {
        ClientConfig clientConfig = mock(ClientConfig.class);
        when(mQClientFactory.getClientConfig()).thenReturn(clientConfig);
        MQClientAPIImpl mqClientAPIImpl = mock(MQClientAPIImpl.class);
        when(mQClientFactory.getMQClientAPIImpl()).thenReturn(mqClientAPIImpl);
        when(mQClientFactory.getTopicRouteTable()).thenReturn(createTopicRouteTable());
        FindBrokerResult findBrokerResult = mock(FindBrokerResult.class);
        when(findBrokerResult.getBrokerAddr()).thenReturn(defaultBrokerAddr);
        when(mQClientFactory.findBrokerAddressInSubscribe(any(), anyLong(), anyBoolean())).thenReturn(findBrokerResult);
        pullAPIWrapper = new PullAPIWrapper(mQClientFactory, defaultGroup, false);
        ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<>();
        filterMessageHookList.add(mock(FilterMessageHook.class));
        FieldUtils.writeDeclaredField(pullAPIWrapper, "filterMessageHookList", filterMessageHookList, true);
    }

    @Test
    public void testProcessPullResult() throws Exception {
        PullResultExt pullResult = mock(PullResultExt.class);
        when(pullResult.getPullStatus()).thenReturn(PullStatus.FOUND);
        when(pullResult.getMessageBinary()).thenReturn(MessageDecoder.encode(createMessageExt(), false));
        SubscriptionData subscriptionData = mock(SubscriptionData.class);
        PullResult actual = pullAPIWrapper.processPullResult(createMessageQueue(), pullResult, subscriptionData);
        assertNotNull(actual);
        assertEquals(0, actual.getNextBeginOffset());
        assertEquals(0, actual.getMsgFoundList().size());
    }

    @Test
    public void testExecuteHook() throws IllegalAccessException {
        FilterMessageContext filterMessageContext = mock(FilterMessageContext.class);
        ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<>();
        FilterMessageHook filterMessageHook = mock(FilterMessageHook.class);
        filterMessageHookList.add(filterMessageHook);
        FieldUtils.writeDeclaredField(pullAPIWrapper, "filterMessageHookList", filterMessageHookList, true);
        pullAPIWrapper.executeHook(filterMessageContext);
        verify(filterMessageHook, times(1)).filterMessage(any(FilterMessageContext.class));
    }

    @Test
    public void testPullKernelImpl() throws Exception {
        PullCallback pullCallback = mock(PullCallback.class);
        when(mQClientFactory.getMQClientAPIImpl()).thenReturn(mqClientAPIImpl);
        PullResult actual = pullAPIWrapper.pullKernelImpl(createMessageQueue(),
                "",
                "",
                1L,
                1L,
                1,
                1,
                PullSysFlag.buildSysFlag(false, false, false, true),
                1L,
                System.currentTimeMillis(),
                defaultTimeout, CommunicationMode.ASYNC, pullCallback);
        assertNull(actual);
        verify(mqClientAPIImpl, times(1)).pullMessage(eq(defaultBroker),
                any(PullMessageRequestHeader.class),
                eq(defaultTimeout),
                any(CommunicationMode.class),
                any(PullCallback.class));
    }

    @Test
    public void testSetConnectBrokerByUser() {
        pullAPIWrapper.setConnectBrokerByUser(true);
        assertTrue(pullAPIWrapper.isConnectBrokerByUser());
    }

    @Test
    public void testRandomNum() {
        int randomNum = pullAPIWrapper.randomNum();
        assertTrue(randomNum > 0);
    }

    @Test
    public void testSetDefaultBrokerId() {
        pullAPIWrapper.setDefaultBrokerId(MixAll.MASTER_ID);
        assertEquals(MixAll.MASTER_ID, pullAPIWrapper.getDefaultBrokerId());
    }

    @Test
    public void testPopAsync() throws RemotingException, InterruptedException, MQClientException {
        PopCallback popCallback = mock(PopCallback.class);
        when(mQClientFactory.getMQClientAPIImpl()).thenReturn(mqClientAPIImpl);
        pullAPIWrapper.popAsync(createMessageQueue(),
                System.currentTimeMillis(),
                1,
                defaultGroup,
                defaultTimeout,
                popCallback,
                true,
                1,
                false,
                "",
                "");
        verify(mqClientAPIImpl, times(1)).popMessageAsync(eq(defaultBroker),
                eq(defaultBrokerAddr),
                any(PopMessageRequestHeader.class),
                eq(13000L),
                any(PopCallback.class));
    }

    private ConcurrentMap<String, TopicRouteData> createTopicRouteTable() {
        TopicRouteData topicRouteData = new TopicRouteData();
        List<BrokerData> brokerDatas = new ArrayList<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName(defaultBroker);
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, defaultBroker);
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDatas.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDatas);
        HashMap<String, List<String>> filterServerTable = new HashMap<>();
        List<String> filterServers = new ArrayList<>();
        filterServers.add(defaultBroker);
        filterServerTable.put(defaultBrokerAddr, filterServers);
        topicRouteData.setFilterServerTable(filterServerTable);
        ConcurrentMap<String, TopicRouteData> result = new ConcurrentHashMap<>();
        result.put(defaultTopic, topicRouteData);
        return result;
    }

    private MessageQueue createMessageQueue() {
        MessageQueue result = new MessageQueue();
        result.setQueueId(0);
        result.setBrokerName(defaultBroker);
        result.setTopic(defaultTopic);
        return result;
    }

    private MessageExt createMessageExt() {
        MessageExt result = new MessageExt();
        result.setBody("body".getBytes(StandardCharsets.UTF_8));
        result.setTopic(defaultTopic);
        result.setBrokerName(defaultBroker);
        result.putUserProperty("key", "value");
        result.getProperties().put(MessageConst.PROPERTY_PRODUCER_GROUP, defaultGroup);
        result.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "TX1");
        long curTime = System.currentTimeMillis();
        result.setBornTimestamp(curTime - 1000);
        result.getProperties().put(MessageConst.PROPERTY_POP_CK, curTime + " " + curTime + " " + curTime + " " + curTime);
        result.setKeys("keys");
        result.setSysFlag(MessageSysFlag.INNER_BATCH_FLAG);
        result.setSysFlag(result.getSysFlag() | MessageSysFlag.NEED_UNWRAP_FLAG);
        SocketAddress bornHost = new InetSocketAddress("127.0.0.1", 12911);
        SocketAddress storeHost = new InetSocketAddress("127.0.0.1", 10911);
        result.setStoreHost(storeHost);
        result.setBornHost(bornHost);
        return result;
    }
}
