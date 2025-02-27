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
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MQAdminImplTest {

    @Mock
    private MQClientInstance mQClientFactory;

    @Mock
    private MQClientAPIImpl mQClientAPIImpl;

    private MQAdminImpl mqAdminImpl;

    private final String defaultTopic = "defaultTopic";

    private final String defaultBroker = "defaultBroker";

    private final String defaultCluster = "defaultCluster";

    private final String defaultBrokerAddr = "127.0.0.1:10911";

    private final long defaultTimeout = 3000L;

    @Before
    public void init() throws RemotingException, InterruptedException, MQClientException {
        when(mQClientFactory.getMQClientAPIImpl()).thenReturn(mQClientAPIImpl);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createRouteData());
        ClientConfig clientConfig = mock(ClientConfig.class);
        when(clientConfig.getNamespace()).thenReturn("namespace");
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

    @Test
    public void assertQueryMessage() throws InterruptedException, MQClientException, MQBrokerException, RemotingException {
        doAnswer(invocation -> {
            InvokeCallback callback = invocation.getArgument(3);
            QueryMessageResponseHeader responseHeader = new QueryMessageResponseHeader();
            responseHeader.setIndexLastUpdatePhyoffset(1L);
            responseHeader.setIndexLastUpdateTimestamp(System.currentTimeMillis());
            RemotingCommand response = mock(RemotingCommand.class);
            when(response.decodeCommandCustomHeader(QueryMessageResponseHeader.class)).thenReturn(responseHeader);
            when(response.getBody()).thenReturn(getMessageResult());
            when(response.getCode()).thenReturn(ResponseCode.SUCCESS);
            callback.operationSucceed(response);
            return null;
        }).when(mQClientAPIImpl).queryMessage(anyString(), any(), anyLong(), any(InvokeCallback.class), any());
        QueryResult actual = mqAdminImpl.queryMessage(defaultTopic, "keys", 100, 1L, 50L);
        assertNotNull(actual);
        assertEquals(1, actual.getMessageList().size());
        assertEquals(defaultTopic, actual.getMessageList().get(0).getTopic());
    }

    @Test
    public void assertQueryMessageByUniqKey() throws InterruptedException, MQClientException, MQBrokerException, RemotingException {
        doAnswer(invocation -> {
            InvokeCallback callback = invocation.getArgument(3);
            QueryMessageResponseHeader responseHeader = new QueryMessageResponseHeader();
            responseHeader.setIndexLastUpdatePhyoffset(1L);
            responseHeader.setIndexLastUpdateTimestamp(System.currentTimeMillis());
            RemotingCommand response = mock(RemotingCommand.class);
            when(response.decodeCommandCustomHeader(QueryMessageResponseHeader.class)).thenReturn(responseHeader);
            when(response.getBody()).thenReturn(getMessageResult());
            when(response.getCode()).thenReturn(ResponseCode.SUCCESS);
            callback.operationSucceed(response);
            return null;
        }).when(mQClientAPIImpl).queryMessage(anyString(), any(), anyLong(), any(InvokeCallback.class), any());
        String msgId = buildMsgId();
        MessageExt actual = mqAdminImpl.queryMessageByUniqKey(defaultTopic, msgId);
        assertNotNull(actual);
        assertEquals(msgId, actual.getMsgId());
        assertEquals(defaultTopic, actual.getTopic());
        actual = mqAdminImpl.queryMessageByUniqKey(defaultCluster, defaultTopic, msgId);
        assertNotNull(actual);
        assertEquals(msgId, actual.getMsgId());
        assertEquals(defaultTopic, actual.getTopic());
        QueryResult queryResult = mqAdminImpl.queryMessageByUniqKey(defaultTopic, msgId, 1, 0L, 1L);
        assertNotNull(queryResult);
        assertEquals(1, queryResult.getMessageList().size());
        assertEquals(defaultTopic, queryResult.getMessageList().get(0).getTopic());
    }

    private String buildMsgId() {
        MessageExt msgExt = createMessageExt();
        int storeHostIPLength = (msgExt.getFlag() & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 : 16;
        int msgIDLength = storeHostIPLength + 4 + 8;
        ByteBuffer byteBufferMsgId = ByteBuffer.allocate(msgIDLength);
        return MessageDecoder.createMessageId(byteBufferMsgId, msgExt.getStoreHostBytes(), msgExt.getCommitLogOffset());
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
        return Collections.singletonList(new BrokerData(defaultCluster, defaultBroker, brokerAddrs));
    }

    private List<QueueData> createQueueData() {
        QueueData queueData = new QueueData();
        queueData.setPerm(6);
        queueData.setBrokerName(defaultBroker);
        queueData.setReadQueueNums(6);
        queueData.setWriteQueueNums(6);
        return Collections.singletonList(queueData);
    }

    private byte[] getMessageResult() throws Exception {
        byte[] bytes = MessageDecoder.encode(createMessageExt(), false);
        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
        byteBuffer.put(bytes);
        return byteBuffer.array();
    }

    private MessageExt createMessageExt() {
        MessageExt result = new MessageExt();
        result.setBody("body".getBytes(StandardCharsets.UTF_8));
        result.setTopic(defaultTopic);
        result.setBrokerName(defaultBroker);
        result.putUserProperty("key", "value");
        result.setKeys("keys");
        SocketAddress bornHost = new InetSocketAddress("127.0.0.1", 12911);
        SocketAddress storeHost = new InetSocketAddress("127.0.0.1", 10911);
        result.setBornHost(bornHost);
        result.setStoreHost(storeHost);
        return result;
    }
}
