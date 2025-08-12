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
package org.apache.rocketmq.client.impl.admin;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MqClientAdminImplTest {

    @Mock
    private RemotingClient remotingClient;

    @Mock
    private RemotingCommand response;

    private MqClientAdminImpl mqClientAdminImpl;

    private final String defaultTopic = "defaultTopic";

    private final String defaultBrokerAddr = "127.0.0.1:10911";

    private final long defaultTimeout = 3000L;

    @Before
    public void init() throws RemotingException, InterruptedException, MQClientException {
        mqClientAdminImpl = new MqClientAdminImpl(remotingClient);
        when(remotingClient.invoke(any(String.class), any(RemotingCommand.class), any(Long.class))).thenReturn(CompletableFuture.completedFuture(response));
    }

    @Test
    public void assertQueryMessageWithSuccess() throws Exception {
        setResponseSuccess(getMessageResult());
        QueryMessageRequestHeader requestHeader = mock(QueryMessageRequestHeader.class);
        when(requestHeader.getTopic()).thenReturn(defaultTopic);
        when(requestHeader.getKey()).thenReturn("keys");
        CompletableFuture<List<MessageExt>> actual = mqClientAdminImpl.queryMessage(defaultBrokerAddr, false, false, requestHeader, defaultTimeout);
        List<MessageExt> messageExtList = actual.get();
        assertNotNull(messageExtList);
        assertEquals(1, messageExtList.size());
    }

    @Test
    public void assertQueryMessageWithNotFound() throws Exception {
        when(response.getCode()).thenReturn(ResponseCode.QUERY_NOT_FOUND);
        QueryMessageRequestHeader requestHeader = mock(QueryMessageRequestHeader.class);
        CompletableFuture<List<MessageExt>> actual = mqClientAdminImpl.queryMessage(defaultBrokerAddr, false, false, requestHeader, defaultTimeout);
        List<MessageExt> messageExtList = actual.get();
        assertNotNull(messageExtList);
        assertEquals(0, messageExtList.size());
    }

    @Test
    public void assertQueryMessageWithError() {
        setResponseError();
        QueryMessageRequestHeader requestHeader = mock(QueryMessageRequestHeader.class);
        CompletableFuture<List<MessageExt>> actual = mqClientAdminImpl.queryMessage(defaultBrokerAddr, false, false, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertGetTopicStatsInfoWithSuccess() throws Exception {
        TopicStatsTable responseBody = new TopicStatsTable();
        setResponseSuccess(RemotingSerializable.encode(responseBody));
        GetTopicStatsInfoRequestHeader requestHeader = mock(GetTopicStatsInfoRequestHeader.class);
        CompletableFuture<TopicStatsTable> actual = mqClientAdminImpl.getTopicStatsInfo(defaultBrokerAddr, requestHeader, defaultTimeout);
        TopicStatsTable topicStatsTable = actual.get();
        assertNotNull(topicStatsTable);
        assertEquals(0, topicStatsTable.getOffsetTable().size());
    }

    @Test
    public void assertGetTopicStatsInfoWithError() {
        setResponseError();
        GetTopicStatsInfoRequestHeader requestHeader = mock(GetTopicStatsInfoRequestHeader.class);
        CompletableFuture<TopicStatsTable> actual = mqClientAdminImpl.getTopicStatsInfo(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertQueryConsumeTimeSpanWithSuccess() throws Exception {
        QueryConsumeTimeSpanBody responseBody = new QueryConsumeTimeSpanBody();
        setResponseSuccess(RemotingSerializable.encode(responseBody));
        QueryConsumeTimeSpanRequestHeader requestHeader = mock(QueryConsumeTimeSpanRequestHeader.class);
        CompletableFuture<List<QueueTimeSpan>> actual = mqClientAdminImpl.queryConsumeTimeSpan(defaultBrokerAddr, requestHeader, defaultTimeout);
        List<QueueTimeSpan> queueTimeSpans = actual.get();
        assertNotNull(queueTimeSpans);
        assertEquals(0, queueTimeSpans.size());
    }

    @Test
    public void assertQueryConsumeTimeSpanWithError() {
        setResponseError();
        QueryConsumeTimeSpanRequestHeader requestHeader = mock(QueryConsumeTimeSpanRequestHeader.class);
        CompletableFuture<List<QueueTimeSpan>> actual = mqClientAdminImpl.queryConsumeTimeSpan(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertUpdateOrCreateTopicWithSuccess() throws Exception {
        setResponseSuccess(null);
        CreateTopicRequestHeader requestHeader = mock(CreateTopicRequestHeader.class);
        CompletableFuture<Void> actual = mqClientAdminImpl.updateOrCreateTopic(defaultBrokerAddr, requestHeader, defaultTimeout);
        assertNull(actual.get());
    }

    @Test
    public void assertUpdateOrCreateTopicWithError() {
        setResponseError();
        CreateTopicRequestHeader requestHeader = mock(CreateTopicRequestHeader.class);
        CompletableFuture<Void> actual = mqClientAdminImpl.updateOrCreateTopic(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertUpdateOrCreateSubscriptionGroupWithSuccess() throws Exception {
        if (!MixAll.isJdk8()) {
            return;
        }
        setResponseSuccess(null);
        SubscriptionGroupConfig config = mock(SubscriptionGroupConfig.class);
        CompletableFuture<Void> actual = mqClientAdminImpl.updateOrCreateSubscriptionGroup(defaultBrokerAddr, config, defaultTimeout);
        assertNull(actual.get());
    }

    @Test
    public void assertUpdateOrCreateSubscriptionGroupWithError() {
        if (!MixAll.isJdk8()) {
            return;
        }
        setResponseError();
        SubscriptionGroupConfig config = mock(SubscriptionGroupConfig.class);
        CompletableFuture<Void> actual = mqClientAdminImpl.updateOrCreateSubscriptionGroup(defaultBrokerAddr, config, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertDeleteTopicInBrokerWithSuccess() throws Exception {
        setResponseSuccess(null);
        DeleteTopicRequestHeader requestHeader = mock(DeleteTopicRequestHeader.class);
        CompletableFuture<Void> actual = mqClientAdminImpl.deleteTopicInBroker(defaultBrokerAddr, requestHeader, defaultTimeout);
        assertNull(actual.get());
    }

    @Test
    public void assertDeleteTopicInBrokerWithError() {
        setResponseError();
        DeleteTopicRequestHeader requestHeader = mock(DeleteTopicRequestHeader.class);
        CompletableFuture<Void> actual = mqClientAdminImpl.deleteTopicInBroker(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertDeleteTopicInNameserverWithSuccess() throws Exception {
        setResponseSuccess(null);
        DeleteTopicFromNamesrvRequestHeader requestHeader = mock(DeleteTopicFromNamesrvRequestHeader.class);
        CompletableFuture<Void> actual = mqClientAdminImpl.deleteTopicInNameserver(defaultBrokerAddr, requestHeader, defaultTimeout);
        assertNull(actual.get());
    }

    @Test
    public void assertDeleteTopicInNameserverWithError() {
        setResponseError();
        DeleteTopicFromNamesrvRequestHeader requestHeader = mock(DeleteTopicFromNamesrvRequestHeader.class);
        CompletableFuture<Void> actual = mqClientAdminImpl.deleteTopicInNameserver(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertDeleteKvConfigWithSuccess() throws Exception {
        setResponseSuccess(null);
        DeleteKVConfigRequestHeader requestHeader = mock(DeleteKVConfigRequestHeader.class);
        CompletableFuture<Void> actual = mqClientAdminImpl.deleteKvConfig(defaultBrokerAddr, requestHeader, defaultTimeout);
        assertNull(actual.get());
    }

    @Test
    public void assertDeleteKvConfigWithError() {
        setResponseError();
        DeleteKVConfigRequestHeader requestHeader = mock(DeleteKVConfigRequestHeader.class);
        CompletableFuture<Void> actual = mqClientAdminImpl.deleteKvConfig(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertDeleteSubscriptionGroupWithSuccess() throws Exception {
        setResponseSuccess(null);
        DeleteSubscriptionGroupRequestHeader requestHeader = mock(DeleteSubscriptionGroupRequestHeader.class);
        CompletableFuture<Void> actual = mqClientAdminImpl.deleteSubscriptionGroup(defaultBrokerAddr, requestHeader, defaultTimeout);
        assertNull(actual.get());
    }

    @Test
    public void assertDeleteSubscriptionGroupWithError() {
        setResponseError();
        DeleteSubscriptionGroupRequestHeader requestHeader = mock(DeleteSubscriptionGroupRequestHeader.class);
        CompletableFuture<Void> actual = mqClientAdminImpl.deleteSubscriptionGroup(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertInvokeBrokerToResetOffsetWithSuccess() throws Exception {
        ResetOffsetBody responseBody = new ResetOffsetBody();
        setResponseSuccess(RemotingSerializable.encode(responseBody));
        ResetOffsetRequestHeader requestHeader = mock(ResetOffsetRequestHeader.class);
        CompletableFuture<Map<MessageQueue, Long>> actual = mqClientAdminImpl.invokeBrokerToResetOffset(defaultBrokerAddr, requestHeader, defaultTimeout);
        assertEquals(0, actual.get().size());
    }

    @Test
    public void assertInvokeBrokerToResetOffsetWithError() {
        setResponseError();
        ResetOffsetRequestHeader requestHeader = mock(ResetOffsetRequestHeader.class);
        CompletableFuture<Map<MessageQueue, Long>> actual = mqClientAdminImpl.invokeBrokerToResetOffset(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertViewMessageWithSuccess() throws Exception {
        setResponseSuccess(getMessageResult());
        ViewMessageRequestHeader requestHeader = mock(ViewMessageRequestHeader.class);
        CompletableFuture<MessageExt> actual = mqClientAdminImpl.viewMessage(defaultBrokerAddr, requestHeader, defaultTimeout);
        MessageExt result = actual.get();
        assertNotNull(result);
        assertEquals(defaultTopic, result.getTopic());
    }

    @Test
    public void assertViewMessageWithError() {
        setResponseError();
        ViewMessageRequestHeader requestHeader = mock(ViewMessageRequestHeader.class);
        CompletableFuture<MessageExt> actual = mqClientAdminImpl.viewMessage(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertGetBrokerClusterInfoWithSuccess() throws Exception {
        ClusterInfo responseBody = new ClusterInfo();
        setResponseSuccess(RemotingSerializable.encode(responseBody));
        CompletableFuture<ClusterInfo> actual = mqClientAdminImpl.getBrokerClusterInfo(defaultBrokerAddr, defaultTimeout);
        ClusterInfo result = actual.get();
        assertNotNull(result);
    }

    @Test
    public void assertGetBrokerClusterInfoWithError() {
        setResponseError();
        CompletableFuture<ClusterInfo> actual = mqClientAdminImpl.getBrokerClusterInfo(defaultBrokerAddr, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertGetConsumerConnectionListWithSuccess() throws Exception {
        ConsumerConnection responseBody = new ConsumerConnection();
        setResponseSuccess(RemotingSerializable.encode(responseBody));
        GetConsumerConnectionListRequestHeader requestHeader = mock(GetConsumerConnectionListRequestHeader.class);
        CompletableFuture<ConsumerConnection> actual = mqClientAdminImpl.getConsumerConnectionList(defaultBrokerAddr, requestHeader, defaultTimeout);
        ConsumerConnection result = actual.get();
        assertNotNull(result);
        assertEquals(0, result.getConnectionSet().size());
    }

    @Test
    public void assertGetConsumerConnectionListWithError() {
        setResponseError();
        GetConsumerConnectionListRequestHeader requestHeader = mock(GetConsumerConnectionListRequestHeader.class);
        CompletableFuture<ConsumerConnection> actual = mqClientAdminImpl.getConsumerConnectionList(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertQueryTopicsByConsumerWithSuccess() throws Exception {
        TopicList responseBody = new TopicList();
        setResponseSuccess(RemotingSerializable.encode(responseBody));
        QueryTopicsByConsumerRequestHeader requestHeader = mock(QueryTopicsByConsumerRequestHeader.class);
        CompletableFuture<TopicList> actual = mqClientAdminImpl.queryTopicsByConsumer(defaultBrokerAddr, requestHeader, defaultTimeout);
        TopicList result = actual.get();
        assertNotNull(result);
        assertEquals(0, result.getTopicList().size());
    }

    @Test
    public void assertQueryTopicsByConsumerWithError() {
        setResponseError();
        QueryTopicsByConsumerRequestHeader requestHeader = mock(QueryTopicsByConsumerRequestHeader.class);
        CompletableFuture<TopicList> actual = mqClientAdminImpl.queryTopicsByConsumer(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertQuerySubscriptionByConsumerWithSuccess() throws Exception {
        SubscriptionData responseBody = new SubscriptionData();
        setResponseSuccess(RemotingSerializable.encode(responseBody));
        QuerySubscriptionByConsumerRequestHeader requestHeader = mock(QuerySubscriptionByConsumerRequestHeader.class);
        CompletableFuture<SubscriptionData> actual = mqClientAdminImpl.querySubscriptionByConsumer(defaultBrokerAddr, requestHeader, defaultTimeout);
        assertNull(actual.get());
    }

    @Test
    public void assertQuerySubscriptionByConsumerWithError() {
        setResponseError();
        QuerySubscriptionByConsumerRequestHeader requestHeader = mock(QuerySubscriptionByConsumerRequestHeader.class);
        CompletableFuture<SubscriptionData> actual = mqClientAdminImpl.querySubscriptionByConsumer(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertGetConsumeStatsWithSuccess() throws Exception {
        ConsumeStats responseBody = new ConsumeStats();
        setResponseSuccess(RemotingSerializable.encode(responseBody));
        GetConsumeStatsRequestHeader requestHeader = mock(GetConsumeStatsRequestHeader.class);
        CompletableFuture<ConsumeStats> actual = mqClientAdminImpl.getConsumeStats(defaultBrokerAddr, requestHeader, defaultTimeout);
        ConsumeStats result = actual.get();
        assertNotNull(result);
        assertEquals(0, result.getOffsetTable().size());
    }

    @Test
    public void assertGetConsumeStatsWithError() {
        setResponseError();
        GetConsumeStatsRequestHeader requestHeader = mock(GetConsumeStatsRequestHeader.class);
        CompletableFuture<ConsumeStats> actual = mqClientAdminImpl.getConsumeStats(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertQueryTopicConsumeByWhoWithSuccess() throws Exception {
        GroupList responseBody = new GroupList();
        setResponseSuccess(RemotingSerializable.encode(responseBody));
        QueryTopicConsumeByWhoRequestHeader requestHeader = mock(QueryTopicConsumeByWhoRequestHeader.class);
        CompletableFuture<GroupList> actual = mqClientAdminImpl.queryTopicConsumeByWho(defaultBrokerAddr, requestHeader, defaultTimeout);
        GroupList result = actual.get();
        assertNotNull(result);
        assertEquals(0, result.getGroupList().size());
    }

    @Test
    public void assertQueryTopicConsumeByWhoWithError() {
        setResponseError();
        QueryTopicConsumeByWhoRequestHeader requestHeader = mock(QueryTopicConsumeByWhoRequestHeader.class);
        CompletableFuture<GroupList> actual = mqClientAdminImpl.queryTopicConsumeByWho(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertGetConsumerRunningInfoWithSuccess() throws Exception {
        ConsumerRunningInfo responseBody = new ConsumerRunningInfo();
        setResponseSuccess(RemotingSerializable.encode(responseBody));
        GetConsumerRunningInfoRequestHeader requestHeader = mock(GetConsumerRunningInfoRequestHeader.class);
        CompletableFuture<ConsumerRunningInfo> actual = mqClientAdminImpl.getConsumerRunningInfo(defaultBrokerAddr, requestHeader, defaultTimeout);
        ConsumerRunningInfo result = actual.get();
        assertNotNull(result);
        assertEquals(0, result.getProperties().size());
    }

    @Test
    public void assertGetConsumerRunningInfoWithError() {
        setResponseError();
        GetConsumerRunningInfoRequestHeader requestHeader = mock(GetConsumerRunningInfoRequestHeader.class);
        CompletableFuture<ConsumerRunningInfo> actual = mqClientAdminImpl.getConsumerRunningInfo(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
    }

    @Test
    public void assertConsumeMessageDirectlyWithSuccess() throws Exception {
        ConsumeMessageDirectlyResult responseBody = new ConsumeMessageDirectlyResult();
        setResponseSuccess(RemotingSerializable.encode(responseBody));
        ConsumeMessageDirectlyResultRequestHeader requestHeader = mock(ConsumeMessageDirectlyResultRequestHeader.class);
        CompletableFuture<ConsumeMessageDirectlyResult> actual = mqClientAdminImpl.consumeMessageDirectly(defaultBrokerAddr, requestHeader, defaultTimeout);
        ConsumeMessageDirectlyResult result = actual.get();
        assertNotNull(result);
        assertTrue(result.isAutoCommit());
    }

    @Test
    public void assertConsumeMessageDirectlyWithError() {
        setResponseError();
        ConsumeMessageDirectlyResultRequestHeader requestHeader = mock(ConsumeMessageDirectlyResultRequestHeader.class);
        CompletableFuture<ConsumeMessageDirectlyResult> actual = mqClientAdminImpl.consumeMessageDirectly(defaultBrokerAddr, requestHeader, defaultTimeout);
        Throwable thrown = assertThrows(ExecutionException.class, actual::get);
        assertTrue(thrown.getCause() instanceof MQClientException);
        MQClientException mqException = (MQClientException) thrown.getCause();
        assertEquals(ResponseCode.SYSTEM_ERROR, mqException.getResponseCode());
        assertTrue(mqException.getMessage().contains("CODE: 1  DESC: null"));
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
        result.setBrokerName("defaultBroker");
        result.putUserProperty("key", "value");
        result.getProperties().put(MessageConst.PROPERTY_PRODUCER_GROUP, "defaultGroup");
        result.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "TX1");
        result.setKeys("keys");
        SocketAddress bornHost = new InetSocketAddress("127.0.0.1", 12911);
        SocketAddress storeHost = new InetSocketAddress("127.0.0.1", 10911);
        result.setBornHost(bornHost);
        result.setStoreHost(storeHost);
        return result;
    }

    private void setResponseSuccess(byte[] body) {
        when(response.getCode()).thenReturn(ResponseCode.SUCCESS);
        when(response.getBody()).thenReturn(body);
    }

    private void setResponseError() {
        when(response.getCode()).thenReturn(ResponseCode.SYSTEM_ERROR);
    }
}
