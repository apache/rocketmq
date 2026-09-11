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
package org.apache.rocketmq.proxy.service.admin;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.impl.admin.MqClientAdminImpl;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultAdminServiceTest {

    @Mock
    private MQClientAPIFactory mqClientAPIFactory;
    @Mock
    private MQClientAPIExt mqClientAPIExt;
    @Mock
    private MqClientAdminImpl mqClientAdmin;

    private DefaultAdminService adminService;

    private static final String ADDR = "127.0.0.1:10911";
    private static final String TOPIC = "topicA";
    private static final String GROUP = "groupA";
    private static final long TIMEOUT = 3000L;
    private static final long GET_TIMEOUT_SECONDS = 5L;

    @Before
    public void setUp() {
        when(mqClientAPIFactory.getClient()).thenReturn(mqClientAPIExt);
        when(mqClientAPIExt.getMqClientAdmin()).thenReturn(mqClientAdmin);
        adminService = new DefaultAdminService(mqClientAPIFactory);
    }

    @After
    public void tearDown() {
        adminService.shutdown();
    }

    private static <T> T get(CompletableFuture<T> future) throws Exception {
        return future.get(GET_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void getBrokerClusterInfoQueriesNameserverTest() throws Exception {
        ClusterInfo clusterInfo = new ClusterInfo();
        when(mqClientAdmin.getBrokerClusterInfo(isNull(), eq(TIMEOUT)))
            .thenReturn(CompletableFuture.completedFuture(clusterInfo));

        assertSame(clusterInfo, get(adminService.getBrokerClusterInfo(TIMEOUT)));
    }

    @Test
    public void getTopicRouteDataDelegatesToNameserverTest() throws Exception {
        TopicRouteData routeData = new TopicRouteData();
        routeData.setOrderTopicConf("orderConf");
        when(mqClientAPIExt.getTopicRouteInfoFromNameServer(eq(TOPIC), anyLong())).thenReturn(routeData);

        TopicRouteData result = get(adminService.getTopicRouteData(TOPIC));

        assertNotNull(result);
        assertEquals("orderConf", result.getOrderTopicConf());
    }

    @Test
    public void getTopicRouteDataWrapsFailuresTest() throws Exception {
        when(mqClientAPIExt.getTopicRouteInfoFromNameServer(eq(TOPIC), anyLong()))
            .thenThrow(new MQClientException("boom", null));

        try {
            get(adminService.getTopicRouteData(TOPIC));
            fail("expected getTopicRouteData to fail");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            while (cause != null && !(cause instanceof MQClientException)) {
                cause = cause.getCause();
            }
            assertNotNull(cause);
            assertTrue(cause.getMessage().contains("boom"));
        }
    }

    @Test
    public void getTopicConfigRunsBlockingCallOnExecutorTest() throws Exception {
        TopicConfigAndQueueMapping topicConfig = new TopicConfigAndQueueMapping();
        topicConfig.setTopicName(TOPIC);
        topicConfig.setReadQueueNums(8);
        when(mqClientAPIExt.getTopicConfig(ADDR, TOPIC, TIMEOUT)).thenReturn(topicConfig);

        TopicConfig result = get(adminService.getTopicConfig(ADDR, TOPIC, TIMEOUT));

        assertSame(topicConfig, result);
        assertEquals(8, result.getReadQueueNums());
    }

    @Test
    public void getTopicStatsBuildsHeaderTest() throws Exception {
        TopicStatsTable statsTable = new TopicStatsTable();
        when(mqClientAdmin.getTopicStatsInfo(eq(ADDR), any(GetTopicStatsInfoRequestHeader.class), eq(TIMEOUT)))
            .thenReturn(CompletableFuture.completedFuture(statsTable));

        assertSame(statsTable, get(adminService.getTopicStats(ADDR, TOPIC, TIMEOUT)));

        ArgumentCaptor<GetTopicStatsInfoRequestHeader> captor =
            ArgumentCaptor.forClass(GetTopicStatsInfoRequestHeader.class);
        verify(mqClientAdmin).getTopicStatsInfo(eq(ADDR), captor.capture(), eq(TIMEOUT));
        assertEquals(TOPIC, captor.getValue().getTopic());
    }

    @Test
    public void getConsumeStatsBuildsHeaderTest() throws Exception {
        ConsumeStats consumeStats = new ConsumeStats();
        when(mqClientAdmin.getConsumeStats(eq(ADDR), any(GetConsumeStatsRequestHeader.class), eq(TIMEOUT)))
            .thenReturn(CompletableFuture.completedFuture(consumeStats));

        assertSame(consumeStats, get(adminService.getConsumeStats(ADDR, GROUP, TOPIC, TIMEOUT)));
        // a blank topic means "every topic of the group" and must reach the broker as ""
        assertSame(consumeStats, get(adminService.getConsumeStats(ADDR, GROUP, null, TIMEOUT)));

        ArgumentCaptor<GetConsumeStatsRequestHeader> captor =
            ArgumentCaptor.forClass(GetConsumeStatsRequestHeader.class);
        verify(mqClientAdmin, times(2))
            .getConsumeStats(eq(ADDR), captor.capture(), eq(TIMEOUT));
        List<GetConsumeStatsRequestHeader> headers = captor.getAllValues();
        assertEquals(GROUP, headers.get(0).getConsumerGroup());
        assertEquals(TOPIC, headers.get(0).getTopic());
        assertEquals(GROUP, headers.get(1).getConsumerGroup());
        assertEquals("", headers.get(1).getTopic());
    }

    @Test
    public void queryConsumeTimeSpanBuildsHeaderTest() throws Exception {
        List<QueueTimeSpan> spans = Collections.singletonList(new QueueTimeSpan());
        when(mqClientAdmin.queryConsumeTimeSpan(eq(ADDR), any(QueryConsumeTimeSpanRequestHeader.class), eq(TIMEOUT)))
            .thenReturn(CompletableFuture.completedFuture(spans));

        assertSame(spans, get(adminService.queryConsumeTimeSpan(ADDR, TOPIC, GROUP, TIMEOUT)));

        ArgumentCaptor<QueryConsumeTimeSpanRequestHeader> captor =
            ArgumentCaptor.forClass(QueryConsumeTimeSpanRequestHeader.class);
        verify(mqClientAdmin).queryConsumeTimeSpan(eq(ADDR), captor.capture(), eq(TIMEOUT));
        assertEquals(TOPIC, captor.getValue().getTopic());
        assertEquals(GROUP, captor.getValue().getGroup());
    }

    @Test
    public void resetOffsetBuildsHeaderTest() throws Exception {
        Map<MessageQueue, Long> offsets = Collections.singletonMap(new MessageQueue(TOPIC, "broker-a", 0), 42L);
        when(mqClientAdmin.invokeBrokerToResetOffset(eq(ADDR), any(ResetOffsetRequestHeader.class), eq(TIMEOUT)))
            .thenReturn(CompletableFuture.completedFuture(offsets));

        Map<MessageQueue, Long> result =
            get(adminService.resetOffset(ADDR, TOPIC, GROUP, 1234L, true, TIMEOUT));

        assertEquals(42L, result.get(new MessageQueue(TOPIC, "broker-a", 0)).longValue());
        ArgumentCaptor<ResetOffsetRequestHeader> captor = ArgumentCaptor.forClass(ResetOffsetRequestHeader.class);
        verify(mqClientAdmin).invokeBrokerToResetOffset(eq(ADDR), captor.capture(), eq(TIMEOUT));
        assertEquals(TOPIC, captor.getValue().getTopic());
        assertEquals(GROUP, captor.getValue().getGroup());
        assertEquals(1234L, captor.getValue().getTimestamp());
        assertTrue(captor.getValue().isForce());
    }

    @Test
    public void getConsumerConnectionListBuildsHeaderTest() throws Exception {
        ConsumerConnection connection = new ConsumerConnection();
        when(mqClientAdmin.getConsumerConnectionList(eq(ADDR),
            any(GetConsumerConnectionListRequestHeader.class), eq(TIMEOUT)))
            .thenReturn(CompletableFuture.completedFuture(connection));

        assertSame(connection, get(adminService.getConsumerConnectionList(ADDR, GROUP, TIMEOUT)));

        ArgumentCaptor<GetConsumerConnectionListRequestHeader> captor =
            ArgumentCaptor.forClass(GetConsumerConnectionListRequestHeader.class);
        verify(mqClientAdmin).getConsumerConnectionList(eq(ADDR), captor.capture(), eq(TIMEOUT));
        assertEquals(GROUP, captor.getValue().getConsumerGroup());
    }

    @Test
    public void queryTopicConsumeByWhoBuildsHeaderTest() throws Exception {
        GroupList groupList = new GroupList();
        when(mqClientAdmin.queryTopicConsumeByWho(eq(ADDR), any(QueryTopicConsumeByWhoRequestHeader.class),
            eq(TIMEOUT))).thenReturn(CompletableFuture.completedFuture(groupList));

        assertSame(groupList, get(adminService.queryTopicConsumeByWho(ADDR, TOPIC, TIMEOUT)));

        ArgumentCaptor<QueryTopicConsumeByWhoRequestHeader> captor =
            ArgumentCaptor.forClass(QueryTopicConsumeByWhoRequestHeader.class);
        verify(mqClientAdmin).queryTopicConsumeByWho(eq(ADDR), captor.capture(), eq(TIMEOUT));
        assertEquals(TOPIC, captor.getValue().getTopic());
    }

    @Test
    public void queryTopicsByConsumerBuildsHeaderTest() throws Exception {
        TopicList topicList = new TopicList();
        when(mqClientAdmin.queryTopicsByConsumer(eq(ADDR), any(QueryTopicsByConsumerRequestHeader.class),
            eq(TIMEOUT))).thenReturn(CompletableFuture.completedFuture(topicList));

        assertSame(topicList, get(adminService.queryTopicsByConsumer(ADDR, GROUP, TIMEOUT)));

        ArgumentCaptor<QueryTopicsByConsumerRequestHeader> captor =
            ArgumentCaptor.forClass(QueryTopicsByConsumerRequestHeader.class);
        verify(mqClientAdmin).queryTopicsByConsumer(eq(ADDR), captor.capture(), eq(TIMEOUT));
        assertEquals(GROUP, captor.getValue().getGroup());
    }

    @Test
    public void getSubscriptionGroupConfigRunsBlockingCallOnExecutorTest() throws Exception {
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(GROUP);
        when(mqClientAPIExt.getSubscriptionGroupConfig(ADDR, GROUP, TIMEOUT)).thenReturn(config);

        SubscriptionGroupConfig result = get(adminService.getSubscriptionGroupConfig(ADDR, GROUP, TIMEOUT));

        assertSame(config, result);
    }

    @Test
    public void updateSubscriptionGroupConfigDelegatesTest() throws Exception {
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(GROUP);
        when(mqClientAdmin.updateOrCreateSubscriptionGroup(ADDR, config, TIMEOUT))
            .thenReturn(CompletableFuture.completedFuture(null));

        get(adminService.updateSubscriptionGroupConfig(ADDR, config, TIMEOUT));

        verify(mqClientAdmin).updateOrCreateSubscriptionGroup(ADDR, config, TIMEOUT);
    }

    @Test
    public void deleteSubscriptionGroupBuildsHeaderTest() throws Exception {
        when(mqClientAdmin.deleteSubscriptionGroup(eq(ADDR), any(DeleteSubscriptionGroupRequestHeader.class),
            eq(TIMEOUT))).thenReturn(CompletableFuture.completedFuture(null));

        get(adminService.deleteSubscriptionGroup(ADDR, GROUP, true, TIMEOUT));

        ArgumentCaptor<DeleteSubscriptionGroupRequestHeader> captor =
            ArgumentCaptor.forClass(DeleteSubscriptionGroupRequestHeader.class);
        verify(mqClientAdmin).deleteSubscriptionGroup(eq(ADDR), captor.capture(), eq(TIMEOUT));
        assertEquals(GROUP, captor.getValue().getGroupName());
        assertTrue(captor.getValue().isCleanOffset());
    }

    @Test
    public void queryMessagePassesUniqueKeyAndDecompressFlagsTest() throws Exception {
        List<MessageExt> messages = Collections.singletonList(new MessageExt());
        when(mqClientAdmin.queryMessage(eq(ADDR), anyBoolean(), anyBoolean(),
            any(QueryMessageRequestHeader.class), eq(TIMEOUT)))
            .thenReturn(CompletableFuture.completedFuture(messages));

        // uniqueKey=true selects the client-generated unique-key index; header fields carried through
        assertSame(messages,
            get(adminService.queryMessage(ADDR, TOPIC, "UNIQ-1", 10, 100L, 200L, true, true, TIMEOUT)));
        ArgumentCaptor<QueryMessageRequestHeader> captor = ArgumentCaptor.forClass(QueryMessageRequestHeader.class);
        verify(mqClientAdmin).queryMessage(eq(ADDR), eq(true), eq(true), captor.capture(), eq(TIMEOUT));
        QueryMessageRequestHeader header = captor.getValue();
        assertEquals(TOPIC, header.getTopic());
        assertEquals("UNIQ-1", header.getKey());
        assertEquals(10, header.getMaxNum().intValue());
        assertEquals(100L, header.getBeginTimestamp().longValue());
        assertEquals(200L, header.getEndTimestamp().longValue());

        // uniqueKey=false matches against the message keys index instead
        get(adminService.queryMessage(ADDR, TOPIC, "key-1", 10, 0L, System.currentTimeMillis(), false, true, TIMEOUT));
        verify(mqClientAdmin).queryMessage(eq(ADDR), eq(false), eq(true),
            any(QueryMessageRequestHeader.class), eq(TIMEOUT));

        // decompressBody=false: VerifyMessage relays the message, so it wants the body as stored
        get(adminService.queryMessage(ADDR, TOPIC, "UNIQ-1", 1, 0L, Long.MAX_VALUE, true, false, TIMEOUT));
        verify(mqClientAdmin).queryMessage(eq(ADDR), eq(true), eq(false),
            any(QueryMessageRequestHeader.class), eq(TIMEOUT));
    }

    @Test
    public void viewMessageBuildsHeaderTest() throws Exception {
        MessageExt messageExt = new MessageExt();
        messageExt.setMsgId("m1");
        when(mqClientAdmin.viewMessage(eq(ADDR), any(ViewMessageRequestHeader.class), eq(TIMEOUT)))
            .thenReturn(CompletableFuture.completedFuture(messageExt));

        MessageExt result = get(adminService.viewMessage(ADDR, TOPIC, 12345L, TIMEOUT));

        assertEquals("m1", result.getMsgId());
        ArgumentCaptor<ViewMessageRequestHeader> captor = ArgumentCaptor.forClass(ViewMessageRequestHeader.class);
        verify(mqClientAdmin).viewMessage(eq(ADDR), captor.capture(), eq(TIMEOUT));
        assertEquals(TOPIC, captor.getValue().getTopic());
        assertEquals(12345L, captor.getValue().getOffset().longValue());
    }

    @Test
    public void searchOffsetByTimestampRunsBlockingCallOnExecutorTest() throws Exception {
        MessageQueue messageQueue = new MessageQueue(TOPIC, "broker-a", 0);
        when(mqClientAPIExt.searchOffset(ADDR, messageQueue, 1234L, TIMEOUT)).thenReturn(42L);

        Long result = get(adminService.searchOffsetByTimestamp(ADDR, messageQueue, 1234L, TIMEOUT));

        assertEquals(42L, result.longValue());
    }

    @Test
    public void topicExistReflectsRouteLookupTest() throws Exception {
        // a resolvable route means the topic exists
        when(mqClientAPIExt.getTopicRouteInfoFromNameServer(eq(TOPIC), anyLong())).thenReturn(new TopicRouteData());
        assertTrue(adminService.topicExist(TOPIC));

        // a route lookup failure is treated as "does not exist"
        when(mqClientAPIExt.getTopicRouteInfoFromNameServer(anyString(), anyLong()))
            .thenThrow(new MQClientException("No route info of this topic", null));
        assertFalse(adminService.topicExist(TOPIC));
    }
}
