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
package org.apache.rocketmq.proxy.grpc.admin;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.DescribeGroupAccumulationResponse.GroupAccumulation;
import apache.rocketmq.v2.DescribeTopicStatusResponse;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.GetTopicRouteResponse;
import apache.rocketmq.v2.MessageQueueItem;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.QueryTimeSpanResponse;
import com.alibaba.fastjson2.JSON;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumeStatus;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AdminModelConverterTest {

    private static final String TOPIC = "topicA";
    private static final String GROUP = "groupA";
    private static final String BROKER_A = "broker-a";
    private static final String BROKER_B = "broker-b";
    private static final String ADDR_A = "127.0.0.1:10911";
    private static final String ADDR_B = "127.0.0.2:10911";

    // ------------------------------------------------------------------ helpers

    private static OffsetWrapper offsetWrapper(long brokerOffset, long consumerOffset, long pullOffset,
        long lastTimestamp) {
        OffsetWrapper wrapper = new OffsetWrapper();
        wrapper.setBrokerOffset(brokerOffset);
        wrapper.setConsumerOffset(consumerOffset);
        wrapper.setPullOffset(pullOffset);
        wrapper.setLastTimestamp(lastTimestamp);
        return wrapper;
    }

    private static ConsumeStats consumeStats(String topic, String brokerName, int queueId, OffsetWrapper wrapper) {
        ConsumeStats stats = new ConsumeStats();
        stats.getOffsetTable().put(new MessageQueue(topic, brokerName, queueId), wrapper);
        return stats;
    }

    private static Map<String, ConsumeStats> statsByBroker(ConsumeStats... statsList) {
        Map<String, ConsumeStats> map = new LinkedHashMap<>();
        for (int i = 0; i < statsList.length; i++) {
            map.put(i == 0 ? ADDR_A : ADDR_B, statsList[i]);
        }
        return map;
    }

    private static QueueTimeSpan queueTimeSpan(String topic, String brokerName, int queueId, long min, long max,
        long consume, long delay) {
        QueueTimeSpan span = new QueueTimeSpan();
        span.setMessageQueue(new MessageQueue(topic, brokerName, queueId));
        span.setMinTimeStamp(min);
        span.setMaxTimeStamp(max);
        span.setConsumeTimeStamp(consume);
        span.setDelayTime(delay);
        return span;
    }

    // ------------------------------------------------------------------ toAccumulation

    @Test
    public void toAccumulationSplitsInflightFromReadyTest() {
        // brokerOffset=100 consumerOffset=60 pullOffset=75:
        // accumulation = 100-60 = 40, inflight = 75-60 = 15, ready = 40-15 = 25
        ConsumeStats stats = consumeStats(TOPIC, BROKER_A, 0, offsetWrapper(100L, 60L, 75L, 0L));

        AdminModelConverter.AccumulationResult result =
            AdminModelConverter.toAccumulation(statsByBroker(stats), GROUP);

        assertEquals(40L, result.total.getAccumulation());
        assertEquals(15L, result.total.getInflightMessages());
        assertEquals(25L, result.total.getReadyMessages());
        GroupAccumulation byTopic = result.byTopic.get(TOPIC);
        assertNotNull(byTopic);
        assertEquals(40L, byTopic.getAccumulation());
        assertEquals(15L, byTopic.getInflightMessages());
        assertEquals(25L, byTopic.getReadyMessages());
    }

    @Test
    public void toAccumulationClampsNegativeDiffsToZeroTest() {
        // consumer ahead of the broker (offset moved back, stats skew): never report negative lag
        ConsumeStats stats = consumeStats(TOPIC, BROKER_A, 0, offsetWrapper(50L, 60L, 40L, 0L));

        AdminModelConverter.AccumulationResult result =
            AdminModelConverter.toAccumulation(statsByBroker(stats), GROUP);

        assertEquals(0L, result.total.getAccumulation());
        assertEquals(0L, result.total.getInflightMessages());
        assertEquals(0L, result.total.getReadyMessages());
        assertEquals(0L, result.byTopic.get(TOPIC).getAccumulation());
    }

    @Test
    public void toAccumulationCapsInflightAtAccumulationTest() {
        // pullOffset beyond brokerOffset: inflight must not exceed accumulation, ready stays >= 0
        ConsumeStats stats = consumeStats(TOPIC, BROKER_A, 0, offsetWrapper(100L, 90L, 120L, 0L));

        AdminModelConverter.AccumulationResult result =
            AdminModelConverter.toAccumulation(statsByBroker(stats), GROUP);

        assertEquals(10L, result.total.getAccumulation());
        assertEquals(10L, result.total.getInflightMessages());
        assertEquals(0L, result.total.getReadyMessages());
    }

    @Test
    public void toAccumulationFoldsPopRetryTopicIntoNormalTopicTest() {
        ConsumeStats normal = consumeStats(TOPIC, BROKER_A, 0, offsetWrapper(100L, 60L, 60L, 0L));
        ConsumeStats popRetryV1 = consumeStats(KeyBuilder.buildPopRetryTopicV1(TOPIC, GROUP), BROKER_A, 0,
            offsetWrapper(10L, 5L, 5L, 0L));
        ConsumeStats popRetryV2 = consumeStats(KeyBuilder.buildPopRetryTopicV2(TOPIC, GROUP), BROKER_B, 0,
            offsetWrapper(20L, 5L, 5L, 0L));
        Map<String, ConsumeStats> byBroker = new LinkedHashMap<>();
        byBroker.put(ADDR_A, merge(normal, popRetryV1));
        byBroker.put(ADDR_B, popRetryV2);

        AdminModelConverter.AccumulationResult result = AdminModelConverter.toAccumulation(byBroker, GROUP);

        // 40 (normal) + 5 (pop retry v1) + 15 (pop retry v2)
        assertEquals(60L, result.total.getAccumulation());
        assertEquals(1, result.byTopic.size());
        assertFalse(result.byTopic.containsKey(KeyBuilder.buildPopRetryTopicV1(TOPIC, GROUP)));
        assertFalse(result.byTopic.containsKey(KeyBuilder.buildPopRetryTopicV2(TOPIC, GROUP)));
        assertEquals(60L, result.byTopic.get(TOPIC).getAccumulation());
    }

    @Test
    public void toAccumulationCountsPullRetryInTotalButNotInByTopicTest() {
        String pullRetryTopic = MixAll.getRetryTopic(GROUP);
        ConsumeStats pullRetry = consumeStats(pullRetryTopic, BROKER_A, 0, offsetWrapper(30L, 10L, 10L, 0L));
        ConsumeStats normal = consumeStats(TOPIC, BROKER_A, 1, offsetWrapper(100L, 60L, 60L, 0L));

        AdminModelConverter.AccumulationResult result =
            AdminModelConverter.toAccumulation(statsByBroker(merge(pullRetry, normal)), GROUP);

        // the pull retry backlog is real lag of the group, so it belongs to the total ...
        assertEquals(60L, result.total.getAccumulation());
        // ... but %RETRY%<group> is not a user-visible topic, so it must not show up per topic
        assertEquals(1, result.byTopic.size());
        assertFalse(result.byTopic.containsKey(pullRetryTopic));
        assertEquals(40L, result.byTopic.get(TOPIC).getAccumulation());
    }

    @Test
    public void toAccumulationSetsLastConsumeTimestampAndDelayTest() {
        long lastTimestamp = System.currentTimeMillis() - 5000L;
        ConsumeStats stats = consumeStats(TOPIC, BROKER_A, 0, offsetWrapper(100L, 60L, 60L, lastTimestamp));

        AdminModelConverter.AccumulationResult result =
            AdminModelConverter.toAccumulation(statsByBroker(stats), GROUP);

        assertEquals(lastTimestamp, result.total.getLastConsumeTimestamp());
        assertTrue(result.total.hasDeliverDelayTime());
        assertTrue(result.total.getDeliverDelayTime().getSeconds() >= 4L);
    }

    @Test
    public void toAccumulationIgnoresTimestampWhenCaughtUpTest() {
        ConsumeStats stats = consumeStats(TOPIC, BROKER_A, 0,
            offsetWrapper(60L, 60L, 60L, System.currentTimeMillis()));

        AdminModelConverter.AccumulationResult result =
            AdminModelConverter.toAccumulation(statsByBroker(stats), GROUP);

        assertEquals(0L, result.total.getAccumulation());
        assertEquals(0L, result.total.getLastConsumeTimestamp());
        assertFalse(result.total.hasDeliverDelayTime());
    }

    @Test
    public void toAccumulationEmptyInputIsZeroTest() {
        AdminModelConverter.AccumulationResult result =
            AdminModelConverter.toAccumulation(Collections.emptyMap(), GROUP);

        assertEquals(0L, result.total.getAccumulation());
        assertEquals(0L, result.total.getInflightMessages());
        assertEquals(0L, result.total.getReadyMessages());
        assertTrue(result.byTopic.isEmpty());
    }

    private static ConsumeStats merge(ConsumeStats first, ConsumeStats second) {
        ConsumeStats merged = new ConsumeStats();
        merged.getOffsetTable().putAll(first.getOffsetTable());
        merged.getOffsetTable().putAll(second.getOffsetTable());
        return merged;
    }

    // ------------------------------------------------------------------ toQueryTimeSpan

    @Test
    public void toQueryTimeSpanMapsAllFieldsTest() {
        List<QueueTimeSpan> spans = Collections.singletonList(
            queueTimeSpan(TOPIC, BROKER_A, 3, 100L, 200L, 150L, 42L));

        QueryTimeSpanResponse response = AdminModelConverter.toQueryTimeSpan(spans);

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(1, response.getQueueTimeSpanListCount());
        QueryTimeSpanResponse.QueueTimeSpan span = response.getQueueTimeSpanList(0);
        assertEquals(TOPIC, span.getMessageQueue().getTopic().getName());
        assertEquals(BROKER_A, span.getMessageQueue().getBroker().getName());
        assertEquals(3, span.getMessageQueue().getId());
        assertEquals(100L, span.getMinTimestamp());
        assertEquals(200L, span.getMaxTimestamp());
        assertEquals(150L, span.getConsumeTimestamp());
        assertEquals(42L, span.getDelayTimeMs());
    }

    @Test
    public void toQueryTimeSpanClampsNegativeDelayToZeroTest() {
        List<QueueTimeSpan> spans = Collections.singletonList(
            queueTimeSpan(TOPIC, BROKER_A, 0, 100L, 200L, 150L, -5L));

        QueryTimeSpanResponse response = AdminModelConverter.toQueryTimeSpan(spans);

        assertEquals(0L, response.getQueueTimeSpanList(0).getDelayTimeMs());
    }

    @Test
    public void toQueryTimeSpanSkipsNullAndHandlesNullListTest() {
        // a null list is tolerated and yields an OK, empty response
        QueryTimeSpanResponse nullResponse = AdminModelConverter.toQueryTimeSpan(null);
        assertEquals(Code.OK, nullResponse.getStatus().getCode());
        assertEquals(0, nullResponse.getQueueTimeSpanListCount());

        // null / queue-less entries are skipped, only the valid span is mapped
        List<QueueTimeSpan> spans = new ArrayList<>();
        spans.add(null);
        spans.add(new QueueTimeSpan());
        spans.add(queueTimeSpan(TOPIC, BROKER_A, 0, 1L, 2L, 3L, 4L));
        QueryTimeSpanResponse response = AdminModelConverter.toQueryTimeSpan(spans);
        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(1, response.getQueueTimeSpanListCount());
    }

    // ------------------------------------------------------------------ toMessageType / toTopicStatus

    @Test
    public void toMessageTypeMapsKnownAndDegradesUnknownTest() {
        assertEquals(MessageType.NORMAL, AdminModelConverter.toMessageType(TopicMessageType.NORMAL));
        assertEquals(MessageType.FIFO, AdminModelConverter.toMessageType(TopicMessageType.FIFO));
        assertEquals(MessageType.DELAY, AdminModelConverter.toMessageType(TopicMessageType.DELAY));
        assertEquals(MessageType.TRANSACTION, AdminModelConverter.toMessageType(TopicMessageType.TRANSACTION));
        assertEquals(MessageType.LITE, AdminModelConverter.toMessageType(TopicMessageType.LITE));
        // MessageType.UNRECOGNIZED cannot be set on a builder, so unknowns must degrade, not blow up
        for (TopicMessageType type : Arrays.asList(TopicMessageType.MIXED, TopicMessageType.PRIORITY,
            TopicMessageType.UNSPECIFIED, null)) {
            MessageType mapped = AdminModelConverter.toMessageType(type);
            assertEquals(MessageType.MESSAGE_TYPE_UNSPECIFIED, mapped);
            assertNotEquals(MessageType.UNRECOGNIZED, mapped);
        }
    }

    @Test
    public void toTopicStatusReadsMessageTypeFromAttributesTest() {
        TopicConfig config = new TopicConfig();
        config.setTopicName(TOPIC);
        config.setReadQueueNums(8);
        config.setWriteQueueNums(16);
        config.setPerm(6);
        config.setTopicMessageType(TopicMessageType.FIFO);

        DescribeTopicStatusResponse response = AdminModelConverter.toTopicStatus(config, TOPIC);

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(MessageType.FIFO, response.getTopicMessageType());
        assertTrue(response.getDescription().contains("readQueueNums=8"));
        assertTrue(response.getDescription().contains("writeQueueNums=16"));
        assertTrue(response.getDescription().contains("perm=6"));
    }

    @Test
    public void toTopicStatusFallsBackWhenConfigThinOrMissingTest() {
        // an empty config (no message-type attribute) defaults to NORMAL
        DescribeTopicStatusResponse normal = AdminModelConverter.toTopicStatus(new TopicConfig(), TOPIC);
        assertEquals(MessageType.NORMAL, normal.getTopicMessageType());

        // a null config stays honest: UNSPECIFIED type + an explanatory description
        DescribeTopicStatusResponse missing = AdminModelConverter.toTopicStatus(null, TOPIC);
        assertEquals(Code.OK, missing.getStatus().getCode());
        assertEquals(MessageType.MESSAGE_TYPE_UNSPECIFIED, missing.getTopicMessageType());
        assertTrue(missing.getDescription().contains("topic config not available for " + TOPIC));
    }

    // ------------------------------------------------------------------ toTopicRoute / toMessageQueue

    @Test
    public void toTopicRouteSerializesRouteDataOrEmptyJsonTest() {
        TopicRouteData routeData = new TopicRouteData();
        routeData.setOrderTopicConf("orderConf");
        GetTopicRouteResponse response = AdminModelConverter.toTopicRoute(routeData);
        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(JSON.toJSONString(routeData), response.getTopicRouteData());

        // a null route serializes to empty JSON rather than the literal "null"
        GetTopicRouteResponse nullResponse = AdminModelConverter.toTopicRoute(null);
        assertEquals(Code.OK, nullResponse.getStatus().getCode());
        assertEquals("{}", nullResponse.getTopicRouteData());
    }

    @Test
    public void toMessageQueueSetsBrokerAndIdTest() {
        apache.rocketmq.v2.MessageQueue v2 =
            AdminModelConverter.toMessageQueue(new MessageQueue(TOPIC, BROKER_A, 3));

        assertEquals(TOPIC, v2.getTopic().getName());
        assertEquals(BROKER_A, v2.getBroker().getName());
        assertEquals(3, v2.getId());
    }

    // ------------------------------------------------------------------ clients

    @Test
    public void toClientInfoParsesHostnameAndPlainAddrTest() {
        // "hostname@ip:port" splits into hostname + egress ip; message model carried through
        Connection withHost = new Connection();
        withHost.setClientId("CID-1");
        withHost.setClientAddr("consumer-host@192.168.1.5:43210");
        withHost.setLanguage(LanguageCode.JAVA);
        withHost.setVersion(355);
        apache.rocketmq.v2.ClientInfo hostInfo = AdminModelConverter.toClientInfo(withHost,
            apache.rocketmq.v2.MessageModel.CLUSTERING);
        assertEquals("CID-1", hostInfo.getClientId());
        assertEquals("JAVA", hostInfo.getLanguage());
        assertEquals("355", hostInfo.getVersion());
        assertEquals("192.168.1.5", hostInfo.getEgressIp());
        assertEquals("consumer-host", hostInfo.getHostname());
        assertEquals(apache.rocketmq.v2.MessageModel.CLUSTERING, hostInfo.getMessageModel());

        // a plain "ip:port" yields no hostname; null message model degrades to UNSPECIFIED
        Connection plain = new Connection();
        plain.setClientId("CID-2");
        plain.setClientAddr("192.168.1.5:43210");
        apache.rocketmq.v2.ClientInfo plainInfo = AdminModelConverter.toClientInfo(plain, null);
        assertEquals("192.168.1.5", plainInfo.getEgressIp());
        assertEquals("", plainInfo.getHostname());
        assertEquals(apache.rocketmq.v2.MessageModel.MESSAGE_MODEL_UNSPECIFIED, plainInfo.getMessageModel());
    }

    @Test
    public void toMessageModelMapsValuesTest() {
        assertEquals(apache.rocketmq.v2.MessageModel.BROADCASTING,
            AdminModelConverter.toMessageModel(MessageModel.BROADCASTING));
        assertEquals(apache.rocketmq.v2.MessageModel.CLUSTERING,
            AdminModelConverter.toMessageModel(MessageModel.CLUSTERING));
        assertEquals(apache.rocketmq.v2.MessageModel.MESSAGE_MODEL_UNSPECIFIED,
            AdminModelConverter.toMessageModel(null));
    }

    @Test
    public void toFilterExpressionMapsSqlAndTagTest() {
        apache.rocketmq.v2.FilterExpression sql =
            AdminModelConverter.toFilterExpression(ExpressionType.SQL92, "a > 1");
        assertEquals(FilterType.SQL, sql.getType());
        assertEquals("a > 1", sql.getExpression());

        apache.rocketmq.v2.FilterExpression tag = AdminModelConverter.toFilterExpression(ExpressionType.TAG, "tagA");
        assertEquals(FilterType.TAG, tag.getType());
        assertEquals("tagA", tag.getExpression());

        apache.rocketmq.v2.FilterExpression nullExpression = AdminModelConverter.toFilterExpression(null, null);
        assertEquals(FilterType.TAG, nullExpression.getType());
        assertEquals("", nullExpression.getExpression());
    }

    @Test
    public void toConsumerRunningInfoMapsAllSectionsTest() {
        ConsumerRunningInfo runningInfo = new ConsumerRunningInfo();
        Properties properties = runningInfo.getProperties();
        properties.setProperty(ConsumerRunningInfo.PROP_CLIENT_VERSION, "V5_0_0");
        properties.setProperty(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, "false");

        SubscriptionData tagSub = new SubscriptionData();
        tagSub.setTopic(TOPIC);
        tagSub.setSubString("tagA");
        tagSub.setExpressionType(ExpressionType.TAG);
        SubscriptionData sqlSub = new SubscriptionData();
        sqlSub.setTopic("topicB");
        sqlSub.setSubString("a > 1");
        sqlSub.setExpressionType(ExpressionType.SQL92);
        runningInfo.getSubscriptionSet().add(tagSub);
        runningInfo.getSubscriptionSet().add(sqlSub);

        ProcessQueueInfo processQueueInfo = new ProcessQueueInfo();
        processQueueInfo.setCommitOffset(10L);
        processQueueInfo.setCachedMsgMinOffset(5L);
        processQueueInfo.setCachedMsgMaxOffset(15L);
        processQueueInfo.setCachedMsgCount(7);
        processQueueInfo.setCachedMsgSizeInMiB(2);
        processQueueInfo.setTransactionMsgMinOffset(1L);
        processQueueInfo.setTransactionMsgMaxOffset(3L);
        processQueueInfo.setTransactionMsgCount(4);
        processQueueInfo.setLocked(true);
        processQueueInfo.setTryUnlockTimes(9L);
        processQueueInfo.setLastLockTimestamp(111L);
        processQueueInfo.setDroped(false);
        processQueueInfo.setLastPullTimestamp(222L);
        processQueueInfo.setLastConsumeTimestamp(333L);
        runningInfo.getMqTable().put(new MessageQueue(TOPIC, BROKER_A, 0), processQueueInfo);

        ConsumeStatus consumeStatus = new ConsumeStatus();
        consumeStatus.setPullRT(1.5D);
        consumeStatus.setPullTPS(2.5D);
        consumeStatus.setConsumeRT(3.5D);
        consumeStatus.setConsumeOKTPS(4.5D);
        consumeStatus.setConsumeFailedTPS(5.5D);
        consumeStatus.setConsumeFailedMsgs(6L);
        runningInfo.getStatusTable().put(TOPIC, consumeStatus);

        apache.rocketmq.v2.ConsumerRunningInfo v2 = AdminModelConverter.toConsumerRunningInfo(runningInfo);

        // 1. properties
        assertEquals("V5_0_0", v2.getPropertiesMap().get(ConsumerRunningInfo.PROP_CLIENT_VERSION));
        assertEquals("false", v2.getPropertiesMap().get(ConsumerRunningInfo.PROP_CONSUME_ORDERLY));
        // 2. subscriptions
        assertEquals(2, v2.getSubscriptionsCount());
        assertEquals(FilterType.TAG, v2.getSubscriptionsMap().get(TOPIC).getType());
        assertEquals("tagA", v2.getSubscriptionsMap().get(TOPIC).getExpression());
        assertEquals(FilterType.SQL, v2.getSubscriptionsMap().get("topicB").getType());
        assertEquals("a > 1", v2.getSubscriptionsMap().get("topicB").getExpression());
        // 3. message queue table
        assertEquals(1, v2.getMessageQueueTableCount());
        MessageQueueItem item = v2.getMessageQueueTable(0);
        assertEquals(TOPIC, item.getMessageQueue().getTopic().getName());
        assertEquals(BROKER_A, item.getMessageQueue().getBroker().getName());
        assertEquals(0, item.getMessageQueue().getId());
        apache.rocketmq.v2.ProcessQueueInfo pqi = item.getProcessQueueInfo();
        assertEquals(10L, pqi.getCommitOffset());
        assertEquals(5L, pqi.getCachedMsgMinOffset());
        assertEquals(15L, pqi.getCachedMsgMaxOffset());
        assertEquals(7, pqi.getCachedMsgCount());
        assertEquals(2, pqi.getCachedMsgSizeInMib());
        assertEquals(1L, pqi.getTransactionMsgMinOffset());
        assertEquals(3L, pqi.getTransactionMsgMaxOffset());
        assertEquals(4, pqi.getTransactionMsgCount());
        assertTrue(pqi.getLocked());
        assertEquals(9L, pqi.getTryUnlockTimes());
        assertEquals(111L, pqi.getLastLockTimestamp());
        assertFalse(pqi.getDropped());
        assertEquals(222L, pqi.getLastPullTimestamp());
        assertEquals(333L, pqi.getLastConsumeTimestamp());
        // 4. consume status table
        assertEquals(1, v2.getConsumeStatusTableCount());
        apache.rocketmq.v2.ConsumeStatus status = v2.getConsumeStatusTableMap().get(TOPIC);
        assertNotNull(status);
        assertEquals(1.5D, status.getReceiveRt(), 0.0001D);
        assertEquals(2.5D, status.getReceiveTps(), 0.0001D);
        assertEquals(3.5D, status.getConsumeRt(), 0.0001D);
        assertEquals(4.5D, status.getConsumeOkTps(), 0.0001D);
        assertEquals(5.5D, status.getConsumeFailedTps(), 0.0001D);
        assertEquals(6L, status.getConsumeFailedMsgs());
    }

    @Test
    public void toConsumerRunningInfoNullReturnsEmptyTest() {
        apache.rocketmq.v2.ConsumerRunningInfo v2 = AdminModelConverter.toConsumerRunningInfo(null);

        assertEquals(0, v2.getPropertiesCount());
        assertEquals(0, v2.getSubscriptionsCount());
        assertEquals(0, v2.getMessageQueueTableCount());
        assertEquals(0, v2.getConsumeStatusTableCount());
    }

    // ------------------------------------------------------------------ misc

    @Test
    public void toAccumulationSkipsNullStatsEntriesTest() {
        Map<String, ConsumeStats> byBroker = new HashMap<>();
        byBroker.put(ADDR_A, null);
        ConsumeStats withNulls = new ConsumeStats();
        // the default offsetTable is a ConcurrentHashMap which forbids null values, so install a
        // plain map to exercise the converter's null guard (deserialized stats can contain one)
        Map<MessageQueue, OffsetWrapper> offsetTable = new HashMap<>();
        offsetTable.put(new MessageQueue(TOPIC, BROKER_A, 0), null);
        offsetTable.put(new MessageQueue(TOPIC, BROKER_A, 1), offsetWrapper(10L, 4L, 4L, 0L));
        withNulls.setOffsetTable(offsetTable);
        byBroker.put(ADDR_B, withNulls);

        AdminModelConverter.AccumulationResult result = AdminModelConverter.toAccumulation(byBroker, GROUP);

        assertEquals(6L, result.total.getAccumulation());
        assertEquals(6L, result.byTopic.get(TOPIC).getAccumulation());
    }
}
