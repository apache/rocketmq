package org.apache.rocketmq.common.admin;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;


public class TopicStatsTableTest {

    private volatile TopicStatsTable topicStatsTable;

    private static final String TEST_TOPIC = "test_topic";

    private static final String TEST_BROKER = "test_broker";

    private static final int QUEUE_ID = 1;

    private static final long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private static final long MAX_OFFSET = CURRENT_TIME_MILLIS + 100;

    private static final long MIN_OFFSET = CURRENT_TIME_MILLIS - 100;

    @Before
    public void buildTopicStatsTable() {
        HashMap<MessageQueue, TopicOffset> offsetTableMap = new HashMap<MessageQueue, TopicOffset>();

        MessageQueue messageQueue = new MessageQueue(TEST_TOPIC, TEST_BROKER, QUEUE_ID);

        TopicOffset topicOffset = new TopicOffset();
        topicOffset.setLastUpdateTimestamp(CURRENT_TIME_MILLIS);
        topicOffset.setMinOffset(MIN_OFFSET);
        topicOffset.setMaxOffset(MAX_OFFSET);

        offsetTableMap.put(messageQueue, topicOffset);

        topicStatsTable = new TopicStatsTable();
        topicStatsTable.setOffsetTable(offsetTableMap);
    }

    @Test
    public void testGetOffsetTable() throws Exception {
        validateTopicStatsTable(topicStatsTable);
    }

    @Test
    public void testFromJson() throws Exception {
        String json = RemotingSerializable.toJson(topicStatsTable, true);
        TopicStatsTable fromJson = RemotingSerializable.fromJson(json, TopicStatsTable.class);

        validateTopicStatsTable(fromJson);
    }

    private static void validateTopicStatsTable(TopicStatsTable topicStatsTable) throws Exception {
        Map.Entry<MessageQueue, TopicOffset> savedTopicStatsTableMap = topicStatsTable.getOffsetTable().entrySet().iterator().next();
        MessageQueue savedMessageQueue = savedTopicStatsTableMap.getKey();
        TopicOffset savedTopicOffset = savedTopicStatsTableMap.getValue();

        Assert.assertTrue(savedMessageQueue.getTopic().equals(TEST_TOPIC));
        Assert.assertTrue(savedMessageQueue.getBrokerName().equals(TEST_BROKER));
        Assert.assertTrue(savedMessageQueue.getQueueId() == QUEUE_ID);

        Assert.assertTrue(savedTopicOffset.getLastUpdateTimestamp() == CURRENT_TIME_MILLIS);
        Assert.assertTrue(savedTopicOffset.getMaxOffset() == MAX_OFFSET);
        Assert.assertTrue(savedTopicOffset.getMinOffset() == MIN_OFFSET);
    }

}
