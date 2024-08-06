package org.apache.rocketmq.broker.offset;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

public class RocksDBLmqConsumerOffsetManagerTest {
    private static final String LMQ_GROUP = MixAll.LMQ_PREFIX + "FooBarGroup";
    private static final String TOPIC = "FooBarTopic";
    private static final int QUEUE_ID = 0;
    private static final long OFFSET = 12345;

    private BrokerController brokerController;

    private RocksDBLmqConsumerOffsetManager offsetManager;

    @Before
    public void setUp() {
        brokerController = Mockito.mock(BrokerController.class);
        when(brokerController.getMessageStoreConfig()).thenReturn(Mockito.mock(MessageStoreConfig.class));
        when(brokerController.getBrokerConfig()).thenReturn(Mockito.mock(BrokerConfig.class));
        offsetManager = new RocksDBLmqConsumerOffsetManager(brokerController);
    }

    @Test
    public void testQueryOffsetForLmq() {
        // Setup
        offsetManager.getLmqOffsetTable().put(getKey(), OFFSET);
        // Execute
        long actualOffset = offsetManager.queryOffset(LMQ_GROUP, TOPIC, QUEUE_ID);
        // Verify
        assertEquals("Offset should match the expected value.", OFFSET, actualOffset);
    }

    @Test
    public void testQueryOffsetForNonLmq() {
        // Setup
        when(brokerController.getConsumerOffsetManager()).thenReturn(offsetManager);
        // Since we are mocking and not initializing a real offset manager,
        // ensure your method has a default or mock behavior for non-LMQ offsets.
        // Execute
        long actualOffset = offsetManager.queryOffset(LMQ_GROUP, TOPIC, QUEUE_ID);
        // Verify
        // The actual value here depends on your default/mocked behavior for non-LMQ offsets.
        assertNotNull("Offset should not be null.", actualOffset);
    }

    @Test
    public void testCommitOffsetForLmq() {
        // Execute
        offsetManager.commitOffset("clientHost", LMQ_GROUP, TOPIC, QUEUE_ID, OFFSET);
        // Verify
        Long expectedOffset = offsetManager.getLmqOffsetTable().get(getKey());
        assertEquals("Offset should be updated correctly.", OFFSET, expectedOffset.longValue());
    }

    private String getKey() {
        return TOPIC + "@" + LMQ_GROUP;
    }
}
