package org.apache.rocketmq.broker.offset;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class RocksDBLmqConsumerOffsetManagerTest {
    private static final String LMQ_GROUP = MixAll.LMQ_PREFIX + "FooBarGroup";
    private static final String NON_LMQ_GROUP = "nonLmqGroup";
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
        long actualOffset = offsetManager.queryOffset(NON_LMQ_GROUP, TOPIC, QUEUE_ID);
        // Verify
        assertEquals("Offset should not be null.", -1, actualOffset);
    }


    @Test
    public void testQueryOffsetForLmqGroupWithExistingOffset() {
        offsetManager.getLmqOffsetTable().put(getKey(), OFFSET);

        // Act
        Map<Integer, Long> actualOffsets = offsetManager.queryOffset(LMQ_GROUP, TOPIC);

        // Assert
        assertNotNull(actualOffsets);
        assertEquals(1, actualOffsets.size());
        assertEquals(OFFSET, (long) actualOffsets.get(0));
    }

    @Test
    public void testQueryOffsetForLmqGroupWithoutExistingOffset() {
        // Act
        Map<Integer, Long> actualOffsets = offsetManager.queryOffset(LMQ_GROUP, "nonExistingTopic");

        // Assert
        assertNotNull(actualOffsets);
        assertTrue("The map should be empty for non-existing offsets", actualOffsets.isEmpty());
    }

    @Test
    public void testQueryOffsetForNonLmqGroup() {
        when(brokerController.getBrokerConfig().getConsumerOffsetUpdateVersionStep()).thenReturn(1L);
        // Arrange
        Map<Integer, Long> mockOffsets = new HashMap<>();
        mockOffsets.put(QUEUE_ID, OFFSET);

        offsetManager.commitOffset("clientHost", NON_LMQ_GROUP, TOPIC, QUEUE_ID, OFFSET);

        // Act
        Map<Integer, Long> actualOffsets = offsetManager.queryOffset(NON_LMQ_GROUP, TOPIC);

        // Assert
        assertNotNull(actualOffsets);
        assertEquals("Offsets should match the mocked return value for non-LMQ groups", mockOffsets, actualOffsets);
    }

    @Test
    public void testCommitOffsetForLmq() {
        // Execute
        offsetManager.commitOffset("clientHost", LMQ_GROUP, TOPIC, QUEUE_ID, OFFSET);
        // Verify
        Long expectedOffset = offsetManager.getLmqOffsetTable().get(getKey());
        assertEquals("Offset should be updated correctly.", OFFSET, expectedOffset.longValue());
    }

    @Test
    public void testEncode() {
        offsetManager.setLmqOffsetTable(new ConcurrentHashMap<>(512));
        offsetManager.getLmqOffsetTable().put(getKey(), OFFSET);
        String encodedData = offsetManager.encode();
        assertTrue(encodedData.contains(String.valueOf(OFFSET)));
    }

    private String getKey() {
        return TOPIC + "@" + LMQ_GROUP;
    }
}
