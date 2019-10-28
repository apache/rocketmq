package org.apache.rocketmq.broker.offset;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class ConsumerOffsetManagerTest {

    @Test
    public void testCommitOffset() {
        ConsumerOffsetManager consumerOffsetManager = new ConsumerOffsetManager();
        String group = "testGroup";
        String topic = "testTopic";
        int queueId = 1;
        long offset = 100;
        consumerOffsetManager.commitOffset("127.0.0.1", group, topic, queueId, offset);
        long actualOffset = consumerOffsetManager.queryOffset(group, topic, queueId);
        Assert.assertEquals(offset, actualOffset);

        Map<Integer, Long> map = consumerOffsetManager.queryOffset(group, topic);
        Assert.assertEquals(offset, map.get(queueId).longValue());

        offset = 101;
        consumerOffsetManager.commitOffset("127.0.0.1", group, topic, queueId, offset);

        actualOffset = consumerOffsetManager.queryOffset(group, topic, queueId);
        Assert.assertEquals(offset, actualOffset);

        map = consumerOffsetManager.queryOffset(group, topic);
        Assert.assertEquals(offset, map.get(queueId).longValue());
    }
}
