package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.HashMap;

/**
 * QueueOffsetAssigner is a component for assigning queue.
 *
 */
public class QueueOffsetAssigner {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private HashMap<String, Long> topicQueueTable = new HashMap<>(1024);
    private HashMap<String, Long> batchTopicQueueTable = new HashMap<>(1024);

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public HashMap<String, Long> getBatchTopicQueueTable() {
        return batchTopicQueueTable;
    }

    public void setBatchTopicQueueTable(HashMap<String, Long> batchTopicQueueTable) {
        this.batchTopicQueueTable = batchTopicQueueTable;
    }

    public synchronized void remove(String topic, Integer queueId) {
        String topicQueueKey = topic + "-" + queueId;
        // Beware of thread-safety
        this.topicQueueTable.remove(topicQueueKey);
        this.batchTopicQueueTable.remove(topicQueueKey);

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }
}