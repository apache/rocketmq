package org.apache.rocketmq.store;

import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.store.queue.ConsumeQueueInterface;

public interface TruncateFilesHook {
    /**
     * truncate consumer offset to certain cqOffset.
     *
     * @param
     */
    void truncateOffset(ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> consumeQueueTable);
}
