package org.apache.rocketmq.store.queue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public abstract class AbstractConsumeQueueStore implements ConsumeQueueStoreInterface {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected final DefaultMessageStore messageStore;
    protected final MessageStoreConfig messageStoreConfig;
    protected final QueueOffsetOperator queueOffsetOperator = new QueueOffsetOperator();
    protected final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface>> consumeQueueTable;

    public AbstractConsumeQueueStore(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
        this.messageStoreConfig = messageStore.getMessageStoreConfig();
        this.consumeQueueTable = new ConcurrentHashMap<>(32);
    }

    @Override
    public void putMessagePositionInfoWrapper(ConsumeQueueInterface consumeQueue, DispatchRequest request) {
        consumeQueue.putMessagePositionInfoWrapper(request);
    }

    @Override
    public Long getMaxOffset(String topic, int queueId) {
        return this.queueOffsetOperator.currentQueueOffset(topic + "-" + queueId);
    }

    @Override
    public void setTopicQueueTable(ConcurrentMap<String, Long> topicQueueTable) {
        this.queueOffsetOperator.setTopicQueueTable(topicQueueTable);
        this.queueOffsetOperator.setLmqTopicQueueTable(topicQueueTable);
    }

    @Override
    public ConcurrentMap getTopicQueueTable() {
        return this.queueOffsetOperator.getTopicQueueTable();
    }

    @Override
    public void assignQueueOffset(MessageExtBrokerInner msg) throws Exception {
        ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(msg.getTopic(), msg.getQueueId());
        consumeQueue.assignQueueOffset(this.queueOffsetOperator, msg);
    }

    @Override
    public void increaseQueueOffset(MessageExtBrokerInner msg, short messageNum) {
        ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(msg.getTopic(), msg.getQueueId());
        consumeQueue.increaseQueueOffset(this.queueOffsetOperator, msg, messageNum);
    }

    @Override
    public void removeTopicQueueTable(String topic, Integer queueId) {
        this.queueOffsetOperator.remove(topic, queueId);
    }

    @Override
    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable() {
        return this.consumeQueueTable;
    }

    @Override
    public ConcurrentMap<Integer, ConsumeQueueInterface> findConsumeQueueMap(String topic) {
        return this.consumeQueueTable.get(topic);
    }
}
