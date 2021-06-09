package org.apache.rocketmq.client.consumer.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingException;

public abstract class AbstractStageOffsetStore implements StageOffsetStore {

    protected final static InternalLogger log = ClientLogger.getLog();

    protected final MQClientInstance mQClientFactory;

    protected final String groupName;

    protected ConcurrentMap<MessageQueue, ConcurrentMap<String/*strategyId*/, AtomicInteger/*offset*/>> offsetTable =
        new ConcurrentHashMap<MessageQueue, ConcurrentMap<String, AtomicInteger>>();

    public AbstractStageOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
    }

    @Override
    public void load() throws MQClientException {

    }

    @Override
    public void updateStageOffset(MessageQueue mq, String strategyId, int stageOffset, boolean increaseOnly) {
        if (mq != null) {
            ConcurrentMap<String, AtomicInteger> map = this.offsetTable.get(mq);
            if (null == map) {
                this.offsetTable.putIfAbsent(mq, new ConcurrentHashMap<>());
                map = this.offsetTable.get(mq);
            }
            AtomicInteger offsetOld = map.get(strategyId);
            if (null == offsetOld) {
                map.putIfAbsent(strategyId, new AtomicInteger(stageOffset));
                offsetOld = map.get(strategyId);
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, stageOffset);
                } else {
                    offsetOld.set(stageOffset);
                }
            }
        }
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {

    }

    @Override
    public void persist(MessageQueue mq) {

    }

    @Override
    public void removeStageOffset(MessageQueue mq) {

    }

    @Override
    public Map<MessageQueue, Map<String, Integer>> cloneStageOffsetTable(String topic) {
        Map<MessageQueue, Map<String, Integer>> cloneOffsetTable = new HashMap<MessageQueue, Map<String, Integer>>();
        for (Map.Entry<MessageQueue, ConcurrentMap<String, AtomicInteger>> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, convert(entry.getValue()));

        }
        return cloneOffsetTable;
    }

    @Override
    public void updateConsumeStageOffsetToBroker(MessageQueue mq, String strategyId, int stageOffset,
        boolean isOneway) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

    }

    protected final Map<String, Integer> convert(Map<String, AtomicInteger> original) {
        if (null == original) {
            return new ConcurrentHashMap<>();
        }
        Map<String, Integer> map = new HashMap<>(original.size());
        for (Map.Entry<String, AtomicInteger> entry : original.entrySet()) {
            String key = entry.getKey();
            AtomicInteger value = entry.getValue();
            map.put(key, value.get());
        }
        return map;
    }
}
