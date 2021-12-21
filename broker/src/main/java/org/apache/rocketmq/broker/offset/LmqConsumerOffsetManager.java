package org.apache.rocketmq.broker.offset;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class LmqConsumerOffsetManager extends ConsumerOffsetManager {
    private ConcurrentHashMap<String, Long> lmqOffsetTable = new ConcurrentHashMap<>(512);

    public LmqConsumerOffsetManager(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public long queryOffset(final String group, final String topic, final int queueId) {
        if (!MixAll.isLmq(group)) {
            return super.queryOffset(group, topic, queueId);
        }
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        Long offset = lmqOffsetTable.get(key);
        if (offset != null) {
            return offset;
        }
        return -1;
    }

    @Override
    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        if (!MixAll.isLmq(group)) {
            return super.queryOffset(group, topic);
        }
        Map<Integer, Long> map = new HashMap<>();
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        Long offset = lmqOffsetTable.get(key);
        if (offset != null) {
            map.put(0, offset);
        }
        return map;
    }

    @Override
    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
        final long offset) {
        if (!MixAll.isLmq(group)) {
            super.commitOffset(clientHost, group, topic, queueId, offset);
            return;
        }
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        lmqOffsetTable.put(key, offset);
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getLmqConsumerOffsetPath(brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            LmqConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, LmqConsumerOffsetManager.class);
            if (obj != null) {
                super.offsetTable = obj.offsetTable;
                this.lmqOffsetTable = obj.lmqOffsetTable;
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentHashMap<String, Long> getLmqOffsetTable() {
        return lmqOffsetTable;
    }

    public void setLmqOffsetTable(ConcurrentHashMap<String, Long> lmqOffsetTable) {
        this.lmqOffsetTable = lmqOffsetTable;
    }
}
