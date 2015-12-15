/**
 * 
 */
package com.alibaba.rocketmq.broker.plugin;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.store.GetMessageResult;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.alibaba.rocketmq.store.MessageStore;
import com.alibaba.rocketmq.store.PutMessageResult;
import com.alibaba.rocketmq.store.QueryMessageResult;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;

/**
 * @author qinan.qn@taobao.com 2015年12月9日
 */
public abstract class AbstractPluginMessageStore implements MessageStore {
    MessageStore next = null;
    protected MessageStorePluginContext context;
    
    public AbstractPluginMessageStore() {
    }
    
    public final void setNext(MessageStore next) {
        this.next = next;
    }

    @Override
    public boolean load() {
        return next.load();
    }

    @Override
    public void start() throws Exception {
        next.start();
    }

    @Override
    public void shutdown() {
        next.shutdown();
    }

    @Override
    public void destroy() {
        next.destroy();
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        return next.putMessage(msg);
    }

    @Override
    public GetMessageResult getMessage(String group, String topic, int queueId, long offset,
            int maxMsgNums, SubscriptionData subscriptionData) {
        return next.getMessage(group, topic, queueId, offset, maxMsgNums, subscriptionData);
    }

    @Override
    public long getMaxOffsetInQuque(String topic, int queueId) {
        return next.getMaxOffsetInQuque(topic, queueId);
    }

    @Override
    public long getMinOffsetInQuque(String topic, int queueId) {
        return next.getMinOffsetInQuque(topic, queueId);
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long cqOffset) {
        return next.getCommitLogOffsetInQueue(topic, queueId, cqOffset);
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        return next.getOffsetInQueueByTime(topic, queueId, timestamp);
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        return next.lookMessageByOffset(commitLogOffset);
    }

    @Override
    public SelectMapedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        return next.selectOneMessageByOffset(commitLogOffset);
    }

    @Override
    public SelectMapedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return next.selectOneMessageByOffset(commitLogOffset, msgSize);
    }

    @Override
    public String getRunningDataInfo() {
        return next.getRunningDataInfo();
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        return next.getRuntimeInfo();
    }

    @Override
    public long getMaxPhyOffset() {
        return next.getMaxPhyOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return next.getMinPhyOffset();
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        return next.getEarliestMessageTime(topic, queueId);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long offset) {
        return next.getMessageStoreTimeStamp(topic, queueId, offset);
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        return next.getMessageTotalInQueue(topic, queueId);
    }

    @Override
    public SelectMapedBufferResult getCommitLogData(long offset) {
        return next.getCommitLogData(offset);
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        return next.appendToCommitLog(startOffset, data);
    }

    @Override
    public void excuteDeleteFilesManualy() {
        next.excuteDeleteFilesManualy();
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin,
            long end) {
        return next.queryMessage(topic, key, maxNum, begin, end);
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        next.updateHaMasterAddress(newAddr);
    }

    @Override
    public long slaveFallBehindMuch() {
        return next.slaveFallBehindMuch();
    }

    @Override
    public long now() {
        return next.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        return next.cleanUnusedTopic(topics);
    }

    @Override
    public void cleanExpiredConsumerQueue() {
        next.cleanExpiredConsumerQueue();
    }

    @Override
    public Map<String, Long> getMessageIds(String topic, int queueId, long minOffset,
            long maxOffset, SocketAddress storeHost) {
        return next.getMessageIds(topic, queueId, minOffset, maxOffset, storeHost);
    }

    @Override
    public boolean checkInDiskByConsumeOffset(String topic, int queueId, long consumeOffset) {
        return next.checkInDiskByConsumeOffset(topic, queueId, consumeOffset);
    }

    @Override
    public long dispatchBehindBytes() {
        return next.dispatchBehindBytes();
    }

}
