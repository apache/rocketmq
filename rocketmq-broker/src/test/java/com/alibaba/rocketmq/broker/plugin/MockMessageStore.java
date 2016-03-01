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
 * @author qinan.qn@taobao.com 2015Äê12ÔÂ12ÈÕ
 */
public class MockMessageStore implements MessageStore {

    @Override
    public boolean load() {
        return false;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void destroy() {

    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {

        return null;
    }

    @Override
    public GetMessageResult getMessage(String group, String topic, int queueId, long offset,
            int maxMsgNums, SubscriptionData subscriptionData) {

        return null;
    }

    @Override
    public long getMaxOffsetInQuque(String topic, int queueId) {

        return 0;
    }

    @Override
    public long getMinOffsetInQuque(String topic, int queueId) {

        return 0;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long cqOffset) {

        return 0;
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {

        return 0;
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset) {

        return null;
    }

    @Override
    public SelectMapedBufferResult selectOneMessageByOffset(long commitLogOffset) {

        return null;
    }

    @Override
    public SelectMapedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {

        return null;
    }

    @Override
    public String getRunningDataInfo() {

        return null;
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {

        return null;
    }

    @Override
    public long getMaxPhyOffset() {

        return 0;
    }

    @Override
    public long getMinPhyOffset() {

        return 0;
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {

        return 0;
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long offset) {

        return 0;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {

        return 0;
    }

    @Override
    public SelectMapedBufferResult getCommitLogData(long offset) {

        return null;
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {

        return false;
    }

    @Override
    public void excuteDeleteFilesManualy() {

    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin,
            long end) {

        return null;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {

    }

    @Override
    public long slaveFallBehindMuch() {

        return 0;
    }

    @Override
    public long now() {

        return 0;
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {

        return 0;
    }

    @Override
    public void cleanExpiredConsumerQueue() {

    }

    @Override
    public Map<String, Long> getMessageIds(String topic, int queueId, long minOffset,
            long maxOffset, SocketAddress storeHost) {

        return null;
    }

    @Override
    public boolean checkInDiskByConsumeOffset(String topic, int queueId, long consumeOffset) {

        return false;
    }

    @Override
    public long dispatchBehindBytes() {

        return 0;
    }

    @Override
    public long flush() {
        return 0;
    }

}
