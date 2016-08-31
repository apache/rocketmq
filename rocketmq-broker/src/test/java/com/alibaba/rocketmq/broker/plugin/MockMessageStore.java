/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 *
 */
package com.alibaba.rocketmq.broker.plugin;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.store.*;

import java.util.HashMap;
import java.util.Set;


/**
 * @author qinan.qn@taobao.com 2015��12��12��
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
    public GetMessageResult getMessage(String group, String topic, int queueId, long offset, int maxMsgNums,
                                       SubscriptionData subscriptionData) {

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
    public long getEarliestMessageTime() {
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
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {

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


    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return true;
    }

    @Override
    public long getConfirmOffset() {
        return 0;
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
    }

    @Override
    public boolean isOSPageCacheBusy() {
        return false;
    }

    @Override
    public long lockTimeMills() {
        return 0;
    }

}
