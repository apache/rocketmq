/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.util.HashMap;
import java.util.Set;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public interface MessageStore {

    boolean load();

    void start() throws Exception;

    void shutdown();

    void destroy();

    PutMessageResult putMessage(final MessageExtBrokerInner msg);

    GetMessageResult getMessage(final String group, final String topic, final int queueId,
        final long offset, final int maxMsgNums, final SubscriptionData subscriptionData);

    long getMaxOffsetInQuque(final String topic, final int queueId);

    long getMinOffsetInQuque(final String topic, final int queueId);

    long getCommitLogOffsetInQueue(final String topic, final int queueId, final long cqOffset);

    long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp);

    MessageExt lookMessageByOffset(final long commitLogOffset);

    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset);

    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset, final int msgSize);

    String getRunningDataInfo();

    HashMap<String, String> getRuntimeInfo();

    long getMaxPhyOffset();

    long getMinPhyOffset();

    long getEarliestMessageTime(final String topic, final int queueId);

    long getEarliestMessageTime();

    long getMessageStoreTimeStamp(final String topic, final int queueId, final long offset);

    long getMessageTotalInQueue(final String topic, final int queueId);

    SelectMappedBufferResult getCommitLogData(final long offset);

    boolean appendToCommitLog(final long startOffset, final byte[] data);

    void excuteDeleteFilesManualy();

    QueryMessageResult queryMessage(final String topic, final String key, final int maxNum,
        final long begin, final long end);

    void updateHaMasterAddress(final String newAddr);

    long slaveFallBehindMuch();

    long now();

    int cleanUnusedTopic(final Set<String> topics);

    void cleanExpiredConsumerQueue();

    boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset);

    long dispatchBehindBytes();

    long flush();

    boolean resetWriteOffset(long phyOffset);

    long getConfirmOffset();

    void setConfirmOffset(long phyOffset);

    boolean isOSPageCacheBusy();

    long lockTimeMills();

    boolean isTransientStorePoolDeficient();
}
