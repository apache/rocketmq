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
package org.apache.rocketmq.broker.pop;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;

public class PopConsumerContext {

    private final String clientHost;

    private final long popTime;

    private final long invisibleTime;

    private final String groupId;

    private final boolean fifo;

    private final String attemptId;

    private final AtomicLong restCount;

    private final StringBuilder startOffsetInfo;

    private final StringBuilder msgOffsetInfo;

    private final StringBuilder orderCountInfo;

    private List<GetMessageResult> getMessageResultList;

    private List<PopConsumerRecord> popConsumerRecordList;

    public PopConsumerContext(String clientHost,
        long popTime, long invisibleTime, String groupId, boolean fifo, String attemptId) {

        this.clientHost = clientHost;
        this.popTime = popTime;
        this.invisibleTime = invisibleTime;
        this.groupId = groupId;
        this.fifo = fifo;
        this.attemptId = attemptId;
        this.restCount = new AtomicLong(0);
        this.startOffsetInfo = new StringBuilder();
        this.msgOffsetInfo = new StringBuilder();
        this.orderCountInfo = new StringBuilder();
    }

    public boolean isFound() {
        return getMessageResultList != null && !getMessageResultList.isEmpty();
    }

    // offset is consumer last request offset
    public void addGetMessageResult(GetMessageResult result,
        String topicId, int queueId, PopConsumerRecord.RetryType retryType, long offset) {

        if (result.getStatus() != GetMessageStatus.FOUND || result.getMessageQueueOffset().isEmpty()) {
            return;
        }

        if (this.getMessageResultList == null) {
            this.getMessageResultList = new ArrayList<>();
        }

        if (this.popConsumerRecordList == null) {
            this.popConsumerRecordList = new ArrayList<>();
        }

        this.getMessageResultList.add(result);
        this.addRestCount(result.getMaxOffset() - result.getNextBeginOffset());

        for (int i = 0; i < result.getMessageQueueOffset().size(); i++) {
            this.popConsumerRecordList.add(new PopConsumerRecord(popTime, groupId, topicId, queueId,
                retryType.getCode(), invisibleTime, result.getMessageQueueOffset().get(i), attemptId));
        }

        ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, topicId, queueId, offset);
        ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, topicId, queueId, result.getMessageQueueOffset());
    }

    public String getClientHost() {
        return clientHost;
    }

    public String getGroupId() {
        return groupId;
    }

    public void addRestCount(long delta) {
        this.restCount.addAndGet(delta);
    }

    public long getRestCount() {
        return restCount.get();
    }

    public long getPopTime() {
        return popTime;
    }

    public boolean isFifo() {
        return fifo;
    }

    public long getInvisibleTime() {
        return invisibleTime;
    }

    public String getAttemptId() {
        return attemptId;
    }

    public int getMessageCount() {
        return getMessageResultList != null ?
            getMessageResultList.stream().mapToInt(GetMessageResult::getMessageCount).sum() : 0;
    }

    public String getStartOffsetInfo() {
        return startOffsetInfo.toString();
    }

    public String getMsgOffsetInfo() {
        return msgOffsetInfo.toString();
    }

    public StringBuilder getOrderCountInfoBuilder() {
        return orderCountInfo;
    }

    public String getOrderCountInfo() {
        return orderCountInfo.toString();
    }

    public List<GetMessageResult> getGetMessageResultList() {
        return getMessageResultList;
    }

    public List<PopConsumerRecord> getPopConsumerRecordList() {
        return popConsumerRecordList;
    }

    @Override
    public String toString() {
        return "PopConsumerContext{" +
            "clientHost=" + clientHost +
            ", popTime=" + popTime +
            ", invisibleTime=" + invisibleTime +
            ", groupId=" + groupId +
            ", isFifo=" + fifo +
            ", attemptId=" + attemptId +
            ", restCount=" + restCount +
            ", startOffsetInfo=" + startOffsetInfo +
            ", msgOffsetInfo=" + msgOffsetInfo +
            ", orderCountInfo=" + orderCountInfo +
            ", getMessageResultList=" + (getMessageResultList != null ? getMessageResultList.size() : 0) +
            ", popConsumerRecordList=" + (popConsumerRecordList != null ? popConsumerRecordList.size() : 0) +
            '}';
    }
}
