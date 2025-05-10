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
package org.apache.rocketmq.tieredstore.metadata.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.rocketmq.common.message.MessageQueue;

public class QueueMetadata {

    @JSONField(ordinal = 1)
    private MessageQueue queue;

    @JSONField(ordinal = 2)
    private long minOffset;

    @JSONField(ordinal = 3)
    private long maxOffset;

    @JSONField(ordinal = 4)
    private long updateTimestamp;

    // default constructor is used by fastjson
    @SuppressWarnings("unused")
    public QueueMetadata() {
    }

    public QueueMetadata(MessageQueue queue, long minOffset, long maxOffset) {
        this.queue = queue;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.updateTimestamp = System.currentTimeMillis();
    }

    public MessageQueue getQueue() {
        return queue;
    }

    public void setQueue(MessageQueue queue) {
        this.queue = queue;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }
}
