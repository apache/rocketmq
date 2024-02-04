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

package org.apache.rocketmq.example.simple;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

public class RandomAsyncCommit {
    private final ConcurrentHashMap<MessageQueue, CachedQueue> mqCachedTable =
        new ConcurrentHashMap<>();

    public void putMessages(final MessageQueue mq, final List<MessageExt> msgs) {
        CachedQueue cachedQueue = this.mqCachedTable.get(mq);
        if (null == cachedQueue) {
            cachedQueue = new CachedQueue();
            this.mqCachedTable.put(mq, cachedQueue);
        }
        for (MessageExt msg : msgs) {
            cachedQueue.getMsgCachedTable().put(msg.getQueueOffset(), msg);
        }
    }

    public void removeMessage(final MessageQueue mq, long offset) {
        CachedQueue cachedQueue = this.mqCachedTable.get(mq);
        if (null != cachedQueue) {
            cachedQueue.getMsgCachedTable().remove(offset);
        }
    }

    public long commitableOffset(final MessageQueue mq) {
        CachedQueue cachedQueue = this.mqCachedTable.get(mq);
        if (null != cachedQueue) {
            return cachedQueue.getMsgCachedTable().firstKey();
        }

        return -1;
    }
}
