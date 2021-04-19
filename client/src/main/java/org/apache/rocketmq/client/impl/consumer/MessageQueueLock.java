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
package org.apache.rocketmq.client.impl.consumer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Message lock,strictly ensure the single queue only one thread at a time consuming
 */
public class MessageQueueLock {
    private ConcurrentMap<MessageQueue, ConcurrentMap<Integer, Object>> mqLockTable =
        new ConcurrentHashMap<MessageQueue, ConcurrentMap<Integer, Object>>(32);

    public Object fetchLockObject(final MessageQueue mq) {
        return fetchLockObject(mq, -1);
    }

    public Object fetchLockObject(final MessageQueue mq, final int shardingKeyIndex) {
        ConcurrentMap<Integer, Object> objMap = this.mqLockTable.get(mq);
        if (null == objMap) {
            objMap = new ConcurrentHashMap<Integer, Object>(32);
            ConcurrentMap<Integer, Object> prevObjMap = this.mqLockTable.putIfAbsent(mq, objMap);
            if (prevObjMap != null) {
                objMap = prevObjMap;
            }
        }

        Object lock = objMap.get(shardingKeyIndex);
        if (null == lock) {
            lock = new Object();
            Object prevLock = objMap.putIfAbsent(shardingKeyIndex, lock);
            if (prevLock != null) {
                lock = prevLock;
            }
        }

        return lock;
    }
}
