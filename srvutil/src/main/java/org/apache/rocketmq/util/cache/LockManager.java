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
package org.apache.rocketmq.util.cache;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;

public class LockManager {
    private static ExpiredLocalCache<String, AtomicBoolean> expiredLocalCache = new ExpiredLocalCache<String, AtomicBoolean>(100000);

    public static boolean tryLock(String key, long lockTime) {
        AtomicBoolean v = expiredLocalCache.get(key);
        if (v == null) {
            return expiredLocalCache.putIfAbsent(key, new AtomicBoolean(false), lockTime) == null;
        } else {
            return v.compareAndSet(true, false);
        }
    }

    public static void unLock(String key) {
        AtomicBoolean v = expiredLocalCache.get(key);
        if (v != null) {
            v.set(true);
        }
    }

    public static String buildKey(PopMessageRequestHeader requestHeader, int queueId) {
        return requestHeader.getConsumerGroup() + PopAckConstants.SPLIT + requestHeader.getTopic() + PopAckConstants.SPLIT + queueId;
    }

    public static String buildKey(String topic, String cid, int queueId) {
        return topic + PopAckConstants.SPLIT + cid + PopAckConstants.SPLIT + queueId;
    }

    public static String buildKey(String prefix, int queueId) {
        return prefix + PopAckConstants.SPLIT + queueId;
    }
}
