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
package org.apache.rocketmq.broker.offset;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.MixAll;

public class BroadcastOffsetStore {

    private final ConcurrentMap<Integer, AtomicLong> offsetTable = new ConcurrentHashMap<>();

    public void updateOffset(int queueId, long offset, boolean increaseOnly) {
        AtomicLong offsetOld = this.offsetTable.get(queueId);
        if (null == offsetOld) {
            offsetOld = this.offsetTable.putIfAbsent(queueId, new AtomicLong(offset));
        }

        if (null != offsetOld) {
            if (increaseOnly) {
                MixAll.compareAndIncreaseOnly(offsetOld, offset);
            } else {
                offsetOld.set(offset);
            }
        }
    }

    public long readOffset(int queueId) {
        AtomicLong offset = this.offsetTable.get(queueId);
        if (offset != null) {
            return offset.get();
        }
        return -1L;
    }

    public Set<Integer> queueList() {
        return offsetTable.keySet();
    }
}
