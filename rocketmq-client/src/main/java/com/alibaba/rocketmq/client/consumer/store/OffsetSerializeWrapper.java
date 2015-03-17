/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.consumer.store;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * Wrapper class for offset serialization
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-25
 */
public class OffsetSerializeWrapper extends RemotingSerializable {
    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>();

    public ConcurrentHashMap<MessageQueue, AtomicLong> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
