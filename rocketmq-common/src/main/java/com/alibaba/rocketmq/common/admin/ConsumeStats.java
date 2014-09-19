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
package com.alibaba.rocketmq.common.admin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * Consumer消费进度
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-14
 */
public class ConsumeStats extends RemotingSerializable {
    private HashMap<MessageQueue, OffsetWrapper> offsetTable = new HashMap<MessageQueue, OffsetWrapper>();
    private long consumeTps = 0;


    public long computeTotalDiff() {
        long diffTotal = 0L;

        Iterator<Entry<MessageQueue, OffsetWrapper>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, OffsetWrapper> next = it.next();
            long diff = next.getValue().getBrokerOffset() - next.getValue().getConsumerOffset();
            diffTotal += diff;
        }

        return diffTotal;
    }


    public HashMap<MessageQueue, OffsetWrapper> getOffsetTable() {
        return offsetTable;
    }


    public void setOffsetTable(HashMap<MessageQueue, OffsetWrapper> offsetTable) {
        this.offsetTable = offsetTable;
    }


    public long getConsumeTps() {
        return consumeTps;
    }


    public void setConsumeTps(long consumeTps) {
        this.consumeTps = consumeTps;
    }
}
