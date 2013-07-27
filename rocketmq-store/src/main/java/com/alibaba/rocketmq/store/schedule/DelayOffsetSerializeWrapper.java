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
package com.alibaba.rocketmq.store.schedule;

import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * 延时消息进度，序列化包装
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class DelayOffsetSerializeWrapper extends RemotingSerializable {
    private ConcurrentHashMap<Integer /* level */, Long/* offset */> offsetTable =
            new ConcurrentHashMap<Integer, Long>(32);


    public ConcurrentHashMap<Integer, Long> getOffsetTable() {
        return offsetTable;
    }


    public void setOffsetTable(ConcurrentHashMap<Integer, Long> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
