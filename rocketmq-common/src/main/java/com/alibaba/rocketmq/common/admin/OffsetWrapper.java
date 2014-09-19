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

/**
 * Offset包装类，含Broker、Consumer
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-14
 */
public class OffsetWrapper {
    private long brokerOffset;
    private long consumerOffset;
    // 消费的最后一条消息对应的时间戳
    private long lastTimestamp;


    public long getBrokerOffset() {
        return brokerOffset;
    }


    public void setBrokerOffset(long brokerOffset) {
        this.brokerOffset = brokerOffset;
    }


    public long getConsumerOffset() {
        return consumerOffset;
    }


    public void setConsumerOffset(long consumerOffset) {
        this.consumerOffset = consumerOffset;
    }


    public long getLastTimestamp() {
        return lastTimestamp;
    }


    public void setLastTimestamp(long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }
}
