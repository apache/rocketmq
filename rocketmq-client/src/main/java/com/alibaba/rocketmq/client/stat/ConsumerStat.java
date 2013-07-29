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
package com.alibaba.rocketmq.client.stat;

import java.util.concurrent.atomic.AtomicLong;


/**
 * Consumer内部运行时统计信息
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-7
 */
public class ConsumerStat {
    // 打点时间戳
    private long createTimestamp = System.currentTimeMillis();

    // 一次消费消息的最大RT
    private final AtomicLong consumeMsgRTMax = new AtomicLong(0);
    // 每次消费消息RT叠加总和
    private final AtomicLong consumeMsgRTTotal = new AtomicLong(0);
    // 消费消息成功次数总和
    private final AtomicLong consumeMsgOKTotal = new AtomicLong(0);
    // 消费消息失败次数总和
    private final AtomicLong consumeMsgFailedTotal = new AtomicLong(0);

    // 拉消息RT叠加总和（只包含成功拉到的）
    private final AtomicLong pullRTTotal = new AtomicLong(0);
    // 拉消息次数（只包含成功拉到的）
    private final AtomicLong pullTimesTotal = new AtomicLong(0);


    public ConsumerStat createSnapshot() {
        ConsumerStat consumerStat = new ConsumerStat();
        consumerStat.getConsumeMsgRTMax().set(this.consumeMsgRTMax.get());
        consumerStat.getConsumeMsgRTTotal().set(this.consumeMsgRTTotal.get());
        consumerStat.getConsumeMsgOKTotal().set(this.consumeMsgOKTotal.get());
        consumerStat.getConsumeMsgFailedTotal().set(this.consumeMsgFailedTotal.get());
        consumerStat.getPullRTTotal().set(this.pullRTTotal.get());
        consumerStat.getPullTimesTotal().set(this.pullTimesTotal.get());

        return consumerStat;
    }


    public AtomicLong getConsumeMsgRTMax() {
        return consumeMsgRTMax;
    }


    public AtomicLong getConsumeMsgRTTotal() {
        return consumeMsgRTTotal;
    }


    public AtomicLong getConsumeMsgOKTotal() {
        return consumeMsgOKTotal;
    }


    public AtomicLong getConsumeMsgFailedTotal() {
        return consumeMsgFailedTotal;
    }


    public long getCreateTimestamp() {
        return createTimestamp;
    }


    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }


    public AtomicLong getPullTimesTotal() {
        return pullTimesTotal;
    }


    public AtomicLong getPullRTTotal() {
        return pullRTTotal;
    }
}
