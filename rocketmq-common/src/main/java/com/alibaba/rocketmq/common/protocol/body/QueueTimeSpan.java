/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.Date;


/**
 * @author manhong.yqd
 */
public class QueueTimeSpan {
    private MessageQueue messageQueue;
    private long minTimeStamp;
    private long maxTimeStamp;
    private long consumeTimeStamp;
    private long delayTime;


    public MessageQueue getMessageQueue() {
        return messageQueue;
    }


    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }


    public long getMinTimeStamp() {
        return minTimeStamp;
    }


    public void setMinTimeStamp(long minTimeStamp) {
        this.minTimeStamp = minTimeStamp;
    }


    public long getMaxTimeStamp() {
        return maxTimeStamp;
    }


    public void setMaxTimeStamp(long maxTimeStamp) {
        this.maxTimeStamp = maxTimeStamp;
    }


    public long getConsumeTimeStamp() {
        return consumeTimeStamp;
    }


    public void setConsumeTimeStamp(long consumeTimeStamp) {
        this.consumeTimeStamp = consumeTimeStamp;
    }


    public String getMinTimeStampStr() {
        return UtilAll.formatDate(new Date(minTimeStamp), UtilAll.yyyy_MM_dd_HH_mm_ss_SSS);
    }


    public String getMaxTimeStampStr() {
        return UtilAll.formatDate(new Date(maxTimeStamp), UtilAll.yyyy_MM_dd_HH_mm_ss_SSS);
    }


    public String getConsumeTimeStampStr() {
        return UtilAll.formatDate(new Date(consumeTimeStamp), UtilAll.yyyy_MM_dd_HH_mm_ss_SSS);
    }


    public long getDelayTime() {
        return delayTime;
    }


    public void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }
}
