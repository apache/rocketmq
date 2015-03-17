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
package com.alibaba.rocketmq.client.consumer;

import java.util.List;

import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class PullResult {
    private final PullStatus pullStatus;
    private final long nextBeginOffset;
    private final long minOffset;
    private final long maxOffset;
    private List<MessageExt> msgFoundList;


    public PullResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
                      List<MessageExt> msgFoundList) {
        super();
        this.pullStatus = pullStatus;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.msgFoundList = msgFoundList;
    }


    public PullStatus getPullStatus() {
        return pullStatus;
    }


    public long getNextBeginOffset() {
        return nextBeginOffset;
    }


    public long getMinOffset() {
        return minOffset;
    }


    public long getMaxOffset() {
        return maxOffset;
    }


    public List<MessageExt> getMsgFoundList() {
        return msgFoundList;
    }


    public void setMsgFoundList(List<MessageExt> msgFoundList) {
        this.msgFoundList = msgFoundList;
    }


    @Override
    public String toString() {
        return "PullResult [pullStatus=" + pullStatus + ", nextBeginOffset=" + nextBeginOffset
                + ", minOffset=" + minOffset + ", maxOffset=" + maxOffset + ", msgFoundList="
                + (msgFoundList == null ? 0 : msgFoundList.size()) + "]";
    }
}
