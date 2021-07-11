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

import java.util.List;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;

public class PullResultWithLogicalQueues extends PullResultExt {
    private PullResultExt origPullResultExt;
    private final LogicalQueueRouteData queueRouteData;

    public PullResultWithLogicalQueues(PullResult pullResult, LogicalQueueRouteData floorQueueRouteData) {
        super(pullResult.getPullStatus(), pullResult.getNextBeginOffset(), pullResult.getMinOffset(), pullResult.getMaxOffset(), pullResult.getMsgFoundList(),
            pullResult instanceof PullResultExt ? ((PullResultExt) pullResult).getSuggestWhichBrokerId() : MixAll.MASTER_ID,
            pullResult instanceof PullResultExt ? ((PullResultExt) pullResult).getMessageBinary() : null);
        if (pullResult instanceof PullResultExt) {
            this.origPullResultExt = (PullResultExt) pullResult;
        } else {
            this.origPullResultExt = new PullResultExt(pullResult.getPullStatus(), pullResult.getNextBeginOffset(), pullResult.getMinOffset(), pullResult.getMaxOffset(), pullResult.getMsgFoundList(), MixAll.MASTER_ID, null);
        }
        this.queueRouteData = floorQueueRouteData;
    }

    public PullResult getOrigPullResultExt() {
        return origPullResultExt;
    }

    public LogicalQueueRouteData getQueueRouteData() {
        return queueRouteData;
    }

    public void setOrigPullResultExt(PullResultExt pullResultExt) {
        this.origPullResultExt = pullResultExt;
    }

    @Override public PullStatus getPullStatus() {
        return origPullResultExt.getPullStatus();
    }

    @Override public long getNextBeginOffset() {
        return origPullResultExt.getNextBeginOffset();
    }

    @Override public long getMinOffset() {
        return origPullResultExt.getMinOffset();
    }

    @Override public long getMaxOffset() {
        return origPullResultExt.getMaxOffset();
    }

    @Override public List<MessageExt> getMsgFoundList() {
        return origPullResultExt.getMsgFoundList();
    }

    @Override public void setMsgFoundList(List<MessageExt> msgFoundList) {
        origPullResultExt.setMsgFoundList(msgFoundList);
    }

    @Override public byte[] getMessageBinary() {
        return origPullResultExt.getMessageBinary();
    }

    @Override public void setMessageBinary(byte[] messageBinary) {
        origPullResultExt.setMessageBinary(messageBinary);
    }

    @Override public long getSuggestWhichBrokerId() {
        return origPullResultExt.getSuggestWhichBrokerId();
    }

    @Override public String toString() {
        return "PullResultWithLogicalQueues{" +
            "origPullResultExt=" + origPullResultExt +
            ", queueRouteData=" + queueRouteData +
            '}';
    }
}
