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
package com.alibaba.rocketmq.client.impl.consumer;

import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class PullRequest {
    private String consumerGroup;
    private MessageQueue messageQueue;
    private ProcessQueue processQueue;
    private long nextOffset;


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public MessageQueue getMessageQueue() {
        return messageQueue;
    }


    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }


    public long getNextOffset() {
        return nextOffset;
    }


    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }


    @Override
    public String toString() {
        return "PullRequest [consumerGroup=" + consumerGroup + ", messageQueue=" + messageQueue
                + ", nextOffset=" + nextOffset + "]";
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((consumerGroup == null) ? 0 : consumerGroup.hashCode());
        result = prime * result + ((messageQueue == null) ? 0 : messageQueue.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PullRequest other = (PullRequest) obj;
        if (consumerGroup == null) {
            if (other.consumerGroup != null)
                return false;
        }
        else if (!consumerGroup.equals(other.consumerGroup))
            return false;
        if (messageQueue == null) {
            if (other.messageQueue != null)
                return false;
        }
        else if (!messageQueue.equals(other.messageQueue))
            return false;
        return true;
    }


    public ProcessQueue getProcessQueue() {
        return processQueue;
    }


    public void setProcessQueue(ProcessQueue processQueue) {
        this.processQueue = processQueue;
    }
}
