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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

public class TopicPublishInfo {
    private boolean orderTopic = false;
    private boolean haveTopicRouterInfo = false;
    private List<MessageQueue> messageQueueList = new ArrayList<>();
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private TopicRouteData topicRouteData;

    public interface QueueFilter {
        boolean filter(MessageQueue mq);
    }

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    public MessageQueue selectOneMessageQueue(QueueFilter ...filter) {
        return selectOneMessageQueue(this.messageQueueList, this.sendWhichQueue, filter);
    }

    private MessageQueue selectOneMessageQueue(List<MessageQueue> messageQueueList, ThreadLocalIndex sendQueue, QueueFilter ...filter) {
        if (messageQueueList == null || messageQueueList.isEmpty()) {
            return null;
        }

        if (filter != null && filter.length != 0) {
            for (int i = 0; i < messageQueueList.size(); i++) {
                int index = Math.abs(sendQueue.incrementAndGet() % messageQueueList.size());
                MessageQueue mq = messageQueueList.get(index);
                boolean filterResult = true;
                for (QueueFilter f: filter) {
                    Preconditions.checkNotNull(f);
                    filterResult &= f.filter(mq);
                }
                if (filterResult) {
                    return mq;
                }
            }

            return null;
        }

        int index = Math.abs(sendQueue.incrementAndGet() % messageQueueList.size());
        return messageQueueList.get(index);
    }

    public void resetIndex() {
        this.sendWhichQueue.reset();
    }

    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                MessageQueue mq = selectOneMessageQueue();
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    private MessageQueue getMessageQueue() {
        int index = this.sendWhichQueue.incrementAndGet();
        int pos = index % this.messageQueueList.size();
        return this.messageQueueList.get(pos);
    }
    
    public MessageQueue selectOneMessageQueue() {
        return getMessageQueue();
    }

    // Adding additional logic, filter by broker name
    public MessageQueue selectOneMessageQueueWithCondition(String lastBrokerName) {
        for (int i = 0; i < this.messageQueueList.size(); i++) {
            MessageQueue mq = getMessageQueue();
            if (!mq.getBrokerName().equals(lastBrokerName)) {
                return mq;
            }
        }
        return null; // Or handle appropriately if no queue matches
    }
        

    public int getWriteQueueNumsByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}
