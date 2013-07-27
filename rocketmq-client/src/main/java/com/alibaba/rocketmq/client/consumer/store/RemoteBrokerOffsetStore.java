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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.FindBrokerResult;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * 消费进度存储到远端Broker，比较可靠
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class RemoteBrokerOffsetStore implements OffsetStore {
    private final static Logger log = ClientLogger.getLog();
    private final MQClientFactory mQClientFactory;
    private final String groupName;
    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>();


    public RemoteBrokerOffsetStore(MQClientFactory mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
    }


    /**
     * 更新Consumer Offset，在Master断网期间，可能会更新到Slave，这里需要优化，或者在Slave端优化， TODO
     */
    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            // TODO 此处可能对Name Server压力过大，需要调优
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setCommitOffset(offset);

            // 使用oneway形式，原因是服务器在删除文件时，这个调用可能会超时
            this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffsetOneway(
                findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        }
        else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }


    private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            // TODO 此处可能对Name Server压力过大，需要调优
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());

            return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(
                findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        }
        else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }


    @Override
    public void load() {
    }


    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            AtomicLong offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                }
                else {
                    offsetOld.set(offset);
                }
            }
        }
    }


    @Override
    public long readOffset(MessageQueue mq, boolean fromStore) {
        if (mq != null) {
            AtomicLong offset = this.offsetTable.get(mq);
            if (fromStore)
                offset = null;

            if (null == offset) {
                try {
                    long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                    offset = new AtomicLong(brokerOffset);
                    this.offsetTable.putIfAbsent(mq, offset);
                }
                // 当前订阅组在服务器没有对应的Offset
                catch (MQBrokerException e) {
                    return -1;
                }
                // 其他通信错误
                catch (Exception e) {
                    log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                    return -2;
                }

                return offset.get();
            }
        }
        return -1;
    }


    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (mqs != null && !mqs.isEmpty()) {
            for (MessageQueue mq : this.offsetTable.keySet()) {
                AtomicLong offset = this.offsetTable.get(mq);
                if (offset != null) {
                    if (mqs.contains(mq)) {
                        try {
                            this.updateConsumeOffsetToBroker(mq, offset.get());
                            log.debug("updateConsumeOffsetToBroker {} {}", mq, offset.get());
                        }
                        catch (Exception e) {
                            log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
                        }
                    }
                }
            }
        }
    }


    @Override
    public void persist(MessageQueue mq) {
        AtomicLong offset = this.offsetTable.get(mq);
        if (offset != null) {
            try {
                this.updateConsumeOffsetToBroker(mq, offset.get());
                log.debug("updateConsumeOffsetToBroker {} {}", mq, offset.get());
            }
            catch (Exception e) {
                log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
            }
        }
    }
}
