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
package org.apache.rocketmq.client.consumer.store;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerStageOffsetRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Remote storage implementation
 */
public class RemoteBrokerStageOffsetStore extends AbstractStageOffsetStore {

    public RemoteBrokerStageOffsetStore(MQClientInstance mqClientFactory, String groupName) {
        super(mqClientFactory, groupName);
    }

    @Override
    public Map<String, Integer> readStageOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    ConcurrentMap<String, AtomicInteger> map = this.offsetTable.get(mq);
                    if (map != null) {
                        return convert(map);
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return new HashMap<>();
                    }
                }
                case READ_FROM_STORE: {
                    try {
                        Map<String, Integer> map = this.fetchConsumeStageOffsetFromBroker(mq);
                        for (Map.Entry<String, Integer> entry : map.entrySet()) {
                            String strategyId = entry.getKey();
                            Integer brokerOffset = entry.getValue();
                            this.updateStageOffset(mq, strategyId, brokerOffset, false);
                        }
                        return map;
                    }
                    // No stage offset in broker
                    catch (MQBrokerException e) {
                        return new HashMap<>();
                    }
                    //Other exceptions
                    catch (Exception e) {
                        log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                        return null;
                    }
                }
                default:
                    break;
            }
        }

        return new HashMap<>();
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty()) {
            return;
        }

        final HashSet<MessageQueue> unusedMq = new HashSet<MessageQueue>();

        for (Map.Entry<MessageQueue, ConcurrentMap<String, AtomicInteger>> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ConcurrentMap<String, AtomicInteger> map = entry.getValue();
            if (map != null) {
                if (mqs.contains(mq)) {
                    try {
                        for (Map.Entry<String, AtomicInteger> integerEntry : map.entrySet()) {
                            String strategyId = integerEntry.getKey();
                            AtomicInteger offset = integerEntry.getValue();
                            this.updateConsumeStageOffsetToBroker(mq, strategyId, offset.get());
                            log.info("[persistAll] Group: {} ClientId: {} updateConsumeStageOffsetToBroker {} {} {}",
                                this.groupName,
                                this.mQClientFactory.getClientId(),
                                mq,
                                strategyId,
                                offset.get());
                        }
                    } catch (Exception e) {
                        log.error("updateConsumeStageOffsetToBroker exception, " + mq.toString(), e);
                    }
                } else {
                    unusedMq.add(mq);
                }
            }
        }

        if (!unusedMq.isEmpty()) {
            for (MessageQueue mq : unusedMq) {
                this.offsetTable.remove(mq);
                log.info("remove unused mq, {}, {}", mq, this.groupName);
            }
        }
    }

    @Override
    public void persist(MessageQueue mq) {
        ConcurrentMap<String, AtomicInteger> map = this.offsetTable.get(mq);
        if (map != null) {
            try {
                for (Map.Entry<String, AtomicInteger> entry : map.entrySet()) {
                    String strategyId = entry.getKey();
                    AtomicInteger offset = entry.getValue();
                    this.updateConsumeStageOffsetToBroker(mq, strategyId, offset.get());
                    log.info("[persist] Group: {} ClientId: {} updateConsumeStageOffsetToBroker {} {} {}",
                        this.groupName,
                        this.mQClientFactory.getClientId(),
                        mq,
                        strategyId,
                        offset.get());
                }
            } catch (Exception e) {
                log.error("updateConsumeStageOffsetToBroker exception, " + mq.toString(), e);
            }
        }
    }

    @Override
    public void removeStageOffset(MessageQueue mq) {
        if (mq != null) {
            this.offsetTable.remove(mq);
            log.info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq,
                offsetTable.size());
        }
    }

    /**
     * Update the Consumer Stage Offset in one way, once the Master is off, updated to Slave, here need to be
     * optimized.
     */
    private void updateConsumeStageOffsetToBroker(MessageQueue mq, String strategyId,
        int stageOffset) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        updateConsumeStageOffsetToBroker(mq, strategyId, stageOffset, true);
    }

    /**
     * Update the Consumer Stage Offset synchronously, once the Master is off, updated to Slave, here need to be
     * optimized.
     */
    @Override
    public void updateConsumeStageOffsetToBroker(MessageQueue mq, String strategyId, int offset,
        boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            UpdateConsumerStageOffsetRequestHeader requestHeader = new UpdateConsumerStageOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setStrategyId(strategyId);
            requestHeader.setCommitStageOffset(offset);

            if (isOneway) {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerStageOffsetOneway(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            } else {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerStageOffset(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            }
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }

    private Map<String, Integer> fetchConsumeStageOffsetFromBroker(
        MessageQueue mq) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {

            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());

            return this.mQClientFactory.getMQClientAPIImpl().queryConsumerStageOffset(
                findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }
}
