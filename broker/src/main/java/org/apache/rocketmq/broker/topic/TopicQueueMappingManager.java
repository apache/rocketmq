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
package org.apache.rocketmq.broker.topic;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.TopicQueueMappingSerializeWrapper;
import org.apache.rocketmq.common.rpc.TopicQueueRequestHeader;
import org.apache.rocketmq.common.rpc.TopicRequestHeader;
import org.apache.rocketmq.common.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingContext;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.rocketmq.remoting.protocol.RemotingCommand.buildErrorResponse;

public class TopicQueueMappingManager extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private transient final Lock lock = new ReentrantLock();

    //this data version should be equal to the TopicConfigManager
    private final DataVersion dataVersion = new DataVersion();
    private transient BrokerController brokerController;

    private final ConcurrentMap<String, TopicQueueMappingDetail> topicQueueMappingTable = new ConcurrentHashMap<>();


    public TopicQueueMappingManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void updateTopicQueueMapping(TopicQueueMappingDetail newDetail, boolean force, boolean isClean, boolean flush) throws Exception {
        boolean locked = false;
        boolean updated = false;
        TopicQueueMappingDetail oldDetail = null;
        try {

            if (lock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                locked = true;
            } else {
                return;
            }
            if (newDetail == null) {
                return;
            }
            assert newDetail.getBname().equals(this.brokerController.getBrokerConfig().getBrokerName());

            newDetail.getHostedQueues().forEach((queueId, items) -> {
                TopicQueueMappingUtils.checkLogicQueueMappingItemOffset(items);
            });

            oldDetail = topicQueueMappingTable.get(newDetail.getTopic());
            if (oldDetail == null) {
                topicQueueMappingTable.put(newDetail.getTopic(), newDetail);
                updated = true;
                return;
            }
            if (force) {
                //bakeup the old items
                oldDetail.getHostedQueues().forEach((queueId, items) -> {
                    newDetail.getHostedQueues().putIfAbsent(queueId, items);
                });
                topicQueueMappingTable.put(newDetail.getTopic(), newDetail);
                updated = true;
                return;
            }
            //do more check
            if (newDetail.getEpoch() < oldDetail.getEpoch()) {
                throw new RuntimeException(String.format("Can't accept data with small epoch %d < %d", newDetail.getEpoch(), oldDetail.getEpoch()));
            }
            if (!newDetail.getScope().equals(oldDetail.getScope())) {
                throw new RuntimeException(String.format("Can't accept data with unmatched scope %s != %s", newDetail.getScope(), oldDetail.getScope()));
            }
            boolean epochEqual = newDetail.getEpoch() == oldDetail.getEpoch();
            for (Integer globalId : oldDetail.getHostedQueues().keySet()) {
                List<LogicQueueMappingItem> oldItems = oldDetail.getHostedQueues().get(globalId);
                List<LogicQueueMappingItem> newItems = newDetail.getHostedQueues().get(globalId);
                if (newItems == null) {
                    if (epochEqual) {
                        throw new RuntimeException("Cannot accept equal epoch with null data");
                    } else {
                        newDetail.getHostedQueues().put(globalId, oldItems);
                    }
                } else {
                    TopicQueueMappingUtils.makeSureLogicQueueMappingItemImmutable(oldItems, newItems, epochEqual, isClean);
                }
            }
            topicQueueMappingTable.put(newDetail.getTopic(), newDetail);
            updated = true;
        }  finally {
            if (locked) {
                this.lock.unlock();
            }
            if (updated && flush) {
                this.dataVersion.nextVersion();
                this.persist();
                log.info("Update topic queue mapping from [{}] to [{}], force {}", oldDetail, newDetail, force);
            }
        }

    }

    public void delete(final String topic) {
        TopicQueueMappingDetail old = this.topicQueueMappingTable.remove(topic);
        if (old != null) {
            log.info("delete topic queue mapping OK, static topic queue mapping: {}", old);
            this.dataVersion.nextVersion();
            this.persist();
        } else {
            log.warn("delete topic queue mapping failed, static topic: {} not exists", topic);
        }
    }

    public TopicQueueMappingDetail getTopicQueueMapping(String topic) {
        return topicQueueMappingTable.get(topic);
    }

    @Override
    public String encode(boolean pretty) {
        TopicQueueMappingSerializeWrapper wrapper = new TopicQueueMappingSerializeWrapper();
        wrapper.setTopicQueueMappingInfoMap(topicQueueMappingTable);
        wrapper.setDataVersion(this.dataVersion);
        return JSON.toJSONString(wrapper, pretty);
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicQueueMappingPath(this.brokerController.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TopicQueueMappingSerializeWrapper wrapper = TopicQueueMappingSerializeWrapper.fromJson(jsonString, TopicQueueMappingSerializeWrapper.class);
            if (wrapper != null) {
                this.topicQueueMappingTable.putAll(wrapper.getTopicQueueMappingInfoMap());
                this.dataVersion.assignNewOne(wrapper.getDataVersion());
            }
        }
    }

    public ConcurrentMap<String, TopicQueueMappingDetail> getTopicQueueMappingTable() {
        return topicQueueMappingTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public TopicQueueMappingContext buildTopicQueueMappingContext(TopicRequestHeader requestHeader) {
        return buildTopicQueueMappingContext(requestHeader, false);
    }

    //Do not return a null context
    public TopicQueueMappingContext buildTopicQueueMappingContext(TopicRequestHeader requestHeader, boolean selectOneWhenMiss) {
        // if lo is set to false explicitly, it maybe the forwarded request
        if (requestHeader.getLo() != null
                && Boolean.FALSE.equals(requestHeader.getLo())) {
            return new TopicQueueMappingContext(requestHeader.getTopic(), null, null, null, null);
        }
        String topic = requestHeader.getTopic();
        Integer globalId = null;
        if (requestHeader instanceof  TopicQueueRequestHeader) {
            globalId = ((TopicQueueRequestHeader) requestHeader).getQueueId();
        }

        TopicQueueMappingDetail mappingDetail = getTopicQueueMapping(topic);
        if (mappingDetail == null) {
            //it is not static topic
            return new TopicQueueMappingContext(topic, null, null, null, null);
        }
        assert mappingDetail.getBname().equals(this.brokerController.getBrokerConfig().getBrokerName());

        if (globalId == null) {
            return new TopicQueueMappingContext(topic, null, mappingDetail, null, null);
        }

        //If not find mappingItem, it encounters some errors
        if (globalId < 0 && !selectOneWhenMiss) {
            return new TopicQueueMappingContext(topic, globalId, mappingDetail, null, null);
        }

        if (globalId < 0) {
            try {
                if (!mappingDetail.getHostedQueues().isEmpty()) {
                    //do not check
                    globalId = mappingDetail.getHostedQueues().keySet().iterator().next();
                }
            } catch (Throwable ignored) {
            }
        }
        if (globalId < 0) {
            return new TopicQueueMappingContext(topic, globalId,  mappingDetail, null, null);
        }

        List<LogicQueueMappingItem> mappingItemList = TopicQueueMappingDetail.getMappingInfo(mappingDetail, globalId);
        LogicQueueMappingItem leaderItem = null;
        if (mappingItemList != null
                && mappingItemList.size() > 0) {
            leaderItem = mappingItemList.get(mappingItemList.size() - 1);
        }
        return new TopicQueueMappingContext(topic, globalId, mappingDetail, mappingItemList, leaderItem);
    }


    public  RemotingCommand rewriteRequestForStaticTopic(TopicQueueRequestHeader requestHeader, TopicQueueMappingContext mappingContext) {
        try {
            if (mappingContext.getMappingDetail() == null) {
                return null;
            }
            TopicQueueMappingDetail mappingDetail = mappingContext.getMappingDetail();
            if (!mappingContext.isLeader()) {
                return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d does not exit in request process of current broker %s", requestHeader.getTopic(), requestHeader.getQueueId(), mappingDetail.getBname()));
            }
            LogicQueueMappingItem mappingItem = mappingContext.getLeaderItem();
            requestHeader.setQueueId(mappingItem.getQueueId());
            return null;
        } catch (Throwable t) {
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
        }
    }

}
