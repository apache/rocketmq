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
package org.apache.rocketmq.broker.slave;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.loadbalance.MessageRequestModeManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sync.MetadataChangeInfo;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMetrics;

public class SlaveSynchronize {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private volatile String masterAddr = null;
    private boolean isIncrementSyncRunning = false;

    private DefaultLitePullConsumer incrementSyncConsumer;

    private long topicConfigSyncOffset = 0;
    private long consumerOffsetSyncOffset = 0;
    private long subscriptionGroupSyncOffset = 0;
    private long delayOffsetSyncOffset = 0;
    private long messageRequestModeSyncOffset = 0;
    private long timerMetricsSyncOffset = 0;


    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {
        if (!isIncrementSyncRunning) {
            try {
                isIncrementSyncRunning = true;
                initIncrementSyncConsumer();

                Callable<Map<MessageQueue, Long>> snapshotSyncCallback = () -> {
                    Map<MessageQueue, Long> messageQueueLongMap = null;
                    try {
                        messageQueueLongMap = syncAllMetadataSnapshots();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return messageQueueLongMap;
                };
                startIncrementalSync(snapshotSyncCallback);

                LOGGER.info("Slave synchronize service started successfully.");
            } catch (Exception e) {
                LOGGER.error("Failed to start slave synchronize service", e);
                isIncrementSyncRunning = false;
            }
        }
    }

    private void initIncrementSyncConsumer() throws MQClientException {
        this.incrementSyncConsumer = new DefaultLitePullConsumer(MixAll.SLAVE_INCREMENT_SYNC_CONSUMER_GROUP);
        this.incrementSyncConsumer.setNamesrvAddr(this.brokerController.getBrokerConfig().getNamesrvAddr());
        this.incrementSyncConsumer.subscribe(TopicValidator.RMQ_SYS_TOPIC_CONFIG_SYNC);
        this.incrementSyncConsumer.subscribe(TopicValidator.RMQ_SYS_CONSUMER_OFFSET_SYNC);
        this.incrementSyncConsumer.subscribe(TopicValidator.RMQ_SYS_SUBSCRIPTION_GROUP_SYNC);
        this.incrementSyncConsumer.subscribe(TopicValidator.RMQ_SYS_DELAY_OFFSET_SYNC);
        this.incrementSyncConsumer.subscribe(TopicValidator.RMQ_SYS_MESSAGE_MODE_SYNC);

        if (brokerController.getMessageStoreConfig().isTimerWheelEnable()) {
            this.incrementSyncConsumer.subscribe(TopicValidator.RMQ_SYS_TIMER_METRICS_SYNC);
        }
    }

    public void startIncrementalSync(Callable<Map<MessageQueue, Long>> syncSnapshotCallback) {
        try {
            this.incrementSyncConsumer.start();
            setConsumerOffset(syncSnapshotCallback.call());
            while (this.incrementSyncConsumer.isRunning()) {
                List<MessageExt> messages = this.incrementSyncConsumer.poll(1000 * 3);
                if (messages != null && !messages.isEmpty()) {
                    for (MessageExt msg : messages) {
                        try {
                            handleMetadataMessage(msg);
                        } catch (Exception e) {
                            LOGGER.error("Failed to handle metadata message", e);
                        }
                    }
                }
                if (isLaggingTooFarBehind()) {
                    LOGGER.info("Incremental sync consumer is lagging too far behind");
                    setConsumerOffset(syncSnapshotCallback.call());
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to start incremental sync consumer", e);
            throw new RuntimeException(e);
        } finally {
            this.incrementSyncConsumer.shutdown();
        }
    }

    private void setConsumerOffset(Map<MessageQueue, Long> consumerOffsets) throws MQClientException {
        for (Map.Entry<MessageQueue, Long> entry : consumerOffsets.entrySet()) {
            MessageQueue queue = entry.getKey();
            Long offset = entry.getValue();
            this.incrementSyncConsumer.seek(queue, offset);
        }
    }

    private boolean isLaggingTooFarBehind() {
        final int threshold = brokerController.getBrokerConfig().getIncrementalSyncConsumerLagThreshold();
        long topicConfigMaxOffset;
        long consumerOffsetMaxOffset;
        long subscriptionGroupMaxOffset;
        long delayOffsetMaxOffset;
        long messageRequestModeMaxOffset;
        long timerMetricsMaxOffset;
        try {
            topicConfigMaxOffset = brokerController.getMessageStore().getMaxOffsetInQueue(TopicValidator.RMQ_SYS_TOPIC_CONFIG_SYNC, 0);
            consumerOffsetMaxOffset = brokerController.getMessageStore().getMaxOffsetInQueue(TopicValidator.RMQ_SYS_CONSUMER_OFFSET_SYNC, 0);
            subscriptionGroupMaxOffset = brokerController.getMessageStore().getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SUBSCRIPTION_GROUP_SYNC, 0);
            delayOffsetMaxOffset = brokerController.getMessageStore().getMaxOffsetInQueue(TopicValidator.RMQ_SYS_DELAY_OFFSET_SYNC, 0);
            messageRequestModeMaxOffset = brokerController.getMessageStore().getMaxOffsetInQueue(TopicValidator.RMQ_SYS_MESSAGE_MODE_SYNC, 0);
            timerMetricsMaxOffset = brokerController.getMessageStore().getMaxOffsetInQueue(TopicValidator.RMQ_SYS_TIMER_METRICS_SYNC, 0);
        } catch (ConsumeQueueException e) {
            return true;
        }
        return (topicConfigMaxOffset - topicConfigSyncOffset) > threshold ||
                (consumerOffsetMaxOffset - consumerOffsetSyncOffset) > threshold ||
                (subscriptionGroupMaxOffset - subscriptionGroupSyncOffset) > threshold ||
                (delayOffsetMaxOffset - delayOffsetSyncOffset) > threshold
                || (messageRequestModeMaxOffset - messageRequestModeSyncOffset) > threshold
                || (timerMetricsMaxOffset - timerMetricsSyncOffset) > threshold;
    }

    private void handleMetadataMessage(MessageExt msg) {
        MetadataChangeInfo metadataChangeInfo = JSONObject.parseObject(new String(msg.getBody(), StandardCharsets.UTF_8), MetadataChangeInfo.class);
        switch (msg.getTopic()) {
            case TopicValidator.RMQ_SYS_TOPIC_CONFIG_SYNC:
                handleTopicConfigChangeInfo(metadataChangeInfo);
                this.topicConfigSyncOffset = msg.getQueueOffset();
                break;
            case TopicValidator.RMQ_SYS_CONSUMER_OFFSET_SYNC:
                handleConsumerOffsetChangeInfo(metadataChangeInfo);
                this.consumerOffsetSyncOffset = msg.getQueueOffset();
                break;
            case TopicValidator.RMQ_SYS_SUBSCRIPTION_GROUP_SYNC:
                handleSubscriptionGroupChangeInfo(metadataChangeInfo);
                this.subscriptionGroupSyncOffset = msg.getQueueOffset();
                break;
            case TopicValidator.RMQ_SYS_DELAY_OFFSET_SYNC:
                handleDelayOffsetChangeInfo(metadataChangeInfo);
                this.delayOffsetSyncOffset = msg.getQueueOffset();
                break;
            case TopicValidator.RMQ_SYS_MESSAGE_MODE_SYNC:
                handleMessageModeChangeInfo(metadataChangeInfo);
                this.messageRequestModeSyncOffset = msg.getQueueOffset();
                break;
            case TopicValidator.RMQ_SYS_TIMER_METRICS_SYNC:
                handleTimerMetricsChangeInfo(metadataChangeInfo);
                this.timerMetricsSyncOffset = msg.getQueueOffset();
                break;
            default:
                break;
        }
    }

    private void handleTopicConfigChangeInfo(MetadataChangeInfo metadataChangeInfo) {
        switch (metadataChangeInfo.getChangeType()) {
            case CREATED:
            case UPDATED:
                this.brokerController.getTopicConfigManager().getTopicConfigTable().put(
                        metadataChangeInfo.getMetadataKey(), JSON.parseObject(metadataChangeInfo.getMetadataValue(), TopicConfig.class));
                break;
            case DELETED:
                this.brokerController.getTopicConfigManager().getTopicConfigTable().remove(metadataChangeInfo.getMetadataKey());
                break;
            default:
                break;

        }
    }

    private void handleConsumerOffsetChangeInfo(MetadataChangeInfo metadataChangeInfo) {
        ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsets = JSONObject.parseObject(metadataChangeInfo.getMetadataValue(), new TypeReference<ConcurrentMap<String, ConcurrentMap<Integer, Long>>>() {
        });
        this.brokerController.getConsumerOffsetManager().getOffsetTable().putAll(offsets);
    }

    private void handleSubscriptionGroupChangeInfo(MetadataChangeInfo metadataChangeInfo) {
        switch (metadataChangeInfo.getChangeType()) {
            case CREATED:
            case UPDATED:
                this.brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().put(
                        metadataChangeInfo.getMetadataKey(), JSON.parseObject(metadataChangeInfo.getMetadataValue(), SubscriptionGroupConfig.class));
                break;
            case DELETED:
                this.brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().remove(metadataChangeInfo.getMetadataKey());
                break;
            default:
                break;
        }

    }

    private void handleDelayOffsetChangeInfo(MetadataChangeInfo metadataChangeInfo) {
        switch (metadataChangeInfo.getChangeType()) {
            case CREATED:
            case UPDATED:
                this.brokerController.getScheduleMessageService().getOffsetTable().put(
                        Integer.valueOf(metadataChangeInfo.getMetadataKey()), Long.parseLong(metadataChangeInfo.getMetadataValue()));
                break;
            default:
                break;
        }
    }

    private void handleMessageModeChangeInfo(MetadataChangeInfo metadataChangeInfo) {
        switch (metadataChangeInfo.getChangeType()) {
            case CREATED:
            case UPDATED:
                ConcurrentHashMap<String, SetMessageRequestModeRequestBody> requestModeMap = new ConcurrentHashMap<>();
                LinkedHashMap<String, SetMessageRequestModeRequestBody> linkedMap = JSONObject.parseObject(metadataChangeInfo.getMetadataValue(),
                        new TypeReference<LinkedHashMap<String, SetMessageRequestModeRequestBody>>() {
                        });
                if (linkedMap != null) {
                    requestModeMap.putAll(linkedMap);
                }
                this.brokerController.getQueryAssignmentProcessor().getMessageRequestModeManager().getMessageRequestModeMap().put(
                        metadataChangeInfo.getMetadataKey(), requestModeMap);
                break;
            default:
                break;
        }
    }

    private void handleTimerMetricsChangeInfo(MetadataChangeInfo metadataChangeInfo) {
        switch (metadataChangeInfo.getChangeType()) {
            case CREATED:
            case UPDATED:
                this.brokerController.getTimerMessageStore().getTimerMetrics().getTimingCount().put(
                        metadataChangeInfo.getMetadataKey(), JSON.parseObject(metadataChangeInfo.getMetadataValue(), TimerMetrics.Metric.class));
                break;
            case DELETED:
                break;
            default:
                break;
        }
    }


    private Map<MessageQueue, Long> syncAllMetadataSnapshots() throws Exception {
        LOGGER.info("Starting full metadata synchronization via snapshots...");
        HashMap<MessageQueue, Long> result = new HashMap<>();


        Triple<ConcurrentMap<String, TopicConfig>, DataVersion, Long> topicConfigSnapshot =
                this.brokerController.getBrokerOuterAPI().getTopicConfigSnapShot(this.masterAddr);
        if (topicConfigSnapshot != null) {
            if (this.topicConfigSyncOffset < topicConfigSnapshot.getRight()) {
                this.brokerController.getTopicConfigManager().setTopicConfigTable(topicConfigSnapshot.getLeft());
                this.brokerController.getTopicConfigManager().persist();
                this.topicConfigSyncOffset = topicConfigSnapshot.getRight();
                result.put(new MessageQueue(TopicValidator.RMQ_SYS_TOPIC_CONFIG_SYNC, brokerController.getBrokerConfig().getBrokerName(), 0), topicConfigSyncOffset);
            }
            LOGGER.info("Topic config synced. Pull will start from offset: {}", consumerOffsetSyncOffset);
        }

        Triple<ConcurrentMap<String, ConcurrentMap<Integer, Long>>, DataVersion, Long> consumerOffsetSnapshot =
                this.brokerController.getBrokerOuterAPI().getConsumerOffsetSnapShot(this.masterAddr);
        if (consumerOffsetSnapshot != null) {
            if (this.consumerOffsetSyncOffset < consumerOffsetSnapshot.getRight()) {
                this.brokerController.getConsumerOffsetManager().setOffsetTable(consumerOffsetSnapshot.getLeft());
                this.brokerController.getConsumerOffsetManager().persist();
                this.consumerOffsetSyncOffset = consumerOffsetSnapshot.getRight();
                result.put(new MessageQueue(TopicValidator.RMQ_SYS_CONSUMER_OFFSET_SYNC, brokerController.getBrokerConfig().getBrokerName(), 0), consumerOffsetSyncOffset);
            }
            this.brokerController.getConsumerOffsetManager().setDataVersion(consumerOffsetSnapshot.getMiddle());
            LOGGER.info("Consumer offset synced. Pull will start from offset: {}", consumerOffsetSyncOffset);
        }

        Triple<ConcurrentMap<String, SubscriptionGroupConfig>, DataVersion, Long> subscriptionGroupSnapshot =
                this.brokerController.getBrokerOuterAPI().getSubscriptionGroupSnapShot(this.masterAddr);
        if (subscriptionGroupSnapshot != null) {
            this.brokerController.getSubscriptionGroupManager().setDataVersion(subscriptionGroupSnapshot.getMiddle());
            if (this.subscriptionGroupSyncOffset < subscriptionGroupSnapshot.getRight()) {
                this.brokerController.getSubscriptionGroupManager().setSubscriptionGroupTable(subscriptionGroupSnapshot.getLeft());
                this.brokerController.getSubscriptionGroupManager().persist();
                this.subscriptionGroupSyncOffset = subscriptionGroupSnapshot.getRight();
                result.put(new MessageQueue(TopicValidator.RMQ_SYS_SUBSCRIPTION_GROUP_SYNC, brokerController.getBrokerConfig().getBrokerName(), 0), subscriptionGroupSyncOffset);
            }
            LOGGER.info("Subscription group synced. Pull will start from offset: {}", subscriptionGroupSyncOffset);
        }

        Triple<ConcurrentMap<String, Long>,DataVersion,Long> delayOffsetSnapshot =
                this.brokerController.getBrokerOuterAPI().getDelayOffsetSnapShot(this.masterAddr);
        if (delayOffsetSnapshot != null) {
            Map<String, Object> jsonOutput = new LinkedHashMap<>();
            jsonOutput.put("dataVersion", delayOffsetSnapshot.getMiddle());
            jsonOutput.put("offsetTable", delayOffsetSnapshot.getLeft());
            String jsonString = JSON.toJSONString(jsonOutput);
            MixAll.string2File(jsonString, StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController.getMessageStoreConfig().getStorePathRootDir()));
            this.brokerController.getScheduleMessageService().loadWhenSyncDelayOffset();
            if (this.delayOffsetSyncOffset < delayOffsetSnapshot.getRight()) {
                this.delayOffsetSyncOffset = delayOffsetSnapshot.getRight();
                result.put(new MessageQueue(TopicValidator.RMQ_SYS_DELAY_OFFSET_SYNC, brokerController.getBrokerConfig().getBrokerName(), 0), delayOffsetSyncOffset);
            }
            LOGGER.info("Delay offset synced. Pull will start from offset: {}", delayOffsetSyncOffset);
        }

        Pair<ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>>, Long> messageModeSnapshot =
                this.brokerController.getBrokerOuterAPI().getSetMessageRequestModeSnapShot(this.masterAddr);
        if (messageModeSnapshot != null) {

            if (this.messageRequestModeSyncOffset < messageModeSnapshot.getObject2()) {
                this.brokerController.getQueryAssignmentProcessor().getMessageRequestModeManager().setMessageRequestModeMap(messageModeSnapshot.getObject1());
                this.brokerController.getQueryAssignmentProcessor().getMessageRequestModeManager().persist();
                this.messageRequestModeSyncOffset = messageModeSnapshot.getObject2();
                result.put(new MessageQueue(TopicValidator.RMQ_SYS_MESSAGE_MODE_SYNC, brokerController.getBrokerConfig().getBrokerName(), 0), messageRequestModeSyncOffset);
            }
            LOGGER.info("Message request mode synced. Pull will start from offset: {}", messageRequestModeSyncOffset);
        }
        LOGGER.info("Full metadata synchronization completed.");

        if (brokerController.getMessageStoreConfig().isTimerWheelEnable()) {
            Triple<ConcurrentMap<String, TimerMetrics.Metric>, DataVersion, Long> timerMetricsSnapshot =
                    this.brokerController.getBrokerOuterAPI().getTopicMetricsSnapShot(this.masterAddr);
            if (timerMetricsSnapshot != null) {
                if (null != brokerController.getMessageStore().getTimerMessageStore()) {
                    this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getDataVersion().assignNewOne(timerMetricsSnapshot.getMiddle());
                    this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getTimingCount().clear();
                    this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getTimingCount().putAll(timerMetricsSnapshot.getLeft());
                    this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().persist();
                    if (this.timerMetricsSyncOffset < timerMetricsSnapshot.getRight()) {
                        this.timerMetricsSyncOffset = timerMetricsSnapshot.getRight();
                        result.put(new MessageQueue(TopicValidator.RMQ_SYS_TIMER_METRICS_SYNC, brokerController.getBrokerConfig().getBrokerName(), 0), timerMetricsSyncOffset);
                    }
                    this.timerMetricsSyncOffset = timerMetricsSnapshot.getRight();
                }
            }
        }
        return result;
    }




    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        if (!StringUtils.equals(this.masterAddr, masterAddr)) {
            LOGGER.info("Update master address from {} to {}", this.masterAddr, masterAddr);
            this.masterAddr = masterAddr;
        }
    }

    public void syncAll() {
        this.syncTopicConfigFull();
        this.syncConsumerOffsetFull();
        this.syncDelayOffsetFull();
        this.syncSubscriptionGroupConfigFull();
        this.syncMessageRequestModeFull();

        if (brokerController.getMessageStoreConfig().isTimerWheelEnable()) {
            this.syncTimerMetricsFull();
        }
    }


    private void syncTopicConfigFull() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                TopicConfigAndMappingSerializeWrapper topicWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);
                TopicConfigManager topicConfigManager = this.brokerController.getTopicConfigManager();
                if (!topicConfigManager.getDataVersion().equals(topicWrapper.getDataVersion())) {

                    topicConfigManager.getDataVersion().assignNewOne(topicWrapper.getDataVersion());

                    ConcurrentMap<String, TopicConfig> newTopicConfigTable = topicWrapper.getTopicConfigTable();
                    ConcurrentMap<String, TopicConfig> topicConfigTable = topicConfigManager.getTopicConfigTable();

                    //delete
                    Iterator<Map.Entry<String, TopicConfig>> iterator = topicConfigTable.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, TopicConfig> entry = iterator.next();
                        if (!newTopicConfigTable.containsKey(entry.getKey())) {
                            iterator.remove();
                        }
                        topicConfigManager.deleteTopicConfig(entry.getKey());
                    }

                    //update
                    newTopicConfigTable.values().forEach(topicConfigManager::putTopicConfig);
                    topicConfigManager.updateDataVersion();
                    topicConfigManager.persist();
                }
                if (topicWrapper.getTopicQueueMappingDetailMap() != null
                        && !topicWrapper.getMappingDataVersion().equals(this.brokerController.getTopicQueueMappingManager().getDataVersion())) {
                    this.brokerController.getTopicQueueMappingManager().getDataVersion()
                            .assignNewOne(topicWrapper.getMappingDataVersion());

                    ConcurrentMap<String, TopicConfig> newTopicConfigTable = topicWrapper.getTopicConfigTable();
                    //delete
                    ConcurrentMap<String, TopicConfig> topicConfigTable = this.brokerController.getTopicConfigManager().getTopicConfigTable();
                    topicConfigTable.entrySet().removeIf(item -> !newTopicConfigTable.containsKey(item.getKey()));
                    //update
                    topicConfigTable.putAll(newTopicConfigTable);

                    this.brokerController.getTopicQueueMappingManager().persist();
                }
                LOGGER.info("Full sync slave topic config from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
            }
        }
    }


    private void syncConsumerOffsetFull() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                ConsumerOffsetSerializeWrapper offsetWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllConsumerOffset(masterAddrBak);
                this.brokerController.getConsumerOffsetManager().getOffsetTable()
                        .putAll(offsetWrapper.getOffsetTable());
                this.brokerController.getConsumerOffsetManager().getDataVersion().assignNewOne(offsetWrapper.getDataVersion());
                this.brokerController.getConsumerOffsetManager().persist();
                LOGGER.info("Full sync slave consumer offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncDelayOffsetFull() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                String delayOffset =
                        this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);
                if (delayOffset != null) {

                    String fileName =
                            StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController
                                    .getMessageStoreConfig().getStorePathRootDir());
                    try {
                        MixAll.string2File(delayOffset, fileName);
                        this.brokerController.getScheduleMessageService().loadWhenSyncDelayOffset();
                    } catch (IOException e) {
                        LOGGER.error("Persist file Exception, {}", fileName, e);
                    }
                }
                LOGGER.info("Full sync slave delay offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncSubscriptionGroupConfigFull() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                SubscriptionGroupWrapper subscriptionWrapper =
                        this.brokerController.getBrokerOuterAPI()
                                .getAllSubscriptionGroupConfig(masterAddrBak);

                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                        .equals(subscriptionWrapper.getDataVersion())) {
                    SubscriptionGroupManager subscriptionGroupManager = this.brokerController.getSubscriptionGroupManager();
                    subscriptionGroupManager.getDataVersion().assignNewOne(subscriptionWrapper.getDataVersion());

                    ConcurrentMap<String, SubscriptionGroupConfig> curSubscriptionGroupTable =
                            subscriptionGroupManager.getSubscriptionGroupTable();
                    ConcurrentMap<String, SubscriptionGroupConfig> newSubscriptionGroupTable =
                            subscriptionWrapper.getSubscriptionGroupTable();
                    // delete
                    Iterator<Map.Entry<String, SubscriptionGroupConfig>> iterator = curSubscriptionGroupTable.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, SubscriptionGroupConfig> configEntry = iterator.next();
                        if (!newSubscriptionGroupTable.containsKey(configEntry.getKey())) {
                            iterator.remove();
                        }
                        subscriptionGroupManager.deleteSubscriptionGroupConfig(configEntry.getKey());
                    }
                    // update
                    newSubscriptionGroupTable.values().forEach(subscriptionGroupManager::updateSubscriptionGroupConfigWithoutPersist);
                    // persist
                    subscriptionGroupManager.persist();
                    LOGGER.info("Full sync slave Subscription Group from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                LOGGER.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncMessageRequestModeFull() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                MessageRequestModeSerializeWrapper messageRequestModeSerializeWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllMessageRequestMode(masterAddrBak);

                MessageRequestModeManager messageRequestModeManager =
                        this.brokerController.getQueryAssignmentProcessor().getMessageRequestModeManager();
                ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> curMessageRequestModeMap =
                        messageRequestModeManager.getMessageRequestModeMap();
                ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> newMessageRequestModeMap =
                        messageRequestModeSerializeWrapper.getMessageRequestModeMap();

                // delete
                curMessageRequestModeMap.entrySet().removeIf(e -> !newMessageRequestModeMap.containsKey(e.getKey()));
                // update
                curMessageRequestModeMap.putAll(newMessageRequestModeMap);
                // persist
                messageRequestModeManager.persist();
                LOGGER.info("Full sync slave Message Request Mode from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncMessageRequestMode Exception, {}", masterAddrBak, e);
            }
        }
    }

    public void syncTimerCheckPoint() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                if (null != brokerController.getMessageStore().getTimerMessageStore() &&
                        !brokerController.getTimerMessageStore().isShouldRunningDequeue()) {
                    TimerCheckpoint checkpoint = this.brokerController.getBrokerOuterAPI().getTimerCheckPoint(masterAddrBak);
                    if (null != this.brokerController.getTimerCheckpoint()) {
                        this.brokerController.getTimerCheckpoint().setLastReadTimeMs(checkpoint.getLastReadTimeMs());
                        this.brokerController.getTimerCheckpoint().setMasterTimerQueueOffset(checkpoint.getMasterTimerQueueOffset());
                        this.brokerController.getTimerCheckpoint().getDataVersion().assignNewOne(checkpoint.getDataVersion());
                    }
                }
            } catch (Exception e) {
                LOGGER.error("syncTimerCheckPoint Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncTimerMetricsFull() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                if (null != brokerController.getMessageStore().getTimerMessageStore()) {
                    TimerMetrics.TimerMetricsSerializeWrapper metricsSerializeWrapper =
                            this.brokerController.getBrokerOuterAPI().getTimerMetrics(masterAddrBak);
                    if (!brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getDataVersion().equals(metricsSerializeWrapper.getDataVersion())) {
                        this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getDataVersion().assignNewOne(metricsSerializeWrapper.getDataVersion());
                        this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getTimingCount().clear();
                        this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getTimingCount().putAll(metricsSerializeWrapper.getTimingCount());
                        this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().persist();
                    }
                }
            } catch (Exception e) {
                LOGGER.error("SyncTimerMetrics Exception, {}", masterAddrBak, e);
            }
        }
    }
}
