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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.loadbalance.MessageRequestModeManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.body.MessageRequestModeSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.IOException;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMetrics;

public class SlaveSynchronize {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private volatile String masterAddr = null;

    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
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
        this.syncTopicConfig();
        this.syncConsumerOffset();
        this.syncDelayOffset();
        this.syncSubscriptionGroupConfig();
        this.syncMessageRequestMode();

        if (brokerController.getMessageStoreConfig().isTimerWheelEnable()) {
            this.syncTimerMetrics();
        }
    }

    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                TopicConfigAndMappingSerializeWrapper topicWrapper =
                    this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);
                if (!this.brokerController.getTopicConfigManager().getDataVersion()
                    .equals(topicWrapper.getDataVersion())) {

                    this.brokerController.getTopicConfigManager().getDataVersion()
                        .assignNewOne(topicWrapper.getDataVersion());
                    this.brokerController.getTopicConfigManager().getTopicConfigTable().clear();
                    this.brokerController.getTopicConfigManager().getTopicConfigTable()
                        .putAll(topicWrapper.getTopicConfigTable());
                    this.brokerController.getTopicConfigManager().persist();
                }
                if (topicWrapper.getTopicQueueMappingDetailMap() != null
                        && !topicWrapper.getMappingDataVersion().equals(this.brokerController.getTopicQueueMappingManager().getDataVersion())) {
                    this.brokerController.getTopicQueueMappingManager().getDataVersion()
                            .assignNewOne(topicWrapper.getMappingDataVersion());
                    this.brokerController.getTopicQueueMappingManager().getTopicQueueMappingTable().clear();
                    this.brokerController.getTopicQueueMappingManager().getTopicQueueMappingTable()
                            .putAll(topicWrapper.getTopicQueueMappingDetailMap());
                    this.brokerController.getTopicQueueMappingManager().persist();
                }
                LOGGER.info("Update slave topic config from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                ConsumerOffsetSerializeWrapper offsetWrapper =
                    this.brokerController.getBrokerOuterAPI().getAllConsumerOffset(masterAddrBak);
                this.brokerController.getConsumerOffsetManager().getOffsetTable()
                    .putAll(offsetWrapper.getOffsetTable());
                this.brokerController.getConsumerOffsetManager().getDataVersion().assignNewOne(offsetWrapper.getDataVersion());
                this.brokerController.getConsumerOffsetManager().persist();
                LOGGER.info("Update slave consumer offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncDelayOffset() {
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
                        this.brokerController.getScheduleMessageService().load();
                    } catch (IOException e) {
                        LOGGER.error("Persist file Exception, {}", fileName, e);
                    }
                }
                LOGGER.info("Update slave delay offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null  && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                SubscriptionGroupWrapper subscriptionWrapper =
                    this.brokerController.getBrokerOuterAPI()
                        .getAllSubscriptionGroupConfig(masterAddrBak);

                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                    .equals(subscriptionWrapper.getDataVersion())) {
                    SubscriptionGroupManager subscriptionGroupManager =
                        this.brokerController.getSubscriptionGroupManager();
                    subscriptionGroupManager.getDataVersion().assignNewOne(
                        subscriptionWrapper.getDataVersion());
                    subscriptionGroupManager.getSubscriptionGroupTable().clear();
                    subscriptionGroupManager.getSubscriptionGroupTable().putAll(
                        subscriptionWrapper.getSubscriptionGroupTable());
                    subscriptionGroupManager.persist();
                    LOGGER.info("Update slave Subscription Group from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                LOGGER.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncMessageRequestMode() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null  && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                MessageRequestModeSerializeWrapper messageRequestModeSerializeWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllMessageRequestMode(masterAddrBak);

                MessageRequestModeManager messageRequestModeManager =
                        this.brokerController.getQueryAssignmentProcessor().getMessageRequestModeManager();
                messageRequestModeManager.getMessageRequestModeMap().clear();
                messageRequestModeManager.getMessageRequestModeMap().putAll(
                        messageRequestModeSerializeWrapper.getMessageRequestModeMap()
                );
                messageRequestModeManager.persist();
                LOGGER.info("Update slave Message Request Mode from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncMessageRequestMode Exception, {}", masterAddrBak, e);
            }
        }
    }

    public void syncTimerCheckPoint() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                TimerCheckpoint checkpoint = this.brokerController.getBrokerOuterAPI().getTimerCheckPoint(masterAddrBak);
                if (null != this.brokerController.getTimerCheckpoint()) {
                    this.brokerController.getTimerCheckpoint().setLastReadTimeMs(checkpoint.getLastReadTimeMs());
                    this.brokerController.getTimerCheckpoint().setMasterTimerQueueOffset(checkpoint.getMasterTimerQueueOffset());
                }
            } catch (Exception e) {
                LOGGER.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncTimerMetrics() {
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
