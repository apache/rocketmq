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
package org.apache.rocketmq.broker.bootstrap;

import com.google.common.collect.Lists;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;

/**
 * nameserver related operations
 * register & unregister
 */
public class BrokerServiceRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerConfig brokerConfig;
    private final BrokerController brokerController;
    private BrokerOuterAPI brokerOuterAPI;

    public BrokerServiceRegistry(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.brokerConfig = brokerController.getBrokerConfig();
        NettyClientConfig nettyClientConfig = brokerController.getNettyClientConfig();

        if (null == nettyClientConfig) {
            throw new IllegalArgumentException("nettyClientConfig can't be null for BrokerServiceRegistry");
        }

        this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
    }

    public void start() {
        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }
    }

    public void shutdown() {
        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.shutdown();
        }
    }

    public synchronized void registerSingleTopicAll(final TopicConfig topicConfig) {
        TopicConfig tmpTopic = topicConfig;
        if (!PermName.isWriteable(this.brokerConfig.getBrokerPermission())
            || !PermName.isReadable(this.brokerConfig.getBrokerPermission())) {
            // Copy the topic config and modify the perm
            tmpTopic = new TopicConfig(topicConfig);
            tmpTopic.setPerm(topicConfig.getPerm() & this.brokerConfig.getBrokerPermission());
        }
        this.brokerOuterAPI.registerSingleTopicAll(this.brokerConfig.getBrokerName(), tmpTopic, 3000);
    }

    public synchronized void registerIncrementBrokerData(List<TopicConfig> topicConfigList, DataVersion dataVersion) {
        if (topicConfigList == null || topicConfigList.isEmpty()) {
            return;
        }

        TopicConfigAndMappingSerializeWrapper topicConfigSerializeWrapper = new TopicConfigAndMappingSerializeWrapper();
        topicConfigSerializeWrapper.setDataVersion(dataVersion);

        ConcurrentMap<String, TopicConfig> topicConfigTable = topicConfigList.stream()
            .map(topicConfig -> {
                TopicConfig registerTopicConfig;
                if (!PermName.isWriteable(this.brokerConfig.getBrokerPermission())
                    || !PermName.isReadable(this.brokerConfig.getBrokerPermission())) {
                    registerTopicConfig =
                        new TopicConfig(topicConfig.getTopicName(),
                            topicConfig.getReadQueueNums(),
                            topicConfig.getWriteQueueNums(),
                            this.brokerConfig.getBrokerPermission(), topicConfig.getTopicSysFlag());
                } else {
                    registerTopicConfig = new TopicConfig(topicConfig);
                }
                return registerTopicConfig;
            })
            .collect(Collectors.toConcurrentMap(TopicConfig::getTopicName, Function.identity()));
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

        Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = topicConfigList.stream()
            .map(TopicConfig::getTopicName)
            .map(topicName -> Optional.ofNullable(brokerController.getBrokerMetadataManager().getTopicQueueMappingManager().getTopicQueueMapping(topicName))
                .map(info -> new AbstractMap.SimpleImmutableEntry<>(topicName, TopicQueueMappingDetail.cloneAsMappingInfo(info)))
                .orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!topicQueueMappingInfoMap.isEmpty()) {
            topicConfigSerializeWrapper.setTopicQueueMappingInfoMap(topicQueueMappingInfoMap);
        }

        doRegisterBrokerAll(true, false, topicConfigSerializeWrapper);
    }

    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway, boolean forceRegister) {
        TopicConfigAndMappingSerializeWrapper topicConfigWrapper = new TopicConfigAndMappingSerializeWrapper();

        topicConfigWrapper.setDataVersion(brokerController.getBrokerMetadataManager().getTopicConfigManager().getDataVersion());
        topicConfigWrapper.setTopicConfigTable(brokerController.getBrokerMetadataManager().getTopicConfigManager().getTopicConfigTable());

        topicConfigWrapper.setTopicQueueMappingInfoMap(brokerController.getBrokerMetadataManager().getTopicQueueMappingManager().getTopicQueueMappingTable().entrySet().stream().map(
            entry -> new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), TopicQueueMappingDetail.cloneAsMappingInfo(entry.getValue()))
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        if (!PermName.isWriteable(this.brokerConfig.getBrokerPermission())
            || !PermName.isReadable(this.brokerConfig.getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                TopicConfig tmp =
                    new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                        topicConfig.getPerm() & this.brokerConfig.getBrokerPermission(), topicConfig.getTopicSysFlag());
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        if (forceRegister || needRegister(this.brokerConfig.getBrokerClusterName(),
            brokerController.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId(),
            this.brokerConfig.getRegisterBrokerTimeoutMills(),
            this.brokerConfig.isInBrokerContainer())) {
            doRegisterBrokerAll(checkOrderConfig, oneway, topicConfigWrapper);
        }
    }

    public void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(
            this.brokerConfig.getBrokerClusterName(),
            brokerController.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId());
    }

    private void doRegisterBrokerAll(boolean checkOrderConfig, boolean oneway,
        TopicConfigSerializeWrapper topicConfigWrapper) {

        if (brokerController.isShutdown()) {
            LOG.info("BrokerController#doResterBrokerAll: broker has shutdown, no need to register any more.");
            return;
        }
        List<RegisterBrokerResult> registerBrokerResultList = this.brokerOuterAPI.registerBrokerAll(
            this.brokerConfig.getBrokerClusterName(),
            brokerController.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId(),
            brokerController.getHAServerAddr(),
            topicConfigWrapper,
            Lists.newArrayList(),
            oneway,
            this.brokerConfig.getRegisterBrokerTimeoutMills(),
            this.brokerConfig.isEnableSlaveActingMaster(),
            this.brokerConfig.isCompressedRegister(),
            this.brokerConfig.isEnableSlaveActingMaster() ? this.brokerConfig.getBrokerNotActiveTimeoutMillis() : null,
            brokerController.getBrokerIdentity());

        handleRegisterBrokerResult(registerBrokerResultList, checkOrderConfig);
    }

    private boolean needRegister(final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final int timeoutMills,
        final boolean isInBrokerContainer) {

        TopicConfigSerializeWrapper topicConfigWrapper = brokerController.getBrokerMetadataManager().getTopicConfigManager().buildTopicConfigSerializeWrapper();
        List<Boolean> changeList = brokerOuterAPI.needRegister(clusterName, brokerAddr, brokerName, brokerId, topicConfigWrapper, timeoutMills, isInBrokerContainer);
        boolean needRegister = false;
        for (Boolean changed : changeList) {
            if (changed) {
                needRegister = true;
                break;
            }
        }
        return needRegister;
    }

    private void handleRegisterBrokerResult(List<RegisterBrokerResult> registerBrokerResultList,
        boolean checkOrderConfig) {
        for (RegisterBrokerResult registerBrokerResult : registerBrokerResultList) {
            if (registerBrokerResult != null) {
                if (brokerController.isUpdateMasterHAServerAddrPeriodically() && registerBrokerResult.getHaServerAddr() != null) {
                    brokerController.getMessageStore().updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
                    brokerController.getMessageStore().updateMasterAddress(registerBrokerResult.getMasterAddr());
                }

                brokerController.getSlaveSynchronize().setMasterAddr(registerBrokerResult.getMasterAddr());
                if (checkOrderConfig) {
                    brokerController.getBrokerMetadataManager().getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
                }
                break;
            }
        }
    }

    public void setBrokerOuterAPI(BrokerOuterAPI brokerOuterAPI) {
        this.brokerOuterAPI = brokerOuterAPI;
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }

}
