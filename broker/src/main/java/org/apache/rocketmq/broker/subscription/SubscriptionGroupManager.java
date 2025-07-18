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
package org.apache.rocketmq.broker.subscription;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.SubscriptionGroupAttributes;
import org.apache.rocketmq.common.attribute.AttributeUtil;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class SubscriptionGroupManager extends ConfigManager {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    protected ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable =
        new ConcurrentHashMap<>(1024);

    private ConcurrentMap<String, ConcurrentMap<String, Integer>> forbiddenTable =
        new ConcurrentHashMap<>(4);

    protected final DataVersion dataVersion = new DataVersion();
    protected transient BrokerController brokerController;

    public SubscriptionGroupManager() {
        this.init();
    }

    public SubscriptionGroupManager(BrokerController brokerController) {
        this(brokerController, true);
    }

    public SubscriptionGroupManager(BrokerController brokerController, boolean init) {
        this.brokerController = brokerController;
        if (init) {
            init();
        }
    }

    protected void init() {
        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.TOOLS_CONSUMER_GROUP);
            putSubscriptionGroupConfig(subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.FILTERSRV_CONSUMER_GROUP);
            putSubscriptionGroupConfig(subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.SELF_TEST_CONSUMER_GROUP);
            putSubscriptionGroupConfig(subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.ONS_HTTP_PROXY_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            putSubscriptionGroupConfig(subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PULL_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            putSubscriptionGroupConfig(subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PERMISSION_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            putSubscriptionGroupConfig(subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_OWNER_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            putSubscriptionGroupConfig(subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_SYS_RMQ_TRANS);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            putSubscriptionGroupConfig(subscriptionGroupConfig);
        }
    }

    public SubscriptionGroupConfig putSubscriptionGroupConfig(SubscriptionGroupConfig subscriptionGroupConfig) {
        return this.subscriptionGroupTable.put(subscriptionGroupConfig.getGroupName(), subscriptionGroupConfig);
    }

    protected SubscriptionGroupConfig putSubscriptionGroupConfigIfAbsent(SubscriptionGroupConfig subscriptionGroupConfig) {
        return this.subscriptionGroupTable.putIfAbsent(subscriptionGroupConfig.getGroupName(), subscriptionGroupConfig);
    }

    protected SubscriptionGroupConfig getSubscriptionGroupConfig(String groupName) {
        return this.subscriptionGroupTable.get(groupName);
    }

    protected SubscriptionGroupConfig removeSubscriptionGroupConfig(String groupName) {
        return this.subscriptionGroupTable.remove(groupName);
    }

    public void updateSubscriptionGroupConfig(final SubscriptionGroupConfig config) {
        updateSubscriptionGroupConfigWithoutPersist(config);
        this.persist();
    }

    public void updateSubscriptionGroupConfigWithoutPersist(SubscriptionGroupConfig config) {
        Map<String, String> newAttributes = request(config);
        Map<String, String> currentAttributes = current(config.getGroupName());

        Map<String, String> finalAttributes = AttributeUtil.alterCurrentAttributes(
            this.subscriptionGroupTable.get(config.getGroupName()) == null,
            SubscriptionGroupAttributes.ALL,
            ImmutableMap.copyOf(currentAttributes),
            ImmutableMap.copyOf(newAttributes));

        config.setAttributes(finalAttributes);

        SubscriptionGroupConfig old = putSubscriptionGroupConfig(config);
        if (old != null) {
            log.info("update subscription group config, old: {} new: {}", old, config);
        } else {
            log.info("create new subscription group, {}", config);
        }

        updateDataVersion();
    }

    public void updateSubscriptionGroupConfigList(List<SubscriptionGroupConfig> configList) {
        if (null == configList || configList.isEmpty()) {
            return;
        }
        configList.forEach(this::updateSubscriptionGroupConfigWithoutPersist);
        this.persist();
    }

    public void updateForbidden(String group, String topic, int forbiddenIndex, boolean setOrClear) {
        if (setOrClear) {
            setForbidden(group, topic, forbiddenIndex);
        } else {
            clearForbidden(group, topic, forbiddenIndex);
        }
    }

    /**
     * set the bit value to 1 at the specific index (from 0)
     *
     * @param group
     * @param topic
     * @param forbiddenIndex from 0
     */
    public void setForbidden(String group, String topic, int forbiddenIndex) {
        int topicForbidden = getForbidden(group, topic);
        topicForbidden |= 1 << forbiddenIndex;
        updateForbiddenValue(group, topic, topicForbidden);
    }

    /**
     * clear the bit value to 0 at the specific index (from 0)
     *
     * @param group
     * @param topic
     * @param forbiddenIndex from 0
     */
    public void clearForbidden(String group, String topic, int forbiddenIndex) {
        int topicForbidden = getForbidden(group, topic);
        topicForbidden &= ~(1 << forbiddenIndex);
        updateForbiddenValue(group, topic, topicForbidden);
    }

    public boolean getForbidden(String group, String topic, int forbiddenIndex) {
        int topicForbidden = getForbidden(group, topic);
        int bitForbidden = 1 << forbiddenIndex;
        return (topicForbidden & bitForbidden) == bitForbidden;
    }

    public int getForbidden(String group, String topic) {
        ConcurrentMap<String, Integer> topicForbiddens = this.forbiddenTable.get(group);
        if (topicForbiddens == null) {
            return 0;
        }
        Integer topicForbidden = topicForbiddens.get(topic);
        if (topicForbidden == null || topicForbidden < 0) {
            topicForbidden = 0;
        }
        return topicForbidden;
    }

    protected void updateForbiddenValue(String group, String topic, Integer forbidden) {
        if (forbidden == null || forbidden <= 0) {
            this.forbiddenTable.remove(group);
            log.info("clear group forbidden, {}@{} ", group, topic);
            return;
        }

        ConcurrentMap<String, Integer> topicsPermMap = this.forbiddenTable.get(group);
        if (topicsPermMap == null) {
            this.forbiddenTable.putIfAbsent(group, new ConcurrentHashMap<>());
            topicsPermMap = this.forbiddenTable.get(group);
        }
        Integer old = topicsPermMap.put(topic, forbidden);
        if (old != null) {
            log.info("set group forbidden, {}@{} old: {} new: {}", group, topic, old, forbidden);
        } else {
            log.info("set group forbidden, {}@{} old: {} new: {}", group, topic, 0, forbidden);
        }

        updateDataVersion();

        this.persist();
    }

    public void disableConsume(final String groupName) {
        SubscriptionGroupConfig old = getSubscriptionGroupConfig(groupName);
        if (old != null) {
            old.setConsumeEnable(false);
            updateDataVersion();
        }
    }

    public SubscriptionGroupConfig findSubscriptionGroupConfig(final String group) {
        SubscriptionGroupConfig subscriptionGroupConfig = getSubscriptionGroupConfig(group);
        if (null == subscriptionGroupConfig) {
            if (brokerController.getBrokerConfig().isAutoCreateSubscriptionGroup()
                    || MixAll.isSysConsumerGroupAndEnableCreate(group, brokerController.getBrokerConfig().isEnableCreateSysGroup())) {
                if (group.length() > Validators.CHARACTER_MAX_LENGTH || TopicValidator.isTopicOrGroupIllegal(group)) {
                    return null;
                }
                subscriptionGroupConfig = new SubscriptionGroupConfig();
                subscriptionGroupConfig.setGroupName(group);
                SubscriptionGroupConfig preConfig = putSubscriptionGroupConfigIfAbsent(subscriptionGroupConfig);
                if (null == preConfig) {
                    log.info("auto create a subscription group, {}", subscriptionGroupConfig.toString());
                }
                updateDataVersion();
                this.persist();
            }
        }

        return subscriptionGroupConfig;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getSubscriptionGroupPath(this.brokerController.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            SubscriptionGroupManager obj = RemotingSerializable.fromJson(jsonString, SubscriptionGroupManager.class);
            if (obj != null) {
                this.subscriptionGroupTable.putAll(obj.subscriptionGroupTable);
                if (obj.forbiddenTable != null) {
                    this.forbiddenTable.putAll(obj.forbiddenTable);
                }
                this.dataVersion.assignNewOne(obj.dataVersion);
                this.printLoadDataWhenFirstBoot(obj);
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    private void printLoadDataWhenFirstBoot(final SubscriptionGroupManager sgm) {
        Iterator<Entry<String, SubscriptionGroupConfig>> it = sgm.getSubscriptionGroupTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionGroupConfig> next = it.next();
            log.info("load exist subscription group, {}", next.getValue().toString());
        }
    }

    public ConcurrentMap<String, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return subscriptionGroupTable;
    }

    public ConcurrentHashMap<String, SubscriptionGroupConfig> subGroupTable(String dataVersion, int groupSeq,
        int maxGroupNum) {
        // [groupSeq, groupSeq + maxGroupNum)
        int beginIndex = groupSeq;
        if (StringUtils.isBlank(dataVersion) || !Objects.equals(DataVersion.fromJson(dataVersion, DataVersion.class), this.dataVersion)) {
            beginIndex = 0;
            log.info("get sub subscription group table from {} due to {}", beginIndex,
                StringUtils.isBlank(dataVersion) ? "DataVersion Empty" : "DataVersion Changed");
        }

        ConcurrentHashMap<String, SubscriptionGroupConfig> subGroupTable = new ConcurrentHashMap<>();
        if (beginIndex < subscriptionGroupTable.size()) {
            int endIndex = Math.min(beginIndex + maxGroupNum, subscriptionGroupTable.size());

            ImmutableSortedMap<String, SubscriptionGroupConfig> sortedMap = ImmutableSortedMap.copyOf(subscriptionGroupTable);
            subGroupTable.putAll(sortedMap.subMap(sortedMap.keySet().asList().get(beginIndex),true,
                sortedMap.keySet().asList().get(endIndex - 1),true));
        }

        return subGroupTable;
    }

    public ConcurrentMap<String, ConcurrentMap<String, Integer>> getForbiddenTable() {
        return forbiddenTable;
    }

    public ConcurrentMap<String, ConcurrentMap<String, Integer>> subForbiddenTable(Set<String> groupSet) {
        if (MapUtils.isEmpty(forbiddenTable) || CollectionUtils.isEmpty(groupSet)) {
            return Maps.newConcurrentMap();
        }

        return forbiddenTable.entrySet().stream()
            .filter(e -> groupSet.contains(e.getKey()))
            .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public void setForbiddenTable(
        ConcurrentMap<String, ConcurrentMap<String, Integer>> forbiddenTable) {
        this.forbiddenTable = forbiddenTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public boolean loadDataVersion() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);
            if (jsonString != null) {
                SubscriptionGroupManager obj = RemotingSerializable.fromJson(jsonString, SubscriptionGroupManager.class);
                if (obj != null) {
                    this.dataVersion.assignNewOne(obj.dataVersion);
                    this.printLoadDataWhenFirstBoot(obj);
                    log.info("load subGroup dataVersion success,{},{}", fileName, obj.dataVersion);
                }
            }
            return true;
        } catch (Exception e) {
            log.error("load subGroup dataVersion failed" + fileName, e);
            return false;
        }
    }

    public void deleteSubscriptionGroupConfig(final String groupName) {
        SubscriptionGroupConfig old = removeSubscriptionGroupConfig(groupName);
        this.forbiddenTable.remove(groupName);
        if (old != null) {
            log.info("delete subscription group OK, subscription group:{}", old);
            updateDataVersion();
            this.persist();
        } else {
            log.warn("delete subscription group failed, subscription groupName: {} not exist", groupName);
        }
    }


    public void setSubscriptionGroupTable(ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable) {
        this.subscriptionGroupTable = subscriptionGroupTable;
    }

    public boolean containsSubscriptionGroup(String group) {
        if (StringUtils.isBlank(group)) {
            return false;
        }

        return subscriptionGroupTable.containsKey(group);
    }

    private Map<String, String> request(SubscriptionGroupConfig subscriptionGroupConfig) {
        return subscriptionGroupConfig.getAttributes() == null ? new HashMap<>() : subscriptionGroupConfig.getAttributes();
    }

    private Map<String, String> current(String groupName) {
        SubscriptionGroupConfig subscriptionGroupConfig = this.subscriptionGroupTable.get(groupName);
        if (subscriptionGroupConfig == null) {
            return new HashMap<>();
        } else {
            Map<String, String> attributes = subscriptionGroupConfig.getAttributes();
            if (attributes == null) {
                return new HashMap<>();
            } else {
                return attributes;
            }
        }
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion.assignNewOne(dataVersion);
    }

    public void updateDataVersion() {
        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
        dataVersion.nextVersion(stateMachineVersion);
    }

}
