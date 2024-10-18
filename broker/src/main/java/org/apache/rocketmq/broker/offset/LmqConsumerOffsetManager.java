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
package org.apache.rocketmq.broker.offset;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class LmqConsumerOffsetManager extends ConsumerOffsetManager {
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private ConcurrentHashMap<String, Long> lmqOffsetTable = new ConcurrentHashMap<>(512);

    public LmqConsumerOffsetManager() {

    }

    public LmqConsumerOffsetManager(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public boolean load() {
        String lmqFilePath = this.configFilePath();
        String normalFilePath = super.configFilePath();
        String targetFilePath = null;
        try {
            targetFilePath = MixAll.loadLatestFile(normalFilePath, lmqFilePath);
            String jsonString = MixAll.file2String(targetFilePath);
            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            } else {
                if (StringUtils.equals(normalFilePath, targetFilePath)) {
                    //should load the old lmq offset
                    String lmqJsonString = MixAll.file2String(lmqFilePath);
                    this.decode(lmqJsonString);
                    this.decodeFromLmq(jsonString);
                } else {
                    this.decode(jsonString);
                }
                LOG.info("load " + targetFilePath + " success");
                return true;
            }
        } catch (Exception e) {
            LOG.error("load " + targetFilePath + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    @Override
    public boolean loadBak() {
        String lmqBakFilePath = this.configFilePath() + ".bak";
        String normalBakFilePath = super.configFilePath() + ".bak";
        String targetBakFilePath = null;
        try {
            targetBakFilePath = MixAll.loadLatestFile(normalBakFilePath, lmqBakFilePath);
            String jsonString = MixAll.file2String(targetBakFilePath);
            if (jsonString != null && jsonString.length() > 0) {
                if (StringUtils.equals(normalBakFilePath, targetBakFilePath)) {
                    String lmqJsonString = MixAll.file2String(lmqBakFilePath);
                    this.decode(lmqJsonString);
                    this.decodeFromLmq(jsonString);
                } else {
                    this.decode(jsonString);
                }
                LOG.info("load " + targetBakFilePath + " OK");
                return true;
            }
        } catch (Exception e) {
            LOG.error("load " + targetBakFilePath + " Failed", e);
            return false;
        }
        return true;
    }

    @Override
    public long queryOffset(final String group, final String topic, final int queueId) {
        if (!MixAll.isLmq(group)) {
            return super.queryOffset(group, topic, queueId);
        }
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        Long offset = lmqOffsetTable.get(key);
        if (offset != null) {
            return offset;
        }
        return -1;
    }

    @Override
    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        if (!MixAll.isLmq(group)) {
            return super.queryOffset(group, topic);
        }
        Map<Integer, Long> map = new HashMap<>();
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        Long offset = lmqOffsetTable.get(key);
        if (offset != null) {
            map.put(0, offset);
        }
        return map;
    }

    @Override
    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
        final long offset) {
        if (!MixAll.isLmq(group)) {
            super.commitOffset(clientHost, group, topic, queueId, offset);
            return;
        }
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        lmqOffsetTable.put(key, offset);
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getLmqConsumerOffsetPath(brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            LmqConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, LmqConsumerOffsetManager.class);
            if (obj != null) {
                super.setOffsetTable(obj.getOffsetTable());
                this.lmqOffsetTable = obj.lmqOffsetTable;
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentHashMap<String, Long> getLmqOffsetTable() {
        return lmqOffsetTable;
    }

    public void setLmqOffsetTable(ConcurrentHashMap<String, Long> lmqOffsetTable) {
        this.lmqOffsetTable = lmqOffsetTable;
    }

    @Override
    public void removeOffset(String group) {
        if (!MixAll.isLmq(group)) {
            super.removeOffset(group);
            return;
        }
        Iterator<Map.Entry<String, Long>> it = this.lmqOffsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> next = it.next();
            String topicAtGroup = next.getKey();
            if (topicAtGroup.contains(group)) {
                String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                if (arrays.length == 2 && group.equals(arrays[1])) {
                    it.remove();
                    removeConsumerOffset(topicAtGroup);
                    LOG.warn("clean lmq group offset {}", topicAtGroup);
                }
            }
        }
    }
}
