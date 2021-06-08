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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Local storage implementation
 */
public class LocalFileStageOffsetStore implements StageOffsetStore {
    public final static String LOCAL_OFFSET_STORE_DIR = System.getProperty(
        "rocketmq.client.localOffsetStoreDir",
        System.getProperty("user.home") + File.separator + ".rocketmq_stage_offsets");
    private final static InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String groupName;
    private final String storePath;
    private ConcurrentMap<MessageQueue, AtomicInteger> offsetTable =
        new ConcurrentHashMap<MessageQueue, AtomicInteger>();

    public LocalFileStageOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
        this.storePath = LOCAL_OFFSET_STORE_DIR + File.separator +
            this.mQClientFactory.getClientId() + File.separator +
            this.groupName + File.separator +
            "stageOffsets.json";
    }

    @Override
    public void load() throws MQClientException {
        StageOffsetSerializeWrapper stageOffsetSerializeWrapper = this.readLocalOffset();
        if (stageOffsetSerializeWrapper != null && stageOffsetSerializeWrapper.getOffsetTable() != null) {
            offsetTable.putAll(stageOffsetSerializeWrapper.getOffsetTable());

            for (MessageQueue mq : stageOffsetSerializeWrapper.getOffsetTable().keySet()) {
                AtomicInteger offset = stageOffsetSerializeWrapper.getOffsetTable().get(mq);
                log.info("load consumer's offset, {} {} {}",
                    this.groupName,
                    mq,
                    offset.get());
            }
        }
    }

    @Override
    public void updateStageOffset(MessageQueue mq, String strategyId, int stageOffset, boolean increaseOnly) {
        if (mq != null) {
            AtomicInteger offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicInteger(stageOffset));
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, stageOffset);
                } else {
                    offsetOld.set(stageOffset);
                }
            }
        }
    }

    @Override
    public int readStageOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    AtomicInteger offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                case READ_FROM_STORE: {
                    StageOffsetSerializeWrapper stageOffsetSerializeWrapper;
                    try {
                        stageOffsetSerializeWrapper = this.readLocalOffset();
                    } catch (MQClientException e) {
                        return -1;
                    }
                    if (stageOffsetSerializeWrapper != null && stageOffsetSerializeWrapper.getOffsetTable() != null) {
                        AtomicInteger offset = stageOffsetSerializeWrapper.getOffsetTable().get(mq);
                        if (offset != null) {
                            this.updateStageOffset(mq, strategyId, offset.get(), false);
                            return offset.get();
                        }
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty()) {
            return;
        }

        StageOffsetSerializeWrapper stageOffsetSerializeWrapper = new StageOffsetSerializeWrapper();
        for (Map.Entry<MessageQueue, AtomicInteger> entry : this.offsetTable.entrySet()) {
            if (mqs.contains(entry.getKey())) {
                AtomicInteger offset = entry.getValue();
                stageOffsetSerializeWrapper.getOffsetTable().put(entry.getKey(), offset);
            }
        }

        String jsonString = stageOffsetSerializeWrapper.toJson(true);
        if (jsonString != null) {
            try {
                MixAll.string2File(jsonString, this.storePath);
            } catch (IOException e) {
                log.error("persistAll consumer offset Exception, " + this.storePath, e);
            }
        }
    }

    @Override
    public void persist(MessageQueue mq) {
    }

    @Override
    public void removeStageOffset(MessageQueue mq) {

    }

    @Override
    public void updateConsumeStageOffsetToBroker(final MessageQueue mq, final int stageOffset, final boolean isOneway)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

    }

    @Override
    public Map<MessageQueue, Integer> cloneStageOffsetTable(String topic) {
        Map<MessageQueue, Integer> cloneOffsetTable = new HashMap<MessageQueue, Integer>();
        for (Map.Entry<MessageQueue, AtomicInteger> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());

        }
        return cloneOffsetTable;
    }

    private StageOffsetSerializeWrapper readLocalOffset() throws MQClientException {
        String content = null;
        try {
            content = MixAll.file2String(this.storePath);
        } catch (IOException e) {
            log.warn("Load local offset store file exception", e);
        }
        if (null == content || content.length() == 0) {
            return this.readLocalOffsetBak();
        } else {
            StageOffsetSerializeWrapper stageOffsetSerializeWrapper = null;
            try {
                stageOffsetSerializeWrapper =
                    StageOffsetSerializeWrapper.fromJson(content, StageOffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception, and try to correct", e);
                return this.readLocalOffsetBak();
            }

            return stageOffsetSerializeWrapper;
        }
    }

    private StageOffsetSerializeWrapper readLocalOffsetBak() throws MQClientException {
        String content = null;
        try {
            content = MixAll.file2String(this.storePath + ".bak");
        } catch (IOException e) {
            log.warn("Load local offset store bak file exception", e);
        }
        if (content != null && content.length() > 0) {
            StageOffsetSerializeWrapper stageOffsetSerializeWrapper = null;
            try {
                stageOffsetSerializeWrapper =
                    StageOffsetSerializeWrapper.fromJson(content, StageOffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception", e);
                throw new MQClientException("readLocalOffset Exception, maybe fastjson version too low"
                    + FAQUrl.suggestTodo(FAQUrl.LOAD_JSON_EXCEPTION),
                    e);
            }
            return stageOffsetSerializeWrapper;
        }

        return null;
    }
}
