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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * Local storage implementation
 */
public class LocalFileOffsetStore implements OffsetStore {
    public final static String LOCAL_OFFSET_STORE_DIR = System.getProperty(
        "rocketmq.client.localOffsetStoreDir",
        System.getProperty("user.home") + File.separator + ".rocketmq_offsets");
    private final static Logger log = LoggerFactory.getLogger(LocalFileOffsetStore.class);
    private final MQClientInstance mqClientFactory;
    private final String groupName;
    private final String storePath;
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable =
        new ConcurrentHashMap<>();

    public LocalFileOffsetStore(MQClientInstance mqClientFactory, String groupName) {
        this.mqClientFactory = mqClientFactory;
        this.groupName = groupName;
        this.storePath = LOCAL_OFFSET_STORE_DIR + File.separator +
            this.mqClientFactory.getClientId() + File.separator +
            this.groupName + File.separator +
            "offsets.json";
    }

    @Override
    public void load() throws MQClientException {
        OffsetSerializeWrapper offsetSerializeWrapper = this.readLocalOffset();
        if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
            offsetTable.putAll(offsetSerializeWrapper.getOffsetTable());

            for (Entry<MessageQueue, AtomicLong> mqEntry : offsetSerializeWrapper.getOffsetTable().entrySet()) {
                AtomicLong offset = mqEntry.getValue();
                log.info("load consumer's offset, {} {} {}",
                        this.groupName,
                        mqEntry.getKey(),
                        offset.get());
            }
        }
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
                } else {
                    offsetOld.set(offset);
                }
            }
        }
    }

    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                case READ_FROM_STORE: {
                    OffsetSerializeWrapper offsetSerializeWrapper;
                    try {
                        offsetSerializeWrapper = this.readLocalOffset();
                    } catch (MQClientException e) {
                        return -1;
                    }
                    if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
                        AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                        if (offset != null) {
                            this.updateOffset(mq, offset.get(), false);
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
        if (null == mqs || mqs.isEmpty())
            return;

        OffsetSerializeWrapper offsetSerializeWrapper = new OffsetSerializeWrapper();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            if (mqs.contains(entry.getKey())) {
                AtomicLong offset = entry.getValue();
                offsetSerializeWrapper.getOffsetTable().put(entry.getKey(), offset);
            }
        }

        String jsonString = offsetSerializeWrapper.toJson(true);
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
    public void removeOffset(MessageQueue mq) {

    }

    @Override
    public void updateConsumeOffsetToBroker(final MessageQueue mq, final long offset, final boolean isOneway)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<>(this.offsetTable.size(), 1);
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());

        }
        return cloneOffsetTable;
    }

    private OffsetSerializeWrapper readLocalOffset() throws MQClientException {
        String content = null;
        try {
            content = MixAll.file2String(this.storePath);
        } catch (IOException e) {
            log.warn("Load local offset store file exception", e);
        }
        if (null == content || content.length() == 0) {
            return this.readLocalOffsetBak();
        } else {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                offsetSerializeWrapper =
                    OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception, and try to correct", e);
                return this.readLocalOffsetBak();
            }

            return offsetSerializeWrapper;
        }
    }

    private OffsetSerializeWrapper readLocalOffsetBak() throws MQClientException {
        String content = null;
        try {
            content = MixAll.file2String(this.storePath + ".bak");
        } catch (IOException e) {
            log.warn("Load local offset store bak file exception", e);
        }
        if (content != null && content.length() > 0) {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                offsetSerializeWrapper =
                    OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception", e);
                throw new MQClientException("readLocalOffset Exception, maybe fastjson version too low"
                    + FAQUrl.suggestTodo(FAQUrl.LOAD_JSON_EXCEPTION),
                    e);
            }
            return offsetSerializeWrapper;
        }

        return null;
    }
}
