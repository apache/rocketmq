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

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * 消费进度存储到Consumer本地
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class LocalFileOffsetStore implements OffsetStore {
    private final static Logger log = ClientLogger.getLog();
    public final static String LocalOffsetStoreDir = System.getProperty(
        "rocketmq.client.localOffsetStoreDir", //
        System.getProperty("user.home") + File.separator + ".rocketmq_offsets");

    private final MQClientFactory mQClientFactory;
    private final String groupName;
    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>();

    // 本地Offset存储路径
    private final String storePath;


    public LocalFileOffsetStore(MQClientFactory mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
        this.storePath = LocalOffsetStoreDir + File.separator + //
                this.mQClientFactory.getClientId() + File.separator + //
                this.groupName + File.separator + //
                "offsets.json";
    }


    private OffsetSerializeWrapper readLocalOffset() {
        String content = MixAll.file2String(this.storePath);
        if (content != null) {
            OffsetSerializeWrapper offsetSerializeWrapper =
                    OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            return offsetSerializeWrapper;
        }

        return null;
    }


    @Override
    public void load() {
        OffsetSerializeWrapper offsetSerializeWrapper = this.readLocalOffset();
        if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
            offsetTable.putAll(offsetSerializeWrapper.getOffsetTable());

            for (MessageQueue mq : offsetSerializeWrapper.getOffsetTable().keySet()) {
                AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                log.info("load consumer's offset, {} {} {}",//
                    this.groupName,//
                    mq,//
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
                OffsetSerializeWrapper offsetSerializeWrapper = this.readLocalOffset();
                if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
                    offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                }

                if (offset != null) {
                    this.updateOffset(mq, offset.get(), false);
                    return offset.get();
                }
            }
        }

        return -1;
    }


    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        OffsetSerializeWrapper offsetSerializeWrapper = new OffsetSerializeWrapper();
        for (MessageQueue mq : this.offsetTable.keySet()) {
            if (mqs.contains(mq)) {
                AtomicLong offset = this.offsetTable.get(mq);
                offsetSerializeWrapper.getOffsetTable().put(mq, offset);
            }
        }

        String jsonString = offsetSerializeWrapper.toJson(true);
        if (jsonString != null) {
            try {
                MixAll.string2File(jsonString, this.storePath);
            }
            catch (IOException e) {
                log.error("persistAll consumer offset Exception, " + this.storePath, e);
            }
        }
    }


    @Override
    public void persist(MessageQueue mq) {
    }
}
