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
package org.apache.rocketmq.broker.config.v2;

import com.alibaba.fastjson2.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.common.constant.PermName;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

/**
 * Key layout: [table-prefix, 1 byte][table-id, 2 bytes][record-type-prefix, 1 byte][topic-len, 2 bytes][topic-bytes]
 * Value layout: [serialization-type, 1 byte][topic-config-bytes]
 */
public class TopicConfigManagerV2 extends TopicConfigManager {
    private final ConfigStorage configStorage;

    public TopicConfigManagerV2(BrokerController brokerController, ConfigStorage configStorage) {
        super(brokerController);
        this.configStorage = configStorage;
    }

    @Override
    public boolean load() {
        return loadDataVersion() && loadTopicConfig();
    }

    public boolean loadDataVersion() {
        try {
            ConfigHelper.loadDataVersion(configStorage, TableId.TOPIC)
                .ifPresent(buf -> ConfigHelper.onDataVersionLoad(buf, dataVersion));
        } catch (RocksDBException e) {
            log.error("Failed to load data version of topic", e);
            return false;
        }
        return true;
    }

    private boolean loadTopicConfig() {
        int keyLen = 1 /* table-prefix */ + 2 /* table-id */ + 1 /* record-type-prefix */;
        ByteBuf beginKey = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(keyLen);
        beginKey.writeByte(TablePrefix.TABLE.getValue());
        beginKey.writeShort(TableId.TOPIC.getValue());
        beginKey.writeByte(RecordPrefix.DATA.getValue());

        ByteBuf endKey = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(keyLen);
        endKey.writeByte(TablePrefix.TABLE.getValue());
        endKey.writeShort(TableId.TOPIC.getValue());
        endKey.writeByte(RecordPrefix.DATA.getValue() + 1);

        try (RocksIterator iterator = configStorage.iterate(beginKey.nioBuffer(), endKey.nioBuffer())) {
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                byte[] value = iterator.value();
                TopicConfig topicConfig = parseTopicConfig(key, value);
                if (null != topicConfig) {
                    super.putTopicConfig(topicConfig);
                }
                iterator.next();
            }
        } finally {
            beginKey.release();
            endKey.release();
        }
        return true;
    }

    /**
     * Key layout: [table-prefix, 1 byte][table-id, 2 bytes][record-type-prefix, 1 byte][topic-len, 2 bytes][topic-bytes]
     * Value layout: [serialization-type, 1 byte][topic-config-bytes]
     *
     * @param key   Topic config key representation in RocksDB
     * @param value Topic config value representation in RocksDB
     * @return decoded topic config
     */
    private TopicConfig parseTopicConfig(byte[] key, byte[] value) {
        ByteBuf keyBuf = Unpooled.wrappedBuffer(key);
        ByteBuf valueBuf = Unpooled.wrappedBuffer(value);
        try {
            // Skip table-prefix, table-id, record-type-prefix
            keyBuf.readerIndex(4);
            short topicLen = keyBuf.readShort();
            assert topicLen == keyBuf.readableBytes();
            CharSequence topic = keyBuf.readCharSequence(topicLen, StandardCharsets.UTF_8);
            assert null != topic;

            byte serializationType = valueBuf.readByte();
            if (SerializationType.JSON == SerializationType.valueOf(serializationType)) {
                CharSequence json = valueBuf.readCharSequence(valueBuf.readableBytes(), StandardCharsets.UTF_8);
                TopicConfig topicConfig = JSON.parseObject(json.toString(), TopicConfig.class);
                assert topicConfig != null;
                assert topic.equals(topicConfig.getTopicName());
                return topicConfig;
            }
        } finally {
            keyBuf.release();
            valueBuf.release();
        }

        return null;
    }

    @Override
    public synchronized void persist() {
        try {
            configStorage.flushWAL();
        } catch (RocksDBException e) {
            log.error("Failed to flush WAL", e);
        }
    }

    @Override
    public TopicConfig selectTopicConfig(final String topic) {
        if (MixAll.isLmq(topic)) {
            return simpleLmqTopicConfig(topic);
        }
        return super.selectTopicConfig(topic);
    }

    @Override
    public void updateTopicConfig(final TopicConfig topicConfig) {
        if (topicConfig == null || MixAll.isLmq(topicConfig.getTopicName())) {
            return;
        }
        super.updateSingleTopicConfigWithoutPersist(topicConfig);

        ByteBuf keyBuf = ConfigHelper.keyBufOf(TableId.TOPIC, topicConfig.getTopicName());
        ByteBuf valueBuf = ConfigHelper.valueBufOf(topicConfig, SerializationType.JSON);
        try (WriteBatch writeBatch = new WriteBatch()) {
            writeBatch.put(keyBuf.nioBuffer(), valueBuf.nioBuffer());
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            ConfigHelper.stampDataVersion(writeBatch, TableId.TOPIC, dataVersion, stateMachineVersion);
            configStorage.write(writeBatch);
            // fdatasync on core metadata change
            this.persist();
        } catch (RocksDBException e) {
            log.error("Failed to update topic config", e);
        } finally {
            keyBuf.release();
            valueBuf.release();
        }
    }

    @Override
    protected TopicConfig removeTopicConfig(String topicName) {
        ByteBuf keyBuf = ConfigHelper.keyBufOf(TableId.TOPIC, topicName);
        try (WriteBatch writeBatch = new WriteBatch()) {
            writeBatch.delete(keyBuf.nioBuffer());
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            ConfigHelper.stampDataVersion(writeBatch, TableId.TOPIC, dataVersion, stateMachineVersion);
            configStorage.write(writeBatch);
        } catch (RocksDBException e) {
            log.error("Failed to delete topic config by topicName={}", topicName, e);
        } finally {
            keyBuf.release();
        }
        return super.removeTopicConfig(topicName);
    }

    @Override
    public boolean containsTopic(String topic) {
        if (MixAll.isLmq(topic)) {
            return true;
        }
        return super.containsTopic(topic);
    }

    private TopicConfig simpleLmqTopicConfig(String topic) {
        return new TopicConfig(topic, 1, 1, PermName.PERM_READ | PermName.PERM_WRITE);
    }
}
