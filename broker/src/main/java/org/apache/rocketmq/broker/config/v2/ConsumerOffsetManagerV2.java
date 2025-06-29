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

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.store.MessageStore;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

/**
 * <p>
 * Layout of consumer offset key:
 * [table-prefix, 1 byte][table-id, 2 bytes][record-prefix, 1 byte][group-len, 2 bytes][group bytes][CTRL_1, 1 byte]
 * [topic-len, 2 bytes][topic bytes][CTRL_1, 1 byte][queue-id, 4 bytes]
 * </p>
 *
 * <p>
 * Layout of consumer offset value: [offset, 8 bytes]
 * </p>
 */
public class ConsumerOffsetManagerV2 extends ConsumerOffsetManager {

    private final ConfigStorage configStorage;

    public ConsumerOffsetManagerV2(BrokerController brokerController, ConfigStorage configStorage) {
        super(brokerController);
        this.configStorage = configStorage;
    }

    @Override
    protected void removeConsumerOffset(String topicAtGroup) {
        if (!MixAll.isLmq(topicAtGroup)) {
            super.removeConsumerOffset(topicAtGroup);
        }

        String[] topicGroup = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
        if (topicGroup.length != 2) {
            LOG.error("Invalid topic group: {}", topicAtGroup);
            return;
        }

        byte[] topicBytes = topicGroup[0].getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = topicGroup[1].getBytes(StandardCharsets.UTF_8);

        int keyLen = 1 /* table-prefix */ + Short.BYTES /* table-id */ + 1 /* record-prefix */
            + Short.BYTES /* group-len */ + groupBytes.length + 1 /* CTRL_1 */
            + Short.BYTES + topicBytes.length + 1;
        // [table-prefix, 1 byte][table-id, 2 bytes][record-prefix, 1 byte][group-len, 2 bytes][group-bytes][CTRL_1, 1 byte]
        // [topic-len, 2 bytes][topic-bytes][CTRL_1]
        ByteBuf beginKey = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(keyLen);
        beginKey.writeByte(TablePrefix.TABLE.getValue());
        beginKey.writeShort(TableId.CONSUMER_OFFSET.getValue());
        beginKey.writeByte(RecordPrefix.DATA.getValue());
        beginKey.writeShort(groupBytes.length);
        beginKey.writeBytes(groupBytes);
        beginKey.writeByte(AbstractRocksDBStorage.CTRL_1);
        beginKey.writeShort(topicBytes.length);
        beginKey.writeBytes(topicBytes);
        beginKey.writeByte(AbstractRocksDBStorage.CTRL_1);

        ByteBuf endKey = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(keyLen);
        endKey.writeByte(TablePrefix.TABLE.getValue());
        endKey.writeShort(TableId.CONSUMER_OFFSET.getValue());
        endKey.writeByte(RecordPrefix.DATA.getValue());
        endKey.writeShort(groupBytes.length);
        endKey.writeBytes(groupBytes);
        endKey.writeByte(AbstractRocksDBStorage.CTRL_1);
        endKey.writeShort(topicBytes.length);
        endKey.writeBytes(topicBytes);
        endKey.writeByte(AbstractRocksDBStorage.CTRL_2);

        try (WriteBatch writeBatch = new WriteBatch()) {
            // TODO: we have to make a copy here as WriteBatch lacks ByteBuffer API here
            writeBatch.deleteRange(ConfigHelper.readBytes(beginKey), ConfigHelper.readBytes(endKey));
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            ConfigHelper.stampDataVersion(writeBatch, TableId.CONSUMER_OFFSET, dataVersion, stateMachineVersion);
            configStorage.write(writeBatch);
        } catch (RocksDBException e) {
            LOG.error("Failed to removeConsumerOffset, topicAtGroup={}", topicAtGroup, e);
        } finally {
            beginKey.release();
            endKey.release();
        }
    }

    @Override
    public void removeOffset(String group) {
        if (!MixAll.isLmq(group)) {
            super.removeOffset(group);
        }

        byte[] groupBytes = group.getBytes(StandardCharsets.UTF_8);
        int keyLen = 1 /* table-prefix */ + Short.BYTES /* table-id */ + 1 /* record-prefix */
            + Short.BYTES /* group-len */ + groupBytes.length + 1 /* CTRL_1 */;

        // [table-prefix, 1 byte][table-id, 2 bytes][record-prefix, 1 byte][group-len, 2 bytes][group bytes][CTRL_1, 1 byte]
        ByteBuf consumerOffsetBeginKey = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(keyLen);
        consumerOffsetBeginKey.writeByte(TablePrefix.TABLE.getValue());
        consumerOffsetBeginKey.writeShort(TableId.CONSUMER_OFFSET.getValue());
        consumerOffsetBeginKey.writeByte(RecordPrefix.DATA.getValue());
        consumerOffsetBeginKey.writeShort(groupBytes.length);
        consumerOffsetBeginKey.writeBytes(groupBytes);
        consumerOffsetBeginKey.writeByte(AbstractRocksDBStorage.CTRL_1);

        ByteBuf consumerOffsetEndKey = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(keyLen);
        consumerOffsetEndKey.writeByte(TablePrefix.TABLE.getValue());
        consumerOffsetEndKey.writeShort(TableId.CONSUMER_OFFSET.getValue());
        consumerOffsetEndKey.writeByte(RecordPrefix.DATA.getValue());
        consumerOffsetEndKey.writeShort(groupBytes.length);
        consumerOffsetEndKey.writeBytes(groupBytes);
        consumerOffsetEndKey.writeByte(AbstractRocksDBStorage.CTRL_2);

        // [table-prefix, 1 byte][table-id, 2 bytes][record-prefix, 1 byte][group-len, 2 bytes][group bytes][CTRL_1, 1 byte]
        ByteBuf pullOffsetBeginKey = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(keyLen);
        pullOffsetBeginKey.writeByte(TablePrefix.TABLE.getValue());
        pullOffsetBeginKey.writeShort(TableId.PULL_OFFSET.getValue());
        pullOffsetBeginKey.writeByte(RecordPrefix.DATA.getValue());
        pullOffsetBeginKey.writeShort(groupBytes.length);
        pullOffsetBeginKey.writeBytes(groupBytes);
        pullOffsetBeginKey.writeByte(AbstractRocksDBStorage.CTRL_1);

        ByteBuf pullOffsetEndKey = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(keyLen);
        pullOffsetEndKey.writeByte(TablePrefix.TABLE.getValue());
        pullOffsetEndKey.writeShort(TableId.PULL_OFFSET.getValue());
        pullOffsetEndKey.writeByte(RecordPrefix.DATA.getValue());
        pullOffsetEndKey.writeShort(groupBytes.length);
        pullOffsetEndKey.writeBytes(groupBytes);
        pullOffsetEndKey.writeByte(AbstractRocksDBStorage.CTRL_2);
        try (WriteBatch writeBatch = new WriteBatch()) {
            // TODO: we have to make a copy here as WriteBatch lacks ByteBuffer API here
            writeBatch.deleteRange(ConfigHelper.readBytes(consumerOffsetBeginKey), ConfigHelper.readBytes(consumerOffsetEndKey));
            writeBatch.deleteRange(ConfigHelper.readBytes(pullOffsetBeginKey), ConfigHelper.readBytes(pullOffsetEndKey));
            MessageStore messageStore = brokerController.getMessageStore();
            long stateMachineVersion = messageStore != null ? messageStore.getStateMachineVersion() : 0;
            ConfigHelper.stampDataVersion(writeBatch, TableId.CONSUMER_OFFSET, dataVersion, stateMachineVersion);
            ConfigHelper.stampDataVersion(writeBatch, TableId.PULL_OFFSET, dataVersion, stateMachineVersion);
            configStorage.write(writeBatch);
        } catch (RocksDBException e) {
            LOG.error("Failed to consumer offsets by group={}", group, e);
        } finally {
            consumerOffsetBeginKey.release();
            consumerOffsetEndKey.release();
            pullOffsetBeginKey.release();
            pullOffsetEndKey.release();
        }
    }

    /**
     * <p>
     * Layout of consumer offset key:
     * [table-prefix, 1 byte][table-id, 2 bytes][record-prefix, 1 byte][group-len, 2 bytes][group bytes][CTRL_1, 1 byte]
     * [topic-len, 2 bytes][topic bytes][CTRL_1, 1 byte][queue-id, 4 bytes]
     * </p>
     *
     * <p>
     * Layout of consumer offset value:
     * [offset, 8 bytes]
     * </p>
     *
     * @param clientHost The client that submits consumer offsets
     * @param group      Group name
     * @param topic      Topic name
     * @param queueId    Queue ID
     * @param offset     Consumer offset of the specified queue
     */
    @Override
    public void commitOffset(String clientHost, String group, String topic, int queueId, long offset) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;

        // We maintain a copy of classic consumer offset table in memory as they take very limited memory footprint.
        // For LMQ offsets, given the volume and number of these type of records, they are maintained in RocksDB
        // directly. Frequently used LMQ consumer offsets should reside either in block-cache or MemTable, so read/write
        // should be blazingly fast.
        if (!MixAll.isLmq(topic)) {
            if (offsetTable.containsKey(key)) {
                offsetTable.get(key).put(queueId, offset);
            } else {
                ConcurrentMap<Integer, Long> map = new ConcurrentHashMap<>();
                ConcurrentMap<Integer, Long> prev = offsetTable.putIfAbsent(key, map);
                if (null != prev) {
                    map = prev;
                }
                map.put(queueId, offset);
            }
        }

        ByteBuf keyBuf = keyOfConsumerOffset(group, topic, queueId);
        ByteBuf valueBuf = ConfigStorage.POOLED_ALLOCATOR.buffer(Long.BYTES);
        try (WriteBatch writeBatch = new WriteBatch()) {
            valueBuf.writeLong(offset);
            writeBatch.put(keyBuf.nioBuffer(), valueBuf.nioBuffer());
            MessageStore messageStore = brokerController.getMessageStore();
            long stateMachineVersion = messageStore != null ? messageStore.getStateMachineVersion() : 0;
            ConfigHelper.stampDataVersion(writeBatch, TableId.CONSUMER_OFFSET, dataVersion, stateMachineVersion);
            configStorage.write(writeBatch);
        } catch (RocksDBException e) {
            LOG.error("Failed to commit consumer offset", e);
        } finally {
            keyBuf.release();
            valueBuf.release();
        }
    }

    private ByteBuf keyOfConsumerOffset(String group, String topic, int queueId) {
        byte[] groupBytes = group.getBytes(StandardCharsets.UTF_8);
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int keyLen = 1 /*table prefix*/ + Short.BYTES /*table-id*/ + 1 /*record-prefix*/
            + Short.BYTES /*group-len*/ + groupBytes.length + 1 /*CTRL_1*/
            + 2 /*topic-len*/ + topicBytes.length + 1 /* CTRL_1*/
            + Integer.BYTES /*queue-id*/;
        ByteBuf keyBuf = ConfigStorage.POOLED_ALLOCATOR.buffer(keyLen);
        keyBuf.writeByte(TablePrefix.TABLE.getValue());
        keyBuf.writeShort(TableId.CONSUMER_OFFSET.getValue());
        keyBuf.writeByte(RecordPrefix.DATA.getValue());
        keyBuf.writeShort(groupBytes.length);
        keyBuf.writeBytes(groupBytes);
        keyBuf.writeByte(AbstractRocksDBStorage.CTRL_1);
        keyBuf.writeShort(topicBytes.length);
        keyBuf.writeBytes(topicBytes);
        keyBuf.writeByte(AbstractRocksDBStorage.CTRL_1);
        keyBuf.writeInt(queueId);
        return keyBuf;
    }

    private ByteBuf keyOfPullOffset(String group, String topic, int queueId) {
        byte[] groupBytes = group.getBytes(StandardCharsets.UTF_8);
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int keyLen = 1 /*table prefix*/ + Short.BYTES /*table-id*/ + 1 /*record-prefix*/
            + Short.BYTES /*group-len*/ + groupBytes.length + 1 /*CTRL_1*/
            + 2 /*topic-len*/ + topicBytes.length + 1 /* CTRL_1*/
            + Integer.BYTES /*queue-id*/;
        ByteBuf keyBuf = ConfigStorage.POOLED_ALLOCATOR.buffer(keyLen);
        keyBuf.writeByte(TablePrefix.TABLE.getValue());
        keyBuf.writeShort(TableId.PULL_OFFSET.getValue());
        keyBuf.writeByte(RecordPrefix.DATA.getValue());
        keyBuf.writeShort(groupBytes.length);
        keyBuf.writeBytes(groupBytes);
        keyBuf.writeByte(AbstractRocksDBStorage.CTRL_1);
        keyBuf.writeShort(topicBytes.length);
        keyBuf.writeBytes(topicBytes);
        keyBuf.writeByte(AbstractRocksDBStorage.CTRL_1);
        keyBuf.writeInt(queueId);
        return keyBuf;
    }

    @Override
    public boolean load() {
        return loadDataVersion() && loadConsumerOffsets();
    }

    @Override
    public synchronized void persist() {
        try {
            configStorage.flushWAL();
        } catch (RocksDBException e) {
            LOG.error("Failed to flush RocksDB config instance WAL", e);
        }
    }

    /**
     * <p>
     * Layout of data version key:
     * [table-prefix, 1 byte][table-id, 2 byte][record-prefix, 1 byte][data-version-bytes]
     * </p>
     *
     * <p>
     * Layout of data version value:
     * [state-machine-version, 8 bytes][timestamp, 8 bytes][sequence counter, 8 bytes]
     * </p>
     */
    public boolean loadDataVersion() {
        try {
            ConfigHelper.loadDataVersion(configStorage, TableId.CONSUMER_OFFSET)
                .ifPresent(buf -> ConfigHelper.onDataVersionLoad(buf, dataVersion));
        } catch (RocksDBException e) {
            LOG.error("Failed to load RocksDB config", e);
            return false;
        }
        return true;
    }

    private boolean loadConsumerOffsets() {
        // [table-prefix, 1 byte][table-id, 2 bytes][record-prefix, 1 byte]
        ByteBuf beginKeyBuf = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(4);
        beginKeyBuf.writeByte(TablePrefix.TABLE.getValue());
        beginKeyBuf.writeShort(TableId.CONSUMER_OFFSET.getValue());
        beginKeyBuf.writeByte(RecordPrefix.DATA.getValue());

        ByteBuf endKeyBuf = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(4);
        endKeyBuf.writeByte(TablePrefix.TABLE.getValue());
        endKeyBuf.writeShort(TableId.CONSUMER_OFFSET.getValue());
        endKeyBuf.writeByte(RecordPrefix.DATA.getValue() + 1);

        try (RocksIterator iterator = configStorage.iterate(beginKeyBuf.nioBuffer(), endKeyBuf.nioBuffer())) {
            int keyCapacity = 256;
            // We may iterate millions of LMQ consumer offsets here, use direct byte buffers here to avoid memory
            // fragment
            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(keyCapacity);
            ByteBuffer valueBuffer = ByteBuffer.allocateDirect(Long.BYTES);
            while (iterator.isValid()) {
                keyBuffer.clear();
                valueBuffer.clear();

                int len = iterator.key(keyBuffer);
                if (len > keyCapacity) {
                    keyCapacity = len;
                    PlatformDependent.freeDirectBuffer(keyBuffer);
                    // Reserve more space for key
                    keyBuffer = ByteBuffer.allocateDirect(keyCapacity);
                    continue;
                }
                len = iterator.value(valueBuffer);
                assert len == Long.BYTES;

                // skip table-prefix, table-id, record-prefix
                keyBuffer.position(1 + 2 + 1);
                short groupLen = keyBuffer.getShort();
                byte[] groupBytes = new byte[groupLen];
                keyBuffer.get(groupBytes);
                byte ctrl = keyBuffer.get();
                assert ctrl == AbstractRocksDBStorage.CTRL_1;

                short topicLen = keyBuffer.getShort();
                byte[] topicBytes = new byte[topicLen];
                keyBuffer.get(topicBytes);
                String topic = new String(topicBytes, StandardCharsets.UTF_8);
                ctrl = keyBuffer.get();
                assert ctrl == AbstractRocksDBStorage.CTRL_1;

                int queueId = keyBuffer.getInt();

                long offset = valueBuffer.getLong();

                if (!MixAll.isLmq(topic)) {
                    String group = new String(groupBytes, StandardCharsets.UTF_8);
                    onConsumerOffsetRecordLoad(topic, group, queueId, offset);
                }
                iterator.next();
            }
            PlatformDependent.freeDirectBuffer(keyBuffer);
            PlatformDependent.freeDirectBuffer(valueBuffer);
        } finally {
            beginKeyBuf.release();
            endKeyBuf.release();
        }
        return true;
    }

    private void onConsumerOffsetRecordLoad(String topic, String group, int queueId, long offset) {
        if (MixAll.isLmq(topic)) {
            return;
        }
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        if (!offsetTable.containsKey(key)) {
            ConcurrentMap<Integer, Long> map = new ConcurrentHashMap<>();
            offsetTable.putIfAbsent(key, map);
        }
        offsetTable.get(key).put(queueId, offset);
    }

    @Override
    public long queryOffset(String group, String topic, int queueId) {
        if (!MixAll.isLmq(topic)) {
            return super.queryOffset(group, topic, queueId);
        }

        ByteBuf keyBuf = keyOfConsumerOffset(group, topic, queueId);
        try {
            byte[] slice = configStorage.get(keyBuf.nioBuffer());
            if (null == slice) {
                return -1;
            }
            assert slice.length == Long.BYTES;
            return ByteBuffer.wrap(slice).getLong();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            keyBuf.release();
        }
    }

    @Override
    public void commitPullOffset(String clientHost, String group, String topic, int queueId, long offset) {
        if (!MixAll.isLmq(topic)) {
            super.commitPullOffset(clientHost, group, topic, queueId, offset);
        }

        ByteBuf keyBuf = keyOfPullOffset(group, topic, queueId);
        ByteBuf valueBuf = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(8);
        valueBuf.writeLong(offset);
        try (WriteBatch writeBatch = new WriteBatch()) {
            writeBatch.put(keyBuf.nioBuffer(), valueBuf.nioBuffer());
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            ConfigHelper.stampDataVersion(writeBatch, TableId.PULL_OFFSET, dataVersion, stateMachineVersion);
            configStorage.write(writeBatch);
        } catch (RocksDBException e) {
            LOG.error("Failed to commit pull offset. group={}, topic={}, queueId={}, offset={}",
                group, topic, queueId, offset);
        } finally {
            keyBuf.release();
            valueBuf.release();
        }
    }

    @Override
    public long queryPullOffset(String group, String topic, int queueId) {
        if (!MixAll.isLmq(topic)) {
            return super.queryPullOffset(group, topic, queueId);
        }

        ByteBuf keyBuf = keyOfPullOffset(group, topic, queueId);
        try {
            byte[] valueBytes = configStorage.get(keyBuf.nioBuffer());
            if (null == valueBytes) {
                return -1;
            }
            return ByteBuffer.wrap(valueBytes).getLong();
        } catch (RocksDBException e) {
            LOG.error("Failed to queryPullOffset. group={}, topic={}, queueId={}", group, topic, queueId);
        } finally {
            keyBuf.release();
        }
        return -1;
    }
}
