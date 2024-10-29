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
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

public class SubscriptionGroupManagerV2 extends SubscriptionGroupManager {

    private final ConfigStorage configStorage;

    public SubscriptionGroupManagerV2(BrokerController brokerController, ConfigStorage configStorage) {
        super(brokerController);
        this.configStorage = configStorage;
    }

    @Override
    public boolean load() {
        return loadDataVersion() && loadSubscriptions();
    }

    public boolean loadDataVersion() {
        try {
            ConfigHelper.loadDataVersion(configStorage, TableId.SUBSCRIPTION_GROUP)
                .ifPresent(buf -> {
                    ConfigHelper.onDataVersionLoad(buf, dataVersion);
                });
        } catch (RocksDBException e) {
            log.error("loadDataVersion error", e);
            return false;
        }
        return true;
    }

    private boolean loadSubscriptions() {
        int keyLen = 1 /* table prefix */ + 2 /* table-id */ + 1 /* record-type-prefix */;
        ByteBuf beginKey = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(keyLen);
        beginKey.writeByte(TablePrefix.TABLE.getValue());
        beginKey.writeShort(TableId.SUBSCRIPTION_GROUP.getValue());
        beginKey.writeByte(RecordPrefix.DATA.getValue());

        ByteBuf endKey = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(keyLen);
        endKey.writeByte(TablePrefix.TABLE.getValue());
        endKey.writeShort(TableId.SUBSCRIPTION_GROUP.getValue());
        endKey.writeByte(RecordPrefix.DATA.getValue() + 1);

        try (RocksIterator iterator = configStorage.iterate(beginKey.nioBuffer(), endKey.nioBuffer())) {
            while (iterator.isValid()) {
                SubscriptionGroupConfig subscriptionGroupConfig = parseSubscription(iterator.key(), iterator.value());
                if (null != subscriptionGroupConfig) {
                    super.updateSubscriptionGroupConfigWithoutPersist(subscriptionGroupConfig);
                }
                iterator.next();
            }
        } finally {
            beginKey.release();
            endKey.release();
        }
        return true;
    }

    private SubscriptionGroupConfig parseSubscription(byte[] key, byte[] value) {
        ByteBuf keyBuf = Unpooled.wrappedBuffer(key);
        ByteBuf valueBuf = Unpooled.wrappedBuffer(value);
        try {
            // Skip table-prefix, table-id, record-type-prefix
            keyBuf.readerIndex(4);
            short groupNameLen = keyBuf.readShort();
            assert groupNameLen == keyBuf.readableBytes();
            CharSequence groupName = keyBuf.readCharSequence(groupNameLen, StandardCharsets.UTF_8);
            assert null != groupName;
            byte serializationType = valueBuf.readByte();
            if (SerializationType.JSON == SerializationType.valueOf(serializationType)) {
                CharSequence json = valueBuf.readCharSequence(valueBuf.readableBytes(), StandardCharsets.UTF_8);
                SubscriptionGroupConfig subscriptionGroupConfig = JSON.parseObject(json.toString(), SubscriptionGroupConfig.class);
                assert subscriptionGroupConfig != null;
                assert groupName.equals(subscriptionGroupConfig.getGroupName());
                return subscriptionGroupConfig;
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
            log.error("Failed to flush RocksDB WAL", e);
        }
    }

    @Override
    public SubscriptionGroupConfig findSubscriptionGroupConfig(final String group) {
        if (MixAll.isLmq(group)) {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(group);
            return subscriptionGroupConfig;
        }
        return super.findSubscriptionGroupConfig(group);
    }

    @Override
    public void updateSubscriptionGroupConfig(final SubscriptionGroupConfig config) {
        if (config == null || MixAll.isLmq(config.getGroupName())) {
            return;
        }
        ByteBuf keyBuf = ConfigHelper.keyBufOf(TableId.SUBSCRIPTION_GROUP, config.getGroupName());
        ByteBuf valueBuf = ConfigHelper.valueBufOf(config, SerializationType.JSON);
        try (WriteBatch writeBatch = new WriteBatch()) {
            writeBatch.put(keyBuf.nioBuffer(), valueBuf.nioBuffer());
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            ConfigHelper.stampDataVersion(writeBatch, dataVersion, stateMachineVersion);
            configStorage.write(writeBatch);
        } catch (RocksDBException e) {
            log.error("update subscription group config error", e);
        } finally {
            keyBuf.release();
            valueBuf.release();
        }
        super.updateSubscriptionGroupConfigWithoutPersist(config);
    }

    @Override
    public boolean containsSubscriptionGroup(String group) {
        if (MixAll.isLmq(group)) {
            return true;
        } else {
            return super.containsSubscriptionGroup(group);
        }
    }

    @Override
    protected SubscriptionGroupConfig removeSubscriptionGroupConfig(String groupName) {
        ByteBuf keyBuf = ConfigHelper.keyBufOf(TableId.SUBSCRIPTION_GROUP, groupName);
        try (WriteBatch writeBatch = new WriteBatch()) {
            writeBatch.delete(ConfigHelper.readBytes(keyBuf));
            long stateMachineVersion = brokerController.getMessageStore().getStateMachineVersion();
            ConfigHelper.stampDataVersion(writeBatch, dataVersion, stateMachineVersion);
            configStorage.write(writeBatch);
        } catch (RocksDBException e) {
            log.error("Failed to remove subscription group config by group-name={}", groupName, e);
        }
        return super.removeSubscriptionGroupConfig(groupName);
    }
}
