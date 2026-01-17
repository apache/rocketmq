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
package org.apache.rocketmq.store.transaction;

import org.apache.rocketmq.store.CommitLogDispatchStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage;
import org.rocksdb.RocksDBException;

import static org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage.TRANS_COLUMN_FAMILY;

/**
 * Adapter class to convert TransMessageRocksDBStore to CommitLogDispatchStore.
 */
public class TransMessageRocksDBStoreAdapter implements CommitLogDispatchStore {
    private final MessageStoreConfig storeConfig;
    private final MessageRocksDBStorage messageRocksDBStorage;

    public TransMessageRocksDBStoreAdapter(MessageStoreConfig storeConfig,
        MessageRocksDBStorage messageRocksDBStorage) {
        this.storeConfig = storeConfig;
        this.messageRocksDBStorage = messageRocksDBStorage;
    }

    @Override
    public Long getDispatchFromPhyOffset() throws RocksDBException {
        if (!storeConfig.isTransRocksDBEnable()) {
            return null;
        }
        Long dispatchFromTransPhyOffset = messageRocksDBStorage.getLastOffsetPy(TRANS_COLUMN_FAMILY);
        if (dispatchFromTransPhyOffset != null && dispatchFromTransPhyOffset > 0) {
            return dispatchFromTransPhyOffset;
        }
        return null;
    }
}

