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
package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.rocksdb.RocksDBException;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class OffsetInitializerRocksDBImpl implements OffsetInitializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetInitializerRocksDBImpl.class);

    private final RocksDBConsumeQueueStore consumeQueueStore;

    public OffsetInitializerRocksDBImpl(RocksDBConsumeQueueStore consumeQueueStore) {
        this.consumeQueueStore = consumeQueueStore;
    }

    @Override
    public long maxConsumeQueueOffset(String topic, int queueId) throws ConsumeQueueException {
        try {
            long offset = consumeQueueStore.getMaxOffsetInQueue(topic, queueId);
            LOGGER.info("Look up RocksDB for max-offset of LMQ[{}:{}]: {}", topic, queueId, offset);
            return offset;
        } catch (RocksDBException e) {
            throw new ConsumeQueueException(e);
        }
    }
}
