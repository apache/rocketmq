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
package org.apache.rocketmq.store.rocksdb;

import java.util.function.LongSupplier;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.AbstractCompactionFilter;
import org.rocksdb.AbstractCompactionFilterFactory;
import org.rocksdb.RemoveConsumeQueueCompactionFilter;

public class ConsumeQueueCompactionFilterFactory extends AbstractCompactionFilterFactory<RemoveConsumeQueueCompactionFilter> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);
    private final LongSupplier minPhyOffsetSupplier;

    public ConsumeQueueCompactionFilterFactory(final LongSupplier minPhyOffsetSupplier) {
        this.minPhyOffsetSupplier = minPhyOffsetSupplier;
    }

    @Override
    public String name() {
        return "ConsumeQueueCompactionFilterFactory";
    }

    @Override
    public RemoveConsumeQueueCompactionFilter createCompactionFilter(final AbstractCompactionFilter.Context context) {
        long minPhyOffset = this.minPhyOffsetSupplier.getAsLong();
        LOGGER.info("manualCompaction minPhyOffset: {}, isFull: {}, isManual: {}",
                minPhyOffset, context.isFullCompaction(), context.isManualCompaction());
        return new RemoveConsumeQueueCompactionFilter(minPhyOffset);
    }
}
