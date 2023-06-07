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

package org.apache.rocketmq.tieredstore.provider.memory;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.TieredDispatcherBaseTest;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;

public class TieredDispatcherForMemoryTest extends TieredDispatcherBaseTest {
    @Override
    public TieredMessageStoreConfig createTieredMessageStoreConfig() {
        TieredMessageStoreConfig storeConfig = new TieredMessageStoreConfig();
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.memory.MemoryFileSegmentWithoutCheck");
        storeConfig.setBrokerName(storeConfig.getBrokerName());
        storeConfig.setBrokerClusterName("test-cluster");
        return storeConfig;
    }

    @Override
    public TieredFileSegment createTieredFileSegment(TieredFileSegment.FileSegmentType type, MessageQueue mq, long baseOffset, TieredMessageStoreConfig storeConfig) {
        return new MemoryFileSegmentWithoutCheck(type, mq, baseOffset, storeConfig);
    }
}
