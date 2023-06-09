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

package org.apache.rocketmq.tieredstore.provider;

import java.lang.reflect.Constructor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class FileSegmentAllocator {

    private static final Logger log = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final TieredMessageStoreConfig storeConfig;

    private final Constructor<? extends TieredFileSegment> fileSegmentConstructor;

    public FileSegmentAllocator(
        TieredMessageStoreConfig storeConfig) throws ClassNotFoundException, NoSuchMethodException {
        this.storeConfig = storeConfig;
        Class<? extends TieredFileSegment> clazz =
            Class.forName(storeConfig.getTieredBackendServiceProvider()).asSubclass(TieredFileSegment.class);
        fileSegmentConstructor = clazz.getConstructor(
            TieredMessageStoreConfig.class, FileSegmentType.class, String.class, Long.TYPE);
    }

    public TieredMessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    public TieredMetadataStore getMetadataStore() {
        return TieredStoreUtil.getMetadataStore(storeConfig);
    }

    public TieredFileSegment createSegment(
        FileSegmentType fileType, String filePath, long baseOffset) {

        switch (fileType) {
            case COMMIT_LOG:
                return this.createCommitLogFileSegment(filePath, baseOffset);
            case CONSUME_QUEUE:
                return this.createConsumeQueueFileSegment(filePath, baseOffset);
            case INDEX:
                return this.createIndexFileSegment(filePath, baseOffset);
        }
        return null;
    }

    public TieredFileSegment createCommitLogFileSegment(String filePath, long baseOffset) {
        TieredFileSegment segment = null;
        try {
            segment = fileSegmentConstructor.newInstance(
                this.storeConfig, FileSegmentType.COMMIT_LOG, filePath, baseOffset);
        } catch (Exception e) {
            log.error("create file segment of commitlog failed, filePath: {}, baseOffset: {}",
                filePath, baseOffset, e);
        }
        return segment;
    }

    public TieredFileSegment createConsumeQueueFileSegment(String filePath, long baseOffset) {
        TieredFileSegment segment = null;
        try {
            segment = fileSegmentConstructor.newInstance(
                this.storeConfig, FileSegmentType.CONSUME_QUEUE, filePath, baseOffset);
        } catch (Exception e) {
            log.error("create file segment of commitlog failed, filePath: {}, baseOffset: {}",
                filePath, baseOffset, e);
        }
        return segment;
    }

    public TieredFileSegment createIndexFileSegment(String filePath, long baseOffset) {
        TieredFileSegment segment = null;
        try {
            segment = fileSegmentConstructor.newInstance(
                this.storeConfig, FileSegmentType.INDEX, filePath, baseOffset);
        } catch (Exception e) {
            log.error("create file segment of commitlog failed, filePath: {}, baseOffset: {}",
                filePath, baseOffset, e);
        }
        return segment;
    }
}
