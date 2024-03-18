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
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;

public class FileSegmentFactory {

    private final MetadataStore metadataStore;
    private final MessageStoreConfig storeConfig;
    private final Constructor<? extends FileSegment> fileSegmentConstructor;

    public FileSegmentFactory(MetadataStore metadataStore, MessageStoreConfig storeConfig) {
        try {
            this.storeConfig = storeConfig;
            this.metadataStore = metadataStore;
            Class<? extends FileSegment> clazz =
                Class.forName(storeConfig.getTieredBackendServiceProvider()).asSubclass(FileSegment.class);
            fileSegmentConstructor = clazz.getConstructor(
                MessageStoreConfig.class, FileSegmentType.class, String.class, Long.TYPE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    public MessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    public FileSegment createSegment(FileSegmentType fileType, String filePath, long baseOffset) {
        try {
            return fileSegmentConstructor.newInstance(this.storeConfig, fileType, filePath, baseOffset);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public FileSegment createCommitLogFileSegment(String filePath, long baseOffset) {
        return this.createSegment(FileSegmentType.COMMIT_LOG, filePath, baseOffset);
    }

    public FileSegment createConsumeQueueFileSegment(String filePath, long baseOffset) {
        return this.createSegment(FileSegmentType.CONSUME_QUEUE, filePath, baseOffset);
    }

    public FileSegment createIndexServiceFileSegment(String filePath, long baseOffset) {
        return this.createSegment(FileSegmentType.INDEX, filePath, baseOffset);
    }
}
