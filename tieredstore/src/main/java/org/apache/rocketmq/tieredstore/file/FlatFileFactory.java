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

package org.apache.rocketmq.tieredstore.file;

import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.provider.FileSegmentFactory;

public class FlatFileFactory {

    private final MetadataStore metadataStore;
    private final MessageStoreConfig storeConfig;
    private final FileSegmentFactory fileSegmentFactory;

    public FlatFileFactory(MetadataStore metadataStore, MessageStoreConfig storeConfig) {
        this.metadataStore = metadataStore;
        this.storeConfig = storeConfig;
        this.fileSegmentFactory = new FileSegmentFactory(metadataStore, storeConfig);
    }

    public MessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    public FlatCommitLogFile createFlatFileForCommitLog(String filePath) {
        return new FlatCommitLogFile(this.fileSegmentFactory, filePath);
    }

    public FlatConsumeQueueFile createFlatFileForConsumeQueue(String filePath) {
        return new FlatConsumeQueueFile(this.fileSegmentFactory, filePath);
    }

    public FlatAppendFile createFlatFileForIndexFile(String filePath) {
        return new FlatAppendFile(this.fileSegmentFactory, FileSegmentType.INDEX, filePath);
    }

    public FlatAppendFile createFlatFileForCompactedIndexFile(String filePath) {
        return new FlatAppendFile(this.fileSegmentFactory, FileSegmentType.INDEX_COMPACTED, filePath);
    }
}
