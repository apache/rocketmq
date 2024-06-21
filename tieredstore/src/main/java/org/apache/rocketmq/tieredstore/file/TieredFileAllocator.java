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

import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.FileSegmentAllocator;

public class TieredFileAllocator {

    private final FileSegmentAllocator fileSegmentAllocator;
    private final TieredMessageStoreConfig storeConfig;

    public TieredFileAllocator(TieredMessageStoreConfig storeConfig)
        throws ClassNotFoundException, NoSuchMethodException {

        this.storeConfig = storeConfig;
        this.fileSegmentAllocator = new FileSegmentAllocator(storeConfig);
    }

    public TieredMessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    public TieredFlatFile createFlatFileForCommitLog(String filePath) {
        TieredFlatFile tieredFlatFile =
            new TieredFlatFile(fileSegmentAllocator, FileSegmentType.COMMIT_LOG, filePath);
        if (tieredFlatFile.getBaseOffset() == -1L) {
            tieredFlatFile.setBaseOffset(0L);
        }
        return tieredFlatFile;
    }

    public TieredFlatFile createFlatFileForConsumeQueue(String filePath) {
        return new TieredFlatFile(fileSegmentAllocator, FileSegmentType.CONSUME_QUEUE, filePath);
    }

    public TieredFlatFile createFlatFileForIndexFile(String filePath) {
        return new TieredFlatFile(fileSegmentAllocator, FileSegmentType.INDEX, filePath);
    }
}
