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

import org.apache.rocketmq.store.logfile.MappedFile;

public class BatchOffsetIndex {

    private final MappedFile mappedFile;
    private final int indexPos;
    private final long msgOffset;
    private final short batchSize;
    private final long storeTimestamp;

    public BatchOffsetIndex(MappedFile file, int pos, long msgOffset, short size, long storeTimestamp) {
        mappedFile = file;
        indexPos = pos;
        this.msgOffset = msgOffset;
        batchSize = size;
        this.storeTimestamp = storeTimestamp;
    }

    public MappedFile getMappedFile() {
        return mappedFile;
    }

    public int getIndexPos() {
        return indexPos;
    }

    public long getMsgOffset() {
        return msgOffset;
    }

    public short getBatchSize() {
        return batchSize;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }
}
