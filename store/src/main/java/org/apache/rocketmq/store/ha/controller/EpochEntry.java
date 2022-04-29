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

package org.apache.rocketmq.store.ha.controller;

public class EpochEntry {

    private final int epoch;
    private final long startOffset;
    private long endOffset = Long.MAX_VALUE;

    public EpochEntry(EpochEntry entry) {
        this.epoch = entry.getEpoch();
        this.startOffset = entry.getStartOffset();
        this.endOffset = entry.getEndOffset();
    }

    public EpochEntry(int epoch, long startOffset) {
        this.epoch = epoch;
        this.startOffset = startOffset;
    }

    public int getEpoch() {
        return epoch;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }
}
