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

package org.apache.rocketmq.remoting.protocol;

import java.util.Objects;

public class EpochEntry extends RemotingSerializable {

    private int epoch;
    private long startOffset;
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

    public EpochEntry(int epoch, long startOffset, long endOffset) {
        this.epoch = epoch;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    @Override
    public String toString() {
        return "EpochEntry{" +
            "epoch=" + epoch +
            ", startOffset=" + startOffset +
            ", endOffset=" + endOffset +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EpochEntry entry = (EpochEntry) o;
        return epoch == entry.epoch && startOffset == entry.startOffset && endOffset == entry.endOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, startOffset, endOffset);
    }
}
