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

package org.apache.rocketmq.store.ha.protocol;

import java.nio.ByteBuffer;

public class PushCommitLogData {

    private long epoch;

    private long epochStartOffset;

    private long confirmOffset;

    private long blockStartOffset;

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public long getEpochStartOffset() {
        return epochStartOffset;
    }

    public void setEpochStartOffset(long epochStartOffset) {
        this.epochStartOffset = epochStartOffset;
    }

    public long getConfirmOffset() {
        return confirmOffset;
    }

    public void setConfirmOffset(long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    public long getBlockStartOffset() {
        return blockStartOffset;
    }

    public void setBlockStartOffset(long blockStartOffset) {
        this.blockStartOffset = blockStartOffset;
    }

    public ByteBuffer encode() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 * 8);
        byteBuffer.putLong(epoch);
        byteBuffer.putLong(epochStartOffset);
        byteBuffer.putLong(confirmOffset);
        byteBuffer.putLong(blockStartOffset);
        byteBuffer.flip();
        return byteBuffer;
    }

    @Override
    public String toString() {
        return "PushCommitLogData{" +
            "epoch=" + epoch +
            ", epochStartOffset=" + epochStartOffset +
            ", confirmOffset=" + confirmOffset +
            ", blockStartOffset=" + blockStartOffset +
            '}';
    }
}
