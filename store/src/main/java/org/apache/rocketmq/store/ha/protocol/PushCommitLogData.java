package org.apache.rocketmq.store.ha.protocol;

import java.nio.ByteBuffer;

public class PushCommitLogData {

    private long epoch;

    private long epochStartOffset;

    private long confirmOffset;

    private long startOffset;

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

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public ByteBuffer encode() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(32);
        byteBuffer.putLong(epoch);
        byteBuffer.putLong(epochStartOffset);
        byteBuffer.putLong(confirmOffset);
        byteBuffer.putLong(startOffset);
        byteBuffer.flip();
        return byteBuffer;
    }

    @Override
    public String toString() {
        return "PushCommitLogData{" +
            "epoch=" + epoch +
            ", epochStartOffset=" + epochStartOffset +
            ", confirmOffset=" + confirmOffset +
            ", startOffset=" + startOffset +
            '}';
    }
}
